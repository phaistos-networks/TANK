// TODO:
// - use jump consistent hashing: https://arxiv.org/pdf/1406.2294.pdf
//
// INVARIANTS
// - TANK Service nodes only *react* to responses from watched Consul keyspaces.
//	Even if, e.g, the node is the leader and updates a key, it will only
// 	react to the update once it gets back the response from Consul, it won't
//	do anything before that happens. e.g, if it's the cluster leader and
//	wants to change the replicas for a partition, it will NOT update in-memory state (i.e
//	the nodes the partition replicates to) immediatetely; it will update the respective
//	key on Consul, and because it will be monitoring that keyspace, it will get the update
//	that the replicas have changed, and only then it will react to it (which will also
//	require updating the in-memory state).
//
//	That's really what k8s's API Server does. See for example https://medium.com/jorgeacetozi/kubernetes-master-components-etcd-api-server-controller-manager-and-scheduler-3a0179fc8186 , for
// 	what happens when youc reate a pod using kubectl:
//	1. kubctl writes to the API server
//	2. API server validates the request, and persists it to etcd
//	3. etcd notifies back the API server(because it was monitoring that key that was just updated by the API server)
//	4. API server invokes the Scheduler
//	5. Scheduler decides where to run the pod on and return that to the API server
//	6. API server persists it to etcd
// 	7. etcd notifies back the API server (because it was monitoring that key that was just updated by the API server)
//	8. API server invokes the Kubelet in the corresponding node
//	9. Kubetlet talks to the Docker deamon, using the API, over the Docker socket, to create the container
// 	10. Kubelet updates the pod status to the API server
//	11. API server persists the new state in etcd.
//	see the image/diagram
//
// See also : https://queue.acm.org/detail.cfm?id=2898444
// Where they talk about Borg, Omega, and k8s. This is extremely important. They also update Chubby state and REACT to it.
//
// TANK service will _not_ update in-memory state before persisting to consul.
// It will instead do what k8s and Omega do; it will update state on Consul, and once it gets back the notification from the Watch request, it will
// then react to it(which will also include updating in-memory state, etc)
//
// The only exception is the ISR which may be updated immediately -- although this is not required and may change
// see SEMANTICS
#include "service.h"
#include <unordered_set>
#include <queue>

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtautological-overlap-compare"
#endif

#include <ext/json/single_include/nlohmann/json.hpp>

#ifdef __clang__
#pragma GCC diagnostic pop
#endif

#include <base64.h>
#include <compress.h>

std::size_t Service::determine_min_fetch_size_for_new_stream() {
        // TODO:
        // We should probably figure out an initial min_fetch_size based on how many streams
        // are currently active and have a global budget that for all of them. For now, start with a relatively low constant fetch size
        // for all new streams
        return 4 * 1024 * 1024;
}

// generate and schedule a new request for peer `node`, for replication of content for `partitions`
void Service::replicate_from(cluster_node *const node, topic_partition *const *partitions, std::size_t partitions_cnt) {
	enum {
		trace = false,
	};

        TANK_EXPECT(node);
        TANK_EXPECT(partitions);
        TANK_EXPECT(partitions_cnt);

        // in case we have scheduled a retry
        abort_retry_consume_from(node);

        // Invariant:
        // for all [partitions, partitions + cnt)
        // the leader is `node`
        // so we really only need one connection
        auto c = node->consume_conn.ch.get();

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "Will attempt to replicate ", partitions_cnt, " from ", node->id, "@", node->ep, ansifmt::reset, "\n");
		SLog("To replicate:", values_repr_with_lambda(partitions, partitions_cnt, [](const auto it) noexcept { 
			return Buffer{}.append(it->owner->name(), '/', it->idx); }), "\n");
        }

        if (c) {
                if (const auto state = c->as.consumer.state; state == connection::As::Consumer::State::Connecting or state == connection::As::Consumer::State::Busy) {
                        // we are waiting for a response from an outstanding/active consume req
                        // or we are still trying to establish a connection
                        if (trace) {
                                SLog("Peer CONSUME connection is busy\n");
                        }

                        return;
                } else if (state == connection::As::Consumer::State::ScheduledShutdown) {
                        if (trace) {
                                SLog("Peer connection was scheduled for shutdown\n");
                        }

                        cancel_timer(&c->as.consumer.attached_timer.node);
                } else if (trace) {
                        SLog("Connection ", ptr_repr(c), " already available ", unsigned(c->as.consumer.state), "\n");
                }
        } else {
                if (trace) {
                        SLog("Requiring NEW connection\n");
                }

                c = new_peer_connection(node);

		if (not c) {
                        IMPLEMENT_ME();
                }

                // once we connect, consider_connected_consumer() will retry
                TANK_EXPECT(c->as.consumer.state == connection::As::Consumer::State::Connecting);
                return;
        }

        if (trace) {
                SLog(ansifmt::color_green, "Will generate a CONSUME request for node ", node->id, "@", node->ep, ansifmt::reset, " (", partitions_cnt, " partitions)\n");
        }

        // topics are expected to be ordered
        // see cluster_node::cluster::leadership::ordered_list()
        auto oq  = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto dvp = get_data_vector_payload();
        auto b   = get_buf();

        b->pack(static_cast<uint8_t>(TankAPIMsgType::ConsumePeer));
        const auto req_size_offset = b->size();

        b->pack(static_cast<uint32_t>(0)); // request length
        b->pack(static_cast<uint16_t>(cluster_state.local_node.id));
        b->pack(static_cast<uint64_t>(4 * 1000)); // TODO: max-wait
        b->pack(static_cast<uint32_t>(0));        // TODO: min.bytes

        const auto topics_cnt_offset = b->size();
        uint16_t   topics_cnt{0};

        // u16 for total topics/u8 for Consume reqs
        b->pack(static_cast<uint16_t>(0));

        dvp->buf = b;
        for (size_t i{0}; i < partitions_cnt;) {
                auto       topic = partitions[i]->owner;
                const auto base  = i;

                b->pack(topic->name());

                const auto total_partitions_offset = b->size();

                // u16 for total partitions/u8 for Consume reqs
                b->pack(static_cast<uint16_t>(0));

                do {
                        auto     p      = partitions[i];
                        auto     stream = p->cluster.rs;
                        uint64_t next;

                        if (const auto v = p->cluster.consume_next_lsn; v != std::numeric_limits<uint64_t>::max()) {
                                next                        = v;
                                p->cluster.consume_next_lsn = std::numeric_limits<uint64_t>::max();
                        } else {
                                next = partition_log(p)->lastAssignedSeqNum + 1;
                        }

                        if (not stream) {
                                // no replication stream for that partition
                                // we need to track all replication streams so that we can know
                                // (peer providing us content for that partition, which is usually the leader but when
                                // the partition leader changes, we need to know that we are not replicating from that node anymore)
                                stream = p->cluster.rs = get_repl_stream();
                                stream->partition      = p;
                                stream->min_fetch_size = determine_min_fetch_size_for_new_stream();

                                cluster_state.replication_streams.push_back(&stream->repl_streams_ll);
                        }

                        stream->ch.set(c);
                        stream->src = node;

                        if (trace) {
                                SLog("Will request topic ", p->owner->name(), "/", p->idx, " from seq ", next, " (local last assigned:", partition_log(p)->lastAssignedSeqNum, "), min_fetch_size = ", stream->min_fetch_size, "\n");
                        }

                        b->pack(p->idx);                                        // partition
                        b->pack(static_cast<uint64_t>(next));                   // absolute sequence number to consume from
                        b->pack(static_cast<uint32_t>(stream->min_fetch_size)); // fetch size

                } while (++i < partitions_cnt and partitions[i]->owner == topic);

                const uint16_t total_partitions = i - base;

                *reinterpret_cast<uint16_t *>(b->data() + total_partitions_offset) = total_partitions;
                ++topics_cnt;
        }

        *reinterpret_cast<uint16_t *>(b->data() + topics_cnt_offset) = topics_cnt;
        *reinterpret_cast<uint32_t *>(b->data() + req_size_offset)   = b->size() - req_size_offset - sizeof(uint32_t);

        dvp->append(b->as_s32());
        oq->push_back(dvp);

        c->as.consumer.state = connection::As::Consumer::State::Busy;
        try_tx(c);
}

// Attempt to replicate from a peer content
// for all partitions this node is a replica of and that peer is their leader
void Service::try_replicate_from(cluster_node *const peer) {
	enum {
		trace = false,
	};
        TANK_EXPECT(peer);

        if (not peer->available()) {
                if (trace) {
                        SLog("Will NOT replicate from peer ", peer->id, "@", peer->ep, " because it is not available\n");
                }

                return;
        }

        auto partitions = partitions_to_replicate_from(peer);

        if (trace) {
                SLog("Total partitions to replicate from peer ", peer->id, '@', peer->ep, ' ', partitions ? partitions->size() : 0, "\n");
        }

        if (partitions and not partitions->empty()) {
                replicate_from(peer, partitions->data(), partitions->size());
        } else if (auto c = peer->consume_conn.ch.get()) {
                if (trace) {
                        SLog("Connection to peer is no longer required, no partitions to replicate\n");
                }

                if (not c->as.consumer.attached_timer.node.node.leaf_p) {
                        // We no longer need this connection, but we 'll keep it around in case we need it later
                        // We 'll ready a timer so that if we don't need this within some time, we 'll shut it down
			// TODO: verify again
                        if (trace) {
                                SLog("Will schedule shutdown of consumer connection\n");
                        }

                        c->as.consumer.state                   = connection::As::Consumer::State::ScheduledShutdown;
                        c->as.consumer.attached_timer.node.key = now_ms + 4 * 1000;
                        c->as.consumer.attached_timer.type     = timer_node::ContainerType::ShutdownConsumerConn;
                        register_timer(&c->as.consumer.attached_timer.node);
                }
        }
}

// For each peer in `peers`, initiate a new CONSUME request for all partitions
// that peer is a leader, unless the connection to that peer is already busy
//
// TODO: maybe just iterate cluster_state.local_node.replication_streams
// and collect all partitions where src is in peers
void Service::try_replicate_from(const std::unordered_set<cluster_node *> &peers) {
	enum {
		trace = false,
	};

        if (trace) {
                SLog("Will attempt to replicate from ", peers.size(), " cluster peers\n");
        }

        for (auto peer : peers) {
                try_replicate_from(peer);
        }
}

// `start`: the partitions to begin replicating from
// `stop`: the partitions to stop replicating from
void Service::replicate_partitions(std::vector<std::pair<topic_partition *, cluster_node *>> *start,
                                   std::vector<std::pair<topic_partition *, cluster_node *>> *stop) {
        enum {
                trace = false,
        };
        auto                  self      = cluster_state.local_node.ref;
        auto &                peers_set = reusable.peers_set;

        if (trace) {
                SLog("REPLICATE PARTITIONS start = ", start ? start->size() : 0, ", stop = ", stop ? stop->size() : 0, "\n");
        }

        peers_set.clear();

#ifdef TANK_RUNTIME_CHECKS
        // sanity check
        if (start) {
                for (auto &it : *start) {
                        TANK_EXPECT(self->is_replica_for(it.first));
                }
        }
#endif

        if (stop) {
                for (auto [p, src] : *stop) {
                        if (trace) {
                                SLog("STOP:", p->owner->name(), "/", p->idx, " from ", src->id, "@", src->ep, "\n");
                        }

                        try_abort_replication(p, src, __LINE__);
                }
        }

        if (start) {
                for (auto [p, src] : *start) {
                        if (trace) {
                                SLog("START: ", p->owner->name(), "/", p->idx, " from ", src->id, "@", src->ep, "\n");
                        }

                        // make sure we are not trying to start replication from us
                        TANK_EXPECT(src != cluster_state.local_node.ref);
                        peers_set.insert(src);
                }
        }

        if (not peers_set.empty()) {
                try_replicate_from(peers_set);
        }
}

// `gen` is the ModifyIndex of the key in configs/
// we use conf-updates/ for updates and we compare against the ModifyIndex of the key in conf-updates
// we still have access to ModifyIndex of the configs/
void Service::process_cluster_config(const str_view32 conf, const uint64_t gen) {
        SLog(ansifmt::bold, ansifmt::inverse, ansifmt::color_red, "CLUSTER: cluster config updates", ansifmt::reset, " ", gen, " [", conf, "]\n");
}

void Service::process_topic_config(const str_view8 topic_name, const str_view32 conf, const uint64_t gen) {
	enum {
		trace = false,
	};
        using json = nlohmann::json;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::inverse, ansifmt::color_red, "CLUSTER: configuration of topic [", topic_name, "] updated", ansifmt::reset, " ", gen, "\n");
        }

        try {
                if (const auto doc = json::parse(conf); doc.is_object()) {
                        for (auto it = doc.begin(); it != doc.end(); ++it) {
                                const auto &key   = it.key();
                                const auto &value = it.value();

                                if (key == "rf") {
                                        const auto rf = value.get<int64_t>();

					// See topic.cluster.rf_ comments for why rf_ cannot be lower than 0
                                        if (rf < 1 || rf > 64) {
                                                if (trace) {
                                                        SLog("Ignoring: bogus RF ", rf, "\n");
                                                }
                                        } else {
						if (trace) {
							SLog("RF to ", rf, " for '", topic_name, "'\n");
						}

                                                if (auto t = topic_by_name(topic_name)) {
                                                        auto _t = cluster_state.updates.get_topic(t);

                                                        _t->set_rf(rf);
                                                        schedule_cluster_updates_apply(__FUNCTION__);
                                                } else if (trace) {
							SLog("Topic [", topic_name, "] is not defined\n");
						}
                                        }
                                } else if (trace) {
					SLog("Unexpected key '", key, "'\n");
				}
                        }
                } else if (trace) {
                        SLog("Unexpected response\n");
                }
        } catch (const std::exception &e) {
                if (trace) {
                        SLog("Failed:", e.what(), "\n");
                }
        }
}

void Service::try_become_cluster_leader(const uint32_t ref) {
        static constexpr bool trace{false};

        if (0 == (consul_state.flags & unsigned(ConsulState::Flags::AttemptBecomeClusterLeader))) {
                if (trace) {
                        SLog("Yes, will try, ref = ", ref, "\n");
                }

                cancel_timer(&try_become_cluster_leader_timer.node); // just in case

                consul_state.flags |= unsigned(ConsulState::Flags::AttemptBecomeClusterLeader);

                schedule_consul_req(consul_state.get_req(consul_request::Type::TryBecomeClusterLeader), true);
        } else if (trace) {
                SLog("Cannot TryBecomeClusterLeader ref = ", ref, "\n");
        }
}

// Invoked whenever we get a response to AcquireNodeID
void Service::cluster_node_id_acquired() {
        static constexpr bool trace{false};
        auto                  n = cluster_state.local_node.ref;
        TANK_EXPECT(n);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::inverse, ansifmt::color_red, "CLUSTER: node ID has been acquired", ansifmt::reset, "\n");
        }

        if (consul_state.flags & unsigned(ConsulState::Flags::BootstrapStateUpdatesProcessed)) {
                // This is the second AcquireNodeID request, which we issued after we updated the bootstrap state updates
                // and it was used to assign our endpoint to this the reserved nodes/id
                //
                // Only now can we try to acquire cluster leadership
                // @see conclude_bootstrap_updates() for reational and why
                // we shouldn't try to become a leader prior to acquiring the node ID the second time by setting our endpoint here
                if (trace) {
                        SLog("Node ID AND endpoint acquired/set\n");
                }

                TANK_EXPECT(consul_state.bootstrap_state_updates_processed());

                if (!cluster_state.leader_id) {
                        if (trace) {
                                SLog("Will NOW try to become cluster leader because no cluster leader\n");
                        }

                        try_become_cluster_leader(__LINE__);
                }
                return;
        }

        // explicitly
        n->available_ = true;

        // We need to immediately begin accepting connections
        // because other nodes(e.g cluster leader) may promote us to leaders for 1+ partitions
        // so we ned to be able to accept client requests now.
        //
        // XXX: Turns out this is a *bad* idea. We only need to begin accepting connections as soon as we have
        // processed the states update collected during bootstrap. This is because otherwise
        // this node may responsd with e.g No Leader to requests from other leeaders or from clients
        //
        // It is OK if we are deferring accepting connections until then because even if
        // other nodes rush to try to replicate from here and fail, they will retry soon thereafter
        //
        // We also now do NOT set local endpoint when we acquire the node id, so that nodes
        // will not try to replicate from us until we have processed the bootstrap updates
        // and once we do, then we acquire the ID again this time with the endpoint set
        // so that other nodes will commence replication.
        //
        // WAS: enable_listener();
        //
        // We also can't try_become_cluster_leader() here; we can only do so if we have
        // acquired the node AND assigned the endpoint to it (see ^^)
        if (const auto bm = unsigned(ConsulState::Flags::RegistrationComplete); 0 == (consul_state.flags & bm)) {
                // this usually takes around 0.2s
                static constexpr bool trace{false};

                consul_state.flags |= bm;

                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_red, ansifmt::bgcolor_green, "CLUSTER: reg is complete now", ansifmt::reset,
                             " took ", duration_repr(Timings::Milliseconds::ToMicros(now_ms - consul_state.reg_init_ts)), "\n");
                }

                force_cluster_updates_apply();
        }
}

void Service::process_fetched_cluster_configurations(consul_request *const req, const str_view32 content) {
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::inverse, ansifmt::color_red, "CLUSTER: all configurations retrieved", ansifmt::reset, "\n");
        }

        process_consul_cluster_configurations(req, content);

        // we now need to create or renew our session because
        // we need it in order to register our ID in the nodes namespace
        if (consul_state.session_id()) {
                // we already have a session created earlier
                // attempt to renew it instead of creting a new one; if that fails, then we will create another
		if (trace) {
			SLog("Will renew session\n");
		}

                schedule_consul_req(consul_state.get_req(consul_request::Type::RenewSession), true);
        } else {
                // we 'll create another
                // this is fast and cheap
		if (trace) {
			SLog("Will create a new session\n");
		}

                schedule_consul_req(consul_state.get_req(consul_request::Type::CreateSession), true);
        }
}

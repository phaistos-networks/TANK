#include "service_common.h"
#include <date.h>

static inline std::size_t aligned_to(const std::size_t v, const std::size_t alignment) noexcept {
        return (v + alignment - 1) & (-alignment);
}

#ifndef HWM_UPDATE_BASED_ON_ACKS
void Service::rebuild_partition_tracked_isrs(topic_partition *const partition) {
        enum {
                trace = false,
        };
        TANK_EXPECT(partition);
        const auto self = cluster_state.local_node.ref;

        partition->cluster.isr.tracker.size = 0;
        for (const auto ptr : partition->cluster.isr.list) {
                const auto isr_e = containerof(isr_entry, partition_ll, ptr);

                TANK_EXPECT(isr_e->partition() == partition);

                const auto node = isr_e->node();

                if (node == self) {
                        continue;
                } else if (isr_e->last_msg_lsn == 0) {
                        // hasn't requested anything yet
                        continue;
                }

                auto &it = partition->cluster.isr.tracker.data[partition->cluster.isr.tracker.size++];

                it.nid = node->id;
                it.lsn = isr_e->last_msg_lsn;
        }

        std::sort(partition->cluster.isr.tracker.data, partition->cluster.isr.tracker.data + partition->cluster.isr.tracker.size,
                  [](const auto &a, const auto &b) noexcept {
                          return a.lsn < b.lsn;
                  });

        if (trace) {
                SLog("After rebuilding\n");
                for (unsigned i = 0; i < partition->cluster.isr.tracker.size; ++i) {
                        const auto &it = partition->cluster.isr.tracker.data[i];

                        SLog(">> Node ", it.nid, " ", it.lsn, "\n");
                }
        }

#ifdef TANK_RUNTIME_CHECKS
        for (unsigned i = 1; i < partition->cluster.isr.tracker.size; ++i) {
                TANK_EXPECT(partition->cluster.isr.tracker.data[i].lsn >= partition->cluster.isr.tracker.data[i - 1].lsn);
        }
#endif
}

// TODO: this is *not* optimal
bool topic_partition::Cluster::ISR::Tracker::confirmed(const uint8_t k, const uint64_t seqnum) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        enum {
                trace = false,
        };

        if (trace) {
                SLog("confirmed() for k = ", k, ", seqnum = ", seqnum, ", size = ", size, "\n");
        }

        if (k > size) {
                // we need to special-case for this
                // see consider_pending_client_produce_responses() impl
                // where we check for (required_acks + 1 > isr_size) { }

                if (trace) {
                        SLog("k > size\n");
                }

                return true;
        } else if (!k) {
                if (trace) {
                        SLog("k == 0\n");
                }

                return false;
        }

        // Producers get an ACK _regardless_ of HWM aadvancement
        // i.e if a producer P produces a message with LSN 100, and ack = 2(requires an implicit ack from the leader, and 2 acks from peers in the ISR)
        // then as soon as 2+ different peers issue a ConsumePeer for LSN > 100, that produce req. can get ack-ed.
        //
        // e.g for seqnum = 8, k = 2
        // we may have data[0].lsn = 7, data[1] = 8
        for (unsigned i = 0; i < k; ++i) {
                const auto v = data[i].lsn;

                if (trace) {
                        SLog("Considering ", seqnum, " / ", v, "\n");
                }

                if (v < seqnum) {
                        if (trace) {
                                SLog("NOPE\n");
                        }

                        return false;
                }
        }

        if (trace) {
                SLog("*CONFIRMED*\n");
        }

        return true;
}
#endif

// See TANKUtil::produce_request_acks()
uint8_t topic::compute_required_peers_acks(const topic_partition *part, const uint8_t required_acks) const {
        // see apply_cluster_state_updates() DEFERRED_REPAIRS comments
        TANK_EXPECT(part->cluster.isr.size()); // can't possibly have an empty ISR

        // It's possible the number of nodes in the ISR  be higher than the number
        // of nodes in the replicas set for a few MS -- this is an edge case and it rarely manifests
        // but it is important to account for that here
        const auto effective_isr_size = std::min<uint8_t>(part->owner->cluster.rf_, part->cluster.isr.size());

        // ISR always includes the local node as well
        // but we care for acknowledgements from peers so we need to compute it here
        const auto peers_in_isr = effective_isr_size - 1;

        if (required_acks == 0) {
                // magic: require acks from all peers
                // so we need to return (effective_isr_size - 1)
                // because ISR always include the local node as well
                // TODO: need a special > 200 value, because 0 is a for acks is an acceptable value
                return peers_in_isr;
        } else if (required_acks == 255) {
                // magic: quorum
                return peers_in_isr / 2 + 1;
        } else {
                return required_acks;
        }
}

// aborts replication of partition `p` from node `src`
// if there is an active replication stream for that node, it will be closed
// if no other partitions are to be replicated from that peer later
void Service::try_abort_replication(topic_partition *p, cluster_node *src, const uint32_t ref) {
        enum {
                trace = false,
        };
        TANK_EXPECT(p);
        TANK_EXPECT(src);
        TANK_EXPECT(cluster_aware());

        if (src == cluster_state.local_node.ref) {
                // we were definitely not replicating from us
                return;
        }

        if (trace) {
                SLog("Attempting to abort replication of ", p->owner->name(), "/", p->idx, " from ", src->id, "@", src->ep, ", ref = ", ref, "\n");
        }

        // just in case
        abort_retry_consume_from(src);

        if (auto stream = p->cluster.rs) {
                if (trace) {
                        SLog("Replication stream available for partition\n");
                }

                if (!stream->src or stream->src != src) {
                        // this is fine
                        // instead of checking if `src` is the lader of `p` before invoking this method, we can cheaply
                        // guard against that here
                        if (trace) {
                                SLog("Stream active for partition ", p->owner->name(), "/", p->idx,
                                     ", but streaming from ", stream->src->ep, " not from ", src->ep, "\n");
                        }

                        return;
                }

                if (auto c = stream->ch.get()) {
                        // we no longer wish to receive any content for (peer, partition)
                        // we may have multiple other partitions that depend on streams from that node
                        // or that node may be pulling from us now, so we don't want to close the connection unless we need to
                        TANK_EXPECT(stream->src == src);
                        TANK_EXPECT(c == src->consume_conn.ch.get());

                        if (trace) {
                                SLog("Stream connection available; will shut it down\n");
                        }

                        stream->ch.reset(); // this stream is no longer bound to replica's connection
                        did_abort_repl_stream(src);
                }

                // in case it wasn't detached
                stream->repl_streams_ll.try_detach();

                put_repl_stream(stream);
                p->cluster.rs = nullptr;
        } else if (trace) {
                SLog("No replication stream for partition\n");
        }
}

bool Service::any_partitions_to_replicate_from(cluster_node *n) const {
        enum {
                trace = false,
        };
        TANK_EXPECT(n);
        TANK_EXPECT(cluster_aware());
        auto self = cluster_state.local_node.ref;

        if (not can_accept_any_messages()) {
                return false;
        }

        if (not n->leadership.dirty) {
                // fast-path
#ifdef TANK_RUNTIME_CHECKS
                for (auto p : *n->leadership.local_replication_list) {
                        TANK_EXPECT(self->is_replica_for(p));
                }
#endif

                if (0 == partitions_io_failed_cnt) {
                        // fast-path
                        return false == n->leadership.local_replication_list->empty();
                } else {
                        for (auto p : *n->leadership.local_replication_list) {
                                if (can_accept_messages(p)) {
                                        return true;
                                }
                        }
                        return false;
                }
        }

        for (auto it : n->leadership.list) {
                const auto p = containerof(topic_partition, cluster.leader.leadership_ll, it);

                if (self->is_replica_for(p) and can_accept_messages(p)) {
                        return true;
                }
        }

        return false;
}

// returns a list of all partitions where leader is `n` and we
// are replicas of (i.e partitions to replicate from it)
const std::vector<topic_partition *> *Service::partitions_to_replicate_from(cluster_node *const n) {
        TANK_EXPECT(n);
        TANK_EXPECT(cluster_aware());
        enum {
                trace = false,
        };
        auto self = cluster_state.local_node.ref;
        TANK_EXPECT(self);

        if (n->leadership.list.empty()) {
                if (trace) {
                        SLog("Node ", n->id, "@", n->ep, " is not a leader for any partitions\n");
                }

                return nullptr;
        }

        if (not n->leadership.local_replication_list) {
                if (trace) {
                        SLog("Will create local_replication_list\n");
                }

                n->leadership.local_replication_list.reset(new std::vector<topic_partition *>());
                n->leadership.dirty = true; // force rebuild
        }

        auto v = n->leadership.local_replication_list.get();

        if (not can_accept_any_messages()) {
                v->clear();
                return v;
        }

        if (n->leadership.dirty) {
                v->clear();

                // for each partition that node's a leader now
                for (auto it : n->leadership.list) {
                        auto p = containerof(topic_partition, cluster.leader.leadership_ll, it);

                        if (self->is_replica_for(p) and can_accept_messages(p)) {
                                v->emplace_back(p);
                        }
                }

                // sort it so that replicate_from() can use this
                std::sort(v->begin(), v->end(), [](const auto a, const auto b) noexcept {
                        return a->owner < b->owner or (a->owner == b->owner and a->idx < b->idx);
                });

                n->leadership.dirty = false;

                if (trace) {
                        SLog("Leadership list dirty for node ", n->id, '@', n->ep, " updated => ", v->size(), " [", values_repr_with_lambda(v->data(), v->size(), [](const auto it) noexcept {
                                     return Buffer{}.append(it->owner->name(), '/', it->idx);
                             }),
                             "\n");
                }
        } else if (trace) {
                SLog("Not Dirty => ", v->size(), "\n");

#ifdef TANK_RUNTIME_CHECKS
                for (auto p : *v) {
                        TANK_EXPECT(self->is_replica_for(p));
                }
#endif
        }

        return v;
}

// returns true if tried to send the response to client and client connection was shut down
// invoked by complete_deferred_produce_response()
bool Service::try_generate_produce_response(produce_response *pr) {
        enum {
                trace = false,
        };
        TANK_EXPECT(pr);

        if (trace) {
                SLog("Attempting to generate PRODUCE response\n");
        }

        TANK_EXPECT(pr->deferred.expiration.ll.empty());

        if (pr->client_ctx.connection_ll.try_detach_and_reset()) {
                // detach from client connection
                if (trace) {
                        SLog("Now detached from client\n");
                }
        }

        auto c = pr->client_ctx.ch.get();

        if (!c or -1 == c->fd) {
                // client connection went away already
                // will check for (-1 == c->fd) because this may have been invoked in cleanup_connection()
                if (trace) {
                        SLog("Connection gone away\n");
                }

                put_produce_response(pr);
                return true;
        }

        const auto res = generate_produce_response(pr);

        put_produce_response(pr);
        return res;
}

// mostly serves as a trampoline for try_generate_produce_response()
// invoked by confirm_deferred_produce_resp_partition()
void Service::complete_deferred_produce_response(produce_response *dpr) {
        enum {
                trace = false,
        };
        TANK_EXPECT(dpr);

        if (trace) {
                SLog("Completing DPR for ", dpr->gen, ", pending_partitions not ack. ", dpr->deferred.pending_partitions, "\n");
        }

        if (!dpr->deferred.expiration.ll.empty()) {
                // unlink from global tracker
                dpr->deferred.expiration.ll.detach_and_reset();

                deferred_produce_responses_next_expiration = deferred_produce_responses_expiration_list.empty()
                                                                 ? std::numeric_limits<uint64_t>::max()
                                                                 : switch_list_entry(produce_response,
                                                                                     deferred.expiration.ll,
                                                                                     deferred_produce_responses_expiration_list.prev)
                                                                       ->deferred.expiration.ts;
        }

        try_generate_produce_response(dpr);
}

// an update associated with a DPR(deferred pending response) has been acknowledged
void Service::confirm_deferred_produce_resp_partition(produce_response *dpr, [[maybe_unused]] topic_partition *p, [[maybe_unused]] const uint8_t pr_participant_idx) {
        enum {
                trace = false,
        };
        TANK_EXPECT(dpr);
        TANK_EXPECT(cluster_aware());
        TANK_EXPECT(p);
        TANK_EXPECT(dpr->deferred.pending_partitions);

        if (trace) {
                SLog("Confirmed DPR ", dpr->gen, " for ", p->owner->name(), "/", p->idx,
                     " pending_partitions = ", dpr->deferred.pending_partitions, "\n");
        }

        if (--(dpr->deferred.pending_partitions)) {
                // more partititons in the DPR pending ack.
                return;
        }

        complete_deferred_produce_response(dpr);
}

#ifdef HWM_UPDATE_BASED_ON_ACKS
// We need to respect the ordering invariant:
// We can't bump the high water mark to X if there are 1+ produce requests pending ack.
// where their (bundle_last_msg_seq_num < X).
//
// Even if a produce request with required_ack = 1 is acknowledged, we can't
// update_hwmark() by its bundle_last_msg_seq_num if there are other previously submitted(i.e in
// pending_client_produce_acks_tracker.pending) product requests pending ack.
void Service::consider_acknowledged_product_reqs(topic_partition *p) {
        enum {
                trace = false,
        };
        TANK_EXPECT(p);
        const auto                                       &q  = p->cluster.pending_client_produce_acks_tracker.pending;
        auto                                             &pq = p->cluster.pending_client_produce_acks_tracker.acknowledged;
        topic_partition::Cluster::pending_ack_bundle_desc target{.last_msg_seqnum = 0};
        uint64_t                                          next;

        if (!q.empty()) {
                // there are is at least 1 product request pending
                const auto &pa = *q.begin();

                next = pa.bundle_desc.last_msg_seqnum;
        } else {
                next = std::numeric_limits<uint64_t>::max();
        }

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_green, "q.size = ", q.size(), ", pq.size = ", pq.size(), ", next = ", next, ansifmt::reset, "\n");
        }

        // go through all acknowledged produce requests in the PQ
        while (!pq.empty()) {
                const auto &bd  = pq.top();
                const auto  top = bd.last_msg_seqnum;

                if (trace) {
                        SLog("Consider top(", top, ")\n");
                }

                if (top < next) {
                        target = bd;
                        pq.pop();
                } else {
                        break;
                }
        }

        if (target.last_msg_seqnum) {
                update_hwmark(p, target);
        }
}
#endif

void Service::consider_pending_client_produce_responses(topic_partition *p, cluster_node *peer, const uint64_t seq_num) {
        enum {
                trace = false,
        };
        TANK_EXPECT(p);
        TANK_EXPECT(peer);
        auto *const isr_e = p->cluster.isr.find(peer);

        if (!isr_e) {
                // peer is not in this partition's ISR
                // we shouldn't have allowed for this to begin with
                //
                // XXX:
                // assuming a client is consuming from EOF in a 3 nodes configuration. if you keep publishing while the last one is stopped
                // and then you kill it, and then later you restart it
                // because it will commence consumption from a past sequence number, q.size() will be 0
                // why?
                // XXX: is this still an issue?
                if (trace) {
                        SLog("ODD: unable to isr.find() for ", p->owner->name(), "/", p->idx, " for ", peer->id, "@", peer->ep, "\n");
                }
                return;
        }

        consider_pending_client_produce_responses(isr_e, p, peer, seq_num);
}

// invoked by peer_consumed_local_partition() and isr_dispose()
// `seq_num` is the sequnece number the peer asked to consume from, i.e (the LSN of the last message persisted locally + 1)
//
// See also Amazon Aurora:
// 	1. https://atscaleconference.com/videos/scale-2018-amazon-aurora-design-considerations-for-high-throughput-cloud-native-relational-databases
//	2. https://www.allthingsdistributed.com/files/p1041-verbitski.pdf
// 	3. https://www.dropbox.com/s/47xbjrkni9bx0g3/aurora2.pdf?dl=0
// where they also employ the same idea (no commit protocol), where a write is not considered committed until enough peers advertise that their local LSN
// is > the LSN the produce request requires to be confirmed(i.e coordinator to bump the highwater mark).
void Service::consider_pending_client_produce_responses(isr_entry *const isr_e, topic_partition *p, cluster_node *peer, const uint64_t seq_num) {
        // https://www.confluent.io/blog/hands-free-kafka-replication-a-lesson-in-operational-simplicity/
        // See connection::As::Tank::pending_produce_reqs_acks
        enum {
                trace = false,
        };
        TANK_EXPECT(p);
        TANK_EXPECT(peer);
        TANK_EXPECT(isr_e);

        if (trace) {
                SLog(ansifmt::color_magenta, ansifmt::inverse, ansifmt::bold, "Considering pending client produce responses for ",
                     p->owner->name(), "/", p->idx, " from peer ", peer->id, "@", peer->ep, ansifmt::reset, "\n");
        }

        auto &q = p->cluster.pending_client_produce_acks_tracker.pending; // this is an std::deque<>

#ifndef HWM_UPDATE_BASED_ON_ACKS
        if (trace) {
                SLog("Considering q of size ", q.size(), ", cluster.isr.tracker.size = ", p->cluster.isr.tracker.size, ", p->cluster.isr.tracker.data[0].lsn = ", p->cluster.isr.tracker.data[0].lsn, "\n");
        }

        for (auto it = q.begin(); it != q.end();) {
                auto &pa  = *it;
                auto  dpr = pa.deferred_resp;
                TANK_EXPECT(dpr);
                const auto dpr_available           = dpr->gen == pa.deferred_resp_gen;
                const auto bundle_desc             = pa.bundle_desc;
                const auto bundle_last_msg_seq_num = bundle_desc.last_msg_seqnum;

                if (trace) {
                        SLog("For pending, bundle_last_msg_seq_num = ", bundle_last_msg_seq_num, ", dpr_available = ", dpr_available, "\n");
                }

                if (seq_num <= bundle_last_msg_seq_num) {
                        break;
                }

                if (dpr_available) {
                        dpr->participants[pa.pr_participant_idx].res = produce_response::participant::OpRes::OK;
                }

                const auto required_acks = pa.required_acks;

                if (trace) {
                        SLog("required_acks = ", required_acks, ", confirmed() = ",
                             p->cluster.isr.tracker.confirmed(required_acks, bundle_last_msg_seq_num), "\n");
                }

                if (!p->cluster.isr.tracker.confirmed(required_acks, bundle_last_msg_seq_num)) {
                        ++it;
                        continue;
                }

                if (dpr_available) {
                        // trampoline to try_generate_produce_response()
                        confirm_deferred_produce_resp_partition(dpr, p, pa.pr_participant_idx);
                }

                it = q.erase(it);
        }
#else
        using mask_t                          = decltype(topic_partition::Cluster::PendingClientProduceAcks::pending_ack::isr_nodes_acknowledged_bm);
        const auto partition_isr_node_id_mask = static_cast<mask_t>(1) << isr_e->partition_isr_node_id;

        if (trace) {
                SLog("q.size() = ", q.size(), " for ", ptr_repr(&q), "\n");
        }

        // don't e.g (for auto it = q.begin(), end = q.end(); it != end; )
        // because we use erase() and thus end can change
        for (auto it = q.begin(); it != q.end();) {
                auto &pa  = *it;
                auto  dpr = pa.deferred_resp; // deferred pending response
                TANK_EXPECT(dpr);
                const auto dpr_available           = dpr->gen == pa.deferred_resp_gen; // cookie check
                const auto bundle_desc             = pa.bundle_desc;
                const auto bundle_last_msg_seq_num = bundle_desc.last_msg_seqnum;

                // even if (false == dpr_available) or the client connection (dpr->client_ctx)
                // has gone away, we still need to respect the invariant semantics
                // we will only update the highwater mark once we got the required acks.

                if (seq_num <= bundle_last_msg_seq_num) {
                        // because all pending_acks in `q` are ordered in ascending order
                        // by bundle_last_msg_seq_num  -- because of how produce() works --
                        // we know we should stop here
                        if (trace) {
                                SLog(seq_num, "<=", bundle_last_msg_seq_num, "\n");
                        }

                        break;
                }

                if (pa.isr_nodes_acknowledged_bm & partition_isr_node_id_mask) {
                        // already got an ack. earlier from that ISR node
                        if (trace) {
                                SLog("Already got ack from that ISR node\n");
                        }

                        ++it;
                        continue;
                }

                if (dpr_available) {
                        // Set status to OK for the PR candidate
                        dpr->participants[pa.pr_participant_idx].res = produce_response::participant::OpRes::OK;
                }

                pa.isr_nodes_acknowledged_bm |= partition_isr_node_id_mask;

                const auto required_acks = pa.required_acks;

                if (trace) {
                        SLog("dpr.gen:", dpr->gen,
                             "(dpr_available=", dpr_available,
                             "): Pending: required_acks = ", required_acks,
                             ", so far:", pa.ack_count(), " (including this)\n");
                }

                if (pa.ack_count() < required_acks) {
                        // we need more ISR nodes to ack. this partition
                        if (trace) {
                                SLog("Need more ISR nodes acks(peer acks so far:", pa.ack_count(), ", required_acks:", required_acks, ")\n");
                        }

                        ++it;
                        continue;
                }

                if (trace) {
                        SLog("Can confirm DPR, dpr_available = ", dpr_available, "\n");
                }

                if (dpr_available) {
                        // trampoline to try_generate_produce_response()
                        confirm_deferred_produce_resp_partition(dpr, p, pa.pr_participant_idx);
                }

                // push into the prio.queue
                // consider_acknowledged_product_reqs() will take care of that pq
                p->cluster.pending_client_produce_acks_tracker.acknowledged.push(bundle_desc);

                it = q.erase(it);
        }

        consider_acknowledged_product_reqs(p);
#endif
}

void Service::consider_pending_client_produce_responses(topic_partition *const p) {
        // this is invoked when the ISR of a partition has changed
        // either because a node has been added or removed from it
        // and we get a chance to consider all pending client pending produce responses
        // where we may be able to satisfy the invariant
        enum {
                trace = false,
        };
        TANK_EXPECT(p);

        if (trace) {
                SLog(ansifmt::color_magenta, ansifmt::bgcolor_green, "Considering pending produce responses from ",
                     p->owner->name(), "/", p->idx, " because ISR likely updated", ansifmt::reset, "\n");
        }

        auto      &q        = p->cluster.pending_client_produce_acks_tracker.pending;
        const auto isr_size = p->cluster.isr.size();

        if (trace) {
                SLog("q.size() = ", q.size(), ", isr_size = ", isr_size, "\n");
        }

#ifndef HWM_UPDATE_BASED_ON_ACKS
        for (auto it = q.begin(); it != q.end();) {
                auto      &pa                      = *it;
                auto       dpr                     = pa.deferred_resp;
                const auto dpr_available           = dpr->gen == pa.deferred_resp_gen;
                const auto required_acks           = pa.required_acks;
                const auto bundle_desc             = pa.bundle_desc;
                const auto bundle_last_msg_seq_num = bundle_desc.last_msg_seqnum;

                if (!p->cluster.isr.tracker.confirmed(required_acks, bundle_last_msg_seq_num)) {
                        ++it;
                        continue;
                }

                if (dpr_available) {
                        // trampoline to try_generate_produce_response()
                        confirm_deferred_produce_resp_partition(dpr, p, pa.pr_participant_idx);
                }

                it = q.erase(it);
        }
#else

        // this is potentially expensive but
        // likely not going to have to do this frequently
        for (auto it = q.begin(); it != q.end();) {
                auto      &pa            = *it;
                auto       dpr           = pa.deferred_resp;
                const auto dpr_available = dpr->gen == pa.deferred_resp_gen; // cookie check
                const auto required_acks = pa.required_acks;
                const auto acknowledged  = pa.ack_count();

                if (trace) {
                        SLog("Considering pending ack, required_acks = ", required_acks,
                             ", dpr_available = ", dpr_available,
                             ", acknowledged by peers = ", acknowledged,
                             ", required_acks = ", required_acks, "\n");
                }

                if (required_acks + 1 /* +1 for local */ > isr_size) {
                        // We need (required_acks) acks. from _PEERS_
                        // and we already have an implict ack from the local node
                        // so if (required_acks + 1 > isr_size), which always includes the local node,
                        // it means we have insufficent nodes in the ISR, so we need to ACK now
                        if (trace) {
                                SLog("Insufficient number of nodes in the ISR(", isr_size, "), will ack now\n");
                        }
                } else if (acknowledged < required_acks) {
                        // not done yet
                        if (trace) {
                                SLog("Not ready, acknowledged(", acknowledged, "), required_acks(", required_acks, ")\n");
                        }

                        ++it;
                        continue;
                }

                const auto bundle_desc = pa.bundle_desc;

                if (trace) {
                        SLog("Ready to ack\n");
                }

                if (dpr_available) {
                        // trampoline to try_generate_produce_response()
                        confirm_deferred_produce_resp_partition(dpr, p, pa.pr_participant_idx);
                }

                // We first push into into the prio.queue and
                // eventually consider_acknowledged_product_reqs() will pop from that pq
                p->cluster.pending_client_produce_acks_tracker.acknowledged.push(bundle_desc);

                it = q.erase(it);
        }

        consider_acknowledged_product_reqs(p);
#endif
}

// called when a peer has issued a `ConsumePeer` request for (p, seq_num)
//
// This is important for:
// 	- ISR tracking
// 	- dealing with pending produce requests pending ack (based on RF)
void Service::peer_consumed_local_partition(topic_partition *p, cluster_node *peer, const uint64_t seq_num) {
        enum {
                trace = false,
        };
        TANK_EXPECT(p);
        TANK_EXPECT(peer);
        auto       log  = partition_log(p);
        const auto last = log->lastAssignedSeqNum;

        if (not cluster_aware()) {
                // we shouldn't invoke this to begin with
                if (trace) {
                        SLog("Not cluster-aware, will not proceed\n");
                }

                return;
        }

        if (p->cluster.leader.node != cluster_state.local_node.ref) {
                // well, it's not us anymore
                if (trace) {
                        SLog("Leader of partition is no longer us\n");
                }

                return;
        }

        if (trace) {
                SLog(ansifmt::inverse, ansifmt::bold, ansifmt::color_brown,
                     "Peer ", peer->id, "@", peer->ep, " consumed content from ", p->owner->name(), "/", p->idx, " at ", seq_num, ", last = ", last, ansifmt::reset, "\n");
        }

        if (not p->cluster.replicas.count(peer->id)) {
                // peer who consumed is not a replica for this partition
                if (trace) {
                        SLog("Unexpected CONSUME from node that is not in the partition's ISR\n");
                }

                return;
        }

        isr_entry *isr_e{nullptr};

        // manage this partition's ISR
        // it's important that we first deal with ISR and then with pending produce responses
        if (seq_num <= last) {
                // `peer` has not caught up yet
                //
                // once the peer attempts to read past the last commited sequence number, we know it
                // has all partition content, and thus we can consider it for ISR/membership
                if (trace) {
                        SLog("Peer hasn't caught up yet because (seq_num(", seq_num, ") <= last(", last, "))\n");
                }

		//2022-08-18: now just returning instead of goto l11;
                //WAS: goto l1;
		return;
        }


        if ((isr_e = p->cluster.isr.find(peer))) {
                // peer is already in this partition's ISR
                TANK_EXPECT(isr_e->partition() == p);

                if (trace) {
                        SLog("Peer already in ISR\n");
                }

                if (isr_e->pending_next_ack_ll.try_detach_and_reset()) {
                        // remove from the pend.ack timers
                        if (trace) {
                                SLog("Will stop the timer\n");
                        }

                        isr_pending_ack_list_next = isr_pending_ack_list.empty()
                                                        ? std::numeric_limits<uint64_t>::max()
                                                        : switch_list_entry(isr_entry, pending_next_ack_ll, isr_pending_ack_list.prev)->pending_next_ack_timeout;
                }

#ifndef HWM_UPDATE_BASED_ON_ACKS
                // notice that we use (seq_num - 1)
                // that is because we track the lsn of the last known. ack message
                isr_touch(isr_e, seq_num - 1);
#endif
        } else {
                // add peer to this partition's ISR

#ifdef HWM_UPDATE_BASED_ON_ACKS
                isr_e = isr_bind(p, peer, p->cluster.isr.next_partition_isr_node_id(), __LINE__);
#else
                isr_e = isr_bind(p, peer, __LINE__);
#endif

		TANK_EXPECT(isr_e);


                persist_isr(p, __LINE__);

#ifndef HWM_UPDATE_BASED_ON_ACKS
                // notice that we use (seq_num - 1)
                // that is because we track the lsn of the last known. ack message
                isr_touch(isr_e, seq_num - 1);
#endif
        }

l1:
        // deal with any pending produce responses
	TANK_EXPECT(isr_e);


        consider_pending_client_produce_responses(isr_e, p, peer, seq_num);
}

static uint8_t choose_compression_codec(const topic_partition::msg *msgs, const size_t size) {
        if (size > 512) {
                return 1;
        }

        size_t sum{0};

        for (size_t i{0}; i < size; ++i) {
                sum += msgs[i].data.size() + msgs[i].key.size();

                if (sum > 1024) {
                        return 1;
                }
        }

        return 0;
}

// content consumed from peer; commit locally
void Service::persist_peer_partitions_content(topic_partition *const partition, const std::vector<topic_partition::msg> &partition_msgs, const bool first_sparse) {
        enum {
                trace = false,
        };
        static thread_local IOBuffer cb_tls;
        const auto                   cnt = partition_msgs.size();
        auto                        &cb{cb_tls};

        TANK_EXPECT(partition);

	if (trace) {
		SLog("Persist ", dotnotation_repr(cnt), "  msgs for ", partition->owner->name(), "/", partition->idx, "\n");
	}

        if (0 == cnt) {
                return;
        }

        auto b = get_buf();

        DEFER({
                put_buf(b);
        });

        // we 'll be packing upto X messages / bundle
        auto     log = partition_log(partition);
        uint64_t next_expected;

        if (first_sparse) {
                // force generate sparse
                next_expected = 0;
        } else {
                next_expected = partition_msgs.front().seqNum;
        }

        for (const auto *p = partition_msgs.data(), *const e = p + cnt; p < e;) {
                // determine batch size
                const auto upto                 = std::min(e, p + 128);
                const auto msgset_first_seq_num = p->seqNum;
                const auto msgset_msgs_cnt      = std::distance(p, upto);
                const auto first_msg            = p;
                const auto last_msg             = upto - 1;
                const auto msgset_last_seq_num  = last_msg->seqNum;
                uint8_t    bundle_flags         = 0;
                bool       as_sparse            = false;
                const auto codec                = choose_compression_codec(p, std::distance(p, upto));

                // is this going to be a sparse batch?
                for (const auto *it = p; it < upto; ++it) {
                        if (const auto seq_num = it->seqNum; seq_num != next_expected) {
                                as_sparse = true;
                                break;
                        } else {
                                next_expected = seq_num;
                        }
                }
                next_expected = upto[-1].seqNum + 1;

                b->clear();

                // BEGIN: bundle header
                // encode the bundle header
                // we need to consider wether compression makes sense, wether this is going to be a sparse bundle, etc
                bundle_flags |= codec & 0b111;

                if (as_sparse) {
                        bundle_flags |= (1u << 6);
                }

                if (msgset_msgs_cnt < 16) {
                        bundle_flags |= msgset_msgs_cnt << 2;
                        b->pack(bundle_flags);
                } else {
                        b->pack(bundle_flags);
                        b->encode_varuint32(msgset_msgs_cnt);
                }

                if (as_sparse) {
                        b->pack(msgset_first_seq_num);

                        if (msgset_msgs_cnt != 1) {
                                b->encode_varuint32((msgset_last_seq_num - msgset_first_seq_num) - 1);
                        }
                }
                // END: bundle header

                // BEGIN: messages set
                if (codec) {
                        cb.clear();

                        do {
                                const auto &m                     = *p;
                                uint8_t     flags                 = m.key ? static_cast<uint8_t>(TankFlags::BundleMsgFlags::HaveKey) : 0;
                                const auto  use_last_specified_ts = p != first_msg and m.ts == p[-1].ts;
                                uint32_t    delta;

                                if (use_last_specified_ts) {
                                        flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                }

                                if (as_sparse and p != first_msg and p != last_msg) {
                                        delta = m.seqNum - p[-1].seqNum - 1;

                                        if (!delta) {
                                                flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                        }
                                } else {
                                        delta = 0;
                                }

                                cb.pack(flags);

                                if (delta) {
                                        cb.encode_varuint32(delta);
                                }

                                if (!use_last_specified_ts) {
                                        cb.pack(m.ts);
                                }

                                if (m.key) {
                                        cb.pack(m.key.size());
                                        cb.serialize(m.key.data(), m.key.size());
                                }

                                cb.encode_varuint32(m.data.size());
                                cb.serialize(m.data.data(), m.data.size());
                        } while (++p < upto);

                        switch (codec) {
                                case 1:
                                        if (!Compression::Compress(Compression::Algo::SNAPPY, cb.data(), cb.size(), b)) {
                                                IMPLEMENT_ME();
                                        }
                                        break;

                                default:
                                        IMPLEMENT_ME();
                        }
                } else {
                        do {
                                const auto &m                     = *p;
                                uint8_t     flags                 = m.key ? static_cast<uint8_t>(TankFlags::BundleMsgFlags::HaveKey) : 0;
                                const auto  use_last_specified_ts = p != first_msg and m.ts == p[-1].ts;
                                uint32_t    delta;

                                if (use_last_specified_ts) {
                                        flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                }

                                if (as_sparse and p != first_msg and p != last_msg) {
                                        delta = m.seqNum - p[-1].seqNum - 1;

                                        if (!delta) {
                                                flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                        }
                                } else {
                                        delta = 0;
                                }

                                b->pack(flags);

                                if (delta) {
                                        b->encode_varuint32(delta);
                                }

                                if (!use_last_specified_ts) {
                                        b->pack(m.ts);
                                }

                                if (m.key) {
                                        b->pack(m.key.size());
                                        b->serialize(m.key.data(), m.key.size());
                                }

                                b->encode_varuint32(m.data.size());
                                b->serialize(m.data.data(), m.data.size());
                        } while (++p < upto);
                }
                // END: messages set

                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_brown, "Generated bundle of ", msgset_msgs_cnt, " size ", size_repr(b->size()), ansifmt::reset, "\n");
                }

                auto res = log->append_bundle(curTime,
                                                    reinterpret_cast<const uint8_t *>(b->data()), b->size(),
                                                    msgset_msgs_cnt,
                                                    msgset_first_seq_num, msgset_last_seq_num);




                if (not res.fdh) {
                        if (res.dataRange.size() == std::numeric_limits<uint32_t>::max()) {
                                // invalid request(offsets)
                                IMPLEMENT_ME();
                        } else {
                                // system error
                                IMPLEMENT_ME();
                        }
                } else {
                        // success

#ifdef TANK_SUPPORT_CONSUME_FLAGS
                        // 2022-08-18: PARTITION_PROVIDER
                        now_awake.clear();
                        consider_append_res(partition, res, &now_awake);

			if (trace) {
				SLog(ansifmt::color_blue, ansifmt::inverse, "now_awake.size() = ", now_awake.size(), ansifmt::reset, "\n");
			}

			for (auto ctx : now_awake) {
				wakeup_wait_ctx(ctx, nullptr);
			}

                        now_awake.clear();
#endif


                }
        }
}

// See TankClient::process_consume()
//
// XXX: we will need to use coroutines/fibers or futures because we _really_ don't want
// to decompress or compress any content here
// we 'll collect for every (topic, partition) all messages, and then we 'll ship that to another thread
// that will decompress whatever is needed and will compress for whatever needs to be persisted to a segment
//
// TODO: see optimization comments for TankClient::process_consume()
bool Service::process_peer_consume_resp(connection *const c, const uint8_t *p, const size_t len) {
        enum { trace = false,
        };
        const auto  end              = p + len;
        auto       &partition_msgs   = reusable.partition_msgs;
        auto       &acquired_buffers = reusable.acquired_buffers;
        str_view8   key;
        const auto  hdr_size      = decode_pod<uint32_t>(p);
        const auto *bundles_chunk = p + hdr_size;            // first partition's bundles chunk (see ^^ about partitions chunks)
        const auto  topics_cnt    = decode_pod<uint16_t>(p); // ConsumePeer: topics_cnt is encoded as u16
        auto        peer          = c->as.consumer.node;
        TANK_EXPECT(peer);

        partition_msgs.clear();
        acquired_buffers.clear();

        if (trace) {
                SLog(ansifmt::color_brown, ansifmt::bold, "GOT consume response for ", topics_cnt, " topics", ansifmt::reset, " from ", peer->id, '@', peer->ep, "\n");
        }

        c->as.consumer.state = connection::As::Consumer::State::Idle;

#pragma mark BEGIN
        for (std::size_t i{0}; i < topics_cnt; ++i) {
                const auto      len = decode_pod<uint8_t>(p);
                const str_view8 topic_name(reinterpret_cast<const char *>(p), len);
                p += len;
                const auto partitions_cnt = decode_pod<uint16_t>(p); // ConsumePeer: partitions_cnt is encoded as u16
                uint64_t   log_base_seqnum;

                if (trace) {
                        SLog("Topic [", topic_name, "], for ", partitions_cnt, " partitions\n");
                }

                if (*reinterpret_cast<const uint16_t *>(p) == std::numeric_limits<uint16_t>::max()) {
                        // unknown topic
                        if (trace) {
                                SLog("Unknown topic\n");
                        }

                        IMPLEMENT_ME();

                        p += sizeof(uint16_t);
                        continue;
                }

                // we may be unable to get the topic, but that's OK, we 'll just ignore content we don't need
                auto topic = topic_by_name(topic_name);

                for (size_t k{0}; k < partitions_cnt; ++k) {
                        const auto partition_id = decode_pod<uint16_t>(p);
                        const auto err_flags    = decode_pod<uint8_t>(p);

                        if (trace) {
                                SLog(ansifmt::color_green, "For partition ", partition_id, ", err_flags ", err_flags, ansifmt::reset, "\n");
                        }

                        if (err_flags == 0xff) {
                                // undefined partiion
                                IMPLEMENT_ME();

                                if (trace) {
                                        SLog("Undefined partition\n");
                                }

                                continue;
                        } else if (err_flags == 0xfd) {
                                // no leader
                                // this is fine, we are likely in the process of acquiring a new leader
                                //
                                // What if we create a new topic with N partitions
                                // and then set RF of topic to 3
                                // there is a window of time that the local node knows that leader is node P
                                // but P doesn't yet know it is the leader.
                                //
                                // We will just try again
                                if (trace or true) {
                                        SLog("No leader yet for ", topic_name, "/", partition_id, " (reported by ", peer->id, "@", peer->ep, ")\n");
                                }

                                continue;
                        } else if (err_flags == (0xfc)) {
                                // different leader
                                // this is fine, we expect this to be resolved soon
                                [[maybe_unused]] const Switch::endpoint ep{decode_pod<uint32_t>(p), decode_pod<uint16_t>(p)};

                                if (trace) {
                                        SLog("Edge case: different leader, leader now is ", ep, "\n");
                                }

                                continue;
                        } else if (err_flags == 0xfe) {
                                // sparse bundle follows
                                log_base_seqnum = 0;

                                if (trace) {
                                        SLog("First Bundle Sparse, log_base_seqnum = 0\n");
                                }
                        } else {
                                // base abs. seq.num of the first message in the first bundle of this chunk
                                log_base_seqnum = decode_pod<uint64_t>(p);

                                if (trace) {
                                        SLog("Not sparse, log_base_seqnum = ", log_base_seqnum, "\n");
                                }
                        }

                        // if we don't have this partition, ignore the received content
                        auto *const                 partition         = topic ? topic->partition(partition_id) : nullptr;
                        [[maybe_unused]] const auto highwater_mark    = decode_pod<uint64_t>(p);
                        const auto                  bundles_chunk_len = decode_pod<uint32_t>(p); // length of this particion's chunk that contains 0+ bundles
                        const uint64_t              requested_seqnum  = partition
                                                                            ? (partition_log(partition)->lastAssignedSeqNum + 1)
                                                                            : std::numeric_limits<uint64_t>::max();

                        if (trace) {
                                SLog("highwater_mark = ", highwater_mark, "\n");
                        }

                        if (err_flags == 0x1) {
                                // boundary check fault
                                //
                                // this can happen when we have just switched to a different leader
                                // so for a few MS we may have to deal with boundary faults
                                // but this is OK because it only lasts for a few MS until this is resolved
                                //
                                // This can also happen if you used tank-cli to mirror a partition from a different cluster/node
                                // and the first available message's LSN is not 1. tank-cli will produce to the leader, but if RF > 1
                                // other peers will try to replicate starting from 1 (because it is a new partition, and thus it will fail
                                //
                                // it also happen when we are dealing with sparse partitions
                                [[maybe_unused]] const auto first_avail_seqnum = decode_pod<uint64_t>(p);
                                uint64_t                    next               = requested_seqnum;

                                if (next < first_avail_seqnum) {
                                        next = first_avail_seqnum;
                                } else if (next > highwater_mark) {
                                        next = highwater_mark + 1;
                                }

                                // next time we 'll fetch content for that partition, we 'll fetch starting from next
                                TANK_EXPECT(partition);
                                partition->cluster.consume_next_lsn = next;

                                if (trace or true) {
                                        SLog("At ", Date::ts_repr(time(nullptr)), ": Boundary check fault, requested_seqnum = ", requested_seqnum, ", first_avail_seqnum = ", first_avail_seqnum, ", highwater_mark = ", highwater_mark, ", next time we will consume from ", next, "\n");
                                }

                                continue;
                        } else if (err_flags and err_flags < 0xfe) {
                                // other type of fault
                                IMPLEMENT_ME();
                                continue;
                        }

                        [[maybe_unused]] const auto drained_partition = (bundles_chunk_len == 0);
                        const auto                  partition_bundles = bundles_chunk;
                        uint64_t                    first_msg_seqnum, last_msg_seqnum;
                        const uint8_t              *need_from, *need_upto;
                        bool                        any_captured{false}, first_sparse{false};

                        bundles_chunk += bundles_chunk_len; // skip bundles for this partition

                        partition_msgs.clear();
                        acquired_buffers.clear();
                        DEFER({
                                for (auto b : acquired_buffers) {
                                        put_buf(b);
                                }
                        });

                        if (trace) {
                                SLog("drained_partition = ", drained_partition, "\n");
                        }

                        // process all bundles in this partition's bundles chunk
                        for (const auto *p = partition_bundles, *const chunk_end = std::min(end, bundles_chunk);;) {
                                need_from = p;
                                need_upto = p + 512;

                                if (unlikely(!Compression::check_decode_varuint32(p, chunk_end))) {
                                        if (trace) {
                                                SLog("Unable to decode bundle_len\n");
                                        }

                                        break;
                                }

                                const auto bundle_len = Compression::decode_varuint32(p);
                                const auto bundle_end = p + bundle_len;

                                // assume we will need until the end of the bundle at least
                                // we will adjust this as we understand better the response structure
                                need_upto = bundle_end + 256;

                                if (unlikely(p >= chunk_end)) {
                                        if (trace) {
                                                SLog("Unable to decode bundle_hdr_flags\n");
                                        }

                                        break;
                                }

                                // BEGIN: bundle header
                                const auto                          bundle_hdr_flags = decode_pod<uint8_t>(p);
                                const auto                          codec            = bundle_hdr_flags & 3;
                                const auto                          sparse_bundle    = bundle_hdr_flags & (1u << 6);
                                uint32_t                            msgset_size      = (bundle_hdr_flags >> 2) & 0xf;
                                uint64_t                            msgset_end;
                                range_base<const uint8_t *, size_t> msgset_content;

                                if (0 == msgset_size) {
                                        if (unlikely(!Compression::check_decode_varuint32(p, chunk_end))) {
                                                break;
                                        } else {
                                                msgset_size = Compression::decode_varuint32(p);
                                        }
                                }

                                if (sparse_bundle) {
                                        if (unlikely(p + sizeof(uint64_t) >= chunk_end)) {
                                                break;
                                        }

                                        first_msg_seqnum = decode_pod<uint64_t>(p);

                                        if (msgset_size != 1) {
                                                if (unlikely(!Compression::check_decode_varuint32(p, chunk_end))) {
                                                        break;
                                                }

                                                last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1;
                                        } else {
                                                last_msg_seqnum = first_msg_seqnum;
                                        }

                                        if (trace) {
                                                SLog("For sparse bundle, first_msg_seqnum = ", first_msg_seqnum, ", last_msg_seqnum = ", last_msg_seqnum, "\n");
                                        }

                                        log_base_seqnum = first_msg_seqnum;
                                        msgset_end      = last_msg_seqnum + 1;

                                        if (need_from == partition_bundles) {
                                                first_sparse = true;
                                        }
                                } else {
                                        msgset_end = log_base_seqnum + msgset_size;
                                }
                                // END: bundle header

                                if (requested_seqnum < std::numeric_limits<uint64_t>::max() and requested_seqnum >= msgset_end) {
                                        // fast path: skip this bundle
                                        p               = bundle_end;
                                        log_base_seqnum = msgset_end;

                                        if (trace) {
                                                SLog("Skipping content bundle\n");
                                        }

                                        continue;
                                }

                                if (codec) {
                                        if (trace) {
                                                SLog("Need to decompress for ", codec, "\n");
                                        }

                                        if (bundle_end > chunk_end) {
                                                if (trace) {
                                                        SLog("Not enough bytes: ", std::distance(chunk_end, bundle_end), " more required\n");
                                                }

                                                break;
                                        }

                                        auto raw_data = get_buf();

                                        acquired_buffers.emplace_back(raw_data);
                                        switch (codec) {
                                                case 1:
                                                        if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, std::distance(p, bundle_end), raw_data)) {
                                                                IMPLEMENT_ME();
                                                        }
                                                        break;

                                                default:
                                                        IMPLEMENT_ME();
                                        }

                                        msgset_content.set(reinterpret_cast<const uint8_t *>(raw_data->data()), raw_data->size());
                                } else {
                                        msgset_content.set(p, std::min(std::distance(p, chunk_end), std::distance(p, bundle_end)));
                                }

                                // advance p past this bundle
                                p = bundle_end;

                                // scan this bundle's message set
                                uint64_t   ts                  = 0;
                                uint32_t   msg_idx             = 0;
                                const auto min_accepted_seqnum = requested_seqnum == std::numeric_limits<uint64_t>::max() ? 0 : requested_seqnum;

                                if (trace) {
                                        SLog("Scanning message set of length ", size_repr(msgset_content.size()), "\n");
                                }

                                for (const auto *p = msgset_content.offset, *const msgset_end = p + msgset_content.size();; ++msg_idx, ++log_base_seqnum) {
                                        if (!codec and any_captured) {
                                                // this makes sense because we didn't need to decompress the bundle
                                                need_upto = p + 256;
                                        }

                                        if (unlikely(p + sizeof(uint8_t) > msgset_end)) {
                                                // likely hit end of the message set
                                                if (trace and p != msgset_end) {
                                                        SLog("Unable to decode msg_flags\n");
                                                }

                                                // important; don't goto next_partition here
                                                break;
                                        }

                                        const auto msg_flags = decode_pod<uint8_t>(p);

                                        if (sparse_bundle) {
                                                if (msg_flags & unsigned(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                        // (prev + 1)
                                                        // no need to advance log_base_seqnum, was advanced in for()
                                                } else if (0 == msg_idx) {
                                                        log_base_seqnum = first_msg_seqnum;
                                                } else if (msg_idx == msgset_size - 1) {
                                                        log_base_seqnum = last_msg_seqnum;
                                                } else {
                                                        if (unlikely(!Compression::check_decode_varuint32(p, msgset_end))) {
                                                                goto next_partition;
                                                        }

                                                        const auto delta = Compression::decode_varuint32(p);

                                                        // not going to set log_base_seqnum to (delta + 1)
                                                        // because we 'll (++log_base_seqnum) at the end of the iteration anyway
                                                        log_base_seqnum += delta;
                                                }
                                        }

                                        const auto msg_abs_seqnum = log_base_seqnum;

                                        if (0 == (msg_flags & unsigned(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                                if (unlikely(p + sizeof(uint64_t) > msgset_end)) {
                                                        if (trace) {
                                                                SLog("Not Enough Content Available\n");
                                                        }

                                                        goto next_partition;
                                                }

                                                ts = decode_pod<uint64_t>(p);
                                        }

                                        if (msg_flags & unsigned(TankFlags::BundleMsgFlags::HaveKey)) {
                                                if (unlikely(p + sizeof(uint8_t) > msgset_end or (p + *p + sizeof(uint8_t) > msgset_end))) {
                                                        if (trace) {
                                                                SLog("Not Enough Content Available\n");
                                                        }

                                                        goto next_partition;
                                                } else {
                                                        key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                                        p += sizeof(uint8_t) + key.size();
                                                }
                                        } else {
                                                key.reset();
                                        }

                                        if (unlikely(!Compression::check_decode_varuint32(p, msgset_end))) {
                                                if (trace) {
                                                        SLog("Not Enough Content Available\n");
                                                }

                                                goto next_partition;
                                        }

                                        const auto len = Compression::decode_varuint32(p);

                                        if (const auto e = p + len; e > msgset_end) {
                                                if (!codec and any_captured) {
                                                        // see above
                                                        need_upto = e + 256;
                                                }

                                                goto next_partition;
                                        }

#if 0 
					// we don't respect the highwater mark here, unlike in TankClient::process_consume()
					// if a topic has RF=2, and for that topic one of the replicas is the master
					// and the other replica is this node, then hwmark will only increment once we
					// confirm that we got the data, so it is meaningless here
	
                                        if (msg_abs_seqnum > highwater_mark) {
                                                // abort immediately
                                                // we need to respect the semantics
                                                goto next_partition;
                                        }
#endif

                                        if (msg_abs_seqnum >= min_accepted_seqnum) {
                                                partition_msgs.emplace_back(topic_partition::msg{
                                                    .seqNum = msg_abs_seqnum,
                                                    .ts     = ts,
                                                    .key    = key,
                                                    .data   = str_view32(reinterpret_cast<const char *>(p), len),
                                                });
                                        }

                                        p += len; // to next bundle for this partition
                                }
                        }

                next_partition:
                        if (trace) {
                                SLog("DONE WITH that partition ", partition_msgs.size(), " collected, need span  = ", std::distance(need_from, need_upto), "\n");
                        }

                        if (not partition) {
                                continue;
                        }

                        assert(need_from <= need_upto);

                        enum : std::size_t {
                                alignment = 512 * 1024,
                        };
                        if (auto stream = partition->cluster.rs) {
                                // there's a replication stream already
                                // we use it to replicate partition messages from the leader to this node
                                // update min_fetch_size for the next CONSUME request from that
                                //
                                // TODO:
                                // Service::determine_min_fetch_size_for_new_stream().
                                // maybe next should also account for that
                                const auto next = aligned_to(std::max<std::size_t>(stream->min_fetch_size,
                                                                                   aligned_to(std::distance(need_from, need_upto), 4096)),
                                                             alignment);

                                stream->min_fetch_size = next;
                        }

                        persist_peer_partitions_content(partition, partition_msgs, first_sparse);
                }
        }
#pragma mark END

        // we always consume again after we have received a consume resp.
        // TODO: deal with connection failures
        try_replicate_from(peer);
        return true;
}

void Service::invalidate_replicated_partitions_from_peer_cache(cluster_node *n) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        enum {
                trace = false,
        };

        TANK_EXPECT(n);

        if (trace) {
                SLog(ansifmt::bold, "|| Invalidating cache of partitions this node replicates from ", n->id, "@", n->ep, ansifmt::reset, "\n");
        }

        n->leadership.dirty = true;
}

void Service::invalidate_replicated_partitions_from_peer_cache_by_partition(topic_partition *p) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        TANK_EXPECT(p);

        if (auto n = p->cluster.leader.node) {
                invalidate_replicated_partitions_from_peer_cache(n);
        }
}

#include "service_common.h"
//====================================================
// We need to update multiple different keys specific to a single partition (topology, leadership, ISR)
// and even when we use transactions to update them atomically . This is because monitoring the whole TANK/clusters/<name>/
// consul keyspace for updates is very expensive. Consul cannot return deltas/changes, so if we were to do that we 'd get back
// *all* keys with their values+metadata when *any* key is modified. To that end, we monitor different namespaces
// in TANK/clusters/<name> so that when a key in each of the monitored keyspace is updated, only that namespace KVs are returned.
// This does mean though that updates may come out of order, because we can't guarantee the order at which we will receive them(depends
// on which consul requests were scheduled first, in what order consul may choose to respond to long-polling requests, etc).
// In addition, consul state updates may a resuilt of e.g curl use where someone manipulates the TANK namespace.
//
// Because of that, we need to be very careful about how we issue updates and how we react to them.
// We will expect that updates are issued in the "correct order" even if sometimes that won't be the case, but at least
// we will assume this to be the case _most of the time_.
//
// The logical order of updates to a partition is
// 1. Nodes
// 2. RF change
// 3. Topology (i.e list of replicas)
// 4. Leadership (i.e leader of the replica)
// 5. ISR (list of in-sync replicas)
// We can batch multiple updates to a partition namespaces using a transaction. However, because of the above semantic
// we can't guarantee that we will also receive them all together, or in order.
// We can defer processing updates for as long as we are getting responses from consul (see schedule_cluster_updates_apply() impl.)
// but that is not sufficient, so we need to _forego updates of multiple partition namespaces_ and instead only update one such partition/namespace
// at a time(see gen_partition_nodes_updates) and then react to the update(see apply_cluster_state_updates) and generate another update etc. 
//
// We generate updates almost always as a reaction to a received update.
// Cluster Leaders are responsible for updating almost all keyspaces; the only exception is that partition leaders
// are responsible for the ISRs of the partitions they manage.
//
// SPECIFICS:
// - ISRs can *only* be updated by the partition leader, i.e only the partition leader may persist_isr(). This is especially important for repair(see `REPAIR`)
// - partitions replicas and partitions leaders can *only* be updated by the cluster leader(for repairs, the partition leader should apply them locally and then update)
// - Minimal filtering for consul state updates when reconciling them with memory state(see `FILTER`)
// - REPAIRs when reconciling consul state with memory state only possible for ISRs (see `REPAIR`)
// - All other repairs _after_ consul state was reconciled with memory state. See `DEFERRED REPAIRS`
void Service::schedule_cluster_updates_apply(const char *src) {
        if (next_cluster_state_apply != 1) {
                // not forced; wait for another 100 ms before apply_cluster_state_updates()
                next_cluster_state_apply = now_ms + 100;
        }
}

// apply_cluster_state_updates() ASAP
void Service::force_cluster_updates_apply() {
        next_cluster_state_apply = 1;
}

void Service::conclude_bootstrap_updates() {
	static constexpr bool trace{false};

        if (0 == (consul_state.flags & unsigned(ConsulState::Flags::BootstrapStateUpdatesProcessed))) {
                if (trace) {
                        SLog(ansifmt::bold, "** Processed bootstrap updates, will enable_listener() **", ansifmt::reset, "\n");
                }

                consul_state.flags |= unsigned(ConsulState::Flags::BootstrapStateUpdatesProcessed);
                enable_listener();

                // we need to acquire the node ID again, this time we will include the endpoint
                // so that other nodes will know we are ready
		//
		// XXX: we _cannot_ attempt to become cluster leaders before registering this node with a valid endpoint
		// because other nodes will ignore the consul leadership update because this node won't be available() yet
		// i.e if the cluster leadership update for this node arrives before the request for acquiring this node (id, ep)
		// then this node ep will be unset so the other nodes will ignore it
                schedule_consul_req(consul_state.get_req(consul_request::Type::AcquireNodeID), true);
        }
}

// By consolidating all logic here as opposed to implementing bits and pieces everywhere we have a better chance of
// doing this right. It also makes it possible to unit-test behavior, whereas that wasn't possible earlier
//
// INVARIANTS
// - ISR set can only contain nodes from the partition's replicas set that are available()
// - Cannot have any nodes in the ISR that are not also present in the replicas set
// - A partition's leader must exist in the replicas set
// - If a partition leader is set, it must also exist in the ISR
// - A partition's leader must exist in the ISR
// - Cluster leader must be available()
// - A partition leader can only be missing or be NA if the partition is no longer active
void Service::apply_cluster_state_updates() {
	enum {
		trace = false,
	};
        TANK_EXPECT(cluster_state.local_node.ref);
        TANK_EXPECT(cluster_state.local_node.id);
        TANK_EXPECT(consul_state.reg_completed());

        cluster_partitions_dirty.clear();
        if (cluster_state.updates.nodes.empty() and
            cluster_state.updates.pm.empty() and
            cluster_state.updates.tm.empty() and
            not cluster_state.updates.cluster_leader.defined) {
                conclude_bootstrap_updates();
                return;
        }

        const auto                                                                 before                 = Timings::Microseconds::Tick();
        auto &                                                                     dirty_nodes            = reusable.dirty_nodes;
        auto &                                                                     dirty_topics           = reusable.dirty_topics;
        auto &                                                                     dirty_partitions       = reusable.dirty_partitions;
        auto &                                                                     v                      = reusable.part_bool_hashmap;
        auto &                                                                     pv                     = reusable.pv;
        auto &                                                                     nodes_replicas_updates = reusable.nodes_replicas_updates;
        auto &                                                                     stream_start           = reusable.stream_start;
        auto &                                                                     stream_stop            = reusable.stream_stop;
        [[maybe_unused]] const auto                                                self                   = cluster_state.local_node.ref;
        auto                                                                       leader_self = cluster_state.leader_self();
        bool                                                                       promoted_to_cluster_leader{false};
        bool                                                                       rebuild_all_available_nodes{false};
        bool                                                                       track_all_partitions_dirty{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "::CLUSTER STATE UPDATE::", ansifmt::reset,
                     ansifmt::color_brown, " nodes = ", cluster_state.updates.nodes.size(), ", topics = ", cluster_state.updates.tm.size(),
                     ", partitions = ", cluster_state.updates.pm.size(),
                     ", cluster_leader {defined: ", cluster_state.updates.cluster_leader.defined, ", value:", cluster_state.updates.cluster_leader.nid, "}", ansifmt::reset, "\n");
        }

	dirty_nodes.clear();
	dirty_topics.clear();
	dirty_partitions.clear();
	v.clear();
	pv.clear();
	nodes_replicas_updates.clear();
	stream_start.clear();
	stream_stop.clear();


#pragma mark NODES
        for (auto &it : cluster_state.updates.nodes) {
                auto node            = it.first;
                auto [ep, avail]     = it.second;
                const auto was_avail = node->available();

                if (node == cluster_state.local_node.ref) {
                        // This is us, so explicitly do this

                        if ((!ep || !avail) and
                            (consul_state.flags & unsigned(ConsulState::Flags::BootstrapStateUpdatesProcessed))) {
                                // This is odd; someone deleted this from /nodes?
				if (trace) {
					SLog(ansifmt::bgcolor_magenta, "This node is not available according to consul. Updating State", ansifmt::reset, "\n");
				}

				schedule_consul_req(consul_state.get_req(consul_request::Type::AcquireNodeID), true);
                        }

                        avail = true;
                        ep    = tank_listen_ep;
                }

                const auto becoming_avail = ep && avail;

                node->available_ = avail;
                node->ep         = ep;

                if (node == cluster_state.local_node.ref) {
                        TANK_EXPECT(node->available());
                }

                if (becoming_avail != was_avail) {
                        if (trace) {
                                SLog(ansifmt::bgcolor_magenta, "Node ", node->id, "@", node->ep, " availability(", was_avail, ") => ", becoming_avail, ansifmt::reset, "\n");
                        }

                        rebuild_all_available_nodes = true;
                        dirty_nodes.insert(node);

			invalidate_replicated_partitions_from_peer_cache(node);

                        if (not becoming_avail) {
                                // so that we will check for a new leader if possible
                                for (auto p : node->replica_for) {
                                        dirty_partitions.insert(p);
                                }

                                // INVARIANT: no longer available nodes cannot be in ISRs
                                for (auto it = node->isr.list.next; it != &node->isr.list;) {
                                        auto isr_e = switch_list_entry(isr_entry, node_ll, it);
                                        auto part  = isr_e->partition();
                                        auto next  = it->next;

                                        if (part->cluster.leader.node == self) {
                                                // REPAIR: only the partition leader can repair its ISR
                                                TANK_EXPECT(isr_e->node() == node);

                                                if (trace) {
                                                        SLog(ansifmt::bgcolor_magenta, "Removed node(N/A) from ISR of ", part->owner->name(), "/", part->idx, ansifmt::reset, "\n");
                                                }

                                                isr_dispose(isr_e);
                                                persist_isr(part, __LINE__);
                                        } else if (trace) {
                                                SLog(ansifmt::bgcolor_magenta, "Node is no longer available, but will not remove from ISR -- only partition leader can remove it", ansifmt::reset, "\n");
                                        }

                                        it = next;
                                }
                        } else if (leader_self) {
                                // if we are the cluster leader, and a new/unknown node becomes available, we may want to use it to satisfy RF of partitions
                                // so we will, for now, make them all dirty
                                // TODO: this is not optimal but it will need to do
                                track_all_partitions_dirty = true;
                        }
                }
        }

        if (rebuild_all_available_nodes) {
                // XXX: maybe in-place updates make more sense but how often are we going to rebuild?
                cluster_state.all_available_nodes.clear();

                for (auto n : cluster_state.all_nodes) {
                        if (n->available()) {
                                cluster_state.all_available_nodes.emplace_back(n);
                        }
                }
        }

#pragma mark Cluster Leader
        if (std::exchange(cluster_state.updates.cluster_leader.defined, false)) {
                auto n = cluster_state.updates.cluster_leader.nid;

                // INVARIANT: a cluster node must be a valid available() node
                if (n) {
                        auto node = cluster_state.find_node(n);

                        if (!node) {
                                // FILTER:
                                if (trace) {
                                        SLog("CLUSTER_LEADER: Cluster Leader cannot change to ", n, " because node is not defined\n");
                                }

                                n = 0;
                        } else if (!node->available()) {
                                // FILTER:
                                if (trace) {
                                        SLog("CLUSTER_LEADER: Cluster Leader cannot change to ", n, " because node is not available\n");
                                }

                                n = 0;
                        }
                }

                if (auto p = cluster_state.leader_id; p != n) {
                        if (trace) {
                                SLog(ansifmt::bgcolor_magenta, "CLUSTER_LEADER: Cluster Leader update from ", p, " => ", n, 
					"( this node:", cluster_state.local_node.id, ")", ansifmt::reset, "\n");
                        }

                        cluster_state.leader_id = n;
                        if (n == cluster_state.local_node.id) {
                                promoted_to_cluster_leader = true;
                                leader_self                = true;
                                // when we become the cluster leader, we need to reconsider everything
                                track_all_partitions_dirty = true;

                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "CLUSTER_LEADER: This node PROMOTED to cluster leader", ansifmt::reset, "\n");
                                }

                        } else if (p == cluster_state.local_node.id) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "CLUSTER_LEADER: This node DEMOTED from cluster leader", ansifmt::reset, "\n");
                                }
                        }
                }

                if (!n) {
                        // no cluster leader? take over if we can
                        if (consul_state.bootstrap_state_updates_processed()) {
                                if (trace) {
                                        SLog("Will try to assume Cluster Leadership ASAP\n");
                                }

                                try_become_cluster_leader(__LINE__);
                        } else if (trace) {
                                SLog("Cannot try_become_cluster_leader() because bootstrap updates not processed\n");
                        }
                }
        }

        if (track_all_partitions_dirty) {
                if (trace) {
                        SLog(ansifmt::bgcolor_magenta, "Will track all partitions as dirty", ansifmt::reset, "\n");
                }

                for (auto &it : topics) {
                        auto t = it.second.get();
                        auto l = t->partitions_;

                        if (!l) {
                                continue;
                        }

                        for (auto &it : *l) {
                                dirty_partitions.insert(it.get());
                        }
                }
        }

#pragma mark TOPICS
        for (auto &it : cluster_state.updates.tm) {
                auto t     = it.first;
                auto state = it.second.get();

#if 0
                if (trace && (state->state.defined || state->total_enabled.defined || state->rf.defined)) {
                        SLog("Topic ", t->name(), " state{defined:", state->state.defined, ", value:", state->state.value,
                             "} total_enabled{defined:", state->total_enabled.defined, ", value:", state->total_enabled.value,
                             "} rf{defined:", state->rf.defined, ", value:", state->rf.value, "}\n");
                }
#endif

                if (state->state.defined) {
                        if (t->enabled != state->state.value) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "TOPICS:Switched state of ", t->name(), " from ", t->enabled, " to ", state->state.value, ansifmt::reset, "\n");
                                }

                                t->enabled = state->state.value;

                                dirty_topics.insert(t);

                                if (auto l = t->partitions_) {
                                        for (auto &it : *l) {
						auto *const p = it.get();

                                                p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GC_ISR);
                                                dirty_partitions.insert(p);
						invalidate_replicated_partitions_from_peer_cache_by_partition(p);
                                        }
                                }
                        }
                }

                if (state->total_enabled.defined) {
                        if (const auto cur = t->total_enabled_partitions; cur != state->total_enabled.value) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "TOPICS:For ", t->name(), " total_enabled_partitions(", cur, ") => ", state->total_enabled.value, ansifmt::reset, "\n");
                                }

                                if (const auto update = state->total_enabled.value; update < cur) {
                                        for (auto i = update; i < cur; ++i) {
                                                auto p = t->partitions_->at(i).get();

                                                TANK_EXPECT(p);
                                                p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GC_ISR);
                                                dirty_partitions.insert(p);
						invalidate_replicated_partitions_from_peer_cache_by_partition(p);
                                        }
                                } else if (update > cur) {
                                        for (auto i = cur; i < update; ++i) {
                                                auto *const p = t->partitions_->at(i).get();

                                                TANK_EXPECT(p);
						p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GC_ISR);
                                                dirty_partitions.insert(p);
						invalidate_replicated_partitions_from_peer_cache_by_partition(p);
                                        }
                                }

                                t->total_enabled_partitions = state->total_enabled.value;
                        }
                }

                if (state->rf.defined) {
                        if (t->cluster.rf_ != state->rf.value) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "TOPICS:For ", t->name(), " RF(", t->cluster.rf_, ") => ", state->rf.value, ansifmt::reset, "\n");
                                }

                                t->cluster.rf_ = state->rf.value;

                                if (auto l = t->partitions_) {
                                        for (auto &it : *l) {
                                                dirty_partitions.insert(it.get());
                                        }
                                }
                        }
                }
        }

	// XXX: order we reconcile partition updates is important
	// (replicas first, leader second, ISR third)
	// Notice that we *only* cleanup ISR (see GC_ISR) after we have
	// applied partitions state because we need to know who the leader is, and we process
	// leadership state update after process topology updates.
#pragma mark PARTITIONS
        for (auto &it : cluster_state.updates.pm) {
                auto p     = it.first;
                auto state = it.second.get();
                bool dirty{false};

#if 0
                if (trace && (state->leader.defined || state->replicas.updated || state->isr_update)) {
                        SLog("Partition ", p->owner->name(), "/", p->idx,
                             " leader{defined:", state->leader.defined, ", id:", state->leader.id,
                             "} replicas:{updated:", state->replicas.updated,
                             ", nodes:", state->replicas.nodes.size(),
                             "} isr_updates:", state->isr_update ? state->isr_update->size() : 0, "\n");
                }
#endif


#pragma mark PARTITION/REPLICAS
                if (std::exchange(state->replicas.updated, false)) {
                        // reconcile RS
                        std::vector<cluster_node *> new_set;
                        const auto &                updates = state->replicas.nodes;
                        size_t                      ui = 0, ei = 0;
                        const auto                  usize = updates.size();
                        const auto                  esize = p->cluster.replicas.nodes.size();

			if (trace) {
				SLog("REPLICAS set was updated\n");
			}

                        while (ui < usize && ei < esize) {
                                auto un = updates[ui];
                                auto en = p->cluster.replicas.nodes[ei];

                                if (un->id == en->id) {
                                        if (trace) {
                                                SLog("RS:Retaining replica of ", p->owner->name(), "/", p->idx, " ", un->id, "@", un->ep, "\n");
                                        }

                                        new_set.emplace_back(un);

                                        ++ui;
                                        ++ei;
                                } else if (en->id < un->id) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_magenta, "RS:Dropping replica of ", p->owner->name(), "/", p->idx, " ", en->id, "@", en->ep, ansifmt::reset, "\n");
                                        }

                                        if (en == self && p->log_open()) {
                                                if (trace) {
                                                        SLog(ansifmt::bgcolor_magenta, "RS:Also closing log", ansifmt::reset, "\n");
                                                }

                                                close_partition_log(p);
                                        }

                                        nodes_replicas_updates.emplace_back(std::make_pair(en, std::make_pair(p, false)));
                                        dirty = true;
                                        ++ei;
                                } else {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_magenta, "RS:New replica for ", p->owner->name(), "/", p->idx, " ", un->id, "@", un->ep, ansifmt::reset, "\n");
                                        }

                                        new_set.emplace_back(un);
                                        nodes_replicas_updates.emplace_back(std::make_pair(un, std::make_pair(p, true)));

                                        dirty = true;
                                        ++ui;
                                }
                        }

                        while (ui < usize) {
                                auto n = updates[ui++];

                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "RS:New replica for ", p->owner->name(), "/", p->idx, " ", n->id, "@", n->ep, ansifmt::reset, "\n");
                                }

                                // force ISR cleanup later
                                dirty = true;

                                new_set.emplace_back(n);
                                nodes_replicas_updates.emplace_back(std::make_pair(n, std::make_pair(p, true)));
                        }

                        while (ei < esize) {
                                auto en = p->cluster.replicas.nodes[ei++];

                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "RS:Dropping replica of ", p->owner->name(), "/", p->idx, " ", en->id, "@", en->ep, ansifmt::reset, "\n");
                                }

                                if (en == self && p->log_open()) {
					if (trace) {
						SLog(ansifmt::bgcolor_magenta, "RS:Also closing log", ansifmt::reset, "\n");
					}

                                        close_partition_log(p);
                                }

                                nodes_replicas_updates.emplace_back(std::make_pair(en, std::make_pair(p, false)));
                                dirty = true;
                        }

                        TANK_EXPECT(std::is_sorted(new_set.begin(), new_set.end(), [](const auto a, const auto b) noexcept { return a->id < b->id; }));

                        // we also need to track as dirty if no nodes are assigned as replicas e.g when
                        // the partition is first defined
                        if (dirty || 0 == usize) {
				p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GC_ISR);

                                p->cluster.replicas.nodes   = new_set;
                                dirty_partitions.insert(p);

				invalidate_replicated_partitions_from_peer_cache_by_partition(p);
                        }
                }

#pragma mark PARTITION/LEADER
                if (state->leader.defined) {
                        auto l = state->leader.id ? cluster_state.find_node(state->leader.id) : nullptr;

                        // FILTER:
                        // We cannot allow leaders that is not in this partition's RS
			// If we do(TODO:) process_peer_consume_resp() will get a bounary check fault
                        if (l && l != p->cluster.leader.node && !p->cluster.replicas.count(l->id)) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_magenta, "PARTITION-FILTER: For ", p->owner->name(), "/", p->idx, " will *ignore* leadership promotion to ", l->id, "@", l->ep, 
						": not in the RS. Will assume <NO LEADER> instead", ansifmt::reset, "\n");
                                }

                                l = nullptr;
                        }

                        if (auto prev = p->cluster.leader.node; prev != l) {
                                if (trace) {
                                        if (prev) {
                                                if (l) {
                                                        SLog(ansifmt::bgcolor_magenta, "PARTITION:For ", p->owner->name(), "/", p->idx, " leader changed from ",
                                                             prev->id, "@", prev->ep, " to ", l->id, "@", l->ep, ansifmt::reset, "\n");
                                                } else {
                                                        SLog(ansifmt::bgcolor_magenta, "PARTITION:For ", p->owner->name(), "/", p->idx, " leader changed from ",
                                                             prev->id, "@", prev->ep, " to <NO LEADER>", ansifmt::reset, "\n");
                                                }
                                        } else if (l) {
                                                SLog(ansifmt::bgcolor_magenta, "PARTITION:For ", p->owner->name(), "/", p->idx, " leader changed from <NO LEADER> to ",
                                                     l->id, "@", l->ep, ansifmt::reset, "\n") ;
                                        }
                                }

                                if (prev) {
                                        // update prev->leadership
					invalidate_replicated_partitions_from_peer_cache(prev);

                                        TANK_EXPECT(!p->cluster.leader.leadership_ll.empty());
                                        TANK_EXPECT(prev->leadership.partitions_cnt);

                                        p->cluster.leader.leadership_ll.detach_and_reset();
                                        p->cluster.leader.node = nullptr;

                                        prev->leadership.partitions_cnt--;
                                        prev->leadership.dirty = true;
                                        prev->leadership.local_replication_list.reset(nullptr);

                                        if (trace && prev == self) {
                                                SLog(ansifmt::bgcolor_magenta, "Resigned leadership of partition; will wakeup_all_consumers()", ansifmt::reset, "\n");
                                        }

                                        wakeup_all_consumers(p);
                                }

                                if (l) {
                                        // update l->leadership
					invalidate_replicated_partitions_from_peer_cache(l);

                                        TANK_EXPECT(p->cluster.leader.leadership_ll.empty());

                                        p->cluster.leader.node = l;
                                        p->cluster.leader.leadership_ll.reset();

                                        l->leadership.partitions_cnt++;
                                        l->leadership.dirty = true;
                                        l->leadership.local_replication_list.reset(nullptr);
                                        l->leadership.list.push_back(&p->cluster.leader.leadership_ll);

                                        if (leader_self and p->enabled() and not l->available()) {
                                                // INVARIANT: a partition leader must be available
                                                // someone is likely messing with us, or this is bogus consul state
						//
						// XXX: we can't use ForceChooseLeader because it is not inconceivable that
						// we will also require an RS update for this 'session', which means that
						// we won't get to consider a new leader (see SEMANTICS)
						//
						// To that end, we will need to move this functionality into gen_partition_nodes_updates()
						// It means it will be somewhat more expensive there but it is otherwise necessary
                                                if (trace) {
                                                        SLog(ansifmt::bgcolor_magenta, "PARTITION:For partition ", p->owner->name(), "/", p->idx, " new leader ", l->id, "@", l->ep,
                                                             " is N/A. We will choose another as soon as possible", ansifmt::reset, "\n");
                                                }
                                        } else if (l == self) {
                                                if (trace) {
                                                        SLog(ansifmt::bgcolor_magenta, "PARTITION:Became leader of partition ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                                                }

                                                // When we assume partition leadership, we need to always GC the partition's ISR
                                                p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GC_ISR);

                                                // IMPORTANT:
                                                // wee need to immediately bump hwmark here to last assigned
                                                // otherewise consume requests may fail with a boundary check fault
						//
						// UPDATE: 2021-10-14 
						// _do_ we really to do that?  maybe we don't
						// we 'd like to _defer_ this for as long as possible 
						// especially considering that open_partition_log() will set_hwmark()
#if 0
                                                auto log = partition_log(p);

                                                if (trace) {
                                                        SLog(ansifmt::bold, ansifmt::bgcolor_magenta, "**Bumping HW from ", partition_hwmark(p),
                                                             " to ", log->lastAssignedSeqNum, ansifmt::reset, "\n");
                                                }

                                                set_hwmark(p, log->lastAssignedSeqNum);
#endif

                                        }
                                } else if (p->require_leader()) {
                                        if (trace) {
                                                SLog(ansifmt::color_magenta, "PARTITION:New leader of ", p->owner->name(), "/", p->idx, " is unknown. Will need to select another as soon as possible", ansifmt::reset, "\n");
                                        }
                                }

                                // we will track partition as dirty and we will check
                                // for replication streams later
                                dirty = true;
                        }
                }



#pragma mark PARTITION/ISR
                if (const auto v = state->isr_update.get()) {
                        std::unordered_set<cluster_node *> set;

                        if (trace) {
                                SLog("ISRs list of ", p->owner->name(), "/", p->idx, ":", values_repr(v->data(), v->size()), "\n");
                        }

                        for (const auto id : *v) {
                                // FILTER: only *available* nodes that are also in the RS may be included in the ISR
                                if (auto node = cluster_state.find_node(id); node && node->available() && p->cluster.replicas.count(id)) {
                                        set.insert(node);
                                }
                        }

                        for (auto it = p->cluster.isr.list.next; it != &p->cluster.isr.list;) {
                                auto next  = it->next;
                                auto isr_e = switch_list_entry(isr_entry, partition_ll, it);
                                auto node  = isr_e->node();

                                if (set.erase(node)) {
                                        // retain
                                } else {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_magenta, "ISR:Removing ", node->id, "@", node->ep,
                                                     " from ISR of ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                                        }

                                        isr_dispose(isr_e);
                                        dirty = true;
                                }

                                it = next;
                        }

                        if (!set.empty()) {
#ifdef HWM_UPDATE_BASED_ON_ACKS
                                auto next = p->cluster.isr.next_partition_isr_node_id();

                                for (auto node : set) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_magenta, "ISR:Adding ", node->id, "@", node->ep,
                                                     " to ISR of ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                                        }

                                        TANK_EXPECT(node);
                                        isr_bind(p, node, next++, __LINE__);
                                        dirty = true;
                                }
#else
                                for (auto node : set) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_magenta, "ISR:Adding ", node->id, "@", node->ep,
                                                     " to ISR of ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                                        }

                                        TANK_EXPECT(node);
                                        isr_bind(p, node, __LINE__);
                                        dirty = true;
                                }
#endif
                        }
                }

                if (dirty) {
                        TANK_EXPECT(p);
                        dirty_partitions.insert(p);
                }
        }

        if (trace) {
                SLog("dirty_partitions:", dirty_partitions.size(), ", leader_self = ", leader_self, "\n");
        }

#pragma mark DEFERRED UPDATES
        // deferred nodes replica_for updates
        // this is for performance and simplicity reasons (as opposed to updating affected nodes wehile reconciling partitions replicas ^^)
	// XXX: make sure those are applied before we run DEFERRED REPAIRS
        std::sort(nodes_replicas_updates.begin(), nodes_replicas_updates.end(), [](const auto &a, const auto &b) noexcept {
                return a.first < b.first;
        });

        for (const auto *p = nodes_replicas_updates.data(), *const e = p + nodes_replicas_updates.size(); p < e;) {
                auto n = p->first;

                v.clear();
                pv.clear();

                do {
                        v.emplace_back(p->second);
                } while (++p < e and p->first == n);

                std::sort(v.begin(), v.end(),
                          [](const auto &a, const auto &b) noexcept { return a.first < b.first; });

                const auto en = n->replica_for.size();
                const auto un = v.size();
                uint32_t   ei = 0, ui = 0;
                auto &     replica_for = n->replica_for;

                while (ei < en and ui < un) {
                        auto ep = replica_for[ei];
                        auto up = v[ui].first;

                        if (ep < up) {
                                // retain
                                pv.emplace_back(ep);
                                ++ei;
                        } else if (up < ep) {
                                if (v[ui].second) {
                                        if (trace) {
                                                SLog(">> Node ", n->id, "@", n->ep, " now replica of ", up->owner->name(), "/", up->idx, "\n");
                                        }

                                        pv.emplace_back(up);
                                }
                                ++ui;
                        } else {
                                if (!v[ui].second) {
                                        // drop
                                        if (trace) {
                                                SLog(">> Node ", n->id, "@", n->ep, " no longer replica of ", up->owner->name(), "/", up->idx, "\n");
                                        }
                                } else {
                                        // retain
                                        pv.emplace_back(ep);
                                }

                                ++ei;
                                ++ui;
                        }
                }

                while (ei < en) {
                        pv.emplace_back(replica_for[ei++]);
                }

                while (ui < un) {
                        if (v[ui].second) {
                                auto up = v[ui].first;

                                if (trace) {
                                        SLog(">> Node ", n->id, "@", n->ep, " now replica of ", up->owner->name(), "/", up->idx, "\n");
                                }

                                pv.emplace_back(up);
                        }

                        ++ui;
                }

                replica_for = pv;

		TANK_EXPECT(std::is_sorted(replica_for.begin(), replica_for.end()));

                if (trace) {
                        SLog(">> Node ", n->id, "@", n->ep, " replica_for.size() = ", replica_for.size(), "\n");
                }
        }


        // now that we have reconciled consul state updates with inmemory state
        // we can repair or react to those based on wether this node is the cluster leader or leader for any of the dirty partitions
#pragma mark DEFERRED REPAIRS
        for (auto p : dirty_partitions) {
                auto       part_leader           = p->cluster.leader.node;
                const auto partition_leader_self = part_leader == self;
                const auto req_leader            = p->require_leader();

#pragma mark REPAIR_ISR
                if (p->enabled() and
                    (p->cluster.flags & unsigned(topic_partition::Cluster::Flags::GC_ISR))) {
                        // REPAIR: self either just became leader of this partition, or the RS of the partition was modified
                        // We are going to make sure that the RS is sane (we are the parttiion's leader)
			//
			// UPDATE:
			// It is important to GC the local ISR even if we are not the partition leader
			// This is because there is race-based edge case we need to account for
			// where a Produce request may fail with Insufficient Replicas.
			// DESCRIPTION:
			//   When we switched RF from 2 to 1, the cluster leader updated the partition RS
			// from 2 to 1 nodes and all nodes received and applied the updates.
			// However, the ISR update arrived *after* the RS update, so for the however many miliseconds
			// it took to receive the subsequent ISR update, the partition leader had
			// topic.cluster.rf_ = 1, partition.cluster.replicas.nodes.size = 1, partitionc.cluster.isr.size = <2>
			// so if while we are in this limbo state get a produce request, we get a produce request with ack == 0
			// we take that to mean that the client requires (ISR size) acks, rf = 2. However,
			// if (rf > topic->cluster.rf_f) is true so the request fails with InsufficientReplicas
			// (indeed, the ISR update arrived after a few ms after the produce request failed)
			//
			// This happens rarely, but it _does_ happen.
			// We could have the cluster leader to GC/persist ISR in the same update session
			// but 
			// 1. the cluster leader and the partition leader may be different nodes
			// 2. We should respect the semantics(only the partition leader can persist ISR)
			// 3. We can't guarantee logical order of updates will be respected
			//
			// To that end:
			// - *all* nodes will repair ISR, not just the partition leader
			// - when RF is updated we will make sure to GC ISR
			//
			// However, this is *STILL* an issue:
			// If we update RF, and we get a produce request *before* the cluster leader gets to process the RF
			// in order to generate RS updates for all affected partitions, at the time we get the prdouce request
			// topic.cluster.rf_ = 1, partition.cluster.replicas.nodes.size = 2, partitionc.cluster.isr.size = <2>
			// which means we din't get to GC ISR here locally. 
			// To that end, our topic::compute_required_peers_acks() we will need to account for topic RF as well.
			// 	
			//
			// ** We still can only persist iff we are the partition leader **
                        bool any{false};

                        if (trace && false) {
                                SLog(ansifmt::color_magenta, "Will need to GC ISR of ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                        }

                        for (auto it = p->cluster.isr.list.next; it != &p->cluster.isr.list;) {
                                auto isr_e = switch_list_entry(isr_entry, partition_ll, it);
                                auto next  = it->next;
                                auto node  = isr_e->node();

                                if (!node->available()) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_red, "REPAIR:Node ", node->id, "@", node->ep, " is in ", p->owner->name(), "/", p->idx,
                                                     " ISR, but node is not available. Will erase from ISR", ansifmt::reset, "\n");
                                        }

                                        isr_dispose(isr_e);
                                        any = true;
                                } else if (!p->cluster.replicas.count(node->id)) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_red, "REPAIR:Node ", node->id, "@", node->ep, " is in ", p->owner->name(), "/", p->idx,
                                                     " ISR, but node is not in RS. Will erase from ISR", ansifmt::reset, "\n");
                                        }

                                        isr_dispose(isr_e);
                                        any = true;
                                }

                                it = next;
                        }

                        if (any && partition_leader_self) {
                                // we *only* persist if we are the partition leader
                                persist_isr(p, __LINE__);
                        }
                }

                if (part_leader == self) {
                        // only the partition leader may update+persist ISR
                        if (not p->enabled()) {
                                // REPAIR:
                                // (only partition leader may update ISR on consul)
                                if (not p->cluster.isr.list.empty()) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_red, "REPAIR:Partition ", p->owner->name(), "/", p->idx,
                                                     " is disabled and this node is the partition leader: will reset ISR of size ",
                                                     p->cluster.isr.size(), ansifmt::reset, "\n");
                                        }

                                        while (not p->cluster.isr.list.empty()) {
                                                auto isr_e = switch_list_entry(isr_entry, partition_ll, p->cluster.isr.list.next);

                                                isr_dispose(isr_e);
                                        }

                                        persist_isr(p, __LINE__);
                                }
                        } else {
                                if (req_leader and
                                    p->cluster.replicas.count(self->id) and
                                    not p->cluster.isr.find(self)) {
                                        // REPAIR: partition requires a leader, we are that leader, we are in the RS
                                        // but for some reason we are not in the ISR
                                        // Looks like the cluster leader promoted us to the partition leader
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_red, "REPAIR:Partition ", p->owner->name(), "/", p->idx,
                                                     " requires a leader, and leader is self, but leader is missing from the ISR and is in the RS. Will add self to it", ansifmt::reset, "\n");
                                        }

                                        isr_bind(p, self, __LINE__);
                                        persist_isr(p, __LINE__);
                                }
                        }
                }

#pragma mark STREAMS
                auto *const peer = p->enabled() and
                                           p->cluster.leader.node and
                                           not partition_leader_self and
                                           p->cluster.leader.node->available() and
                                           self->is_replica_for(p)
                                       ? p->cluster.leader.node
                                       : nullptr;

                if (auto stream = p->cluster.rs) {
                        auto src = stream->src;

                        if (src != peer) {
                                if (trace) {
                                        if (peer) {
                                                SLog(ansifmt::bgcolor_red, "STREAMS: Stream source of ", p->owner->name(), "/", p->idx,
                                                     " changed from ", src->id, "@", src->ep, " to ", peer->id, "@", peer->ep, ansifmt::reset, "\n");
                                        } else {
                                                SLog(ansifmt::bgcolor_red, "STREAMS: Stream source of ", p->owner->name(), "/", p->idx,
                                                     " changed from ", src->id, "@", src->ep, " to <NO NODE>", ansifmt::reset, "\n");
                                        }
                                }

                                stream_stop.emplace_back(p, src);

                                if (peer) {
                                        if (trace) {
                                                SLog(ansifmt::bgcolor_red, "STREAMS: Will (re)start replication of ", p->owner->name(), "/", p->idx,
                                                     " from ", peer->id, "@", peer->ep, ansifmt::reset, "\n");
                                        }

                                        stream_start.emplace_back(p, peer);
                                }
                        }
                } else if (peer) {
                        if (trace) {
                                SLog(ansifmt::bgcolor_red, "STREAMS: This node is a replica of ", p->owner->name(), "/", p->idx,
                                     ", but no active rep.stream. Will replicate from ", peer->id, "@", peer->ep, ansifmt::reset, "\n");
                        }

                        stream_start.emplace_back(p, peer);
                }

                // IMPORTANT:
                p->cluster.flags &= ~(unsigned(topic_partition::Cluster::Flags::GC_ISR));

                if (!p->enabled()) {
                        if (p->log_open()) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_red, "*CLOSING* partition ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                                }

                                close_partition_log(p);
                        }

                        if (p->safe_to_reset()) {
                                if (trace) {
                                        SLog(ansifmt::bgcolor_red, "*RESETTING* partition ", p->owner->name(), "/", p->idx, ansifmt::reset, "\n");
                                }

                                reset_partition_log(p);
                        }
                }

                if (leader_self) {
                        cluster_partitions_dirty.emplace_back(p);
                }
        }

	conclude_bootstrap_updates();

#pragma mark FINALIZE
        if (trace) {
                SLog("cluster_partitions_dirty: ", cluster_partitions_dirty.size(),
                     ", stream_stop: ", stream_stop.size(),
                     ", stream_start:", stream_start.size(),
                     ", promoted_to_cluster_leader: ", promoted_to_cluster_leader, "\n");
        }

        if (not stream_start.empty() or not stream_stop.empty()) {
                replicate_partitions(&stream_start, &stream_stop);
        }

        if (promoted_to_cluster_leader) {
                // TODO: do whatever
        }

        cluster_state.updates.nodes.clear();
        cluster_state.updates.pm.clear();
        cluster_state.updates.tm.clear();
	cluster_state.updates.a.reuse();

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, ansifmt::bgcolor_blue, "::END CLUSTER STATE UPDATE::", ansifmt::reset, " took ", duration_repr(Timings::Microseconds::Since(before)), "\n");
        }
}

void Service::apply_deferred_updates() {
        enum {
                trace = false,
        };

        if (not cluster_aware()) {
                for (auto p : isr_dirty_list) {
                        p->cluster.flags &= ~unsigned(topic_partition::Cluster::Flags::ISRDirty);
                }

                isr_dirty_list.clear();
                return;
        }

        if (not consul_state.reg_completed()) {
                if (trace) {
                        SLog("Registration not completed\n");
                }

                return;
        }

        auto &reduced    = reusable.reduced;
        auto &expanded   = reusable.expanded;
        auto &promotions = reusable.promotions;

        reduced.clear();
        expanded.clear();
        promotions.clear();

        // other than applying deferred updates it will also potentially add
        // partitions into cluster_partitions_dirty
        apply_cluster_state_updates();

        if (const auto n = cluster_partitions_dirty.size()) {
                if (trace) {
                        SLog(n, " cluster partitions dirty\n");
                }

                gen_partition_nodes_updates(cluster_partitions_dirty.data(), n,
                                            &reduced,
                                            &expanded,
                                            &promotions);

                cluster_partitions_dirty.clear();
        }

        if (trace) {
                SLog("reduced = ", reduced.size(),
                     ", expanded = ", expanded.size(),
                     ", promotions = ", promotions.size(),
                     ", isr_dirty_list = ", isr_dirty_list.size(), "\n");
        }

        if (not reduced.empty() or
            not expanded.empty() or
            not promotions.empty() or
            not isr_dirty_list.empty()) {
                // we need to schedule 1 or more transactions for updates
                // XXX: what happens if the request fails?
		if (trace) {
			SLog("Will need a topology update transaction\n");
                }

                if (auto req = schedule_topology_update_tx(reduced,
                                                           expanded,
                                                           promotions,
                                                           isr_dirty_list.data(),
                                                           isr_dirty_list.size())) {
                        schedule_consul_req(req, true);
                }
        }

        isr_dirty_list.clear();
        cluster_partitions_dirty.clear();
}

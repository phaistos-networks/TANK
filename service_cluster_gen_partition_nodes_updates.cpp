#include "service_common.h"

// See service_cluster_apply_state_updates.cpp semantics
// We will either generate an RS update OR a leader update for the same partition. It is not allowed to
// generate both an RS and a leader update event for the same partition. See semantics
void Service::gen_partition_nodes_updates(topic_partition **list, const size_t n,
                                          std::vector<NodesPartitionsUpdates::reduced_rs>           *reduced,
                                          std::vector<NodesPartitionsUpdates::expanded_rs>          *expanded,
                                          std::vector<NodesPartitionsUpdates::leadership_promotion> *promotions) {
        enum {
                trace = false,
        };
        TANK_EXPECT(list || 0 == n);
        TANK_EXPECT(reduced);
        TANK_EXPECT(expanded);
        TANK_EXPECT(promotions);
        TANK_EXPECT(cluster_state.leader_self()); // only the cluster leader may update partitions (RS, leader)

        reduced->clear();
        expanded->clear();
        promotions->clear();

        if (0 == n or not cluster_aware()) {
                return;
        }

        if (trace) {
                puts("\n");
                SLog(ansifmt::bold, ansifmt::color_red, ansifmt::inverse, "Will consider ", n, " partitions, all_nodes = ", cluster_state.all_nodes.size(),
                     ", all_available_nodes = ", cluster_state.all_available_nodes.size(), ansifmt::reset, "\n");
        }

        struct tracked_node final {
                cluster_node *n;
                struct {
                        size_t leader_for;
                        size_t replica_for;
                } counts;
        };

        const auto tracked_node_replicas_cmp = [](const tracked_node *a, const tracked_node *b) noexcept {
                if (b->counts.replica_for < a->counts.replica_for) {
                        return true;
                } else if (b->counts.replica_for > a->counts.replica_for) {
                        return false;
                } else {
                        return b->counts.leader_for < a->counts.leader_for;
                }
        };

        // TODO: reuse
        simple_allocator                                                                                      a;
        std::priority_queue<tracked_node *, std::vector<tracked_node *>, decltype(tracked_node_replicas_cmp)> pq_for_replicas{tracked_node_replicas_cmp};
        static constexpr size_t                                                                               leadership_threshold = std::numeric_limits<size_t>::max();
        static constexpr size_t                                                                               replica_threshold    = std::numeric_limits<size_t>::max();
        robin_hood::unordered_map<cluster_node *, uint16_t>                                                   map;
        robin_hood::unordered_map<topic_partition *, uint16_t>                                                map2, map3;
        std::vector<tracked_node *>                                                                           deferred_push;
        std::vector<topic_partition *>                                                                        choose_leader;
        std::unordered_set<nodeid_t>                                                                          set;
        robin_hood::unordered_map<nodeid_t, tracked_node *>                                                   nodes_map;

        // we need to _first_ release replicas
        // and _then try to bind replicas because we may haven't been able to do that before
        //
        // Note that we can either update the RS or the leaer of a partition here; not both
        for (size_t i{0}; i < n; ++i) {
                auto p = list[i];
                TANK_EXPECT(p);
                auto topic = p->owner;
                TANK_EXPECT(topic);

                if (not p->defined()) {
                        // XXX:
                        // This is important.
                        // We cannot update a disabled partition's replicas, because doing so would inadvertedely create it even if it was deleted
                        // i.e if we DELETE the topic or the topic/partition, the cluster leader will reset its RS and then try to persist that
                        // but this would result in topic/partition to be created again where the value would be the empty RS
                        //
                        // Maybe this would make sense in some situations?
                        // For now, we are explicitly ignoring it here for RS updates
                        //
                        // !! Notice that we are using topic_partition::defined(), not topic_partition::enabled() !!
                        if (trace) {
                                SLog("Will ", ansifmt::bold, "*ignore*", ansifmt::reset, " partition ", p->owner->name(), "/", p->idx, " because it is disabled. We do not update diabled partition's RS\n");
                        }

                        continue;
                }

                const auto effective_rf = p->required_replicas();
                const auto replicas_cnt = p->cluster.replicas.nodes.size();

                // IMPORTANT: reset it here
                p->cluster.flags &= ~unsigned(topic_partition::Cluster::Flags::GeneratedUpdate);

                if (replicas_cnt <= effective_rf) {
                        continue;
                }

                if (trace) {
                        SLog("Release replicas for ", p->owner->name(), "/", p->idx, ", replicas_cnt = ", replicas_cnt, " => effective_rf = ", effective_rf, "\n");
                }

                // TODO: maybe prefer most overloaded replicas over other when deciding which replicas to 'release'?
                // i.e sort the [0, replicas_cnt) nodes in p->cluster.replicas.nodes
                // by load factor and then iterate but will need re-sort by id again later
                for (size_t i{effective_rf}; i < replicas_cnt; ++i) {
                        auto n = p->cluster.replicas.nodes[i];

                        if (trace) {
                                SLog(ansifmt::color_red, "Will release node ", n->id, "@", n->ep, ansifmt::reset, "\n");
                        }

                        map.emplace(n, 0).first->second++;
                }

                map2.emplace(p, effective_rf);
                reduced->emplace_back(NodesPartitionsUpdates::reduced_rs{.p        = p,
                                                                         .new_size = effective_rf});

                p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GeneratedUpdate);
        }

        // now populate the PQs
        for (auto n : cluster_state.all_available_nodes) {
                TANK_EXPECT(n->available());

                const auto lcnt = n->leadership.partitions_cnt;

                if (lcnt >= leadership_threshold) {
                        continue;
                }

                auto rcnt = n->replica_for.size();

                if (const auto it = map.find(n); it != map.end()) {
                        const auto v = it->second;

                        if (trace) {
                                SLog("Released ", v, " partitions from RS of ", n->id, "@", n->ep, ", rcnt = ", rcnt, "\n");
                        }

                        TANK_EXPECT(rcnt >= v);
                        rcnt -= v;
                }

                if (rcnt >= replica_threshold) {
                        continue;
                }

                // new tracked node
                auto tn = static_cast<tracked_node *>(a.Alloc(sizeof(tracked_node)));

                tn->n                  = n;
                tn->counts.leader_for  = static_cast<uint32_t>(lcnt);
                tn->counts.replica_for = static_cast<uint32_t>(rcnt);

                pq_for_replicas.push(tn);
                nodes_map.emplace(n->id, tn);
        }

        if (trace) {
                SLog("pq_for_replicas.size() = ", pq_for_replicas.size(), "\n");
        }

        for (size_t i{0}; i < n; ++i) {
                auto p = list[i];

                if (p->cluster.flags & unsigned(topic_partition::Cluster::Flags::GeneratedUpdate)) {
                        // we already have an update(i.e RS reduction).
                        // See SEMANTICS
                        continue;
                }

                const auto req_leader = p->require_leader();
                auto       topic      = p->owner;
                auto       leader     = p->cluster.leader.node;
                const bool leader_av  = leader && leader->available();
                TANK_EXPECT(topic);
                const auto effective_rf = p->required_replicas();
                const auto replicas_cnt = p->cluster.replicas.nodes.size();
                TANK_EXPECT(std::is_sorted(p->cluster.replicas.nodes.begin(), p->cluster.replicas.nodes.end(), [](const auto a, const auto b) noexcept { return a->id < b->id; }));

                if (trace) {
                        if (leader) {
                                SLog(ansifmt::color_magenta, ansifmt::inverse, "Considering partition ", topic->name(), "/", p->idx,
                                     ", leader_av ", leader_av,
                                     ", topic.effective_rf = ", effective_rf, ", replicas_cnt = ", replicas_cnt,
                                     ", leader = ", leader->id, "@", leader->ep, " (avail:", leader->available(), "), req_leader = ", req_leader, ansifmt::reset, "\n");
                        } else {
                                SLog(ansifmt::color_magenta, ansifmt::inverse, "Considering partition ", topic->name(), "/", p->idx,
                                     ", leader_av ", leader_av,
                                     ", topic.effective_rf = ", effective_rf, ", replicas_cnt = ", replicas_cnt, ", no leader"_s32, ", req_leader = ", req_leader, ", enabled = ", p->enabled(), ansifmt::reset, "\n");
                        }

                        for (const auto n : p->cluster.replicas.nodes) {
                                SLog("Replica:", n->id, "@", n->ep, "\n");
                        }
                }

                // We are only considering a replicas update if the partition is enabled
                // See above for rationale ^^
                if (p->enabled() and replicas_cnt < effective_rf) {
                        const auto n = effective_rf - replicas_cnt;

                        if (trace) {
                                SLog("Need ", n, " more replicas: ", replicas_cnt, " => ", effective_rf, "\n");
                        }

                        for (size_t collected{0}; not pq_for_replicas.empty() and collected < n;) {
                                auto it = pq_for_replicas.top();
                                auto n  = it->n;

                                pq_for_replicas.pop();

                                if (n->is_replica_for(p)) {
                                        // already replica for that node turns out
                                        if (trace) {
                                                SLog("Ignoring node ", n->id, "@", n->ep, " because already replica for partition\n");
                                        }

                                        deferred_push.emplace_back(it);
                                } else {
                                        if (trace) {
                                                SLog("Assigning ", n->id, "@", n->ep, " as a replica of ", p->owner->name(), "/", p->idx, "\n");
                                        }

                                        p->cluster.replicas.nodes.emplace_back(it->n);

                                        if (++it->counts.replica_for > replica_threshold) {
                                                // too many
                                        } else {
                                                // so that we won't pick it again in this loop
                                                // a node can't be a replica > 1 for the same partition
                                                deferred_push.emplace_back(it);
                                                ++collected;
                                        }
                                }
                        }

                        while (not deferred_push.empty()) {
                                pq_for_replicas.push(deferred_push.back());
                                deferred_push.pop_back();
                        }

                        if (const auto now = p->cluster.replicas.nodes.size(); now > replicas_cnt) {
                                if (trace) {
                                        SLog(ansifmt::color_green, "Assigned ", now - replicas_cnt,
                                             " new replicas, now ", p->cluster.replicas.nodes.size(), " out of effective_rf = ", effective_rf, ansifmt::reset, "\n");
                                }

                                expanded->emplace_back(NodesPartitionsUpdates::expanded_rs{
                                    .p             = p,
                                    .original_size = static_cast<uint16_t>(replicas_cnt),
                                });

                                p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::GeneratedUpdate);
                                map3.emplace(p, static_cast<uint16_t>(replicas_cnt));
                        }
                }

                // Schedule for leader update by placing into choose_leader()
                // we need to choose_leader() after we have updated RS for every partition involved here
                if (not req_leader and leader) {
                        // if we no longer need a leader(why?) and we have an assigned leader, let it go
                        if (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx, " has a leader; does not need a leader\n");
                        }

                        promotions->emplace_back(NodesPartitionsUpdates::leadership_promotion{
                            .p = p,
                            .n = nullptr,
                        });
                        continue;
                }

                if (req_leader and not leader_av) {
                        // partition requires a leader, but we have no *available* leader
                        if (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx, " need leader; has no leader\n");
                        }

                        choose_leader.emplace_back(p);
                        continue;
                }

                if (req_leader and leader and not p->cluster.replicas.count(leader->id, replicas_cnt)) {
                        // require a leader, but leader is not in the replicas list
                        // we will need to try to select another leader
                        //
                        // This is not expensive; count() will perform a binary search among a few nodes anyway
                        if (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx,
                                     "'s leader is ", leader->id, "@", leader->ep, ", but is missing from the RS. Will choose another\n");
                        }

                        choose_leader.emplace_back(p);
                }
        }

        if (trace) {
                SLog("choose_leader.size() = ", choose_leader.size(), "\n");
        }

        for (auto p : choose_leader) {
                tracked_node *best{nullptr};

                if (p->cluster.flags & unsigned(topic_partition::Cluster::Flags::GeneratedUpdate)) {
                        // we already generated an RS update
                        // We cannot generate both an RS and a Leaders update in the same session
                        // see SEMANTICS
                        continue;
                }

                auto   leader = p->cluster.leader.node;
                bool   leader_from_replicas{false};
                size_t span;

                // See above ^^
                // we will try to select from the RS
                if (const auto it = map3.find(p); it == map3.end()) {
                        span = p->cluster.replicas.nodes.size();
                } else {
                        span = it->second;
                }

                if (trace) {
                        SLog("Partition ", p->owner->name(), "/", p->idx,
                             " leadership election. leader:", leader ? leader->id : 0, ", leader avail:", leader ? leader->available() : false,
                             ", isr.size = ", p->cluster.isr.size(), ", RS.size = ", p->cluster.replicas.nodes.size(), "\n");

                        if (leader and not p->cluster.replicas.count(leader->id, span)) {
                                SLog("Cluster is not in the RS\n");
                        }

                        for (const auto n : p->cluster.replicas.nodes) {
                                SLog("Replica:", n->id, "@", n->ep, "\n");
                        }
                }

                // XXX:
                // If we have RF = 1 for topic T1( so that node N1
                // is the only node in the partition's RS, and also its leader)
                // and then we stop that node, and then we start another N2.
                //
                // That topic will no longer be available because RF == 1 and
                // N2 wasn't the single node int he partition's RS.
                //
                // However, if we set RF = 2 before we start N2, then N2 will
                // become the cluster leader and because it knows that RS.size == 1
                // and RF == 2, it will add itself in the RS for that partition. The probem is that
                // because now RS.size = 2 and previous leader(1) is not in the RS, we will try to
                // select a leader among the RS, so we will wind up selecting N2. But N2 doesn't have
                // the data N1 had.
                //		-- :We shouldn't be allowed to promote N2 as a leader --
                //
                // - We should ONLY be able to select a leader from the ISR.
                // - We should be able to select from the RS only if
                //	- no leader (regardless if available()) is assigned to the partition
                // 	- leader is NOT in the partition RS
                if (not leader) {
                        // we have no leader
                        // choose among the replicas
                        if (trace) {
                                SLog("Leader is not available. Will select one from the RS\n");
                        }

                        leader_from_replicas = true;
                } else if (not p->cluster.replicas.count(leader->id, span)) {
                        // we have a leader, but for some odd reason it is missing from the replicas
                        // choose among the replicas
                        if (trace) {
                                SLog("Have leader ", leader->id, "@", leader->ep, ", but leader is missing from ", p->owner->name(), "/", p->idx, " RS. Will select another from RS\n");
                        }

                        leader_from_replicas = true;
                }

                if (leader_from_replicas) {
                        for (size_t i{0}; i < span; ++i) {
                                auto       n   = p->cluster.replicas.nodes[i];
                                const auto nit = nodes_map.find(n->id);

                                if (nit == nodes_map.end()) {
                                        continue;
                                }

                                auto ctx = nit->second;
                                TANK_EXPECT(ctx);

                                auto node = ctx->n;

                                if (!node->available()) {
                                        continue;
                                }

                                if (trace) {
                                        SLog("Can accept RS node ", node->id, "@", node->ep, "\n");
                                }

                                if (!best) {
                                        best = ctx;
                                } else if (ctx->counts.leader_for < best->counts.leader_for or
                                           (ctx->counts.leader_for == best->counts.leader_for and ctx->counts.replica_for < best->counts.replica_for)) {
                                        best = ctx;
                                }
                        }
                } else {
                        // we have a leader, but the leader is not available, and is part of the replicas
                        // choose among the ISRs
                        for (auto it : p->cluster.isr.list) {
                                auto       n   = switch_list_entry(isr_entry, partition_ll, it)->node();
                                const auto nit = nodes_map.find(n->id);

                                if (nit == nodes_map.end()) {
                                        // likely erase()d earlier(see below)
                                        if (trace) {
                                                SLog("Weird: unable to find() ", n->id, "\n");
                                        }
                                        continue;
                                }

                                auto ctx = nit->second;
                                TANK_EXPECT(ctx);

                                auto node = ctx->n;

                                if (!node->available()) {
                                        // this is actually possible
                                        continue;
                                }

                                if (trace) {
                                        SLog("Can accept ISR node ", node->id, "@", node->ep, "\n");
                                }

                                if (!best) {
                                        best = ctx;
                                } else if (ctx->counts.leader_for < best->counts.leader_for or
                                           (ctx->counts.leader_for == best->counts.leader_for and ctx->counts.replica_for < best->counts.replica_for)) {
                                        best = ctx;
                                }
                        }
                }

                if (!best) {
                        if (trace) {
                                SLog("Unable to assign leader to partition\n");
                        }

                        continue;
                }

                auto node = best->n;

                if (++(best->counts.leader_for) >= leadership_threshold) {
                        // so that we won't be able to use it for other leader assignments
                        nodes_map.erase(node->id);
                }

                if (trace) {
                        SLog(ansifmt::color_brown, "Promoted ", node->id, "@", node->ep,
                             " as leader of ", p->owner->name(), "/", p->idx, " (replicas:", p->cluster.replicas.nodes.size(), ")", ansifmt::reset, "\n");
                }

                promotions->emplace_back(NodesPartitionsUpdates::leadership_promotion{
                    .p = p,
                    .n = node,
                });
        }

        if (trace) {
                SLog("out: reduced = ", reduced->size(), ", expanded = ", expanded->size(), ", promoted = ", promotions->size(), "\n");
        }
}

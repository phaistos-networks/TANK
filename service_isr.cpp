#include "service_common.h"

#ifdef HWM_UPDATE_BASED_ON_ACKS
isr_entry *Service::isr_bind(topic_partition *p, cluster_node *peer, const uint8_t pinid, const uint32_t ref) {
        static constexpr bool trace{false};
        TANK_EXPECT(p);
        TANK_EXPECT(peer);
#ifdef HWM_UPDATE_BASED_ON_ACKS
        TANK_EXPECT(pinid < (sizeof(topic_partition::Cluster::PendingClientProduceAcks::pending_ack::isr_nodes_acknowledged_bm) * 8));
#endif

#ifdef TANK_RUNTIME_CHECKS
        for (auto it : p->cluster.isr.list) {
                auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                TANK_EXPECT(node != peer);
        }
#endif

        auto isr_e = get_isr_entry();

        isr_e->partition_ = p;
        isr_e->node_      = peer;
#ifdef HWM_UPDATE_BASED_ON_ACKS
        isr_e->partition_isr_node_id = pinid;
#endif

        p->cluster.isr.list.push_back(&isr_e->partition_ll);

        TANK_EXPECT(static_cast<size_t>(p->cluster.isr.cnt) + 1 < std::numeric_limits<decltype(p->cluster.isr.cnt)>::max());
        p->cluster.isr.cnt++;
        p->cluster.isr.dirty = true;

        peer->isr.list.push_back(&isr_e->node_ll);
        peer->isr.cnt++;

        TANK_EXPECT(p->cluster.isr.list.size() == p->cluster.isr.cnt);
        TANK_EXPECT(peer->isr.list.size() == peer->isr.cnt);

        if (trace) {
                SLog(ansifmt::color_blue, ansifmt::bgcolor_green, "ISR: adding ", peer->id, "@", peer->ep,
                     " to ISR of ", p->owner->name(), "/", p->idx, " at ", ref, ansifmt::reset, "\n");
        }

#ifdef TANK_RUNTIME_CHECKS
        {
                std::unordered_set<nodeid_t>       s1;
                std::unordered_set<cluster_node *> s2;

                for (auto it : p->cluster.isr.list) {
                        auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                        TANK_EXPECT(s1.insert(node->id).second);
                        TANK_EXPECT(s2.insert(node).second);
                }
        }
#endif

        // you are supposed to persist_isr()
        return isr_e;
}
#else
isr_entry *Service::isr_bind(topic_partition *p, cluster_node *peer, const uint32_t ref) {
        static constexpr bool trace{false};
        TANK_EXPECT(p);
        TANK_EXPECT(peer);

#ifdef TANK_RUNTIME_CHECKS
        for (auto it : p->cluster.isr.list) {
                auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                TANK_EXPECT(node != peer);
        }
#endif

        auto isr_e = get_isr_entry();

        isr_e->partition_ = p;
        isr_e->node_      = peer;

        p->cluster.isr.list.push_back(&isr_e->partition_ll);

        TANK_EXPECT(static_cast<size_t>(p->cluster.isr.cnt) + 1 < std::numeric_limits<decltype(p->cluster.isr.cnt)>::max());
        p->cluster.isr.cnt++;
        p->cluster.isr.dirty = true;

        peer->isr.list.push_back(&isr_e->node_ll);
        peer->isr.cnt++;

        TANK_EXPECT(p->cluster.isr.list.size() == p->cluster.isr.cnt);
        TANK_EXPECT(peer->isr.list.size() == peer->isr.cnt);

        if (trace) {
                SLog(ansifmt::color_blue, ansifmt::bgcolor_green, "ISR: adding ", peer->id, "@", peer->ep,
                     " to ISR of ", p->owner->name(), "/", p->idx, " at ", ref, ansifmt::reset, "\n");
        }

#ifdef TANK_RUNTIME_CHECKS
        {
                std::unordered_set<nodeid_t>       s1;
                std::unordered_set<cluster_node *> s2;

                for (auto it : p->cluster.isr.list) {
                        auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                        TANK_EXPECT(s1.insert(node->id).second);
                        TANK_EXPECT(s2.insert(node).second);
                }
        }
#endif

        // you are supposed to persist_isr()
        return isr_e;
}
#endif

void Service::isr_dispose(isr_entry *isr) {
        static constexpr bool trace{false};
        TANK_EXPECT(isr);
        auto partition = isr->partition();
        auto node      = isr->node();

        TANK_EXPECT(partition);
        TANK_EXPECT(node);
        TANK_EXPECT(node->isr.cnt);
        TANK_EXPECT(!node->isr.list.empty());
        TANK_EXPECT(node->isr.cnt);
        TANK_EXPECT(!node->isr.list.empty());
        TANK_EXPECT(node->isr.cnt);
        // make sure we didn't forget this
        TANK_EXPECT(isr->pending_next_ack_ll.empty());

        isr->node_ll.detach();
        node->isr.cnt--;

        isr->partition_ll.detach();
        partition->cluster.isr.cnt--;
        partition->cluster.isr.dirty = true;

        TANK_EXPECT(partition->cluster.isr.list.size() == partition->cluster.isr.cnt);
        TANK_EXPECT(node->isr.list.size() == node->isr.cnt);

        put_isr_entry(isr);

        if (trace) {
                SLog(ansifmt::color_blue, ansifmt::bgcolor_green, "ISR: removed ", node->id, "@", node->ep,
                     " from ISR of ", partition->owner->name(), "/", partition->idx, ", now partition ISR size = ", partition->cluster.isr.cnt, ansifmt::reset, "\n");
        }

#ifdef TANK_RUNTIME_CHECKS
        {
                std::unordered_set<nodeid_t>       s1;
                std::unordered_set<cluster_node *> s2;

                // make sure we haven't done that already
                for (auto it : partition->cluster.isr.list) {
                        auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                        TANK_EXPECT(s1.insert(node->id).second);
                        TANK_EXPECT(s2.insert(node).second);
                }
        }
#endif

#ifndef HWM_UPDATE_BASED_ON_ACKS
	rebuild_partition_tracked_isrs(partition);
#endif

        // if we are removing a broker from the ISR of a partition, we need to consider if
        // the invariants are satisfied for a pending client produce response
        consider_pending_client_produce_responses(partition);
}

// called when failed to get a CONSUME request for the last partition message didn't happen in time
void Service::expire_isr_ack(isr_entry *isr) {
        TANK_EXPECT(isr);
        static constexpr bool trace{false};
        auto                  partition = isr->partition();
        auto                  node      = isr->node();

        if (trace) {
                SLog(ansifmt::inverse, ansifmt::bold, ansifmt::color_green, "Expiring ", node->id, "@", node->ep, 
			" from ISR of ", partition->owner->name(), "/", partition->idx, ansifmt::reset, "\n");
        }

        // detach from pending ack.list if not detached already
        isr->pending_next_ack_ll.try_detach_and_reset();

        // and get rid of this ISR entry
        isr_dispose(isr);

        persist_isr(partition, __LINE__);
}

void Service::consider_isr_pending_ack() {
        while (!isr_pending_ack_list.empty()) {
                auto isr_e = switch_list_entry(isr_entry, pending_next_ack_ll, isr_pending_ack_list.prev);

                if (isr_e->pending_next_ack_timeout > now_ms) {
                        break;
                }

                // took too long for node in an partition's ISR
                // to consume past a sequence number
                expire_isr_ack(isr_e);
        }

        isr_pending_ack_list_next = isr_pending_ack_list.empty()
                                        ? std::numeric_limits<uint64_t>::max()
                                        : switch_list_entry(isr_entry, pending_next_ack_ll, isr_pending_ack_list.prev)->pending_next_ack_timeout;
}

void Service::persist_isr(topic_partition *p, const uint32_t ref) {
        static constexpr bool trace{false};
        TANK_EXPECT(p);
        TANK_EXPECT(cluster_state.local_node.ref);
	TANK_EXPECT(cluster_state.leader_self() || p->cluster.leader.node == cluster_state.local_node.ref);

#ifdef TANK_RUNTIME_CHECKS
        {
                std::unordered_set<nodeid_t>       s1;
                std::unordered_set<cluster_node *> s2;

                // make sure we haven't done that already
                for (auto it : p->cluster.isr.list) {
                        auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                        TANK_EXPECT(s1.insert(node->id).second);
                        TANK_EXPECT(s2.insert(node).second);
                }
        }
#endif

        if (trace) {
                puts("\n");
                SLog(ansifmt::bold, ansifmt::color_brown, ansifmt::bgcolor_red, ansifmt::inverse, "Would have persisted ISR of ",
                     p->owner->name(), "/", p->idx,
                     " => ", p->cluster.isr.cnt, " ref=", ref, ansifmt::reset, "\n");

                for (const auto it : p->cluster.isr.list) {
                        const auto isr_e = switch_list_entry(isr_entry, partition_ll, it);

                        SLog(">> ISR NODE ", isr_e->node()->id, "@", isr_e->node()->ep, "\n");
                }
        }

#ifdef TANK_RUNTIME_CHECKS
        {
                std::unordered_set<nodeid_t> s;

                for (auto it : p->cluster.isr.list) {
                        auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                        TANK_EXPECT(s.insert(node->id).second);
                }
        }
#endif

        // We are going to _defer_ this to the end of the loop
        // so that we can collect multiple ISR updates and use a transaction to update them
        // thereby reducing the number of requests to consul which shoul improve performance etc
        // (if you disable a topic that has 100 partitions, we will need two transactions requests as opposed to 100)
        //
        // this makes sense because we will likely be updating ISRs very frequently
	if (!p->cluster.isr_dirty()) {
                p->cluster.flags |= unsigned(topic_partition::Cluster::Flags::ISRDirty);
		TANK_EXPECT(p->cluster.isr_dirty());

                isr_dirty_list.emplace_back(p);
        }

	p->cluster.isr.dirty = false;
}

#ifndef HWM_UPDATE_BASED_ON_ACKS
// seqnum would be the LSN of the last message persisted on peer's end, as advertised by the peer
void Service::isr_touch(isr_entry *const isr_e, const uint64_t advertised_lastmsgpersisted_id) {
        static constexpr bool trace{false};
        TANK_EXPECT(isr_e);

        if (trace) {
                SLog("touching ISR ent, advertised_lastmsgpersisted_id(", advertised_lastmsgpersisted_id, "), isr_e->last_msg_lsn(", isr_e->last_msg_lsn, ")\n");
        }

        if (advertised_lastmsgpersisted_id <= isr_e->last_msg_lsn) {
                if (trace) {
                        SLog("No need to update\n");
                }

                return;
        }

        isr_e->last_msg_lsn = advertised_lastmsgpersisted_id;

        auto       p            = isr_e->partition();
        const auto node         = isr_e->node();
        const auto nid          = node->id;
        const auto size         = p->cluster.isr.tracker.size;
        auto &     data         = p->cluster.isr.tracker.data;
        uint8_t    insert_index = 0;

        if (trace) {
                SLog("Tracking ", size, " (node, lsn) for ", p->owner->name(), "/", p->idx, ", node ", node->id, "@", node->ep, "\n");
        }

        for (unsigned i = 0; i < size; ++i) {
                auto &it = data[i];

                if (it.nid == nid) {
                        if (it.lsn != advertised_lastmsgpersisted_id) {
                                //sort in place
                                it.lsn = advertised_lastmsgpersisted_id;

                                if (trace) {
                                        SLog("Updated and sorting in place\n");
                                }

                                while (++i < size && data[i - 1].lsn > data[i].lsn) {
                                        std::swap(data[i - 1], data[i]);
                                }

                                if (trace) {
                                        for (unsigned i = 0; i < size; ++i) {
                                                SLog(i, ": ", data[i].nid, " ", data[i].lsn, "\n");
                                        }
                                }

                                update_hwmark(p, data[0].lsn);
                        }

                        return;
                } else if (advertised_lastmsgpersisted_id > it.lsn) {
                        insert_index = i + 1;
                }
        }

        if (trace) {
                SLog("node not tracked, will track\n");
        }

        TANK_EXPECT(size != sizeof_array(p->cluster.isr.tracker.data));

        memmove(data + insert_index + 1, data + insert_index, sizeof(data[0]) * (++(p->cluster.isr.tracker.size) - insert_index));
        data[insert_index].nid = nid;
        data[insert_index].lsn = advertised_lastmsgpersisted_id;

        if (trace) {
                for (unsigned i = 0; i < size; ++i) {
                        SLog(i, ": ", data[i].nid, " ", data[i].lsn, "\n");
                }
        }

        update_hwmark(p, data[0].lsn);
}
#endif

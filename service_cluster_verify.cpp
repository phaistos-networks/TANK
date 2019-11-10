#include "service_common.h"

// see cluster_invariants.md
void Service::verify_cluster_invariants(topic_partition *p) {
        TANK_EXPECT(p);

	if (!cluster_aware()) {
		return;
	}

	if (!p->cluster.leader.node) {
		return;
	}

	if (p->cluster.leader.node != cluster_state.local_node.ref) {
		return;
	}

        for (auto it : p->cluster.isr.list) {
                auto isr_e = switch_list_entry(isr_entry, partition_ll, it);
                auto node  = isr_e->node();

                if (!node->is_replica_for(p)) {
                        SLog("Unexpected: partition ", p->owner->name(), "/", p->idx, " isr size = ", p->cluster.isr.list.size(), ", RS size = ", p->cluster.replicas.nodes.size(), ": node ", node->id, "@", node->ep, " in ISR but not in RS\n");
                        std::abort();
                }
        }

        if (p->cluster.isr.list.size() > p->cluster.replicas.nodes.size()) {
                SLog("Unexpected: partition ", p->owner->name(), "/", p->idx, " isr size = ", p->cluster.isr.list.size(), ", RS size = ", p->cluster.replicas.nodes.size(), ": ISR larger than RS\n");
                std::abort();
        }

        if (auto l = p->cluster.leader.node) {
                if (std::find(p->cluster.replicas.nodes.begin(), p->cluster.replicas.nodes.end(), l) == p->cluster.replicas.nodes.end()) {
                        SLog("Unexpected: partition ", p->owner->name(), "/", p->idx, " isr size = ", p->cluster.isr.list.size(), ", RS size = ", p->cluster.replicas.nodes.size(), ": leader ", l->id, "@", l->ep, " not found in RS\n");
                        std::abort();
                }

                if (!p->cluster.isr.find(l)) {
                        SLog("Unexpected: partition ", p->owner->name(), "/", p->idx, " isr size = ", p->cluster.isr.list.size(), ", RS size = ", p->cluster.replicas.nodes.size(), ": leader ", l->id, "@", l->ep, " not found in ISR\n");
                        std::abort();
                }
        }
}

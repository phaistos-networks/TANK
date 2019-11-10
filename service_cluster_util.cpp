#include "service_common.h"

uint8_t topic::replication_factor() const noexcept {
        return cluster.rf_;
}

bool cluster_node::is_replica_for(const topic_partition *p) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        // this works because we always std::sort() whenever replica_for is dirty
        // this is sorted by idx
        const auto all = replica_for.data();
        TANK_EXPECT(std::is_sorted(replica_for.begin(), replica_for.end()));

        for (int32_t top = static_cast<int32_t>(replica_for.size()) - 1, btm = 0; btm <= top;) {
                const auto mid    = (btm + top) / 2;
                const auto at_mid = all[mid];

                if (p < at_mid) {
                        top = mid - 1;
                } else if (p > at_mid) {
                        btm = mid + 1;
                } else {
                        return true;
                }
        }
        return false;
}

bool topic_partition::require_leader() const noexcept {
        const auto topic = owner;

        if (!topic->enabled) {
                return false;
        }

        if (!enabled()) {
                return false;
        }

        if (!topic->cluster.rf_) {
                return false;
        }

        return true;
}

// Recall that this is the number of _copies_ required
// not the number of replicas other than the leader that need to replicate from that node
uint16_t topic_partition::required_replicas() const noexcept {
        const auto topic = owner;

        if (!topic->enabled) {
                return 0;
        }

        if (!enabled()) {
                return 0;
        }

        return topic->cluster.rf_;
}

bool Service::should_manage_topic_partition_replicas(const topic *t) const noexcept {
        if (!cluster_aware() || !cluster_state.leader_self() || !t->enabled || !t->partitions_ || t->partitions_->empty()) {
                return false;
        }

        return true;
}

bool Service::is_valid_topic_name(const str_view8 n) noexcept {
        if (!n || n.size() > 64) {
                return false;
        }

        bool any{false};

        for (const auto c : n) {
                switch (c) {
                        case 'a' ... 'z':
                        case 'A' ... 'Z':
                        case '0' ... '9':
                                any = true;
                                break;

                        case '_':
                                break;

                        default:
                                return false;
                }
        }

        return any;
}

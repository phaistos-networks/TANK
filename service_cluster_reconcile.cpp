#include "service_common.h"

void Service::apply_isr(topic_partition *p, const range_base<nodeid_t *, uint8_t> replicas) {
        TANK_EXPECT(p);
        auto part = cluster_state.updates.get_partition(p);

        if (!part->isr_update) {
                part->isr_update.reset(new std::vector<nodeid_t>());
        }

        part->isr_update->clear();
        part->isr_update->insert(part->isr_update->end(), replicas.offset, replicas.offset + replicas.size());
}

void Service::reconcile_cluster_ISR(std::vector<partition_isr_info> *const isr_state) {
        static constexpr bool trace{false};
        bool                  any{false};

        std::sort(isr_state->begin(), isr_state->end(), [](const auto &a, const auto &b) noexcept {
                const auto r = a.topic.Cmp(b.topic);

                return r < 0 || (0 == r && a.partition < b.partition);
        });

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_black, ansifmt::bgcolor_green, "Reconciling ISR", ansifmt::reset, "\n");
        }

        for (const auto *p = isr_state->data(), *const e = p + isr_state->size(); p < e;) {
                const auto tn    = p->topic;
                auto       topic = topic_by_name(tn);

                if (!topic) {
                        // ignore everything about this topic
                        if (trace) {
                                SLog("Unknown topic ", tn, "\n");
                        }

                        do {
                                //
                        } while (++p < e && p->topic == tn);
                        continue;
                }

                do {
                        const auto pid       = p->partition;
                        auto       partition = topic->partition(pid);

                        if (!partition) {
                                if (trace) {
                                        SLog("Odd; unknown partition ", pid, "\n");
                                }

                                continue;
                        }

                        if (p->gen == partition->cluster.isr.gen) {
                                // fast-path: ISR for this topic didn't change
                                if (trace) {
                                        SLog("No need to update ", partition->owner->name(), "/", partition->idx, "\n");
                                }

                                continue;
                        }

                        if (trace) {
                                SLog("Have updated ISR for ", partition->owner->name(), "/", partition->idx, " => ", p->replicas.size(), "\n");
                        }

                        any = true;
                        apply_isr(partition, p->replicas);
                        partition->cluster.isr.gen = p->gen;
                } while (++p < e && p->topic == tn);
        }

        if (any) {
                schedule_cluster_updates_apply(__FUNCTION__);
        }
}

void Service::reconcile_cluster_nodes(const std::vector<cluster_nodeid_update> &updates) {
        static constexpr bool trace{false};
        auto &                set = reusable.nodes_set;

        if (trace) {
                SLog("Have ", updates.size(), " updates for /nodes\n");
        }

	set.clear();
        for (const auto &it : updates) {
                const auto id = it.id;
                auto       n  = cluster_state.find_node(id);

                if (!n) {
                        n = new cluster_node(id);

                        cluster_state.all_nodes.emplace_back(n);
                        cluster_state.nodes.emplace(id, std::move(n));
                        cluster_state.all_nodes_dirty = true;
                } 

		set.insert(n);

		if (trace) {
			SLog("Node ", n->id, " ", it.ep, " ", it.have_owner, "\n");
		}


                cluster_state.updates.nodes[n] = std::make_pair(it.ep, it.have_owner);
        }

	// updates can be empty
	// because e.g someone DELETE nodes/?recurse=true
	// so we are handling this here
        for (auto n : cluster_state.all_nodes) {
                if (!set.count(n)) {
			if (trace) {
				SLog("Will force reset ", n->id, "@", n->ep, "\n");
			}

                        cluster_state.updates.nodes[n] = std::make_pair(Switch::endpoint{}, false);
                }
        }

        schedule_cluster_updates_apply(__FUNCTION__);
}

void Service::reconcile_leadership(std::vector<partition_leader_update> *new_leadership) {
        static constexpr bool trace{false};
        bool                  any{false};

        std::sort(new_leadership->begin(), new_leadership->end(), [](const auto &a, const auto &b) noexcept {
                const auto r = a.topic.Cmp(b.topic);

                return r < 0 || (r == 0 && a.partition < b.partition);
        });

        for (const auto *p = new_leadership->data(), *const e = p + new_leadership->size(); p < e;) {
                const auto topic = p->topic;

                if (!topic) {
                        TANK_EXPECT(!p->partition);

                        any = true;
                        cluster_state.updates.set_cluster_leader(p->locked ? p->leader_id : 0);
                        ++p;
                } else {
                        if (const auto t = topic_by_name(topic)) {
                                do {
                                        const auto pid = p->partition;

                                        if (auto partition = t->partition(pid)) {
                                                cluster_state.updates.get_partition(partition)->set_leader(p->leader_id);
                                                any = true;
                                        } else if constexpr (trace) {
                                                SLog("Unknown ", topic, "/", pid, "\n");
                                        }

                                } while (++p < e && p->topic == topic);
                        } else {
                                if constexpr (trace) {
                                        SLog("Unknown topic [", topic, "]\n");
                                }

                                do {
                                        // skip everything about this topic
                                } while (++p < e && p->topic == topic);
                        }
                }
        }

        if (any) {
                schedule_cluster_updates_apply(__FUNCTION__);
        }
}

// `no_apply` is false if this is a response to MonitorConfUpdatesNoApply
// which is the initial request to conf-updates, where we only need the ModifyIndex for each key
// as a subsequent request RetrieveConfigs will fetch them all anyway
void Service::consider_updated_consul_configs(const std::vector<std::pair<str_view8, uint64_t>> &updates, const bool no_apply) {
	enum {
		trace = false,
	};

        if constexpr (trace) {
                SLog("From /conf-updates/ ", updates.size(), ", no_apply = ", no_apply, "\n");
        }

        // Those would be the names of topics OR the special reserved name ".cluster"
        // in which case the cluster configuration has been updated
        //
        // We will need to compare the revision (i.e the Flags) of the local configuration against
        // what was advertised here, and if there are differences, we need to fetch the configurations we need
        for (const auto &it : updates) {
                if constexpr (trace) {
                        SLog("Updated [", it.first, "] [", it.second, "]\n");
                }

                if (it.first.Eq(_S(".cluster"))) {
                        if (it.second > cluster_state.local_conf.last_update_gen) {
                                if constexpr (trace) {
                                        SLog(ansifmt::color_brown, ansifmt::bold, "Cluster Configuration Updated", ansifmt::reset, "\n");
                                }

                                cluster_state.local_conf.last_update_gen = it.second;

                                if (false == no_apply) {
                                        request_cluster_config();
                                } else if constexpr (trace) {
                                        SLog("Will NOT apply\n");
                                }
                        }
                } else {
                        auto t = topic_by_name(it.first);

                        if (!t) {
                                if constexpr (trace) {
                                        SLog("Unknown topic ", it.first, "\n");
                                }

                                continue;
                        }

                        if (t->cluster.last_update_gen == it.second) {
                                if constexpr (trace) {
                                        SLog("Topic configuration is not dirty for ", it.first, "\n");
                                }

                                continue;
                        }

                        if constexpr (trace) {
                                SLog("Topic configuration updated for ", it.first, "\n");
                        }

                        t->cluster.last_update_gen = it.second;

                        if (false == no_apply) {
                                request_topic_config(t);
                        } else if constexpr (trace) {
                                SLog("Will NOT apply\n");
                        }
                }
        }
}

// UPDATE: 2021-10-14
// will deal with it in open_partition_log() now; will create directories if necessary
// see open_partition_log() definition comments
//#define __MKDIRS 1

// the topology/ of the cluster has changed
// this is a somewhat convoluted impl. because we need to take care of too much state and transitions
//
// (topic, partition, gen, replica...)
// TODO: https://github.com/phaistos-networks/TANK/issues/70
void Service::reconcile_cluster_topology(std::vector<topology_partition> *const new_topology) {
	enum {
		trace = false,
	};
        partition_config                                                pc;
        bool                                                            any{false};
        auto &                                                          v                  = reusable.v;
        auto &                                                          updates            = reusable.updates;
        auto &                                                          pending_partitions = reusable.pending_partitions;
        auto &                                                          pending_reg_topics = reusable.pending_reg_topics;
        auto &                                                          retained_topics    = reusable.retained_topics;
#ifdef __MKDIRS
        char                                                            topic_path[PATH_MAX];
#endif


        v.clear();
	updates.clear();
	pending_partitions.clear();
	pending_reg_topics.clear();
	retained_topics.clear();

        // group by topic
        std::sort(new_topology->begin(), new_topology->end(), [](const auto &a, const auto &b) noexcept {
                const auto r = a.topic.Cmp(b.topic);

                return r < 0 || (0 == r && a.partition < b.partition);
        });

        if constexpr (trace) {
                SLog(ansifmt::bold, ansifmt::color_black, ansifmt::bgcolor_green, "Reconcile ", new_topology->size(), " ", new_topology->size(), " partitions", ansifmt::reset, "\n");
        }

        // validate topology
        for (const auto *p = new_topology->data(), *const e = p + new_topology->size(); p < e;) {
                const auto tn = p->topic;
                uint16_t   partitions{0};

                if (not is_valid_topic_name(tn)) {
			// bogus?
                        if (trace) {
                                SLog("Ignoring invalid topic name\n");
                        }

                        do {
                                //
                        } while (++p < e and p->topic == tn);
                        continue;
                }

                do {
                        const auto partition_id = p->partition;

                        if (partition_id != partitions) {
                                // unless we get all partitions from [0, total topic partitions)
                                // we are going to ignore this topic
				//
				// UPDATE: we are no longer doing that because its easy
				// for someone to accidently create a sparse topic by creating a key in the topic partitions 
				// that is not +1 from the last partition id and that could result in _all_ partitions
				// to become disabled(because we used to disable the partition) and that could lead
				// to service disrupt. and data loss. So fo now
				// we are going to retain upto partition_id instead
				// (i.e outside the loop where we skip to the end of (p->topic == tn)
				// we will not explicitly set partitions = 0
                                if constexpr (trace) {
                                        SLog("Expected partition ", partitions, ", got ", partition_id, " for ", tn, "\n");
                                }

                                do {
                                        //
                                } while (++p < e and p->topic == tn);
                                break;
                        }

                        ++partitions;
                } while (++p < e && p->topic == tn);

                if (partitions) {
                        // we need at least 1 topic otherwise we ignore the topic
                        v.emplace_back(tn, partitions);
                }
        }


        // now process validated topology
        for (const auto &it : v) {
                const auto [topic_name, partitions] = it;

                if constexpr (trace) {
                        SLog("Consider topic [", topic_name, "] => ", partitions, " partitions\n");
                }

                if (auto t = topic_by_name(topic_name)) {
                        // topic already defined
                        auto       topic_partitions = t->partitions_;
                        const bool was_enabled      = t->enabled;
                        TANK_EXPECT(topic_partitions);

                        cluster_state.updates.get_topic(t)->set_enabled();
                        retained_topics.insert(t);

                        if (was_enabled != t->enabled or t->total_enabled_partitions != partitions) {
                                // for simplicity sake, include all
                                // will also include partitions in updates
                                if (trace) {
                                        SLog("Will include all current partitions  (total_enabled_partitions = ", t->total_enabled_partitions,
                                             ") because was_enabled = ", was_enabled, ", enabled = ", t->enabled, ", new partitions = ", partitions, "\n");
                                }

                                for (auto p : *topic_partitions) {
                                        cluster_state.updates.get_partition(p.get());
                                }

                                any = true;
                        }

                        if (const auto cnt = topic_partitions->size(); cnt < partitions) {
                                // more partitions than however many we have for this topic
                                if (trace) {
                                        SLog("Registering ", partitions - cnt, " extra partitions for topic [", topic_name, "]\n");
                                }

                                pending_partitions.emplace_back(t, range_base<uint16_t, uint16_t>(cnt, partitions - cnt));
                                any = true;
                        } else if (partitions != t->total_enabled_partitions) {
                                // reduced/expanded number of partitions
                                if constexpr (trace) {
                                        SLog("NOW setting total enabled partitions to ", partitions,
                                             " from total_enabled_partitions = ", t->total_enabled_partitions, " for topic [", topic_name, "]\n");
                                }

				cluster_state.updates.get_topic(t)->set_total_enabled_partitions(partitions);
                                any = true;
                        }
                } else {
                        // we need to create a new topic for those partitions
#ifdef __MKDIRS
                        [[maybe_unused]] const auto topic_path_len = snprintf(topic_path, sizeof(topic_path), "%.*s/%.*s/",
                                                                              basePath_.size(), basePath_.data(),
                                                                              topic_name.size(), topic_name.data());

                        if constexpr (trace) {
                                SLog(ansifmt::color_brown, ansifmt::bold, ansifmt::inverse, "Undefined topic ", topic_name,
                                     ansifmt::reset, " ", topic_path, ", partitions = ", partitions, "\n");
                        }

                        if (-1 == mkdir(topic_path, S_IRWXU | S_IRWXG)) {
                                if (EEXIST == errno) {
                                        // OK
                                } else {
                                        IMPLEMENT_ME();
                                }
                        }
#endif

                        auto new_topic = std::make_shared<topic>(topic_name, pc);

                        pending_partitions.emplace_back(new_topic.get(), range_base<uint16_t, uint16_t>{0, partitions});
                        pending_reg_topics.emplace_back(std::move(new_topic));
                        any = true;
                }
        }

        std::mutex                                                        collected_lock;
        std::vector<std::pair<topic *, std::shared_ptr<topic_partition>>> collected;
        std::vector<std::shared_ptr<topic_partition>>                     partitions;

        for (auto &it : pending_partitions) {
                auto [topic, range] = it;
#ifdef __MKDIRS
                const auto topic_name     = topic->name();
                const auto topic_path_len = snprintf(topic_path, sizeof(topic_path), "%.*s/%.*s/",
                                                     basePath_.size(), basePath_.data(),
                                                     topic_name.size(), topic_name.data());
#endif

                if (trace) {
                        SLog("Pending for ", topic->name(), " ", range, "\n");
                }

                for (const auto partition : range) {
                        // we are only going to define the partition
                        // we won't initialize it's log unless we become a replica
                        auto p = define_partition(partition, topic);

                        // the partition dataset may already be available locally e.g for
                        // when migrating from a single node to a cluster-aware configureation
#ifndef TANK_SRV_LAZY_PARTITION_INIT
                        try {
                                open_partition_log(p.get(), pc);
                        } catch (...) {
                                throw;
                        }
#else


#ifdef __MKDIRS
                        // we still need to create the directories so that open_partition_log() won't fail
                        sprintf(topic_path + topic_path_len, "%u", partition);

                        if (-1 == mkdir(topic_path, S_IRWXU | S_IRWXG) and EEXIST != errno) {
                                IMPLEMENT_ME();
                        }
#endif

#endif

#if 0
                        // 2021-10-07
                        // if we are the cluster leadder, then we need to initialize the partition's highwater_mark.seq_num (see topic_partition::hwmark() impl.)
                        // from partition_log(p)->lastAssignedSeqNum;
                        // if the partition exists already on disk(this is important for transitioning from single node to cluster-aware)
			//
			// 2021-10-14: we are no longer doing this because its not necessary and its also expensive if we
			// are not going to really need to access the partition; defer it (XXX: make sure this assumption holds, even
			// though I verified it does)
                        if (cluster_state.leader_self()) {
                                if (trace) {
                                        SLog("Will need to update hwmark from disk\n");
                                }

                                const auto v = partition_log(p.get())->lastAssignedSeqNum;

                                if (trace) {
                                        SLog("lastAssignedSeqNum = ", v, "\n");
                                }

                                p->highwater_mark.seq_num = v;
                        }
#endif

                        collected_lock.lock();
                        collected.emplace_back(topic, std::move(p));
                        collected_lock.unlock();
                }
        }

        std::sort(collected.begin(), collected.end(), [](const auto &a, const auto &b) noexcept {
                return a.first < b.first;
        });

        for (size_t i{0}; i < collected.size();) {
                auto topic = collected[i].first;

                do {
                        auto p = collected[i].second;

                        cluster_state.updates.get_partition(p.get());
                        partitions.emplace_back(std::move(p));
                } while (++i < collected.size() and collected[i].first == topic);

                if constexpr (trace) {
                        SLog("NEW: For topic ", topic->name_, " ", partitions.size(), " new partitions\n");
                }

                topic->register_partitions(partitions.data(), partitions.size());
                partitions.clear();
                any = true;
        }

        // for all just created topics
        for (auto &it : pending_reg_topics) {
                if constexpr (trace) {
                        SLog("Registering Topic [", it->name_, "]\n");
                }

                retained_topics.insert(it.get());
                register_topic(std::move(it));
                any = true;
        }

        // we do not delete anything
        // we simply disable topics that are no longer present in the topology
        for (auto &it : topics) {
                const auto topic = it.second.get();

                if ((topic->enabled or topic->total_enabled_partitions) and
                    not retained_topics.count(topic)) {
                        // topic was previously defined, but is no longer in the topology tree
                        // first make sure total enabled partitions of that topic is reset to 0 so that we can capture whatever in updates
                        auto _t = cluster_state.updates.get_topic(topic);

			SLog("Topic was enabled with partitions but no longer retained\n");

                        _t->set_disabled();
                        _t->set_total_enabled_partitions(0);
                        any = true;
                }
        }

        // go through all (topic, partitions=>replicas) defined in the latest topology tree
        // and figure out if we need to update the memory resident partitions replicas state
	auto &nodes = reusable.nodes; 

	nodes.clear();
        for (const auto *p = new_topology->data(), *const e = p + new_topology->size(); p < e;) {
                const auto tn    = p->topic;
                auto       topic = topic_by_name(tn);

                if (not topic) {
                        // likely is_valid_topic_name() failed for that topic name earlier
                        do {
                                //
                        } while (++p < e and p->topic == tn);
                        continue;
                }

		if (trace) {
			SLog("Considering topology update for '", tn, "' for replicas updates\n");
		}

                do {
                        const auto p_id     = p->partition;
                        const auto replicas = p->replicas;
                        const auto gen      = p->gen;

                        if (auto p = topic->partition(p_id)) {
                                if (p->cluster.replicas.last_update_gen != gen) {
                                        // consul thankfully updates the ModifyIndex of each key when it is updated so we can just
                                        // skip updates to partition replicas if the generation(i.e the ModifyIndex value) didn't change since
                                        // the last time we processed the replicas for this partition
                                        auto part = cluster_state.updates.get_partition(p);

                                        nodes.clear();
                                        for (size_t i{0}; i < replicas.size(); ++i) {
                                                if (auto n = cluster_state.find_node(replicas.offset[i])) {
                                                        nodes.emplace_back(n);
                                                }
                                        }

#ifdef HAVE_SWITCH
                                        if (trace) {
                                                SLog("Updated replicas set to [", values_repr_with_lambda(nodes.data(), nodes.size(), [](const auto it) noexcept { return it->id; }), "]\n");
                                        }
#endif

                                        part->set_replicas(nodes.data(), nodes.size());
                                        p->cluster.replicas.last_update_gen = gen;
                                        any                                 = true;
                                } else if constexpr (trace) {
                                        // SLog("Ignoring, gen didn't change for ", tn, "/", p_id, "\n");
                                }
                        } else if (trace) {
				SLog("Unable to lookup partition ", p_id, " of topic ", tn, "\n");
			}

                } while (++p < e and p->topic == tn);
        }

	if (trace) {
		SLog("any updates:", any, "\n");
	}

        if (any) {
                schedule_cluster_updates_apply(__FUNCTION__);
        }
}

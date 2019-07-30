#include <ext/Catch/catch.hpp>
#include "service.h"

TEST_CASE_METHOD(Service, "SRV") {
        partition_config      config{.maxSegmentSize = 128 * 1024 * 1024, .maxIndexSize = 128 * 1024 * 1024, .curSegmentMaxAge = 8192 * 1000};
        const str_view8       topic_name("foo");
        auto                  t = Switch::make_sharedref<topic>(str_view8(topic_name.Copy(), topic_name.size()), config);
        Buffer                k, c;
        SwitchFS::deltree_ctx deltree_ctx;
        REQUIRE(t->partitions_);
        REQUIRE(t->partitions_->empty());

        t->cluster.rf_ = 1;

        basePath_.clear();
        basePath_.append("/tmp/TDEBUG/"_s32);
        SwitchFS::DelTree(basePath_.c_str(), &deltree_ctx);
        SwitchFS::BuildPath(Buffer{}.append(basePath_.as_s32(), "/foo/").c_str());

        auto part = init_local_partition(0, t.get(), config);

        REQUIRE(t->partitions_->empty());

        t->register_partition(part.release());
        REQUIRE(t->partitions_->size() == 1);

        auto p = t->partition(0);
        REQUIRE(p != nullptr);

        std::vector<topic_partition *> partitions{p};

        SECTION("APPLY") {
                INFO("Create a bunch of nodes");
                std::vector<cluster_node *> all_nodes;

                for (size_t i{1}; i < 16; ++i) {
                        auto n = new cluster_node(i);

                        all_nodes.emplace_back(n);
                        cluster_state.all_nodes.emplace_back(n);
                        cluster_state.nodes.emplace(i, std::move(n));
                        cluster_state.all_nodes_dirty = true;
                }

                INFO("Becoming cluster aware");
                cluster_state._name[0]  = 'X';
                cluster_state._name_len = 1;

                INFO("Assuming identity");
                cluster_state.local_node.ref = all_nodes[0];
                cluster_state.local_node.id  = all_nodes[0]->id;
                tank_listen_ep               = Switch::endpoint{BUILD_IPADDR4(127, 0, 0, 1), 11011};

                SECTION("leadership") {
                        REQUIRE(cluster_state.leader_id == 0);

                        SECTION("undefined leader") {
                                cluster_state.updates.cluster_leader.defined = true;
                                cluster_state.updates.cluster_leader.nid     = 255;
                                apply_cluster_state_updates();
                                INFO("Expecting no changes because cluster_leader.nid is bogus");
                                REQUIRE(cluster_partitions_dirty.empty());
                        }

                        SECTION("leader self") {
                                cluster_state.updates.cluster_leader.defined = true;
                                cluster_state.updates.cluster_leader.nid     = cluster_state.local_node.id;
                                apply_cluster_state_updates();
                                INFO("Expecting to fail because node is unavailable");
                                REQUIRE(cluster_partitions_dirty.empty());
                        }

                        SECTION("set local node ep,AVAILABLE and leader self") {
                                cluster_state.updates.nodes[cluster_state.local_node.ref] = std::make_pair(tank_listen_ep, true);
                                apply_cluster_state_updates();
                                INFO("Expecting no updates");
                                REQUIRE(cluster_partitions_dirty.empty());
                                REQUIRE(cluster_state.leader_id == 0);

                                cluster_state.updates.cluster_leader.defined = true;
                                cluster_state.updates.cluster_leader.nid     = cluster_state.local_node.id;
                                apply_cluster_state_updates();
                                INFO("Expecting updates");
                                REQUIRE(cluster_state.leader_id == all_nodes[0]->id);
                                REQUIRE(cluster_partitions_dirty.empty());

                                SECTION("Expect No Updates") {
                                        apply_cluster_state_updates();
                                        REQUIRE(cluster_partitions_dirty.empty());
                                }

                                SECTION("Partitions") {
                                        INFO("Creating a bunch of partitions");
                                        partition_config               pc;
                                        auto                           _t = Switch::make_sharedref<topic>("foobar", pc);
                                        auto                           t  = _t.get();
                                        std::vector<topic_partition *> all_partitions;

                                        REQUIRE(t->use_count() == 1);
                                        register_topic(_t.release());

                                        for (size_t i{0}; i < 16; ++i) {
                                                auto p = define_partition(i, t);

                                                REQUIRE(p->use_count() == 1);
                                                all_partitions.emplace_back(p.release());
                                        }

                                        t->register_partitions(all_partitions.data(), all_partitions.size());
                                        t->total_enabled_partitions = all_partitions.size();

                                        SECTION("a") {
                                                auto part = cluster_state.updates.get_partition(all_partitions[0]);

                                                apply_cluster_state_updates();
                                                INFO("Didn't register any updates for partition");
                                                REQUIRE(all_partitions.front()->cluster.replicas.nodes.empty());
                                                REQUIRE(cluster_partitions_dirty.empty());

                                                INFO("Providing Replicas");
                                                part                   = cluster_state.updates.get_partition(all_partitions[0]);
                                                part->replicas.updated = true;
                                                part->replicas.nodes.emplace_back(all_nodes[0]);
                                                apply_cluster_state_updates();
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.size() == 1);
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.front() == all_nodes.front());
                                                REQUIRE(cluster_partitions_dirty.size() == 1);
                                                REQUIRE(cluster_partitions_dirty.front() == all_partitions[0]);

                                                INFO("Repeating and expecting no changes");
                                                part                   = cluster_state.updates.get_partition(all_partitions[0]);
                                                part->replicas.updated = true;
                                                part->replicas.nodes.emplace_back(all_nodes[0]);
                                                apply_cluster_state_updates();
                                                REQUIRE(cluster_partitions_dirty.size() == 0);
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.size() == 1);
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.front() == all_nodes.front());
                                                REQUIRE(cluster_partitions_dirty.front() == all_partitions[0]);

                                                INFO("Adding another replica");
                                                part                   = cluster_state.updates.get_partition(all_partitions[0]);
                                                part->replicas.updated = true;
                                                part->replicas.nodes.emplace_back(all_nodes[0]);
                                                part->replicas.nodes.emplace_back(all_nodes[1]);
                                                apply_cluster_state_updates();
                                                REQUIRE(cluster_partitions_dirty.size() == 1);
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.size() == 2);
                                                REQUIRE(cluster_partitions_dirty.front() == all_partitions[0]);

                                                INFO("Removing one replica");
                                                part                   = cluster_state.updates.get_partition(all_partitions[0]);
                                                part->replicas.updated = true;
                                                part->replicas.nodes.emplace_back(all_nodes[1]);
                                                apply_cluster_state_updates();
                                                REQUIRE(cluster_partitions_dirty.size() == 1);
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.size() == 1);
                                                REQUIRE(all_partitions[0]->cluster.replicas.nodes.front() == all_nodes[1]);
                                                REQUIRE(cluster_partitions_dirty.front() == all_partitions[0]);

                                                INFO("Setting dummy leader");
                                                part                 = cluster_state.updates.get_partition(all_partitions[0]);
                                                part->leader.defined = true;
                                                part->leader.id      = all_nodes.front()->id;
                                                apply_cluster_state_updates();
                                                REQUIRE(cluster_partitions_dirty.empty());

                                                INFO("Setting real leader");
                                                REQUIRE(all_partitions.front()->cluster.leader.node == nullptr);
                                                part                 = cluster_state.updates.get_partition(all_partitions[0]);
                                                part->leader.defined = true;
                                                part->leader.id      = all_nodes[1]->id;
                                                apply_cluster_state_updates();
                                                INFO("Require that we marked as dirty because we need to select a new leader");
                                                REQUIRE(cluster_partitions_dirty.size() == 1);

                                                INFO("Making node available");
                                                cluster_state.updates.nodes[all_nodes[1]] = std::make_pair(Switch::endpoint{BUILD_IPADDR4(127, 0, 0, 2), 11011}, true);
                                                apply_cluster_state_updates();
                                        }
                                }
                        }
                }
        }
}

#include <ext/Catch/catch.hpp>
#include "tank_client.h"

TEST_CASE_METHOD(TankClient, "connections") {
        auto c = get_connection();

        SECTION("basics") {
                REQUIRE(c->in.b == nullptr);
                REQUIRE(c->fd == -1);
                REQUIRE(c->list.empty());
                REQUIRE(c->all_conns_list_ll.empty());
        }
}

TEST_CASE_METHOD(TankClient, "misc") {
        now_ms = 0;

        SECTION("retries") {
                request_partition_ctx *ptrs[2], *p;
                api_request            api_req;

                api_req.reset();
                api_req.type = api_request::Type::Consume;

                ptrs[0] = static_cast<request_partition_ctx *>(malloc(sizeof(request_partition_ctx)));
                ptrs[1] = static_cast<request_partition_ctx *>(malloc(sizeof(request_partition_ctx)));

                p = ptrs[0];
                p->reset();
                p->topic.set("orders");
                p->partition = 0;

                p = ptrs[1];
                p->reset();
                p->topic.set("orders");
                p->partition = 1;

                REQUIRE(api_req.retry_bundles_list.empty());
                schedule_retry(&api_req, ptrs, sizeof_array(ptrs), 10);
                REQUIRE_FALSE(api_req.retry_bundles_list.empty());
                REQUIRE(api_req.retry_bundles_list.size() == 1);

                auto b = switch_list_entry(retry_bundle, retry_bundles_ll, api_req.retry_bundles_list.next);

                REQUIRE(b->api_req == &api_req);
                REQUIRE(b->size == sizeof_array(ptrs));
                REQUIRE(b->expiration == now_ms + 10);

                SECTION("do.abort") {
                        std::vector<request_partition_ctx *> contexts;

                        REQUIRE(b->node.node.leaf_p != nullptr);
                        eb64_delete(&b->node);
                        REQUIRE(b->node.node.leaf_p == nullptr);

                        abort_api_request_retry_bundles(&api_req, &contexts);

                        REQUIRE(api_req.retry_bundles_list.empty());
                }

                SECTION("do.retry") {
                        REQUIRE(b->node.node.leaf_p != nullptr);
                        eb64_delete(&b->node);
                        REQUIRE(b->node.node.leaf_p == nullptr);
                }
        }
}

TEST_CASE_METHOD(TankClient, "abort.1") {
        now_ms = 0;

        auto                                 api_req = get_api_request(10);
        auto                                 br_req1 = get_broker_api_request();
        auto                                 br_req2 = get_broker_api_request();
        std::vector<request_partition_ctx *> contexts;

	REQUIRE(rsrc_tracker.api_request == 1);
	REQUIRE(rsrc_tracker.broker_api_request == 2);


        br_req1->api_req = api_req.get();
        br_req2->api_req = api_req.get();

        api_req->broker_requests_list.push_back(&br_req1->broker_requests_list_ll);
        api_req->broker_requests_list.push_back(&br_req2->broker_requests_list_ll);

        REQUIRE(api_req->broker_requests_list.size() == 2);

        abort_api_request_brokers_reqs(api_req.get(), &contexts);
        REQUIRE(api_req->broker_requests_list.empty());
}

TEST_CASE_METHOD(TankClient, "process_undeliverable_broker_req") {
        const auto make_breq_part = [this](const auto topic_name, const auto partition) {
                auto topic    = intern_topic(topic_name);
                auto req_part = get_request_partition_ctx();
                auto e        = leader_for(topic, partition);
                auto br       = broker_by_endpoint(e);

                req_part->topic     = topic;
                req_part->partition = partition;

                return std::make_pair(br, req_part);
        };

        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;
        auto                                                      ar = get_api_request(10);

        set_leader("orders"_s8, 1, "10.5.5.20:8182"_s32);
        set_leader("orders"_s8, 0, "10.5.5.10:8182"_s32);

        contexts.emplace_back(make_breq_part("orders"_s8, 0));
        contexts.emplace_back(make_breq_part("orders"_s8, 1));

        REQUIRE(ar->broker_requests_list.empty());
        auto it = assign_req_partitions_to_api_req(ar.get(), &contexts);
        REQUIRE(ar->broker_requests_list.size() == 2);

        auto breq1 = switch_list_entry(broker_api_request, broker_requests_list_ll, ar->broker_requests_list.next);

        REQUIRE(it == &breq1->broker_requests_list_ll);

        auto breq2 = switch_list_entry(broker_api_request, broker_requests_list_ll, ar->broker_requests_list.next->next);

        SECTION("first") {
                process_undeliverable_broker_req(breq1, true);
        }

        SECTION("first - with connection") {
                auto c  = get_connection();
                auto br = contexts.front().first;

                c->fd         = open("/tmp/dummy.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                c->type       = connection::Type::Tank;
                c->as.tank.br = br;
                br->ch.set(c);

                process_undeliverable_broker_req(breq1, true);
        }

        SECTION("first - with connection - pending outgoing payload") {
                auto c  = get_connection();
                auto br = contexts.front().first;
                auto p1 = build_broker_req_payload(breq1);

                br->outgoing_content.push_back(p1);

                c->fd         = open("/tmp/dummy.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                c->type       = connection::Type::Tank;
                c->as.tank.br = br;
                br->ch.set(c);

                process_undeliverable_broker_req(breq1, true);
        }

        SECTION("first - with connection - pending outgoing payload") {
                auto c  = get_connection();
                auto br = contexts.front().first;
                auto p1 = build_broker_req_payload(breq1);

                br->outgoing_content.push_back(p1);

                c->fd         = open("/tmp/dummy.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                c->type       = connection::Type::Tank;
                c->as.tank.br = br;
                br->ch.set(c);

                REQUIRE(br->outgoing_content.size() == 1);
                process_undeliverable_broker_req(breq1, true);
                REQUIRE(br->outgoing_content.size() == 0);
        }

        SECTION("first - with connection - pending outgoing payload - partially transferred") {
                auto c  = get_connection();
                auto br = contexts.front().first;
                auto p1 = build_broker_req_payload(breq1);

                br->outgoing_content.push_back(p1);
                br->outgoing_content.first_payload_partially_transferred = true;

                TANK_EXPECT(br->outgoing_content.front()->broker_req == breq1);

                c->fd         = open("/tmp/dummy.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                c->type       = connection::Type::Tank;
                c->as.tank.br = br;
                br->ch.set(c);

                process_undeliverable_broker_req(breq1, true);
        }

        SECTION("first - with connection - pending outgoing payload - partially transferred - have another payload") {
                auto c  = get_connection();
                auto br = contexts.front().first;
                auto p1 = build_broker_req_payload(breq1);
		auto p2 = build_broker_req_payload(breq2);

                br->outgoing_content.push_back(p1);
		br->outgoing_content.push_back(p2);
                br->outgoing_content.first_payload_partially_transferred = true;

                TANK_EXPECT(br->outgoing_content.front()->broker_req == breq1);

                c->fd         = open("/tmp/dummy.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                c->type       = connection::Type::Tank;
                c->as.tank.br = br;
                br->ch.set(c);

                process_undeliverable_broker_req(breq1, true);
        }
}

TEST_CASE_METHOD(TankClient, "bokers_util:main") {
        auto                                                      ar = get_api_request(100);
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        now_ms = 0;

        ar->as.consume.max_wait = 100;
        ar->as.consume.min_size = 100;
        ar->type                = api_request::Type::Consume;

        const auto make_breq_part = [this](const auto topic_name, const auto partition) {
                auto topic    = intern_topic(topic_name);
                auto req_part = get_request_partition_ctx();
                auto e        = leader_for(topic, partition);
                auto br       = broker_by_endpoint(e);

                req_part->topic     = topic;
                req_part->partition = partition;

                return std::make_pair(br, req_part);
        };

        auto [br, req_part] = make_breq_part("orders"_s8, 0);

        req_part->as_op.consume.seq_num        = 1;
        req_part->as_op.consume.min_fetch_size = 100;
        contexts.emplace_back(std::make_pair(br, req_part));

        REQUIRE(contexts.size() == 1);

        assign_req_partitions_to_api_req(ar.get(), &contexts);

        REQUIRE(ar->broker_requests_list.size() == 1);
        auto breq = switch_list_entry(broker_api_request, broker_requests_list_ll, ar->broker_requests_list.next);

        REQUIRE(breq->partitions_list.size() == 1);
        REQUIRE(br->outgoing_content.size() == 0);
        REQUIRE(br->outgoing_content.empty());
        REQUIRE(br->can_safely_abort_first_payload());

        SECTION("payload.1") {
                auto payload = build_broker_req_payload(breq);

                REQUIRE(payload->broker_req == breq);

                br->outgoing_content.push_back(payload);
                REQUIRE(br->outgoing_content.size() == 1);
                REQUIRE_FALSE(br->outgoing_content.empty());

                SECTION("partilly_transferred") {
                        auto p = br->outgoing_content.front();

                        br->outgoing_content.first_payload_partially_transferred = true;
                        REQUIRE(br->outgoing_content.size() == 1);
                        REQUIRE_FALSE(br->can_safely_abort_first_payload());
                        REQUIRE_FALSE(br->can_safely_abort_broker_request_payload(breq));
                        REQUIRE(br->abort_broker_request_payload(nullptr) == nullptr);
                        REQUIRE(br->abort_broker_request_payload(breq));
                        REQUIRE(br->outgoing_content.empty());
                        REQUIRE(br->outgoing_content.size() == 0);
                }

                SECTION("not partially transferred") {
                        auto p = br->outgoing_content.front();

                        REQUIRE(br->can_safely_abort_first_payload());
                        REQUIRE(br->can_safely_abort_broker_request_payload(breq));
                        REQUIRE(br->abort_broker_request_payload(breq) != nullptr);
                        REQUIRE(br->outgoing_content.empty());
                        REQUIRE(br->outgoing_content.size() == 0);
                }

                SECTION("multiple") {
                        auto [br, req_part] = make_breq_part("orders"_s8, 1);

                        req_part->as_op.consume.seq_num        = 1;
                        req_part->as_op.consume.min_fetch_size = 100;

                        contexts.clear();
                        contexts.emplace_back(std::make_pair(br, req_part));

                        auto it = assign_req_partitions_to_api_req(ar.get(), &contexts);

                        REQUIRE(it != &ar->broker_requests_list);

                        auto breq2   = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                        auto payload = build_broker_req_payload(breq2);

                        REQUIRE(br->outgoing_content.size() == 1);
                        br->outgoing_content.push_back(payload);
                        REQUIRE(br->outgoing_content.size() == 2);

                        REQUIRE(br->can_safely_abort_first_payload());
                        REQUIRE(br->abort_broker_request_payload(nullptr) == nullptr);
                        REQUIRE(br->outgoing_content.size() == 2);

                        SECTION("x2") {
                                SECTION("not partially transferred") {
                                        REQUIRE(br->can_safely_abort_broker_request_payload(breq));
                                        REQUIRE(br->can_safely_abort_broker_request_payload(breq2));

                                        REQUIRE(br->outgoing_content.back() != nullptr);
                                        REQUIRE(br->outgoing_content.back()->broker_req == breq2);
                                        REQUIRE(br->abort_broker_request_payload(breq2) != nullptr);
                                        REQUIRE(br->outgoing_content.size() == 1);
                                        REQUIRE(br->outgoing_content.front()->broker_req == breq);
                                }

                                SECTION("partially transferred") {
                                        br->outgoing_content.first_payload_partially_transferred = true;
                                        REQUIRE_FALSE(br->can_safely_abort_broker_request_payload(breq));
                                        REQUIRE(br->can_safely_abort_broker_request_payload(breq2));
                                }
                        }
                }
        }

        SECTION("flush_broker") {
                flush_broker(br);

                REQUIRE(br->outgoing_content.empty());
                REQUIRE(br->outgoing_content.size() == 0);
        }
}

TEST_CASE_METHOD(TankClient, "abort.2") {
        const auto make_breq_part = [this](const auto topic_name, const auto partition) {
                auto topic    = intern_topic(topic_name);
                auto req_part = get_request_partition_ctx();
                auto e        = leader_for(topic, partition);
                auto br       = broker_by_endpoint(e);

                req_part->topic     = topic;
                req_part->partition = partition;

                return std::make_pair(br, req_part);
        };
        auto                                                      ar = get_api_request(10);
        std::vector<std::pair<broker *, request_partition_ctx *>> v1, v2;

        ar->type = api_request::Type::Consume;
        now_ms   = 0;

        set_leader("orders"_s8, 0, "10.5.5.20:1");
        set_leader("orders"_s8, 1, "10.5.5.20:1");

        v1.emplace_back(make_breq_part("orders"_s8, 0));

        // use assign_req_partitions_to_api_req() twice
        // because otherwise we 'd end up with a single broker_api_request
        auto it = assign_req_partitions_to_api_req(ar.get(), &v1);
        REQUIRE(it != &ar->broker_requests_list);

        v2.emplace_back(make_breq_part("orders"_s8, 1));
        auto it2 = assign_req_partitions_to_api_req(ar.get(), &v2);
        REQUIRE(it2 != &ar->broker_requests_list);

        REQUIRE(ar->broker_requests_list.size() == 2);
        REQUIRE(switch_list_entry(broker_api_request, broker_requests_list_ll, it)->br == v1[0].first);
        REQUIRE(switch_list_entry(broker_api_request, broker_requests_list_ll, it->next)->br == v2[0].first);

        REQUIRE(switch_list_entry(broker_api_request, broker_requests_list_ll, ar->broker_requests_list.next)->br == v1[0].first);
        REQUIRE(switch_list_entry(broker_api_request, broker_requests_list_ll, ar->broker_requests_list.next->next)->br == v2[0].first);

        SECTION("flush_broker:no outgoing content") {
                auto br = v1[0].first;

                REQUIRE(br->outgoing_content.empty());
                flush_broker(br);
                REQUIRE(br->outgoing_content.empty());
        }

        SECTION("flush_broker:have outgoing content") {
                auto br      = v1[0].first;
                auto breq    = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                auto payload = build_broker_req_payload(breq);

                br->outgoing_content.push_back(payload);
                REQUIRE(br->outgoing_content.size() == 1);
                flush_broker(br);
                // will be 1 because we have rescheduled that
                REQUIRE(br->outgoing_content.size() == 1);
        }

        SECTION("abort_broker_req:no connection") {
                auto breq = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                auto br   = breq->br;

                REQUIRE(breq->partitions_list.size() == 1);

                auto       p         = switch_list_entry(request_partition_ctx, partitions_list_ll, breq->partitions_list.next);
                const auto topic     = p->topic;
                const auto partition = p->partition;

                REQUIRE(ar->broker_requests_list.size() == 2);
                REQUIRE(br);
                REQUIRE(br->ch.get() == nullptr);

                REQUIRE(all_captured_faults.empty());
                abort_broker_req(breq);
                REQUIRE(all_captured_faults.size() == 1);

                auto fault = all_captured_faults.front();

                REQUIRE(fault.type == fault::Type::Timeout);
                REQUIRE(fault.topic == topic);
                REQUIRE(fault.partition == partition);

                REQUIRE(ar->broker_requests_list.size() == 1);
                REQUIRE(br);
        }

        SECTION("abort_broker_req:have connection:no outgoing content") {
                auto breq = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                auto br   = breq->br;
                auto c    = get_connection();

                c->fd = open("/tmp/dummy1.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                br->ch.set(c);

                REQUIRE(breq->partitions_list.size() == 1);

                auto       p         = switch_list_entry(request_partition_ctx, partitions_list_ll, breq->partitions_list.next);
                const auto topic     = p->topic;
                const auto partition = p->partition;

                REQUIRE(ar->broker_requests_list.size() == 2);
                REQUIRE(br);

                REQUIRE(all_captured_faults.empty());
                abort_broker_req(breq);
                REQUIRE(all_captured_faults.size() == 1);

                auto fault = all_captured_faults.front();

                REQUIRE(fault.type == fault::Type::Timeout);
                REQUIRE(fault.topic == topic);
                REQUIRE(fault.partition == partition);

                REQUIRE(ar->broker_requests_list.size() == 1);
                REQUIRE(br);
        }

        SECTION("abort_broker_req:have connection:have outgoing content") {
                auto breq    = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                auto br      = breq->br;
                auto c       = get_connection();
                auto payload = build_broker_req_payload(breq);

                br->outgoing_content.push_back(payload);

                c->fd = open("/tmp/dummy1.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                br->ch.set(c);

                REQUIRE(breq->partitions_list.size() == 1);

                auto       p         = switch_list_entry(request_partition_ctx, partitions_list_ll, breq->partitions_list.next);
                const auto topic     = p->topic;
                const auto partition = p->partition;

                REQUIRE(ar->broker_requests_list.size() == 2);
                REQUIRE(br);

                REQUIRE(all_captured_faults.empty());
                abort_broker_req(breq);
                REQUIRE(all_captured_faults.size() == 1);

                auto fault = all_captured_faults.front();

                REQUIRE(fault.type == fault::Type::Timeout);
                REQUIRE(fault.topic == topic);
                REQUIRE(fault.partition == partition);

                REQUIRE(ar->broker_requests_list.size() == 1);
                REQUIRE(br);

                REQUIRE(br->ch.get());
        }

        SECTION("abort_broker_req:have connection:have outgoing content:partial transfer") {
                auto breq    = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                auto br      = breq->br;
                auto c       = get_connection();
                auto payload = build_broker_req_payload(breq);

                br->outgoing_content.push_back(payload);
                br->outgoing_content.first_payload_partially_transferred = true;

                c->fd = open("/tmp/dummy1.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                br->ch.set(c);

                REQUIRE(breq->partitions_list.size() == 1);

                auto       p         = switch_list_entry(request_partition_ctx, partitions_list_ll, breq->partitions_list.next);
                const auto topic     = p->topic;
                const auto partition = p->partition;

                REQUIRE(ar->broker_requests_list.size() == 2);
                REQUIRE(br);

                REQUIRE(all_captured_faults.empty());
                abort_broker_req(breq);
                REQUIRE(all_captured_faults.size() == 1);

                auto fault = all_captured_faults.front();

                REQUIRE(fault.type == fault::Type::Timeout);
                REQUIRE(fault.topic == topic);
                REQUIRE(fault.partition == partition);

                REQUIRE(ar->broker_requests_list.size() == 1);
                REQUIRE(br);

                // should have shut it down
                REQUIRE(br->ch.get() == nullptr);
        }

        SECTION("abort_broker_req:have connection:have outgoing content:partial transfer for other req") {
                auto breq     = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
                auto breq2    = switch_list_entry(broker_api_request, broker_requests_list_ll, it->next);
                auto br       = breq->br;
                auto c        = get_connection();
                auto payload  = build_broker_req_payload(breq);
                auto payload2 = build_broker_req_payload(breq2);

                br->outgoing_content.push_back(payload);
                br->outgoing_content.push_back(payload2);
                br->outgoing_content.first_payload_partially_transferred = true;

                c->fd = open("/tmp/dummy1.data", O_WRONLY | O_TRUNC | O_CREAT, 0775);
                REQUIRE(c->fd != -1);
                br->ch.set(c);
                REQUIRE(br->ch.get() != nullptr);

                REQUIRE(breq->partitions_list.size() == 1);

                auto       p         = switch_list_entry(request_partition_ctx, partitions_list_ll, breq2->partitions_list.next);
                const auto topic     = p->topic;
                const auto partition = p->partition;

                REQUIRE(ar->broker_requests_list.size() == 2);
                REQUIRE(br);

                REQUIRE(all_captured_faults.empty());
                abort_broker_req(breq2);
                REQUIRE(all_captured_faults.size() == 1);

                auto fault = all_captured_faults.front();

                REQUIRE(fault.type == fault::Type::Timeout);
                REQUIRE(fault.topic == topic);
                REQUIRE(fault.partition == partition);

                REQUIRE(ar->broker_requests_list.size() == 1);
                REQUIRE(br);

                // should have retained it
                REQUIRE(br->ch.get() != nullptr);
        }
}

TEST_CASE_METHOD(TankClient, "reachability") {
        const auto make_breq_part = [this](const auto topic_name, const auto partition) {
                auto topic    = intern_topic(topic_name);
                auto req_part = get_request_partition_ctx();
                auto e        = leader_for(topic, partition);
                auto br       = broker_by_endpoint(e);

                req_part->topic     = topic;
                req_part->partition = partition;

                return std::make_pair(br, req_part);
        };
        auto [br, part] = make_breq_part("orders"_s8, 0);
        std::vector<std::pair<broker *, request_partition_ctx *>> v;
        auto                                                      ar = get_api_request(10);

        ar->type = api_request::Type::Consume;
        v.emplace_back(std::make_pair(br, part));

        auto it = assign_req_partitions_to_api_req(ar.get(), &v);

        REQUIRE(it != &ar->broker_requests_list);

        auto breq = switch_list_entry(broker_api_request, broker_requests_list_ll, it);

        SECTION("blocked too many failures") {
                br->reachability  = broker::Reachability::Blocked;
                br->blocked_until = 10;

                REQUIRE(schedule_broker_req(breq));
                REQUIRE(br->consequtive_connection_failures == broker::max_consequtive_connection_failures - 1);
                REQUIRE(br->reachability == broker::Reachability::LikelyUnreachable);
                REQUIRE(br->blocked_until == 0);
                REQUIRE(br->outgoing_content.size() == 1);
        }

        SECTION("blocked remaining blocked") {
                br->reachability  = broker::Reachability::Blocked;
                br->blocked_until = now_ms + 100000;

                REQUIRE_FALSE(schedule_broker_req(breq));
                REQUIRE(br->reachability == broker::Reachability::Blocked);
                REQUIRE(br->outgoing_content.empty());
        }

        SECTION("likely reachable") {
                br->reachability = broker::Reachability::LikelyReachable;

                REQUIRE(schedule_broker_req(breq));
                REQUIRE(br->reachability == broker::Reachability::LikelyReachable);
                REQUIRE(br->outgoing_content.size() == 1);
        }

        SECTION("unreachable") {
                br->consequtive_connection_failures = 1;
                make_unreachable(br);
                REQUIRE(br->reachability == broker::Reachability::LikelyUnreachable);

                REQUIRE(schedule_broker_req(breq));
                REQUIRE(br->reachability == broker::Reachability::LikelyUnreachable);
                REQUIRE(br->outgoing_content.size() == 1);

                SECTION("check_unreachable_brokers") {
                        now_ms = br->unreachable_brokers_tree_node.key + 1;

                        REQUIRE(br->unreachable_brokers_tree_node.node.leaf_p != nullptr);
                        check_unreachable_brokers();
                        REQUIRE(br->unreachable_brokers_tree_node.node.leaf_p == nullptr);
                        REQUIRE(br->consequtive_connection_failures < broker::max_consequtive_connection_failures);
                }
        }
}

TEST_CASE_METHOD(TankClient, "responses tracker") {
        const auto make_breq_part = [this](const auto topic_name, const auto partition) {
                auto topic    = intern_topic(topic_name);
                auto req_part = get_request_partition_ctx();
                auto e        = leader_for(topic, partition);
                auto br       = broker_by_endpoint(e);

                req_part->topic     = topic;
                req_part->partition = partition;

                return std::make_pair(br, req_part);
        };
        std::vector<std::pair<broker *, request_partition_ctx *>> v;
        auto                                                      ar = get_api_request(10);

        v.emplace_back(make_breq_part("orders"_s8, 0));
        auto it = assign_req_partitions_to_api_req(ar.get(), &v);
        auto br = v.front().first;
        auto c  = get_connection();

        c->type       = connection::Type::Tank;
        c->fd         = open("/tmp/DATA1.dummy", O_WRONLY | O_CREAT | O_TRUNC, 0775);
        c->as.tank.br = br;

        auto br_req = switch_list_entry(broker_api_request, broker_requests_list_ll, it);
        auto p      = build_broker_req_payload(br_req);

        REQUIRE(br->outgoing_content.empty());
        br->outgoing_content.push_back(p);
        REQUIRE(br->outgoing_content.size() == 1);

        REQUIRE(br->pending_responses_list.empty());
        tx_tank(c);
        REQUIRE(br->pending_responses_list.size() == 1);

        SECTION("flush: have connection") {
                br->ch.set(c);
                flush_broker(br);
                REQUIRE(br->pending_responses_list.size() == 1);
        }

        SECTION("flush: no connection") {
                flush_broker(br);
                REQUIRE(br->pending_responses_list.size() == 0);
        }
}

TEST_CASE_METHOD(TankClient, "consume response") {
        IOBuffer b;
        auto     c = get_connection();

        b.ReadFromFile("/tmp/consume.resp");

        const auto len  = b.size();
        const auto data = reinterpret_cast<const uint8_t *>(b.data());

        c->in.b = get_managed_buffer();

        SECTION("unknown request") {
                process_consume(c, data, len);
        }

        SECTION("known request") {
                for (size_t i{7700 - 1}; i < 7700 + 256 && i < len; ++i) {
                        //for (size_t i{0}; i < 512 && i < len; i += 5) {
                        Print("For ", i, "\n\n");

                        auto part   = get_request_partition_ctx();
                        auto br_req = get_broker_api_request();
                        auto ar     = get_api_request(10);

                        part->topic.set(_S("overseer"));
                        part->partition             = 0;
                        part->as_op.consume.seq_num = 0;

                        br_req->br      = nullptr;
                        br_req->api_req = ar.get();
                        br_req->id      = 1;

                        br_req->partitions_list.push_back(&part->partitions_list_ll);
                        pending_brokers_requests.emplace(br_req->id, br_req);
                        ar->broker_requests_list.push_front(&br_req->broker_requests_list_ll);

                        const auto res = process_consume(c, data, i);

                        if (i < 42) {
                                REQUIRE_FALSE(res);
                        }
                }
        }
}

TEST_CASE_METHOD(TankClient, "api request expiration") {
        auto ar = get_api_request(10);

        REQUIRE(eb_is_empty(&api_reqs_expirations_tree));
        track_pending_resp(std::move(ar));
        REQUIRE_FALSE(eb_is_empty(&api_reqs_expirations_tree));

        now_ms += 10;

        check_pending_api_responses();

        REQUIRE(eb_is_empty(&api_reqs_expirations_tree));
}

TEST_CASE_METHOD(TankClient, "fail_api_request") {
	auto ar = get_api_request(10);

	fail_api_request(std::move(ar));
}

TEST_CASE("basics") {
	struct ent final {
		const int id;
		switch_dlist ll{&ll, &ll};

		ent(const int i)
			: id{i} {
		}
	};

	ent a{0}, b{1}, c{2}, d{3};
	switch_dlist L{&L, &L};

	L.reset();
	a.ll.reset(); L.push_back(&a.ll); 
	b.ll.reset(); L.push_back(&b.ll);
	c.ll.reset(); L.push_back(&c.ll);

	TANK_EXPECT(L.size() == 3);
	TANK_EXPECT(switch_list_entry(ent, ll, L.next) == &c);
	TANK_EXPECT(switch_list_entry(ent, ll, L.next->next) == &b);
	TANK_EXPECT(switch_list_entry(ent, ll, L.next->next->next) == &a);

	L.reset();
	a.ll.reset(); L.push_front(&a.ll); 
	b.ll.reset(); L.push_front(&b.ll);
	c.ll.reset(); L.push_front(&c.ll);

	TANK_EXPECT(L.size() == 3);
	TANK_EXPECT(switch_list_entry(ent, ll, L.prev) == &c);
	TANK_EXPECT(switch_list_entry(ent, ll, L.prev->prev) == &b);
	TANK_EXPECT(switch_list_entry(ent, ll, L.prev->prev->prev) == &a);
}

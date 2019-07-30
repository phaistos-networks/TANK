#include "client_common.h"
#include <sys/eventfd.h>

void TankClient::wait_scheduled(const uint32_t req_id) {
        TANK_EXPECT(req_id);

        while (should_poll()) {
                poll(1000);

                if (unlikely(!faults().empty())) {
                        throw Switch::data_error("Fault while waiting responses");
                }

                for (const auto &it : produce_acks()) {
                        if (it.clientReqId == req_id) {
                                return;
                        }
                }

                for (const auto &it : consumed()) {
                        if (it.clientReqId == req_id) {
                                return;
                        }
                }
        }
}

TankClient::TankClient(const strwlen32_t endpoints) {
        interrupt_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);

        if (-1 == interrupt_fd) {
                throw Switch::system_error("eventfd() failed:", strerror(errno));
        }

        poller.insert(interrupt_fd, EPOLLIN, &interrupt_fd);

        for (auto s : endpoints.split(',')) {
                s.TrimWS();

                if (const auto e = Switch::ParseSrvEndpoint(s, {_S("tank")}, 11011); !e) {
                        throw Switch::system_error("Failed to parse TANK endpoint");
                } else {
                        auto b = broker_by_endpoint(e); // will insert broker into all_brokers

                        // this is an important broker
                        // reset() should retain it
                        b->flags |= 1u << unsigned(broker::Flags::Important);
                }
        }
}

TankClient::~TankClient() {
        if (-1 != interrupt_fd) {
                close(interrupt_fd);
        }

        reset(true);
}

// XXX: we may have missed something here, and it's important that we get it right otherwise we
// - may leak memory
// - may wind up dereferencing memory that's been released here but something in the memory region lingers
//
// reset() may be invoked fairly frequently e.g when a fault fault::Type::TIMEOUT is returned to the client
// so it's important that we get this right. It's common to get a timeout when e.g your max_wait for consume() is low. 
//
// However, it is also invoked in the dtor, so even if it is never invoked by the TANK client application,
// make sure there are no leaks is important.
void TankClient::reset(const bool dtor_context) {
        static constexpr const bool       trace{false};
        std::vector<broker_api_request *> reqs;
        auto                              maybe_reuse_allocator = [](auto &allocator) {
#ifdef TANK_RUNTIME_CHECKS
                // reset so that ASAN will catch use-after-free
                // as opposed to reusing which will just result in weird silent corruption
                allocator.reset();
#else
                allocator.reuse();
#endif
        };

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, "BEGIN: RESETTING CLIENT \n\n\n\n\n", ansifmt::reset, "\n");
        }

        for (auto it = brokers.begin(); it != brokers.end();) {
                auto br = it->second.get();

                for (auto it = br->outgoing_content.front(); it;) {
                        auto br_req = it->broker_req;
                        auto next   = it->next;

                        reqs.emplace_back(br_req);
                        put_payload(it, __LINE__);

                        it = next;
                }
                br->outgoing_content.clear();

                for (auto it : br->pending_responses_list) {
                        auto breq = containerof(broker_api_request, pending_responses_list_ll, it);

                        reqs.emplace_back(breq);
                }
                br->pending_responses_list.reset();

                eb64_delete(&br->unreachable_brokers_tree_node);

                br->reachability = broker::Reachability::MaybeReachable;
                br->ch.reset();

                if (dtor_context || (0 == (br->flags & (1u << unsigned(broker::Flags::Important))))) {
                        // we cannot e.g brokers.clear; all_brokers.reset()
                        // because we would be stuck without any brokers, and any_broker() would abort
                        // we are going to retain the "important" brokers and get rid of all other brokers/endpoints
                        if (trace) {
                                SLog("Will GC broker ", br->ep, "\n");
                        }

                        br->all_brokers_ll.try_detach_and_reset();
                        it = brokers.erase(it);
                } else {
                        if (trace) {
                                SLog("Will retain broker ", br->ep, "\n");
                        }

                        br->ch.reset();
                        br->consequtive_connection_failures = 0;

                        ++it;
                }
        }

        if (trace) {
                SLog("GC ", reqs.size(), " broker api reqs\n");
        }

        for (auto br_req : reqs) {
                while (!br_req->partitions_list.empty()) {
                        auto part    = containerof(request_partition_ctx, partitions_list_ll,
                                                br_req->partitions_list.next);
                        auto api_req = br_req->api_req;

                        TANK_EXPECT(api_req);

                        part->partitions_list_ll.detach();
                        clear_request_partition_ctx(api_req, part);
                        put_request_partition_ctx(part);
                }
                put_broker_api_request(br_req);
        }

        if (trace) {
                SLog("pending_brokers_requests.size() = ", pending_brokers_requests.size(), "\n");
        }

        for (auto [_, breq] : pending_brokers_requests) {
                auto api_req{breq->api_req};

                for (auto it : breq->partitions_list) {
                        auto part = containerof(request_partition_ctx, partitions_list_ll, it);

                        clear_request_partition_ctx(api_req, part);
                }
        }
        pending_brokers_requests.clear();

        if (trace) {
                SLog("all_conns_list.size() = ", all_conns_list.size(), "\n");
        }

        while (!all_conns_list.empty()) {
                auto c = containerof(connection, all_conns_list_ll, all_conns_list.next);

                if (auto fd = c->fd; fd != -1) {
                        if (trace) {
                                SLog("Closing open connection\n");
                        }

                        poller.erase(fd);
                        close(fd);
                }

#ifdef HAVE_NETIO_THROTTLE
                c->throttler.read.ll.try_detach_and_reset();
                c->throttler.write.ll.try_detach_and_reset();
#endif

                if (auto b = std::exchange(c->in.b, nullptr)) {
                        release_mb(b);
                }

                c->all_conns_list_ll.detach();
                put_connection(c);
        }
        all_conns_list.reset();
        gc_ready_responses();

        reusable_api_requests.clear();
        maybe_reuse_allocator(reqs_allocator); // affects get_broker_api_request(), get_request_partition_ctx()
        reusable_request_partition_contexts.clear();
        reusable_buffers.clear();
        reusable_managed_buffers.clear();
        reusable_payloads.clear();
        maybe_reuse_allocator(core_allocator);
        reusable_connections.clear();
        reusable_broker_api_requests.clear();

        api_reqs_expirations_tree = EB_ROOT;
        timers_ebtree_root        = EB_ROOT;
        retry_bundles_ebt_root    = EB_ROOT;
        unreachable_brokers_tree  = EB_ROOT;

        api_reqs_expirations_tree_next = timers_ebtree_next = retry_bundles_next = unreachable_brokers_tree_next = std::numeric_limits<uint64_t>::max();
#ifdef HAVE_NETIO_THROTTLE
        throttled_connections_read_list_next = throttled_connections_write_list_next = std::numeric_limits<uint64_t>::max();
#endif

        conns_pend_est_list.reset();
        conns_pend_est_next_expiration = std::numeric_limits<uint64_t>::max();

        next_conns_gen = 1;

        topics_intern_map.clear();
        maybe_reuse_allocator(resultsAllocator);

        consumed_content.clear();
        all_captured_faults.clear();
        produce_acks_v.clear();
        all_discovered_partitions.clear();
        reload_conf_results_v.clear();
        created_topics_v.clear();
        collected_cluster_status_v.clear();

        ready_responses.clear();

        sleeping            = false;
        now_ms              = 0;
        next_curtime_update = 0;
        cur_time            = 0;

        ranges.clear();
        leaders.clear();
        ready_responses.clear();
        pending_responses.clear();
        rsrc_tracker.clear();

        api_reqs_expirations_tree.b[EB_LEFT] = nullptr;
        retry_bundles_ebt_root.b[EB_LEFT]    = nullptr;
        unreachable_brokers_tree.b[EB_LEFT]  = nullptr;

        // verify
        TANK_EXPECT(reusable_broker_api_requests.empty());
        TANK_EXPECT(eb_is_empty(&api_reqs_expirations_tree));
        TANK_EXPECT(eb_is_empty(&retry_bundles_ebt_root));
        TANK_EXPECT(eb_is_empty(&unreachable_brokers_tree));
        TANK_EXPECT(brokers.size() == all_brokers.size());
        TANK_EXPECT(all_conns_list.empty());
        TANK_EXPECT(consumed_content.empty());
        TANK_EXPECT(all_captured_faults.empty());
        TANK_EXPECT(produce_acks_v.empty());
        TANK_EXPECT(all_discovered_partitions.empty());
        TANK_EXPECT(reload_conf_results_v.empty());
        TANK_EXPECT(created_topics_v.empty());
        TANK_EXPECT(collected_cluster_status_v.empty());
        TANK_EXPECT(pending_brokers_requests.empty());
        TANK_EXPECT(pending_responses.empty());
        TANK_EXPECT(reusable_api_requests.empty());
        TANK_EXPECT(reusable_request_partition_contexts.empty());
        TANK_EXPECT(reusable_buffers.empty());
        TANK_EXPECT(reusable_managed_buffers.empty());
        TANK_EXPECT(reusable_payloads.empty());
        TANK_EXPECT(reusable_connections.empty());
        TANK_EXPECT(ready_responses.empty());

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, "END: RESETTING CLIENT \n\n\n\n\n", ansifmt::reset, "\n");
        }
}

void TankClient::set_leader(const str_view8 topic, const uint16_t partition, const str_view32 endpoint) {
        const auto e = Switch::ParseSrvEndpoint(endpoint, {_S("tank")}, 11011);

        if (!e) {
                throw Switch::data_error("Unable to parse leader endpoint");
        }

        set_leader(topic, partition, e);
}

void TankClient::set_leader(const str_view8 topic, const uint16_t partition, const Switch::endpoint e) {
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "Setting leader for [", topic, "/", partition, "] to ", e, ansifmt::reset, "\n");
        }

        leaders[topic_partition{topic, partition}] = e;
}

Switch::endpoint TankClient::leader_for(const str_view8 topic, const uint16_t partition) {
        if (const auto it = leaders.find(topic_partition{topic, partition}); it != leaders.end()) {
                return it->second;
        } else if (!all_brokers.empty()) {
                return (containerof(broker, all_brokers_ll, all_brokers.next))->ep;
        } else {
                return Switch::endpoint{};
        }
}

void TankClient::interrupt_poll() {
        std::atomic_signal_fence(std::memory_order_seq_cst);
        if (sleeping.load(std::memory_order_relaxed)) {
                uint64_t one{1};

                write(interrupt_fd, &one, sizeof(one));
        }
}

#define _BUFFERS_PRESSURE_THRESHOLD (4 * 1024 * 1024)
#define _BUFFERS_POOL_THRESHOLD 512

void TankClient::put_buffer(IOBuffer *const b) {
        TANK_EXPECT(b);

        if (reusable_buffers.size() > _BUFFERS_POOL_THRESHOLD) {
                delete b;
        } else {
                b->clear();
                reusable_buffers.emplace_back(std::move(b));
        }
}

void TankClient::set_default_leader(const Switch::endpoint e) {
        if (!e) {
                throw Switch::data_error("Unable to parse default leader endpoint");
        }

        // we no longer have a default leader
        broker_by_endpoint(e);
}

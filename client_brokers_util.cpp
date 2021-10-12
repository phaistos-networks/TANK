#include "client_common.h"

// XXX: This is only used for testing the client
void TankClient::abort_broker_req(broker_api_request *br_req) {
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::inverse, "Aboring broker request to ", br_req->br->ep, ansifmt::reset, "\n");
        }

        process_undeliverable_broker_req(br_req, true, __LINE__);
}

// api request timed out
// we'll just make it ready
void TankClient::abort_api_request(api_request *api_req) {
        static constexpr const bool trace{false};
        TANK_EXPECT(api_req);

        if (trace) {
                SLog("API request timed out, will abort\n");
#ifdef TANK_RUNTIME_CHECKS
                SLog("Request was initialised ", duration_repr(Timings::Milliseconds::ToMicros(now_ms - api_req->init_ms)), " ago\n");
#endif
        }

        make_api_req_ready(api_req, __LINE__);
}

// Whenever a broker fails or otherwise any associated payloads can't be delivered
// this is invoked to deal with them
void TankClient::flush_broker(broker *const br) {
        enum {
                trace = false,
        };
        std::vector<broker_api_request *> reqs, failed_reqs;

        assert(not br->ch.get()); // should have shut down this connection

        if (trace) {
                SLog(ansifmt::color_red, ansifmt::bgcolor_brown, "BEGIN:flush_broker()  ", ptr_repr(br), " ",
                     br->ep,
                     ", outgoing content = ", br->outgoing_content.size(),
                     ", pending_responses = ", br->pending_responses_list.size(),
                     ansifmt::reset, "\n");
        }

        for (auto it = br->outgoing_content.front(); it;) {
                auto br_req = it->broker_req;
                auto next   = it->next;

                TANK_EXPECT(br_req->have_payload);
                br_req->have_payload = false;

                reqs.emplace_back(br_req);
                put_payload(it, __LINE__);
                it = next;
        }
        br->outgoing_content.clear();
        TANK_EXPECT(not br->outgoing_content.first_payload_partially_transferred);

        // Once we have successfully scheduled a paylaod for transmission,
        // we track the request in pending_responses_list.
        //
        // this method is ivoked when the connection to the peer is severed
        // so we need to deal with all those such pending responses.
        //
        // We don't know wether the peer has actually processed the request(1)
        // or maybe that it did and we didn't get the response for any reason(2).
        // What this means is that we can't assume anything, but it is important
        // to play it safe, so we will only retry the request if we know
        // it is idempotent, otherwise we will fail it (network error).
        for (auto it : br->pending_responses_list) {
                auto breq = containerof(broker_api_request, pending_responses_list_ll, it);
                assert(not breq->have_payload);

                auto api_req = breq->api_req;
                assert(api_req);

                const bool is_idempotent =
                    (api_req->type == api_request::Type::Consume or
                     api_req->type == api_request::Type::DiscoverPartitions or
                     api_req->type == api_request::Type::ReloadConfig or
                     api_req->type == api_request::Type::DiscoverTopics or
                     api_req->type == api_request::Type::DiscoverTopology or
                     api_req->type == api_request::Type::SrvStatus);

                if (trace) {
                        SLog("pending broker request resp, is_idempotent = ", is_idempotent, "\n");
                }

                if (is_idempotent) {
                        reqs.emplace_back(breq);
                } else {
                        failed_reqs.emplace_back(breq);
                }
        }
        br->pending_responses_list.reset();

        if (trace) {
                SLog("reqs.size = ", reqs.size(), ", failed_reqs.size = ", failed_reqs.size(), "\n");
        }

        // It's important that we place all dequeued rquests into reqs
        // and then process them here after we have drained
        // the outgoing_content list etc
        for (auto br_req : reqs) {
                process_undeliverable_broker_req(br_req, false, __LINE__);
        }

        for (auto br_req : failed_reqs) {
                auto api_req = br_req->api_req;

                unlink_broker_req(br_req, __LINE__);

                if (trace) {
                        SLog("Considering FAILED request\n");
                }

                while (not br_req->partitions_list.empty()) {
                        auto req_part = containerof(request_partition_ctx,
                                                    partitions_list_ll, br_req->partitions_list.next);

                        capture_network_fault(api_req, req_part->topic, req_part->partition);

                        req_part->partitions_list_ll.detach();

                        discard_request_partition_ctx(api_req, req_part);
                }
                br_req->partitions_list.reset();

                put_broker_api_request(br_req);

                try_make_api_req_ready(api_req, __LINE__);
        }

        if (trace) {
                SLog("END:flush_broker()\n");
        }
}

////////////////////////////////////////////////////////////////////////////////////////////////
//
// each request_partition_ctx encapsulates both request and response specific state
// for e.g produce requests, that could be the actual content
//
// note that response specific state may have been migrated/merged into an api_request specific state
// we don't want to destroy/release resources twice
void TankClient::clear_request_partition_ctx(api_request *const api_req, request_partition_ctx *const par) {
        enum {
                trace = false,
        };
        assert(api_req);
        assert(par);

        const auto resp_valid = std::exchange(par->as_op.response_valid, false);

        if (trace) {
                SLog("Will clear_request_partition_ctx() for type ", unsigned(api_req->type), ", resp_valid = ", resp_valid, "\n");
        }

        switch (api_req->type) {
                case api_request::Type::Produce:
                        [[fallthrough]];
                case api_request::Type::ProduceWithSeqnum:
                        if (auto v = std::exchange(par->as_op.produce.payload.data, nullptr)) {
                                std::free(v);
                        }
                        break;

                case api_request::Type::Consume:
                        if (resp_valid) {
                                auto &resp = par->as_op.consume.response;

                                if (resp.msgs.cnt <= sizeof_array(resp.msgs.list.small)) {
                                        // SBO
                                } else {
                                        std::free(resp.msgs.list.large);
                                }

                                if (const auto n = resp.used_buffers.size) {
                                        for (size_t i{0}; i < n; ++i) {
                                                put_buffer(resp.used_buffers.data[i]);
                                        }

                                        std::free(resp.used_buffers.data);
                                }
                        }
                        break;

                case api_request::Type::DiscoverPartitions:
                        if (resp_valid) {
                                auto &resp = par->as_op.discover_partitions.response;

                                delete resp.all;
                        }
                        break;

                case api_request::Type::DiscoverTopics:
                        if (resp_valid) {
                                auto &resp = par->as_op.discover_topics.response;

                                delete resp.all;
                        }
                        break;

                case api_request::Type::DiscoverTopology:
                        if (resp_valid) {
                                auto &resp = par->as_op.discover_topology.response;

                                delete resp.all;
                                resp.cluster_name.try_free();
                        }
                        break;

                case api_request::Type::CreateTopic:
                case api_request::Type::ReloadConfig:
                case api_request::Type::SrvStatus:
                        break;

                default:
                        IMPLEMENT_ME();
        }
}

void TankClient::abort_api_request_retry_bundles(api_request *api_req, std::vector<request_partition_ctx *> *contexts) {
        enum {
                trace = false,
        };
        assert(api_req);

        if (trace) {
                if (!api_req->retry_bundles_list.empty()) {
                        SLog("Aborting all outstanding retry bundles for api request(", api_req->retry_bundles_list.size(), ")\n");
                }
        }

        while (not api_req->retry_bundles_list.empty()) {
                auto rb = containerof(retry_bundle, retry_bundles_ll, api_req->retry_bundles_list.next);

                contexts->insert(contexts->end(), rb->data, rb->data + rb->size);

                eb64_delete(&rb->node);
                rb->retry_bundles_ll.detach();

                std::free(rb);
        }

        retry_bundles_next = eb_is_empty(&retry_bundles_ebt_root)
                                 ? std::numeric_limits<uint64_t>::max()
                                 : eb64_first(&retry_bundles_ebt_root)->key;
}

void TankClient::abort_api_request_brokers_reqs(api_request *api_req, std::vector<request_partition_ctx *> *contexts, const uint32_t ref) {
        [[maybe_unused]] static constexpr bool trace{false};
        TANK_EXPECT(api_req);

        if (trace) {
                if (!api_req->broker_requests_list.empty()) {
                        SLog(ansifmt::bgcolor_red, "Aborting all outstanding brokers requests for api request", ansifmt::reset, " broker_requests_list.size = ", api_req->broker_requests_list.size(), ", ref = ", ref, "\n");
                }
        }

        while (!api_req->broker_requests_list.empty()) {
                auto br_req = containerof(broker_api_request, broker_requests_list_ll, api_req->broker_requests_list.next);

                if (trace) {
                        SLog("Aborting broker ", br_req->br->ep, " partitions\n");
                }

                while (!br_req->partitions_list.empty()) {
                        auto part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

                        part->partitions_list_ll.detach();
                        contexts->emplace_back(part);
                }

                unlink_broker_req(br_req, __LINE__);
                put_broker_api_request(br_req);
        }
}

// ready api_requests (i.e their responses) are retained
// until the next reactor loop iteration, so that the callee/client that embeds the tank client
// gets the chance to use the collected responses. We don't want to reclaim/destroy resources
// associated with those ready responses before that happens.
void TankClient::gc_api_request(std::unique_ptr<api_request> api_req) {
        assert(api_req);
        assert(api_req->api_reqs_expirations_tree_node.node.leaf_p == nullptr);

        enum {
                trace = false,
        };
        std::vector<request_partition_ctx *> contexts; // TODO: maybe reuse?
        auto                                 api_req_ptr = api_req.get();

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "BEGIN: gc_api_request()", ansifmt::reset,
                     ": broker_requests_lists.size = ", api_req->broker_requests_list.size(), ", ready_partitions.size = ", api_req->ready_partitions_list.size(), "\n");
        }

        abort_api_request_retry_bundles(api_req.get(), &contexts);
        abort_api_request_brokers_reqs(api_req.get(), &contexts, __LINE__);

        // reclaim-release resources associated with request_partition_ctx responses
        for (auto it = api_req->ready_partitions_list.next; it != &api_req->ready_partitions_list;) {
                auto next = it->next;
                auto p    = containerof(request_partition_ctx, partitions_list_ll, it);

                discard_request_partition_ctx(api_req.get(), p);
                it = next;
        }

        api_req->ready_partitions_list.reset();

        switch (api_req->type) {
                case api_request::Type::DiscoverTopology:
                        delete api_req->materialized_resp.discover_topology.v;

                        if (auto ptr = api_req->materialized_resp.discover_topology._cluster_name_ptr) {
                                std::free(ptr);
                        }
                        break;

                case api_request::Type::DiscoverTopics:
                        delete api_req->materialized_resp.discover_topics.v;
                        break;

                case api_request::Type::DiscoverPartitions:
                        if (auto p = api_req_ptr->materialized_resp.discover_partitions.v) {
                                std::free(p);
                        }
                        break;

                case api_request::Type::CreateTopic:
                        api_req_ptr->as.create_topic.config.try_free();
                        break;

                case api_request::Type::Produce:
                case api_request::Type::ProduceWithSeqnum:
                case api_request::Type::ReloadConfig:
                case api_request::Type::SrvStatus:
                case api_request::Type::Consume:
                        break;

                default:
                        IMPLEMENT_ME();
        }

        // release collected contexts
        for (auto part : contexts) {
                discard_request_partition_ctx(api_req_ptr, part);
        }

        // release any retained memory buffers
        for (auto b : api_req->managed_bufs) {
                b->unlock();
                release_mb(b);
        }
        api_req->managed_bufs.clear();

        // reuse the api request(and we are done)
        put_api_request(std::move(api_req));

        if (trace) {
                SLog("END: gc_api_request()\n");
        }
}

void TankClient::try_make_api_req_ready(api_request *api_req, const uint32_t ref) {
        assert(api_req);

        if (likely(api_req->request_id) and api_req->ready()) {
                make_api_req_ready(api_req, ref);
        }
}

// All broker_api_request' associated with an api_request
// have been processed or failed, thereby making the api request ready
void TankClient::make_api_req_ready(api_request *api_req, const uint32_t ref) {
        enum {
                trace = false,
        };
        TANK_EXPECT(api_req);
        const auto                           id     = api_req->request_id;
        const bool                           failed = api_req->failed();
        std::vector<request_partition_ctx *> contexts;

        if (0 == id) [[unlikely]] {
                // was already made ready
                // this shouldn't happen, but we may as well just do nothing here
                return;
        }

        if (api_req->api_reqs_expirations_tree_node.node.leaf_p) {
                // abort timer, in case it wasn't already aborted earlier (i.e if
                // make_api_req_ready() wasn't a result of check_pending_api_responses() checks)
                const auto k = api_req->api_reqs_expirations_tree_node.key;

                eb64_delete(&api_req->api_reqs_expirations_tree_node);

                TANK_EXPECT(api_req->api_reqs_expirations_tree_node.node.leaf_p == nullptr);

                if (k <= api_reqs_expirations_tree_next) {
                        api_reqs_expirations_tree_next = eb_is_empty(&api_reqs_expirations_tree)
                                                             ? std::numeric_limits<uint64_t>::max()
                                                             : eb64_first(&api_reqs_expirations_tree)->key;
                }

                if (trace) {
                        SLog("Unklinked from api_reqs_expirations_tree\n");
                }
        }

        const auto it = pending_responses.find(id);

        TANK_EXPECT(it != pending_responses.end());

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_green, "API request response is READY for ", id, ansifmt::reset, " (ref = ", ref, ")\n");
        }

        // if we have any outstanding work, abort it
        // we will capture a timeout error for any such work(i.e request_partition_ctx')
        abort_api_request_retry_bundles(api_req, &contexts);
        abort_api_request_brokers_reqs(api_req, &contexts, ref);

        for (const auto part : contexts) {
                if (not failed) {
                        enum {
                                trace = false,
                        };

                        if (trace) {
                                SLog("Capturing timeout from make_api_req_ready() invoked at ", ref, "\n");
                        }

                        capture_timeout(api_req, part->topic, part->partition, __LINE__);
                }

                discard_request_partition_ctx(api_req, part);
        }

        // materialize whatever we have for this request
        const auto retain_for_next_iteration = materialize_api_response(api_req);

        // see earlier comment
        api_req->request_id = 0;

        // stop tracking this request
        // it's no longer a pending response (i.e it's ready)
        it->second.release(); // release because we will transfer ownership to ready_responses or via gc_api_request()
        pending_responses.erase(it);

        if (retain_for_next_iteration) {
                // will be destroyed in begin_reactor_loop_iteration()
                // gc_api_request() will be ivoked for this api request
                ready_responses.emplace_back(api_req);

                if (trace) {
                        SLog("Retained for next iteration\n");
                }
        } else {
                gc_api_request(std::unique_ptr<api_request>(api_req));

                if (trace) {
                        SLog("was OK to GC now\n");
                }
        }
}

// a connection's input buffer's data is required by
// an api_request, which means
// tha buffers' data need to be retained(no reallocations are possible anymore)
// and that buffer needs to be retained, and tracked by the api_request to be release()ed
// in gc_api_request()
void TankClient::retain_conn_inbuf(connection *const c, api_request *api_req) {
        TANK_EXPECT(c);
        TANK_EXPECT(api_req);
        auto b = c->in.b;
        TANK_EXPECT(b);
        TANK_EXPECT(b->use_count());

        retain_mb(b);

        TANK_EXPECT(b->use_count() > 1);

        b->lock(); // noone can resize it
        api_req->managed_bufs.emplace_back(b);

        // we will defer acting on the ready api request until we are done processing I/O
}

// build a payload for a specific broker_api_request
// this is the representation of that broker api request to be transmitted to the broker
TankClient::broker_outgoing_payload *TankClient::build_broker_req_payload(broker_api_request *const req) {
        enum {
                trace = false,
        };

        assert(req);
        assert(not req->have_payload);

        const auto               api_req = req->api_req;
        broker_outgoing_payload *payload;

        assert(api_req);

        if (trace) {
                SLog("Building request payload for ", unsigned(api_req->type), "\n");
        }

        switch (api_req->type) {
                case api_request::Type::Consume:
                        payload = build_consume_broker_req_payload(req);
                        break;

                case api_request::Type::DiscoverTopics:
                        payload = build_discover_topics_broker_req_payload(req);
                        break;

                case api_request::Type::DiscoverTopology:
                        payload = build_discover_topology_req_payload(req);
                        break;

                case api_request::Type::DiscoverPartitions:
                        payload = build_discover_partitions_broker_req_payload(req);
                        break;

                case api_request::Type::Produce:
                case api_request::Type::ProduceWithSeqnum:
                        payload = build_produce_broker_req_payload(req);
                        break;

                case api_request::Type::ReloadConfig:
                        payload = build_reload_partition_conf_broker_req_payload(req);
                        break;

                case api_request::Type::CreateTopic:
                        payload = build_create_topic_broker_req_payload(req);
                        break;

                case api_request::Type::SrvStatus:
                        payload = build_srv_status_broker_req_payload(req);
                        break;

                default:
                        payload = nullptr;
                        break;
        }

        TANK_EXPECT(payload);

        // have_payload will be reset to false once
        // the payload has been transferred in full or released via e.g unlink_broker_req()
        payload->broker_req = const_cast<broker_api_request *>(req);
        req->have_payload   = true;
        return payload;
}

// once all request_partition_ctx has been generated by e.g consume()
// and associated with a broker who may be the current leader of the (topic, partition)
// this method's responsible for generating broker_api_request's for each of the brokers involved, and
// pairing all request_partition_ctx's with it
switch_dlist *TankClient::assign_req_partitions_to_api_req(api_request *const                                         api_req, // will pair with this api_request,
                                                           std::vector<std::pair<broker *, request_partition_ctx *>> *contexts,
                                                           const uint32_t                                             limit) {
        enum {
                trace = false,
        };
        auto *next = &api_req->broker_requests_list;

        // sort by (ptr(broker) asc, topic asc, partition asc)
        std::sort(contexts->begin(), contexts->end(), [](const auto &a, const auto &b) noexcept {
                if (a.first < b.first) {
                        return true;
                } else if (b.first < a.first) {
                        return false;
                }

                if (const auto r = a.second->topic.Cmp(b.second->topic); r < 0) {
                        return true;
                } else if (0 == r) {
                        return a.second->partition < b.second->partition;
                } else {
                        return false;
                }
        });

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_blue, ansifmt::inverse, "Assigning ", contexts->size(),
                     " to api requests (limit = ", limit, ")", ansifmt::reset, "\n");
        }

        for (const auto *p = contexts->data(), *const e = p + contexts->size(); p < e;) {
                auto br         = p->first;
                auto broker_req = get_broker_api_request();

                // a new broker request for this api request
                // will be tracked individually via pending_brokers_requests
                broker_req->br      = br;
                broker_req->api_req = api_req;
                broker_req->id      = next_broker_request_id++;

                if (next == &api_req->broker_requests_list) {
                        next = &broker_req->broker_requests_list_ll;
                }

                // some requests may need too many iovecs
                // so a single broker_outgoing_payload may not be able to hold in its iovecs::data
                // everything, so limit can be set to something sane so that
                // multiple broker requests to the same endpoint may be potentially generated to accomodate that need
                //
                // currently, only produce() overrides the default limit value
                // XXX: verify this
                const auto upto = std::min(e, p + limit);

                do {
                        auto req_part = p->second;

                        // track this request partition of this broker
                        req_part->partitions_list_ll.reset();
                        broker_req->partitions_list.push_back(&req_part->partitions_list_ll);

                        if (trace) {
                                SLog("For broker ", br->ep, " (", req_part->topic, "/", req_part->partition, ")\n");
                        }

                } while (++p < upto and p->first == br);

                if (unlikely(broker_req->partitions_list.size() > 250)) {
                        // we do not currently allow more than 250 partitions per topic
                        // to support that, we 'd need to change the protocol (i.e use u16 instead of u8 for number of
                        // partitions in a topic, in a request and in a response) and possibly other implementation details
                        // TODO: do something more sensible here than aborting
                        std::abort();
                }

                // track this new broker api request
                pending_brokers_requests.emplace(broker_req->id, broker_req);

                // associate it with the api request
                // (impotant to use push_front(), not push_back() here.)
                api_req->broker_requests_list.push_front(&broker_req->broker_requests_list_ll);
        }

        if (trace) {
                size_t n{0};

                for (auto it = next; it != &api_req->broker_requests_list; it = it->next) {
                        ++n;
                }

                SLog("New dinstinct brokers requests ", n, "\n");
        }

        return next;
}

// We will be tracking all known brokers
//
// Whenever a node fails, it will be moved to the tail, and we will always use the broker at the head
// this is so that we will always attempt to use a broker that hasn't failed.
//
// We need this functionality for when we haven't had the chance to associate a (topic, partition) to a leader
// and for when we need to issue a request that's not particular to a topic or a partition(e.g discover_partitions)
TankClient::broker *TankClient::any_broker() {
        if (all_brokers.empty()) [[unlikely]] {
                throw std::runtime_error("No brokers");
        }

        // always use head
        return containerof(broker, all_brokers_ll, all_brokers.next);
}

TankClient::broker *TankClient::broker_by_endpoint(const Switch::endpoint e) {
        const auto res = brokers.emplace(e, nullptr);

        if (res.second) {
                auto b = std::make_unique<broker>(e);

                TANK_EXPECT(b->all_brokers_ll.empty());
                all_brokers.push_front(&b->all_brokers_ll);

                res.first->second = std::move(b);
        }

        return res.first->second.get();
}

void TankClient::shift_failed_broker(broker *br) {
        TANK_EXPECT(br);

        // because any_broker() always returns head, we 'll just move this to tail
        br->all_brokers_ll.try_detach_and_reset();
        all_brokers.push_front(&br->all_brokers_ll);
}

// this is important
// in flush_broker() we can check all outstanding request_partition_ctx, and
// partition_leader(ctx.topic, ctx.partition) == broker, we need to retry later for the same broker, if not
// then we mark the broker as failed and use any_broker() again to get another node, and if we can't then wait anyway
TankClient::broker *TankClient::partition_leader(const str_view8 topic, const uint16_t partition) {
        if (const auto it = leaders.find(topic_partition{topic, partition}); it != leaders.end()) {
                const auto e = it->second;

                return broker_by_endpoint(e);
        }

        // we don't know about that yet
        return nullptr;
}

str_view8 TankClient::intern_topic(const str_view8 topic_name) {
        const auto res = topics_intern_map.emplace(topic_name, true);

        if (res.second) {
                const_cast<str_view8 *>(&res.first->first)->p =
                    core_allocator.CopyOf(topic_name.data(), topic_name.size());
        }

        return res.first->first;
}

// Create a new payload, suitable for a broker API request
TankClient::broker_outgoing_payload *TankClient::new_req_payload(broker_api_request *req) {
        auto payload = get_payload();

        payload->broker_req = req;
        return payload;
}

bool TankClient::schedule_broker_req(broker_api_request *const breq) {
        auto payload = build_broker_req_payload(breq);

        return schedule_broker_payload(breq, payload);
}

// Enqueues payload with a broker's outgoing payloads queue
bool TankClient::schedule_broker_payload([[maybe_unused]] broker_api_request *br_req,
                                         broker_outgoing_payload             *payload) {
        enum {
                trace = false,
        };
        assert(payload);
        assert(payload->iovecs.size);
        assert(payload->broker_req);
        assert(payload->broker_req->br);
        auto br = payload->broker_req->br;

        if (trace) {
                SLog("Scheduling payload for broker ", ptr_repr(br), " ", br->ep, " br->outgoing_content.size = ", br->outgoing_content.size(), ", payload->iovecs.size = ", payload->iovecs.size, "\n");
        }

        if (br->reachability == broker::Reachability::Blocked) {
                enum {
                        trace = false,
                };

                if (now_ms > br->blocked_until) {
                        if (trace) {
                                SLog("Was blocked, no longer blocked, will give it another try\n");
                        }

                        br->consequtive_connection_failures = broker::max_consequtive_connection_failures - 1;
                        br->set_reachability(broker::Reachability::LikelyUnreachable, __LINE__);
                        br->blocked_until = 0;
                        try_stop_track_unreachable(br);
                } else {
                        if (trace) {
                                SLog("Is blocked, can't try right now. Will be blocked for another ", duration_repr(Timings::Milliseconds::ToMicros(br->blocked_until - now_ms)), "\n");
                        }

                        put_payload(payload, __LINE__);
                        return false;
                }
        }

        const auto before = br->outgoing_content.size();

        br->outgoing_content.push_back(payload);

        TANK_EXPECT(br->outgoing_content.size() == before + 1);

        if (br->unreachable_brokers_tree_node.node.leaf_p) {
                if (trace) {
                        SLog("Node is tracked as unreachable, will wait until it can become reachable again: consequtive_connection_failures = ",
                             br->consequtive_connection_failures, ", br->outgoing_content.size now = ", br->outgoing_content.size(), "\n");
                }

                return true;
        }

        return try_transmit(br);
}

// Each created API request is tracked
// the request id generated is what's passed to the library user
uint32_t TankClient::track_pending_resp(std::unique_ptr<api_request> req) {
        static constexpr bool trace{false};
        TANK_EXPECT(req);
        TANK_EXPECT(req->request_id == 0);
        const auto id = next_api_request_id++;

        req->request_id = id;
        if (const auto expiration = req->expiration()) {
                api_reqs_expirations_tree_next = std::min<uint64_t>(expiration, api_reqs_expirations_tree_next);
                eb64_insert(&api_reqs_expirations_tree, &req->api_reqs_expirations_tree_node);
        }

        const auto res = pending_responses.emplace(id, std::move(req));

        TANK_EXPECT(res.second);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::bgcolor_green, "SCHEDULING new api request", ansifmt::reset, "\n");
        }

        return id;
}

// an API request cannot be fullfilled so it needs to be aborted immediately
void TankClient::fail_api_request(std::unique_ptr<api_request> ar) {
        static constexpr bool trace{false};
        TANK_EXPECT(ar);
        TANK_EXPECT(ar->api_reqs_expirations_tree_node.node.leaf_p == nullptr); // wasn't tracked -- see track_pending_resp()
        TANK_EXPECT(ar->request_id == 0);                                       // ^
        std::vector<request_partition_ctx *> contexts;

        if (trace) {
                SLog("FAILING api request\n");
        }

        abort_api_request_retry_bundles(ar.get(), &contexts);
        abort_api_request_brokers_reqs(ar.get(), &contexts, __LINE__);

        for (auto part : contexts) {
                // XXX: timeout?
                discard_request_partition_ctx(ar.get(), part);
        }

        for (auto b : ar->managed_bufs) {
                release_mb(b);
        }
        ar->managed_bufs.clear();

        put_api_request(std::move(ar));
}

// Generate a new payload for each broker_api_request associated with this api_request and schedule them
uint32_t TankClient::schedule_new_api_req(std::unique_ptr<api_request> api_req) {
        static constexpr const bool trace{false};
        TANK_EXPECT(!api_req->broker_requests_list.empty());

        if (trace) {
                SLog("Scheduling new API request api_req->broker_requests_list.size() = ", api_req->broker_requests_list.size(), "\n");
        }

        for (auto it = api_req->broker_requests_list.next; it != &api_req->broker_requests_list; it = it->next) {
                auto broker_req = containerof(broker_api_request, broker_requests_list_ll, it);

                if (!schedule_broker_req(broker_req)) {
                        static constexpr const bool trace{false};
                        // we will need to _fail_ this request
                        //
                        // we have a few options here:
                        // - we return 0
                        // - we capture a fault for every broker broker request
                        //
                        // For now, we 'll just abort this API request and return 0
                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, ansifmt::inverse, "Unable to schedule API request broker request", ansifmt::reset, "\n");
                        }

                        fail_api_request(std::move(api_req));
                        return 0;
                }
        }

        return track_pending_resp(std::move(api_req));
}

void TankClient::unlink_broker_req(broker_api_request *br_req, const size_t ref) {
        enum {
                trace = false,
        };
        assert(br_req);

        connection *c{nullptr};
        auto        br = br_req->br;

        if (trace) {
                SLog("Unlinking broker_api_request(Boker ID:", br_req->id,
                     "), have_payload = ", br_req->have_payload,
                     ", pending:", not br_req->pending_responses_list_ll.empty(), ", ref = ", ref, "\n");
        }

        if (br_req->have_payload) {
                // this gets complicated fast
                // we have generated a payload for this request, and that payload may not have been scheduled
                // for transmission to the peer yet
                assert(br);

                c = br->ch.get();

                if (br->can_safely_abort_broker_request_payload(br_req)) {
                        if (trace) {
                                SLog("can_safely_abort_broker_request_payload()\n");
                        }

                        if (auto payload = br->abort_broker_request_payload(br_req)) {
                                if (trace) {
                                        SLog("Did abort payload, broker->outgoing_content.size = ", br->outgoing_content.size(), " for ", br->ep, "\n");
                                }

                                put_payload(payload, __LINE__);
                                c = nullptr;
                        } else if (trace) {
                                SLog("Was not able to abort payload\n");
                        }
                } else {
                        // if it's not safe, we 'll remove it anyway
                        // because we will shut down the connection
                        if (trace) {
                                SLog("Not safe to abort payload\n");
                        }

                        if (auto payload = br->abort_broker_request_payload(br_req)) {
                                if (trace) {
                                        SLog("Brute-force aborting payload\n");
                                }

                                put_payload(payload, __LINE__);
                        } else if (trace) {
                                SLog("odd: unable to brute-force abort payload\n");
                        }
                }

                br_req->have_payload = false;
        }

        br_req->broker_requests_list_ll.try_detach_and_reset();   // from api_req
        br_req->pending_responses_list_ll.try_detach_and_reset(); // from broker

        // this may have been erased from pending_brokers_requests
        // see process_consume_content_impl()
        pending_brokers_requests.erase(br_req->id);

        if (c) {
                // if we are going to shut down the connection, what are we going to do
                // with all other payloads that were outstanding for that broker?
                if (trace) {
                        SLog("Will shutdown connection because we couldn't abort the broker request payload. br->outgoing_content.size() = ", br->outgoing_content.size(), "\n");
                }

                shutdown(c, __LINE__, false);

                if (br) {
                        assert(br->ch.get() == nullptr);

                        if (not br->outgoing_content.empty()) {
                                if (trace) {
                                        SLog("There's outstanding outgoing content for that broker\n");
                                }

                                try_transmit(br);
                        } else if (trace) {
                                SLog("No need to try to deliver content now\n");
                        }
                }
        }
}

void TankClient::process_undeliverable_broker_req(broker_api_request *br_req, const bool reason_timeout, [[maybe_unused]] const uint32_t ref) {
        static constexpr bool trace{false};
        TANK_EXPECT(br_req);
        const auto br_req_id = br_req->id;
        TANK_EXPECT(br_req_id);
        auto api_req = br_req->api_req;
        TANK_EXPECT(api_req);
        const auto expiration = api_req->expiration();
        auto       br         = br_req->br;

        if (trace) {
                SLog(ansifmt::color_cyan, "Was not able to delivery request to ",
                     br_req->br->ep, ansifmt::reset, ",  outgoing_content.size = ",
                     br->outgoing_content.size(), ", reason_timeout:", reason_timeout, ", have_payload = ", br_req->have_payload, ", ref = ", ref, "\n");
        }

        // stop tracking for expiration and in the global dictionary
        unlink_broker_req(br_req, __LINE__);

        if (expiration && expiration < now_ms) {
                if (trace) {
                        SLog("Already expired (expiration:", expiration, ", now_ms:", now_ms, ") time out ", br_req->partitions_list.size(), " partitions\n");
                }

                while (!br_req->partitions_list.empty()) {
                        auto part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

                        capture_timeout(api_req, part->topic, part->partition, __LINE__);

                        part->partitions_list_ll.detach();

                        discard_request_partition_ctx(api_req, part);
                }
        } else {
                // this payload is associated with a broker_api_request
                // we will take apart the the various request_partition_ctx that make this the request
                std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

                if (reason_timeout) {
                        while (!br_req->partitions_list.empty()) {
                                auto req_part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

                                capture_timeout(api_req, req_part->topic, req_part->partition, __LINE__);

                                req_part->partitions_list_ll.detach();

                                discard_request_partition_ctx(api_req, req_part);
                        }
                        br_req->partitions_list.reset();

                        if (trace) {
                                SLog("Reason timeout\n");
                        }

                } else {
                        std::vector<broker_api_request *> reqs;
                        bool                              any_failed{false};

                        for (auto it : br_req->partitions_list) {
                                auto req_part = containerof(request_partition_ctx, partitions_list_ll, it);
                                auto broker   = partition_leader(req_part->topic, req_part->partition) ?: any_broker();

                                contexts.emplace_back(std::make_pair(broker, req_part));
                        }
                        br_req->partitions_list.reset();

                        if (trace) {
                                SLog("Broker requested was associated with ", contexts.size(), " partitions\n");
                        }

                        // assign them back to this api request
                        // and dispatch new payload
                        for (auto it                                  = assign_req_partitions_to_api_req(api_req, &contexts);
                             it != &api_req->broker_requests_list; it = it->next) {
                                auto broker_req = containerof(broker_api_request, broker_requests_list_ll, it);

                                reqs.emplace_back(broker_req);
                        }

                        for (auto br_req : reqs) {
                                if (!schedule_broker_req(br_req)) {
                                        while (!br_req->partitions_list.empty()) {
                                                auto part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

                                                capture_network_fault(api_req, part->topic, part->partition);

                                                part->partitions_list_ll.detach();

                                                discard_request_partition_ctx(api_req, part);
                                        }

                                        unlink_broker_req(br_req, __LINE__);
                                        put_broker_api_request(br_req);
                                        any_failed = true;
                                }
                        }

                        if (any_failed) {
                                make_api_req_ready(api_req, __LINE__);
                                goto l1;
                        }
                }
        }

        try_make_api_req_ready(api_req, __LINE__);
l1:
        put_broker_api_request(br_req);
        try_stop_track_unreachable(br);
}

void TankClient::try_stop_track_unreachable(broker *br) {
        static constexpr bool trace{false};

        if (br->unreachable_brokers_tree_node.node.leaf_p) {
                // if we are tracking this node in unreachable nodes tree
                if (!br->outgoing_content.empty()) {
                        // need to keep tracking it
                        return;
                }

                if (trace) {
                        SLog("No longer tracking broker for reachability\n");
                }

                // and if there is nothing more to transmit to the broker
                // then detach from the tree
                const auto k = br->unreachable_brokers_tree_node.key;

                eb64_delete(&br->unreachable_brokers_tree_node);

                if (k <= unreachable_brokers_tree_next) {
                        unreachable_brokers_tree_next = eb_is_empty(&unreachable_brokers_tree)
                                                            ? std::numeric_limits<uint64_t>::max()
                                                            : eb64_first(&unreachable_brokers_tree)->key;
                }
        }
}

// generate broker_api_request' and assign them to an api request and then
// for each newly assigned broker request, generate a matching payload and try to schedule it for transmission
bool TankClient::schedule_req_partitions(api_request *api_req, std::vector<std::pair<broker *, request_partition_ctx *>> *contexts) {
        for (auto it = assign_req_partitions_to_api_req(api_req, contexts); it != &api_req->broker_requests_list; it = it->next) {
                auto broker_req = containerof(broker_api_request, broker_requests_list_ll, it);

                if (!schedule_broker_req(broker_req)) {
                        return false;
                }
        }

        return true;
}

bool TankClient::schedule_req_partitions(api_request *api_req, request_partition_ctx **l, const size_t cnt) {
        static constexpr bool                                     trace{false};
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        // pair each partition with a node (likely the leader)
        for (size_t i{0}; i < cnt; ++i) {
                auto req_part = l[i];
                auto broker   = partition_leader(req_part->topic, req_part->partition) ?: any_broker();

                if (trace) {
                        SLog("For ", req_part->topic, "/", req_part->partition, " ", broker->ep, "\n");
                }

                contexts.emplace_back(std::make_pair(broker, req_part));
        }

        return schedule_req_partitions(api_req, &contexts);
}

bool TankClient::schedule_req_partitions(api_request *api_req, std::vector<request_partition_ctx *> *v) {
        return schedule_req_partitions(api_req, v->data(), v->size());
}

bool TankClient::init_broker_connection(broker *br) {
        static constexpr bool trace{false};
        int                   fd;
        sockaddr_in           sa;

        do {
                fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        } while (-1 == fd && (EINTR == errno || EAGAIN == errno));

        if (-1 == fd) {
                if (trace) {
                        SLog("Failed to socket()\n");
                }

                return false;
        }

        if (sndBufSize && setsockopt(fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char *>(&sndBufSize), sizeof(sndBufSize)) == -1) {
                //
        }

        if (rcvBufSize && setsockopt(fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char *>(&rcvBufSize), sizeof(rcvBufSize)) == -1) {
                //
        }

        sa.sin_family      = AF_INET;
        sa.sin_addr.s_addr = br->ep.addr4;
        sa.sin_port        = htons(br->ep.port);

        for (;;) {
                if (-1 == connect(fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa))) {
                        if (EINTR == errno) {
                                continue;
                        } else if (EINPROGRESS == errno) {
                                break;
                        }

                        if (trace) {
                                SLog("connect() failed:", strerror(errno), "\n");
                        }

                        close(fd);
                        return false;
                } else {
                        break;
                }
        }

        auto c = get_connection();

        all_conns_list.push_back(&c->all_conns_list_ll);

        c->fd = fd;
        br->ch.set(c);
        c->as.tank.reset();
        c->as.tank.br = br;

        c->state.flags = (1u << uint8_t(connection::State::Flags::ConnectionAttempt)) | (1u << uint8_t(connection::State::Flags::NeedOutAvail));
        poller.insert(fd, EPOLLIN | EPOLLOUT, c);

        c->expiration = now_ms + 1000 * 2; // XXX: arbitrary delay
        if (conns_pend_est_list.empty()) {
                conns_pend_est_next_expiration = c->expiration;
        }

        // XXX: we should push_front() not push_back(). Right?
        conns_pend_est_list.push_front(&c->list);

        if (trace) {
                SLog("New Connection ", ptr_repr(c), ", outgoing_content.size() = ", br->outgoing_content.size(), "\n");
        }

        return true;
}

// tries to transmit any scheduled payloads
bool TankClient::try_transmit(broker *const br) {
        enum {
                trace = false,
        };

        if (trace) {
                SLog("Attempting to transmit to ", br->ep, "\n");
        }

        auto c = br->ch.get();

        if (not c) {
                if (trace) {
                        SLog("Need a new connection\n");
                }

                if (not init_broker_connection(br)) {
                        if (trace) {
                                SLog("Failed to initialize broker connection\n");
                        }

                        flush_broker(br);
                        return false;
                }

                c = br->ch.get();
        } else {
                TANK_EXPECT(c->fd != -1);
        }

        TANK_EXPECT(c);

        if (0 == (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))) {
                if (trace) {
                        SLog("Can tx() now\n");
                }

                return tx(c);
        }

        return true;
}

static constexpr bool trace_captured_faults{true};

void TankClient::capture_unsupported_request(api_request *api_req, const uint32_t ref) {
        if (trace_captured_faults) {
                SLog("Captured FAULT: unsupported request, at ", ref, "\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::UnsupportedReq,
            .topic       = ""_s8,
            .partition   = 0,
        });
}

void TankClient::capture_topic_already_exists(api_request *api_req, const str_view8 topic_name) {
        if (trace_captured_faults) {
                SLog("Captured FAULT: topic already exists\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::AlreadyExists,
            .topic       = topic_name,
        });
}

void TankClient::capture_network_fault(api_request *api_req, const str_view8 topic_name, const uint16_t partition, const uint32_t ref) {
        if (trace_captured_faults) {
                SLog("Captured FAULT at ", ref, "\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::Network,
            .topic       = topic_name,
            .partition   = partition,
        });
}

void TankClient::capture_insuficient_replicas(api_request *api_req, const str_view8 topic_name, const uint16_t partition) {
        if (trace_captured_faults) {
                SLog("Captured FAULT\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::InsufficientReplicas,
            .topic       = topic_name,
            .partition   = partition,
        });
}

void TankClient::capture_system_fault(api_request *api_req, const str_view8 topic_name, const uint16_t partition) {
        if (trace_captured_faults) {
                SLog("Captured FAULT\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::SystemFail,
            .topic       = topic_name,
            .partition   = partition,
        });
}

void TankClient::capture_timeout(api_request *api_req, const str_view8 topic_name, const uint16_t partition, const uint32_t ref) {
        TANK_EXPECT(api_req);

        if (trace_captured_faults) {
                SLog("Captured FAULT(timeout at ", ref, ") for api request of type ", unsigned(api_req->type), "\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::Timeout,
            .topic       = topic_name,
            .partition   = partition,
        });
}

void TankClient::capture_unknown_topic_fault(api_request *api_req, const str_view8 topic_name) {
        if (trace_captured_faults) {
                SLog("Captured FAULT '", topic_name, "'\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::UnknownTopic,
            .topic       = topic_name,
        });
}

void TankClient::capture_unknown_partition_fault(api_request *api_req, const str_view8 topic_name, const uint16_t partition) {
        if (trace_captured_faults) {
                SLog("Captured FAULT\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::UnknownPartition,
            .topic       = topic_name,
            .partition   = partition,
        });
}

void TankClient::capture_invalid_req_fault(api_request *api_req, const str_view8 topic_name, const uint16_t partition) {
        if (trace_captured_faults) {
                SLog("Captured FAULT\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::InvalidReq,
            .topic       = topic_name,
            .partition   = partition,
        });
}

void TankClient::capture_readonly_fault(api_request *api_req) {
        if (trace_captured_faults) {
                SLog("Captured FAULT\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId = api_req->request_id,
            .type        = fault::Type::NotAllowed,
            .topic       = ""_s8,
            .partition   = 0,
        });
}

void TankClient::capture_boundary_access_fault(api_request *api_req, const str_view8 topic_name, const uint16_t partition, const uint64_t first_avail_seqnum, const uint64_t highwater_mark) {
        if (trace_captured_faults) {
                SLog("Captured FAULT\n");
        }

        api_req->set_failed();

        all_captured_faults.emplace_back(fault{
            .clientReqId          = api_req->request_id,
            .type                 = fault::Type::BoundaryCheck,
            .topic                = topic_name,
            .partition            = partition,
            .ctx.firstAvailSeqNum = first_avail_seqnum,
            .ctx.highWaterMark    = highwater_mark,
        });
}

void TankClient::schedule_retry(api_request *api_req, request_partition_ctx **v, const uint16_t size, const uint64_t expiration) {
        static constexpr bool trace{false};
        TANK_EXPECT(v);
        TANK_EXPECT(api_req);
        TANK_EXPECT(size);
        TANK_EXPECT(expiration);
        auto b = static_cast<struct retry_bundle *>(malloc(sizeof(struct retry_bundle) + sizeof(request_partition_ctx *) * size));

        if (trace) {
                SLog("Generating new retry bundle of size ", size, "\n");
        }

        TANK_EXPECT(b);

        b->expiration = expiration;
        b->api_req    = api_req;
        b->size       = size;
        memcpy(b->data, v, sizeof(request_partition_ctx *) * size);

        eb64_insert(&retry_bundles_ebt_root, &b->node);
        retry_bundles_next = std::min(retry_bundles_next, expiration);
        TANK_EXPECT(eb64_entry(&b->node, retry_bundle, node) == b);

        b->retry_bundles_ll.reset();
        api_req->retry_bundles_list.push_back(&b->retry_bundles_ll);
}

void TankClient::retry_bundle_impl(retry_bundle *rb) {
        static constexpr bool trace{false};
        TANK_EXPECT(rb);
        TANK_EXPECT(!rb->retry_bundles_ll.empty());
        TANK_EXPECT(rb->size);
        TANK_EXPECT(rb->api_req);
        TANK_EXPECT(rb->node.node.leaf_p == nullptr); // make sure it was deleted in check_pending_retries()

        if (trace) {
                SLog("Will process retry bundle of size ", rb->size, "\n");
        }

        rb->retry_bundles_ll.detach();
        if (!schedule_req_partitions(rb->api_req, rb->data, rb->size)) {
                std::free(rb);

                IMPLEMENT_ME();
        }

        std::free(rb);
}

void TankClient::broker::set_reachability(const Reachability r, const size_t ref) {
        static constexpr bool trace{false};

        if (r == reachability) {
                return;
        }

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_green, "Reachability from ", uint8_t(reachability), " to ", uint8_t(r), ansifmt::reset, "\n");
        }

        reachability = r;
}

TankClient::broker_outgoing_payload *TankClient::broker::abort_broker_request_payload(const broker_api_request *breq) {
        broker_outgoing_payload *prev{nullptr};

        for (auto it = outgoing_content.front(); it; prev = it, it = it->next) {
                if (it->broker_req == breq) {
                        if (prev) {
                                prev->next = it->next;
                                if (!prev->next) {
                                        outgoing_content.back_ = prev;
                                }
                        } else {
                                outgoing_content.front_ = it->next;
                                if (!outgoing_content.front_) {
                                        outgoing_content.back_ = nullptr;
                                }
                        }

                        return it;
                }
        }

        return nullptr;
}

void TankClient::update_api_req(api_request                                *api_req,
                                const bool                                  any_faults,
                                std::vector<request_partition_ctx *> *const no_leader,
                                std::vector<request_partition_ctx *> *const retry) {
        enum {
                trace = false,
        };
        TANK_EXPECT(api_req);
        const auto expiration = api_req->expiration();

        if (trace) {
                SLog("In update_api_req() any_faults = ", any_faults, ", expiration = ", expiration, ", now_ms = ", now_ms, "\n");
        }

        // UPDATE: 2021-10-06
        // now (expiration and expiration < now_ms)
        if (any_faults or (expiration and expiration < now_ms)) {
                if (no_leader) {
                        // no leader available for some partitions
                        for (auto part : *no_leader) {
                                discard_request_partition_ctx(api_req, part);
                        }
                }

                if (retry) {
                        // we need to retry (different leader ?) some partitions requests
                        for (auto part : *retry) {
                                discard_request_partition_ctx(api_req, part);
                        }
                }

                return;
        }

        auto exp = now_ms + 2000; // XXX: arbitrary

        if (expiration and expiration < exp) {
                exp = expiration;
        }

        if (exp > now_ms and exp - now_ms > 100) {
                if (no_leader and not no_leader->empty()) {
                        schedule_retry(api_req, no_leader->data(), no_leader->size(), exp);
                }
        } else if (no_leader) {
                for (auto part : *no_leader) {
                        discard_request_partition_ctx(api_req, part);
                }
        }

        if (retry and not retry->empty()) {
                if (trace) {
                        SLog("Need to retry ", retry->size(), "\n");
                }

                if (not schedule_req_partitions(api_req, retry)) {
                        for (auto part : *retry) {
                                discard_request_partition_ctx(api_req, part);
                        }

                        IMPLEMENT_ME();
                }
        }
}

void TankClient::gc_ready_responses() {
        while (not ready_responses.empty()) {
                std::unique_ptr<api_request> api_req(std::move(ready_responses.back()));

                ready_responses.pop_back();
                gc_api_request(std::move(api_req));
        }
}

#include "client_common.h"

uint32_t TankClient::discover_partitions(const str_view8 topic_name) {
        auto                                                      api_req    = get_api_request(4 * 1000);
        const auto                                                topic      = intern_topic(topic_name);
        auto                                                      req_part   = get_request_partition_ctx();
        auto                                                      br         = any_broker();
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

	TANK_EXPECT(br);

        api_req->type = api_request::Type::DiscoverPartitions;

        req_part->topic     = topic;
        req_part->partition = std::numeric_limits<uint16_t>::max(); // not used for this request

        contexts.emplace_back(std::make_pair(br, req_part));
        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        return schedule_new_api_req(std::move(api_req));
}

TankClient::broker_outgoing_payload *TankClient::build_discover_partitions_broker_req_payload(const broker_api_request *broker_req) {
        auto                  payload  = new_req_payload(const_cast<broker_api_request *>(broker_req));
        [[maybe_unused]] auto broker   = broker_req->br;
        auto                  b        = payload->b;
        [[maybe_unused]] auto api_req  = broker_req->api_req;
        auto                  req_part = containerof(request_partition_ctx, partitions_list_ll, broker_req->partitions_list.next);

        TANK_EXPECT(b);

        b->pack(static_cast<uint8_t>(TankAPIMsgType::DiscoverPartitions));
        b->pack(static_cast<uint32_t>(0)); // will patch

        b->pack(broker_req->id);
        b->pack(req_part->topic);

        if (req_part->partition == std::numeric_limits<uint16_t>::max()) {
                // all, discover partitions
		// this is so that we comply with the protocol without any changes whatsoever
        } else {
                // for select partitions
                for (auto it : broker_req->partitions_list) {
                        const auto req_part = containerof(request_partition_ctx, partitions_list_ll, it);

                        b->pack(req_part->partition);
                }
        }

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1;

        return payload;
}

bool TankClient::process_discover_partitions(connection *const c, const uint8_t *const content, const size_t len) {
        enum {
                trace = false,
        };
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        const auto                 *p      = content;
        [[maybe_unused]] const auto end    = p + len;
        const auto                  req_id = decode_pod<uint32_t>(p);
        const auto                  _it    = pending_brokers_requests.find(req_id);

        if (_it == pending_brokers_requests.end()) {
                // probably timed out earlier
                if (trace) {
                        SLog("Likely timed out earlier\n");
                }

                return true;
        }

        auto br_req  = _it->second;
        auto api_req = br_req->api_req;

        TANK_EXPECT(not br_req->partitions_list.empty());

        auto      it       = br_req->partitions_list.next;
        auto      req_part = containerof(request_partition_ctx, partitions_list_ll, it);
        str_view8 topic_name(reinterpret_cast<const char *>(p) + 1, *p);

        p += topic_name.size() + sizeof(uint8_t);

        if (topic_name != req_part->topic) {
                // what is this?
                IMPLEMENT_ME();
        }

        // total partitions
        auto cnt = decode_pod<uint16_t>(p);

        if (0 == cnt) {
                if (trace) {
                        SLog("Topic unknown?\n");
                }

                capture_unknown_topic_fault(api_req, topic_name);
                make_api_req_ready(api_req, __LINE__);
                return true;
        }

        std::vector<request_partition_ctx *> no_leader, retry;
        auto                                 all = std::make_unique<std::vector<std::pair<uint16_t, std::pair<uint64_t, uint64_t>>>>();

        topic_name = intern_topic(topic_name);
        if (req_part->partition == std::numeric_limits<uint16_t>::max()) {
                // first request, where we didn't specify partitions
                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "Got *first* response for discover_partitions", ansifmt::reset, "\n");
                }

                for (uint16_t i{0}; i < cnt; ++i) {
                        const auto first_available_seqnum = decode_pod<uint64_t>(p);
                        const auto hwmark                 = decode_pod<uint64_t>(p);

                        if (first_available_seqnum == std::numeric_limits<uint64_t>::max() and hwmark == std::numeric_limits<uint64_t>::max()) {
                                // special whatever
                                const auto reason = decode_pod<uint8_t>(p);

                                if (reason == 0xfb) {
                                        // system failure, likely transient
                                        // for now, we 'll fail this outright
                                        capture_system_fault(api_req, topic_name, i);
                                        make_api_req_ready(api_req, __LINE__);
                                        return true;
                                } else if (reason == 0xfd) {
                                        auto req_part = get_request_partition_ctx();

                                        if (trace) {
                                                SLog("No Leader for ", topic_name, "/", i, "\n");
                                        }

                                        req_part->topic     = topic_name;
                                        req_part->partition = i;

                                        no_leader.emplace_back(req_part);
                                } else if (reason == 0xfc) {
                                        auto                   req_part = get_request_partition_ctx();
                                        const auto             addr4    = decode_pod<uint32_t>(p);
                                        const auto             port     = decode_pod<uint16_t>(p);
                                        const Switch::endpoint ep{addr4, port};

                                        if (trace) {
                                                SLog("Different leader for ", topic_name, "/", i, " ", ep, "\n");
                                        }

                                        set_leader(topic_name, i, ep);

                                        req_part->topic     = topic_name;
                                        req_part->partition = i;

                                        retry.emplace_back(req_part);
                                } else {
                                        IMPLEMENT_ME();
                                }
                        } else {
                                if (trace) {
                                        SLog("For partition ", i, " ", first_available_seqnum, ", ", hwmark, "\n");
                                }

                                set_leader(req_part->topic, i, br_req->br->ep);
                                all->emplace_back(std::make_pair(i, std::make_pair(first_available_seqnum, hwmark)));
                        }
                }

                // i.e api_req->track_ready_part_req(req_part);
                req_part->partitions_list_ll.detach_and_reset();
                api_req->ready_partitions_list.push_back(&req_part->partitions_list_ll);

		req_part->as_op.response_valid = true;
                req_part->as_op.discover_partitions.response.all = all.release();
        } else {
                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "Got *second* response for discover_partitions", ansifmt::reset, "\n");
                }

                while (it != &br_req->partitions_list) {
                        auto        next      = it->next;
                        auto *const req_part  = containerof(request_partition_ctx, partitions_list_ll, it);
                        const auto  partition = req_part->partition;

                        TANK_EXPECT(p + sizeof(uint64_t) + sizeof(uint64_t) <= end);

                        const auto first_available_seqnum = decode_pod<uint64_t>(p);
                        const auto hwmark                 = decode_pod<uint64_t>(p);

                        it->detach_and_reset();

                        if (first_available_seqnum == std::numeric_limits<uint64_t>::max() && hwmark == std::numeric_limits<uint64_t>::max()) {
                                const auto reason = decode_pod<uint8_t>(p);

                                if (reason == 0xfd) {
                                        if (trace) {
                                                SLog("No leader for ", req_part->topic, "/", req_part->partition, "\n");
                                        }

                                        no_leader.emplace_back(req_part);
                                } else if (reason == 0xfc) {
                                        const auto addr4 = decode_pod<uint32_t>(p);
                                        const auto port  = decode_pod<uint16_t>(p);

                                        if (trace) {
                                                SLog("New leader for ", req_part->topic, "/", req_part->partition, " ", Switch::endpoint{addr4, port}, "\n");
                                        }

                                        set_leader(req_part->topic, partition, {addr4, port});
                                        retry.emplace_back(req_part);
                                } else {
                                        IMPLEMENT_ME();
                                }
                        } else {
                                if (trace) {
                                        SLog("For ", req_part->topic, "/", req_part->partition, " {", first_available_seqnum, ", ", hwmark, "}\n");
                                }

                                set_leader(req_part->topic, partition, br_req->br->ep);
                                all->emplace_back(std::make_pair(partition, std::make_pair(first_available_seqnum, hwmark)));

                                api_req->ready_partitions_list.push_back(it);
                        }

                        it = next;
                }

		req_part->as_op.response_valid = true;
                req_part->as_op.discover_partitions.response.all = all.release();
        }

        release_broker_req(br_req);

        update_api_req(api_req, false, &no_leader, &retry);

        try_make_api_req_ready(api_req, __LINE__);
        return true;
}

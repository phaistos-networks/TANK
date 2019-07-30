#include "client_common.h"

TankClient::broker_outgoing_payload *TankClient::build_reload_partition_conf_broker_req_payload(const broker_api_request *br_req) {
        auto payload = new_req_payload(const_cast<broker_api_request *>(br_req));
        auto b       = payload->b;
        auto api_req = br_req->api_req;
	TANK_EXPECT(api_req);
	TANK_EXPECT(br_req->partitions_list.size() == 1);
	auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

        TANK_EXPECT(b);
        b->pack(static_cast<uint8_t>(TankAPIMsgType::ReloadConf));
        b->pack(static_cast<uint32_t>(0));

	b->pack(br_req->id);
	b->pack(req_part->topic, req_part->partition);

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1;

        return payload;
}


uint32_t TankClient::reload_partition_conf(const strwlen8_t t, const uint16_t partition) {
	static constexpr bool trace{false};
        auto       api_req  = get_api_request(4 * 1000);
        const auto topic    = intern_topic(t);
        auto       req_part = get_request_partition_ctx();
        auto       br       = any_broker();
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

	TANK_EXPECT(topic);

        api_req->type       = api_request::Type::ReloadConfig;
        req_part->topic     = topic;
        req_part->partition = partition;

        contexts.emplace_back(std::make_pair(br, req_part));
        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        if (trace) {
                SLog("Will reload ", topic, "/", partition, "\n");
        }

        return schedule_new_api_req(std::move(api_req));
}

bool TankClient::process_reload_partition_conf(connection *const c, const uint8_t *const content, const size_t len) {
        [[maybe_unused]] static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        const auto *p      = content;
        const auto  req_id = decode_pod<uint32_t>(p);
        const auto  _it    = pending_brokers_requests.find(req_id);

        if (_it == pending_brokers_requests.end()) {
                return true;
        }

        auto br_req  = _it->second;
        auto api_req = br_req->api_req;
        TANK_EXPECT(br_req->partitions_list.size() == 1);
        auto            req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);
        const str_view8 topic_name(reinterpret_cast<const char *>(p) + 1, *p);

        p += topic_name.size() + sizeof(uint8_t);
        const auto pid = decode_pod<uint16_t>(p);
        const auto err = decode_pod<uint8_t>(p);

        TANK_EXPECT(topic_name == req_part->topic);
        TANK_EXPECT(pid == req_part->partition);

        req_part->partitions_list_ll.detach_and_reset();

	if (trace) {
		SLog("Got err:", err, "\n");
	}


        if (err == 12) {
                // cluster aware
		capture_unsupported_request(api_req);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 11) {
                // invalid request
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 2) {
                // I/O
		capture_system_fault(api_req, req_part->topic, req_part->partition);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 3) {
                // failed to apply configuration
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 0) {
                // OK
                api_req->ready_partitions_list.push_back(&req_part->partitions_list_ll);
	} else if (err == 1) {
		capture_unknown_topic_fault(api_req, req_part->topic);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
	} else if (err == 10) {
		capture_unknown_partition_fault(api_req, req_part->topic, req_part->partition);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else {
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
                IMPLEMENT_ME();
        }

        unlink_broker_req(br_req, __LINE__);
        put_broker_api_request(br_req);

        try_make_api_req_ready(api_req, __LINE__);
        return true;
}

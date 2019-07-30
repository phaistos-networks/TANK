#include "client_common.h"

TankClient::broker_outgoing_payload *TankClient::build_create_topic_broker_req_payload(const broker_api_request *br_req) {
        auto payload  = new_req_payload(const_cast<broker_api_request *>(br_req));
        auto b        = payload->b;
	auto api_req = br_req->api_req;
	TANK_EXPECT(api_req);
	TANK_EXPECT(br_req->partitions_list.size() == 1);
        auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

	TANK_EXPECT(b);
	b->pack(static_cast<uint8_t>(TankAPIMsgType::CreateTopic));
	b->pack(static_cast<uint32_t>(0));

        b->pack(br_req->id);
        b->pack(req_part->topic);
	b->pack(api_req->as.create_topic.partitions);
	b->encode_varuint32(api_req->as.create_topic.config.size());
	b->serialize(api_req->as.create_topic.config.data(), api_req->as.create_topic.config.size());

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1;

        return payload;
}

uint32_t TankClient::create_topic(const strwlen8_t t, const uint16_t partitions, const strwlen32_t config) {
        auto                                                      api_req  = get_api_request(4 * 1000);
        const auto                                                topic    = intern_topic(t);
        auto                                                      req_part = get_request_partition_ctx();
        auto                                                      br       = any_broker();
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        TANK_EXPECT(topic);
        TANK_EXPECT(partitions);

        api_req->type                       = api_request::Type::CreateTopic;
        api_req->as.create_topic.config.len = config.size();
        api_req->as.create_topic.config.p   = config.Copy();
        api_req->as.create_topic.partitions = partitions;

        req_part->topic     = topic;
        req_part->partition = 0; // not used

        contexts.emplace_back(std::make_pair(br, req_part));
        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        return schedule_new_api_req(std::move(api_req));
}

bool TankClient::process_create_topic(connection *const c, const uint8_t *const content, const size_t len) {
	static constexpr bool trace{false};
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
        const str_view8 tn(reinterpret_cast<const char *>(p) + 1, *p);

        p += sizeof(uint8_t) + tn.size();

        if (unlikely(tn != req_part->topic)) {
		gc_api_request(std::unique_ptr<api_request>(api_req));
		throw std::runtime_error("Unexpected create_topic response");
        }

        const auto err = decode_pod<uint8_t>(p);

        req_part->partitions_list_ll.detach_and_reset();

        if (err == 4) {
                // invalid configuration
		capture_invalid_req_fault(api_req, req_part->topic, req_part->partition);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 2) {
                // system error
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 1) {
                // already exists
		capture_topic_already_exists(api_req, req_part->topic);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
	} else if (err == 10) {
		// invalid request. too many partitions? too few? invalid topic name?
		capture_invalid_req_fault(api_req, req_part->topic, req_part->partition);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 11) {
		// not supported
		if (trace) {
			SLog("Cluster-aware?\n");
		}

		capture_unsupported_request(api_req);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else if (err == 5) {
                // read only, or optherwise not possible right now
		capture_readonly_fault(api_req);
                clear_request_partition_ctx(api_req, req_part);
                put_request_partition_ctx(req_part);
        } else {
                api_req->ready_partitions_list.push_back(&req_part->partitions_list_ll);
        }

        unlink_broker_req(br_req, __LINE__);
        put_broker_api_request(br_req);

        try_make_api_req_ready(api_req, __LINE__);
        return true;
}

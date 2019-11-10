#include "client_common.h"

bool TankClient::process_srv_status(connection *const c, const uint8_t *const content, const size_t len) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        const auto *p      = content;
        const auto  e      = p + len;
        const auto  req_id = decode_pod<uint32_t>(p);
        const auto  _it    = pending_brokers_requests.find(req_id);

        if (_it == pending_brokers_requests.end()) {
                return true;
        }

        auto br_req  = _it->second;
        auto api_req = br_req->api_req;
        TANK_EXPECT(br_req->partitions_list.size() == 1);
        auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

        [[maybe_unused]] const auto global_flags  = decode_pod<uint8_t>(p);
        const auto                  cluster_aware = decode_pod<bool>(p);
        auto &                      counts        = req_part->as_op.srv_status.counts;
        auto &                      metrics       = req_part->as_op.srv_status.metrics;

        counts.topics     = decode_pod<uint32_t>(p);
        counts.partitions = decode_pod<uint32_t>(p);

        if (cluster_aware) {
                [[maybe_unused]] const auto cluster_flags = decode_pod<uint8_t>(p);
                const auto                  l = req_part->as_op.srv_status.cluster_name.len = decode_pod<uint8_t>(p);

                memcpy(req_part->as_op.srv_status.cluster_name.data, p, l);
                p += l;
                req_part->as_op.srv_status.counts.nodes = decode_pod<uint32_t>(p);
        } else {
                req_part->as_op.srv_status.counts.nodes         = 1;
                req_part->as_op.srv_status.cluster_name.len     = 0;
                req_part->as_op.srv_status.cluster_name.data[0] = '\0';
        }

        counts.open_partitions                = 0;
        req_part->as_op.srv_status.startup_ts = 0;
        req_part->as_op.srv_status.version    = 0;
        memset(&metrics, 0, sizeof(metrics));
        if (p < e) {
                counts.open_partitions                = decode_pod<uint32_t>(p);
                metrics.time_open_partitions          = decode_pod<uint32_t>(p);
                req_part->as_op.srv_status.startup_ts = decode_pod<time32_t>(p);

                if (p < e) {
                        req_part->as_op.srv_status.version = decode_pod<uint32_t>(p);
                }
        }

        req_part->partitions_list_ll.detach_and_reset();
        api_req->ready_partitions_list.push_back(&req_part->partitions_list_ll);

        unlink_broker_req(br_req, __LINE__);
        put_broker_api_request(br_req);

        try_make_api_req_ready(api_req, __LINE__);
        return true;
}

TankClient::broker_outgoing_payload *TankClient::build_srv_status_broker_req_payload(const broker_api_request *br_req) {
        auto payload = new_req_payload(const_cast<broker_api_request *>(br_req));
        auto b       = payload->b;
        auto api_req = br_req->api_req;
        TANK_EXPECT(api_req);
        TANK_EXPECT(br_req->partitions_list.size() == 1);
        [[maybe_unused]] auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

        TANK_EXPECT(b);
        b->pack(static_cast<uint8_t>(TankAPIMsgType::Status));
        b->pack(static_cast<uint32_t>(0));

        b->pack(br_req->id);

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1;

        return payload;
}

uint32_t TankClient::service_status() {
        auto                                                      api_req  = get_api_request(4 * 1000);
        auto                                                      br       = any_broker();
        auto                                                      req_part = get_request_partition_ctx(); // dummy
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        api_req->type = api_request::Type::SrvStatus;

        req_part->topic.reset();
        req_part->partition = 0; // no tused

        contexts.emplace_back(std::make_pair(br, req_part));
        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        return schedule_new_api_req(std::move(api_req));
}

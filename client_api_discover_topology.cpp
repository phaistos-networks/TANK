#include "client_common.h"

bool TankClient::process_discover_topology(connection *const c, const uint8_t *const content, const size_t len) {
        assert(c);
        assert(c->type == connection::Type::Tank);
        const auto                 *p      = content;
        [[maybe_unused]] const auto e      = p + len;
        const auto                  req_id = decode_pod<uint32_t>(p);
        const auto                  _it    = pending_brokers_requests.find(req_id);

        if (_it == pending_brokers_requests.end()) {
                return true;
        }

        auto br_req  = _it->second;
        auto api_req = br_req->api_req;

        assert(api_req);
        assert(1u == br_req->partitions_list.size());

        auto       all      = std::make_unique<std::vector<srv_node>>();
        auto       req_part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);
        const auto n        = decode_pod<uint16_t>(p);

        if (n == std::numeric_limits<uint16_t>::max()) {
                // single-node configuration
                req_part->as_op.discover_topology.response.cluster_name.reset();
        } else {
                const str_view8 cn(reinterpret_cast<const char *>(p) + 1, *p);
                srv_node        node;

                p += sizeof(uint8_t) + cn.size();

                for (std::size_t i = 0; i < n; ++i) {
                        node.id = decode_pod<uint16_t>(p);

                        const auto flags = decode_pod<uint8_t>(p);

                        node.available = flags & (1u << 1);
                        node.blocked   = flags & (1u << 2);

                        if (flags & (1u << 0)) {
                                node.ep.addr4 = decode_pod<uint32_t>(p);
                                node.ep.port  = decode_pod<uint16_t>(p);
                        } else {
                                node.ep.addr4 = INADDR_NONE;
                        }

                        node.partitions = decode_pod<uint16_t>(p);
                        all->emplace_back(node);
                }

                req_part->as_op.discover_topology.response.cluster_name = cn.make_copy();
        }

        req_part->as_op.response_valid                 = true;
        req_part->as_op.discover_topology.response.all = all.release();

        api_req->track_ready_part_req(req_part);

        release_broker_req(br_req);
        try_make_api_req_ready(api_req, __LINE__);

        return true;
}

TankClient::broker_outgoing_payload *TankClient::build_discover_topology_req_payload(const broker_api_request *const br_req) {
        auto                  payload  = new_req_payload(const_cast<broker_api_request *>(br_req));
        auto                  b        = payload->b;
        auto                  api_req  = br_req->api_req;
        [[maybe_unused]] auto req_part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

        assert(b);
        assert(api_req);
        assert(br_req->partitions_list.size() == 1u);
        assert(req_part->topic.empty());

        b->pack(static_cast<uint8_t>(TankAPIMsgType::DiscoverTopology));
        b->pack(static_cast<uint32_t>(0));

        b->pack(br_req->id);
        b->pack(static_cast<uint8_t>(0u)); // flags

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch length

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1u;

        return payload;
}

uint32_t TankClient::discover_topology() {
        auto                                                      api_req  = get_api_request(4 * 1000);
        auto                                                      br       = any_broker();
        auto                                                      req_part = get_request_partition_ctx();
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        api_req->type = api_request::Type::DiscoverTopology;

        req_part->topic.reset();
        req_part->partition = 0u;

        contexts.emplace_back(std::make_pair(br, req_part));
        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        return schedule_new_api_req(std::move(api_req));
}

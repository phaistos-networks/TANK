#include "client_common.h"

// api_request: represents an API request initiated by the client
// broker_api_request: represents a request to a broker, associated with an API request. There can be multiple such requests for each api_request, and could be more than one such request for the same broker
// Partition-specific context associated with an broker_api_request, based on the api_request
bool TankClient::process_discover_topics(connection *const c, const uint8_t *const content, const size_t len) {
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

        assert(1u == br_req->partitions_list.size());

        auto       all           = std::make_unique<std::vector<srv_topic>>();
        auto       req_part      = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next); // was dummy, but we needed it
        const auto n             = decode_pod<uint32_t>(p);
        const auto cluster_aware = decode_pod<bool>(p);

        for (std::size_t i = 0; i < n; ++i) {
                const str_view8 name = intern_topic(str_view8(reinterpret_cast<const char *>(p) + 1, *p));

                p += sizeof(uint8_t) + name.size();

                const bool    enabled = decode_pod<bool>(p);
                const auto    np      = decode_pod<uint16_t>(p);
                const uint8_t rf      = cluster_aware ? decode_pod<uint8_t>(p) : 1u;

                all->emplace_back(srv_topic{
                    .name        = name,
                    .partitions  = np,
                    .enabled     = enabled,
                    .repl_factor = rf,
                });
        }

        req_part->as_op.response_valid               = true;
        req_part->as_op.discover_topics.response.all = all.release();

        // we will detach req_part from br_req->partitions_list
        // and will push it back into api_req->ready_partitions_list
        api_req->track_ready_part_req(req_part);

        // will inboke unlink_broker_req() and put_broker_api_request()
        // unlink_broker_req() will erase from pending_brokers_requests, and remove br_req from its api_req and from the pending responses of the broker
        release_broker_req(br_req);

        // try_make_api_req_ready() will check if there it is ready() and if so, will make_api_req_ready() it
        // an api_request is ready when it has no retry bundles and its broker_requests_list is empty
        //
        // make_api_req_ready() will
        // - cancel the timer if there is one set to abort the api request
        // - abort any retry bundles of the api_req
        // - abort any broker requests of the api_req
        // - deal with the api_req contexts
        // - delete it from the pending_responses
        // - will either retain it for the next iteratior, or gc it
        try_make_api_req_ready(api_req, __LINE__);
        return true;
}

// see assign_req_partitions_to_api_req()
// whereas one(usually) broker_api_request was created for each involved broker in an api_request
TankClient::broker_outgoing_payload *TankClient::build_discover_topics_broker_req_payload(const broker_api_request *const br_req) {
        auto                  payload  = new_req_payload(const_cast<broker_api_request *>(br_req)); // just get_payload() and assign broker_req to br_req
        auto                  b        = payload->b;
        auto                  api_req  = br_req->api_req;
        [[maybe_unused]] auto req_part = containerof(request_partition_ctx, partitions_list_ll, br_req->partitions_list.next);

        assert(b);
        assert(api_req);
        assert(br_req->partitions_list.size() == 1u);
        assert(req_part->topic.empty());

        b->pack(static_cast<uint8_t>(TankAPIMsgType::DiscoverTopics));
        b->pack(static_cast<uint32_t>(0u));

        b->pack(br_req->id);
        b->pack(static_cast<uint8_t>(0u)); // flags

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch length

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1u;

        return payload;
}

uint32_t TankClient::discover_topics() {
        auto                                                      api_req  = get_api_request(4 * 1000);
        auto                                                      br       = any_broker();
        auto                                                      req_part = get_request_partition_ctx(); // dummy
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        api_req->type = api_request::Type::DiscoverTopics;

        // we need a ddummy req_part (i.e a request partition context)
        req_part->topic.reset();
        req_part->partition = 0u;

        contexts.emplace_back(std::make_pair(br, req_part));
        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        return schedule_new_api_req(std::move(api_req));
}

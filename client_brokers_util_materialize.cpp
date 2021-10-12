// XXX: request_partition_ctx::as_op.response_valid
#include "client_common.h"

bool TankClient::materialize_consume_api_request(api_request *api_req) {
        enum {
                trace = false,
        };
        const auto req_id = api_req->request_id;

        if (trace) {
                SLog("Materializing consume request, ", api_req->ready_partitions_list.size(), " partitions\n");
        }

        for (auto it : api_req->ready_partitions_list) {
                auto                                       req_part = containerof(request_partition_ctx, partitions_list_ll, it);
                const auto                                &resp_ctx = req_part->as_op.consume.response;
                const range_base<consumed_msg *, uint32_t> msgs(
                    const_cast<consumed_msg *>(resp_ctx.msgs.cnt <= sizeof_array(resp_ctx.msgs.list.small)
                                                   ? resp_ctx.msgs.list.small
                                                   : resp_ctx.msgs.list.large),
                    resp_ctx.msgs.cnt);

                if (trace) {
                        SLog("Got ", resp_ctx.msgs.cnt, " for ", req_part->topic, "/", req_part->partition, "\n");
                }

                consumed_content.emplace_back(partition_content{
                    .clientReqId       = req_id,
                    .topic             = req_part->topic,
                    .partition         = req_part->partition,
                    .msgs              = msgs,
                    .respComplete      = true,
                    .drained           = resp_ctx.drained,
                    .next.seqNum       = resp_ctx.next.seq_num,
                    .next.minFetchSize = static_cast<uint32_t>(resp_ctx.next.min_size),
                });

                // we will not reset req_part->as_op.response_valid to false
                // because we not migrate anything to the api_request
        }

        return true;
}

bool TankClient::materialize_reload_config_request(api_request *api_req) {
        if (not api_req->ready_partitions_list.empty()) {
                const auto req_part = containerof(request_partition_ctx, partitions_list_ll, api_req->ready_partitions_list.next);

                reload_conf_results_v.emplace_back(reload_conf_result{
                    .clientReqId = api_req->request_id,
                    .topic       = req_part->topic,
                    .partition   = req_part->partition,
                });
        }

        return false;
}

bool TankClient::materialize_create_topic_request(api_request *api_req) {
        if (not api_req->ready_partitions_list.empty()) {
                const auto req_part = containerof(request_partition_ctx, partitions_list_ll, api_req->ready_partitions_list.next);

                created_topics_v.emplace_back(created_topic{
                    .clientReqId = api_req->request_id,
                    .topic       = req_part->topic,
                });
        }

        return false;
}

bool TankClient::materialize_discover_topology_request(api_request *api_req) {
        assert(api_req);
        assert(api_req->type == api_request::Type::DiscoverTopology);

        if (api_req->ready_partitions_list.empty()) {
                return false;
        }

        const auto req_part = containerof(request_partition_ctx,
                                          partitions_list_ll,
                                          api_req->ready_partitions_list.next);
        auto       all      = req_part->as_op.discover_topology.response.all;

        _discovered_topologies.emplace_back(discovered_topology{
            .client_req_id = api_req->request_id,
            .cluster_name  = req_part->as_op.discover_topology.response.cluster_name,
            .nodes.size    = static_cast<uint32_t>(all->size()),
            .nodes.data    = all->data(),
        });

        api_req->materialized_resp.discover_topology.v                 = all;
        api_req->materialized_resp.discover_topology._cluster_name_ptr = const_cast<char *>(req_part->as_op.discover_topology.response.cluster_name.p);

        req_part->as_op.response_valid = false;
        return true; // retain
}

bool TankClient::materialize_discover_topics_request(api_request *api_req) {
        assert(api_req);

        if (api_req->ready_partitions_list.empty()) {
                return false;
        }

        // just one, dummy, part context
        const auto req_part = containerof(request_partition_ctx,
                                          partitions_list_ll,
                                          api_req->ready_partitions_list.next);
        auto       all      = req_part->as_op.discover_topics.response.all;

        all_discovered_topics.emplace_back(discovered_srv_topics{
            .client_req_id = api_req->request_id,
            .topics.size   = static_cast<uint32_t>(all->size()),
            .topics.data   = all->data(),
        });

        api_req->materialized_resp.discover_topics.v = all;

        req_part->as_op.response_valid = false;
        return true; // retain
}

bool TankClient::materialize_srv_request(api_request *api_req) {
        if (api_req->ready_partitions_list.empty()) {
                return false;
        }

        const auto req_part = containerof(request_partition_ctx,
                                          partitions_list_ll,
                                          api_req->ready_partitions_list.next);
        srv_status s;

        s.counts     = req_part->as_op.srv_status.counts;
        s.metrics    = req_part->as_op.srv_status.metrics;
        s.startup_ts = req_part->as_op.srv_status.startup_ts;
        s.version    = req_part->as_op.srv_status.version;
        TANK_EXPECT(req_part->as_op.srv_status.cluster_name.len <= sizeof(s.cluster_name.data));
        s.cluster_name.len = req_part->as_op.srv_status.cluster_name.len;
        memcpy(s.cluster_name.data,
               req_part->as_op.srv_status.cluster_name.data,
               req_part->as_op.srv_status.cluster_name.len);

        collected_cluster_status_v.emplace_back(s);
        return false;
}

bool TankClient::materialize_produce_request(api_request *api_req) {
        enum {
                trace = false,
        };

        if (trace) {
                SLog("To materialize:", api_req->ready_partitions_list.size(), "\n");
        }

        for (const auto it : api_req->ready_partitions_list) {
                const auto req_part = containerof(request_partition_ctx, partitions_list_ll, it);

                produce_acks_v.emplace_back(produce_ack{
                    .clientReqId = api_req->request_id,
                    .topic       = req_part->topic,
                    .partition   = req_part->partition});
        }

        return false;
}

bool TankClient::materialize_discover_partitions_request(api_request *api_req) {
        enum {
                trace = false,
        };
        const auto req_id = api_req->request_id;

        if (trace) {
                SLog("Materializing discover_partitions request, ", api_req->ready_partitions_list.size(), " partitions\n");
        }

        // we can have multiple because multiple brokers may have been involved
        std::unique_ptr<std::vector<std::pair<uint16_t, std::pair<uint64_t, uint64_t>>>> all;
        str_view8                                                                        topic;

        while (not api_req->ready_partitions_list.empty()) {
                auto it       = api_req->ready_partitions_list.next;
                auto req_part = containerof(request_partition_ctx, partitions_list_ll, it);

                if (auto a = req_part->as_op.discover_partitions.response.all) {
                        topic = req_part->topic;

                        if (not all) {
                                all.reset(a);
                        } else {
                                all->insert(all->end(), a->begin(), a->end());

                                // no need to keep this around
                                delete a;
                        }
                }

                req_part->as_op.response_valid = false;
                discard_request_partition_ctx(api_req, req_part);

                it->detach_and_reset();
        }

        if (const size_t n = all ? all->size() : 0) {
                // we need to sort here because they may have arrived out of order
                std::sort(all->begin(), all->end(), [](const auto &a, const auto &b) noexcept {
                        return a.first < b.first;
                });

                if (trace) {
                        for (const auto &[partition, ctx] : *all) {
                                SLog(partition, " ", ctx, "\n");
                        }
                }

                const auto max = all->back().first;
                auto       v   = static_cast<std::pair<uint64_t, uint64_t> *>(malloc(sizeof(std::pair<uint64_t, uint64_t>) * (max + 1)));
                uint16_t   partition{0}, i{0};

                while (partition <= max) {
                        if (all->at(i).first == partition) {
                                v[partition] = all->at(i++).second;
                        } else {
                                v[partition] = {0, 0};
                        }

                        ++partition;
                }

                all_discovered_partitions.emplace_back(discovered_topic_partitions{
                    .clientReqId = req_id,
                    .topic       = topic,
                    .watermarks  = {v, static_cast<uint16_t>(max + 1)},
                });

                api_req->materialized_resp.discover_partitions.v = v;
                return true;
        }

        return false;
}

// Whenever an api_request becomes ready, it is processed to generate
// a response or whatever is appropriate based on the type of request
//
// returns true if the the api_req will be retained to be destroyed by gc_api_request() in the next
// iteration (in begin_reactor_loop_iteration() impl.) as opposed to immediately
bool TankClient::materialize_api_response(api_request *const api_req) {
        assert(api_req);

        switch (api_req->type) {
                case api_request::Type::Consume:
                        return materialize_consume_api_request(api_req);

                case api_request::Type::DiscoverPartitions:
                        return materialize_discover_partitions_request(api_req);

                case api_request::Type::Produce:
                case api_request::Type::ProduceWithSeqnum:
                        return materialize_produce_request(api_req);

                case api_request::Type::CreateTopic:
                        return materialize_create_topic_request(api_req);

                case api_request::Type::ReloadConfig:
                        return materialize_reload_config_request(api_req);

                case api_request::Type::SrvStatus:
                        return materialize_srv_request(api_req);

                case api_request::Type::DiscoverTopics:
                        return materialize_discover_topics_request(api_req);

                case api_request::Type::DiscoverTopology:
                        return materialize_discover_topology_request(api_req);

                default:
                        IMPLEMENT_ME();
        }
}

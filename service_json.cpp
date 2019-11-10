#include "service_common.h"
#include <ext/json/single_include/nlohmann/json.hpp>
#include <iostream>
#include <base64.h>
#include <boost/sort/spreadsort/spreadsort.hpp>

// https://nlohmann.github.io/json/classnlohmann_1_1basic__json_a5a0339361f3282cb8fd2f9ede6e17d72.html#a5a0339361f3282cb8fd2f9ede6e17d72
// https://github.com/nlohmann/json#examples
using json = nlohmann::json;

Switch::endpoint Service::decode_endpointb64(const std::string &s) {
        static constexpr bool trace{false};
        Switch::endpoint      res{.addr4 = 0, .port = 0};

        if (!s.empty()) {
                base64_dec_buffer.clear();

                if (const auto r = Base64::Decode(reinterpret_cast<const uint8_t *>(s.data()), s.size(), &base64_dec_buffer); r == sizeof(uint32_t) + sizeof(uint16_t)) {
                        const auto *p = reinterpret_cast<const uint8_t *>(base64_dec_buffer.data());

                        res.addr4 = decode_pod<uint32_t>(p);
                        res.port  = decode_pod<uint16_t>(p);
                } else if (r > "0.0.0.0:"_len) {
                        // maybe it's really an endpoint
                        if (const auto s = base64_dec_buffer.as_s32().divided(':'); s.first.size() >= "0.0.0.0"_len && s.second.all_of_digits()) {
                                const char *p = s.first.data(), *const end = p + s.first.size();
                                uint32_t octet;
                                // otherwise we 'd need to (res<<8)|octer and return htonl(res)
                                uint8_t *const octets = reinterpret_cast<uint8_t *>(&res.addr4);
                                uint8_t        i{0};

                                for (;; ++p) {
                                        if (p == end || !isdigit(*p)) {
                                                break;
                                        }

                                        for (octet = *p++ - '0'; p != end && isdigit(*p); ++p) {
                                                octet = octet * 10 + (*p - '0');
                                        }

                                        if (unlikely(octet > 255)) {
                                                break;
                                        }

                                        octets[i] = octet;
                                        if (i != 3) {
                                                if (p == end || *p != '.') {
                                                        break;
                                                } else {
                                                        ++i;
                                                }
                                        } else {
                                                break;
                                        }
                                }

                                if (i == 3 && p == end) {
                                        if (const auto u64 = s.second.as_uint64(); u64 && u64 <= std::numeric_limits<uint64_t>::max()) {
                                                res.port = u64;
                                        }
                                }
                        } else if (trace) {
                                SLog("Failed to decode endpoint from ", base64_dec_buffer.as_s32(), "\n");
                        }
                } else if (trace) {
                        SLog("Failed to decode endpoint from repr of length ", r, "\n");
                }
        } else if (trace) {
                SLog("Cannot decode endpoint from empty string\n");
        }

        return res;
}

static void decode_replica_ids(const str_view32 s, std::vector<nodeid_t> *out) {
        // we just use ',' separated list of IDs here
        // it's not as efficient as encoding u16s but that's never a problem in practice so it will need to do
        for (const auto it : s.split(',')) {
                if (it.all_of_digits()) {
                        if (const auto v = it.as_uint64(); v && v <= std::numeric_limits<uint16_t>::max()) {
                                out->emplace_back(static_cast<uint16_t>(v));
                        }
                }
        }
}

// set modify_index_limit to 0 if you don't want only the
// keys that were updated since last time we monitored that keyspace
static void iterate_consul_keys(const str_view32 tank_ns, const str_view32 cluster_name,
                                const str_view32 content, const uint64_t modify_index_limit, const str_view32 ns, std::function<void(const str_view32, const uint64_t, const json &)> f) {
        if (!content) {
                return;
        }

        try {
                if (const auto doc = json::parse(content); doc.is_array()) {
                        for (const auto &v : doc) {
                                if (!v.is_object()) {
                                        continue;
                                }

                                if (const auto key = v["Key"]; key.is_string()) {
                                        auto       key_str = key.get<std::string>();
                                        str_view32 key_s32(key_str.data(), key_str.size());

                                        if (!key_s32.StripPrefix(tank_ns.data(), tank_ns.size()) || !key_s32.StripPrefix(_S("/clusters/"))) {
                                                SLog("Expected <namespace>/clusters/\n");
                                                continue;
                                        }

                                        if (!key_s32.StripPrefix(cluster_name.data(), cluster_name.size())) {
                                                SLog("Expected <cluster_name>, instead [", key_s32, "] cluster name [", cluster_name, "]\n");
                                                continue;
                                        }

                                        if (!key_s32.StripPrefix(_S("/")) || !key_s32.StripPrefix(ns.data(), ns.size()) || !key_s32.StripPrefix(_S("/"))) {
                                                SLog("Epected /", ns, "/ from [", key_s32, "]\n");
                                                continue;
                                        }

                                        if (key_s32.empty() || key_s32.size() > 64) {
                                                continue;
                                        }

                                        uint64_t modify_index;

                                        if (const auto it = v.find("ModifyIndex"); it != v.end() && it->is_number()) {
                                                modify_index = it->get<uint64_t>();

                                                if (modify_index <= modify_index_limit) {
                                                        // ideally, consul would only
                                                        // return keys where their (ModifyIndex > request["index"])
                                                        // but for now at least, it doesn't so we are going to filter
                                                        // those keys here
                                                        //
                                                        // For some namespaces, we don't want to filter any keys based
                                                        // on their ModifyIndex
                                                        continue;
                                                }
                                        } else {
                                                modify_index = 0;
                                        }

                                        f(key_s32, modify_index, v);

                                } else {
                                        SLog("Key is not set or not a string\n");
                                }
                        }
                }

        } catch (const std::exception &e) {
                IMPLEMENT_ME();
        }
}

// Whenever a configuration is updated, an atomic transaction will be used to
// update the actual key in /configs and in /conf-updates j
bool Service::handle_consul_resp_conf_updates(const str_view32 content, const bool no_apply) {
        auto &a       = reusable.json_a;
        auto &updates = reusable.conf_updates;

        a.reuse();
        updates.clear();

        iterate_consul_keys(cluster_state.tank_ns, cluster_state.name(),
                            content, consul_state.conf_updates_monitor_modify_index,
                            "conf-updates"_s32, [&](const auto key, const uint64_t modify_index, const json &v) {
                                    updates.emplace_back(std::make_pair(a.make_copy(key.as_s8()), modify_index));
                            });

        std::sort(updates.begin(), updates.end(), [](const auto &a, const auto &b) noexcept {
                return a.first.Cmp(b.first) < 0;
        });

        consider_updated_consul_configs(updates, no_apply);
        return true;
}

// we expect to process one key per partition
// <topic>/<partition> (except for the CLUSTER leader which is named CLUSTER)
// where Flags is used to hold the replica_id (there is no need to use the value)
bool Service::handle_consul_resp_leaders(const str_view32 content) {
        static constexpr bool trace{false};
        auto &                a              = reusable.json_a;
        auto &                new_leadership = reusable.new_leadership;
        auto &                intern_map     = reusable.intern_map;

        a.reuse();
        new_leadership.clear();
        intern_map.clear();

        iterate_consul_keys(cluster_state.tank_ns, cluster_state.name(),
                            content, /* consul_state.leaders_monitor_modify_index */ 0,
                            "leaders"_s32, [&](const auto key, const uint64_t modify_index, const json &v) {
                                    bool locked{false};

                                    if (const auto it = v.find("Session"); it != v.end()) {
                                            if (it->is_string() && it->get<std::string>().size() == 36) {
                                                    locked = true;
                                            }
                                    }

                                    // we now have <topic_name>/<partition>
                                    const auto [first, second] = key.divided('/');
                                    uint64_t  _partition_id;
                                    str_view8 topic_name;
                                    uint64_t  replica_id;

                                    if (first.Eq(_S("CLUSTER")) && !second) {
                                            // Special-cased for cluster; we need to discover who the leader is
                                            _partition_id = 0;
                                    } else {
                                            if (!first || !second || !second.all_of_digits()) {
                                                    if constexpr (trace) {
                                                            SLog("Expected <token>/<partition> from ", key, "\n");
                                                    }

                                                    return;
                                            }

                                            if (first.size() > 64) {
                                                    IMPLEMENT_ME();
                                            }

                                            _partition_id = second.as_uint64();
                                            if (_partition_id > std::numeric_limits<uint16_t>::max()) {
                                                    IMPLEMENT_ME();
                                            }

                                            topic_name = first.as_s8();

                                            const auto res = intern_map.emplace(topic_name, true);

                                            if (res.second) {
                                                    *const_cast<str_view8 *>(&res.first->first) = a.make_copy(topic_name);
                                            }

                                            topic_name = res.first->first;
                                    }

                                    if (const auto it = v.find("Value"); it != v.end()) {
                                            if (it->is_string()) {
                                                    const auto v_str = it->get<std::string>();

                                                    base64_dec_buffer.clear();
                                                    if (Base64::Decode(reinterpret_cast<const uint8_t *>(v_str.data()), v_str.size(), &base64_dec_buffer) > 0) {
                                                            replica_id = base64_dec_buffer.as_s32().as_uint64();
                                                    } else {
                                                            replica_id = 0;
                                                    }
                                            } else if (it->is_null()) {
                                                    // allowed
                                                    replica_id = 0;
                                            } else {
                                                    if constexpr (trace) {
                                                            SLog("Unexpected value\n");
                                                    }

                                                    return;
                                            }
                                    } else {
                                            if constexpr (trace) {
                                                    SLog("Value is missing\n");
                                            }

                                            return;
                                    }

                                    // replica_id can be 0
                                    // if there is no current leader
                                    if (replica_id > std::numeric_limits<uint16_t>::max()) {
                                            if constexpr (trace) {
                                                    SLog("Invalid replica ", replica_id, " for [", first, "]\n");
                                            }

                                            return;
                                    }

                                    if constexpr (trace) {
                                            SLog(ansifmt::bold, ansifmt::color_green, "Leader ", topic_name, "/", _partition_id, " =>  ", replica_id, ", locked = ", locked, ansifmt::reset, "\n");
                                    }

                                    new_leadership.emplace_back(partition_leader_update{
                                        .topic     = topic_name,
                                        .partition = static_cast<uint16_t>(_partition_id),
                                        .leader_id = static_cast<uint16_t>(replica_id),
                                        .locked    = locked,
                                    });
                            });

        reconcile_leadership(&new_leadership);
        return true;
}

// List of all known nodes has changed
// Maybe nodes were added or removed or their endpoints have changed
// XXX: we need to check if Session is present; if not, it means the node
// that once owned that id is not available now. It's very important
bool Service::handle_consul_resp_nodes(const str_view32 content) {
        static constexpr bool trace{false};
        auto &                updates = reusable.nodes_updates;

        if (trace) {
                SLog("Procesing response from:", content, "\n");
        }

        updates.clear();
        iterate_consul_keys(cluster_state.tank_ns, cluster_state.name(),
                            content, /* consul_state.nodes_monitor_modify_index */ 0,
                            "nodes"_s32, [&](const auto key, const uint64_t modify_index, const json &v) {
                                    // we now have <topic_name>/<partition>
                                    if (!key.all_of_digits()) {
                                            if (trace) {
                                                    SLog("Expected valid node id\n");
                                            }
                                            return;
                                    }

                                    const auto node_id = key.as_uint64();

                                    if (!node_id || node_id > std::numeric_limits<nodeid_t>::max()) {
                                            if (trace) {
                                                    SLog("Unexpected node id\n");
                                            }
                                            return;
                                    }

                                    std::string v_str, sess_str;

                                    if (const auto it = v.find("Value"); it != v.end()) {
                                            if (it->is_string()) {
                                                    v_str = it->get<std::string>();
                                            } else if (it->is_null()) {
                                                    // allowed
                                            } else {
                                                    if (trace) {
                                                            SLog("Unexpected value\n");
                                                    }
                                                    return;
                                            }
                                    } else {
                                            if (trace) {
                                                    SLog("Value is missing\n");
                                            }
                                            return;
                                    }

                                    if (const auto it = v.find("Session"); it != v.end()) {
                                            if (it->is_string()) {
                                                    sess_str = it->get<std::string>();
                                            } else if (it->is_null()) {
                                                    // allowed
                                            } else {
                                                    if (trace) {
                                                            SLog("Unexpected value\n");
                                                    }
                                                    return;
                                            }
                                    }

                                    auto e = decode_endpointb64(v_str);

                                    // if we have a session, that node that (used) to own
                                    // that session is still alive
                                    if (trace) {
                                            SLog("Got node [", node_id, "] session [", sess_str, "] ", e, "\n");
                                    }

                                    updates.emplace_back(cluster_nodeid_update{
                                        .id         = static_cast<nodeid_t>(node_id),
                                        .ep         = e,
                                        .have_owner = sess_str.size() == 36});
                            });

        std::sort(updates.begin(), updates.end(), [](const auto &a, const auto &b) noexcept {
                return a.id < b.id;
        });

        reconcile_cluster_nodes(updates);
        return true;
}

// This is pretty much identical to handle_consul_resp_topology()
bool Service::handle_consul_resp_isr(const str_view32 content) {
        static constexpr bool trace{false};
        auto &                a            = reusable.json_a;
        auto &                updates      = reusable.isr_updates;
        auto &                all_replicas = reusable.all_replicas;
        auto &                intern_map   = reusable.intern_map;

        a.reuse();
        updates.clear();
        all_replicas.clear();
        intern_map.clear();

        if (trace) {
                SLog("ISR CONTENT:", content, "\n");
        }

        iterate_consul_keys(cluster_state.tank_ns, cluster_state.name(),
                            content, consul_state.isrs_monitor_modify_index,
                            "ISR"_s32, [&](const auto key, const uint64_t modify_index, const json &v) {
                                    const auto [first, second] = key.divided('/');

                                    if (!first || !second || !second.all_of_digits()) {
                                            if (trace) {
                                                    SLog("Expected <token>/<partition> from ", key, "\n");
                                            }
                                            return;
                                    }

                                    if (first.size() > 64) {
                                            IMPLEMENT_ME();
                                    }

                                    const auto _partition_id = second.as_uint64();

                                    if (_partition_id > std::numeric_limits<uint16_t>::max()) {
                                            IMPLEMENT_ME();
                                    }

                                    auto        topic_name = first.as_s8();
                                    const auto  res        = intern_map.emplace(topic_name, true);
                                    std::string v_str;

                                    if (res.second) {
                                            *const_cast<str_view8 *>(&res.first->first) = a.make_copy(topic_name);
                                    }
                                    topic_name = res.first->first;

                                    if (const auto it = v.find("Value"); it != v.end()) {
                                            if (it->is_string()) {
                                                    v_str = it->get<std::string>();
                                            } else if (it->is_null()) {
                                                    // allowed
                                            } else {
                                                    SLog("Unexpected value\n");
                                                    return;
                                            }
                                    } else {
                                            SLog("Value is missing\n");
                                            return;
                                    }

                                    const auto n = all_replicas.size();

                                    if (!v_str.empty()) {
                                            base64_dec_buffer.clear();

                                            if (Base64::Decode(reinterpret_cast<const uint8_t *>(v_str.data()), v_str.size(), &base64_dec_buffer) > 0) {
                                                    decode_replica_ids(base64_dec_buffer.as_s32(), &all_replicas);

                                                    if (const auto cnt = all_replicas.size() - n; cnt > ClusterState::K_max_replicas) {
                                                            all_replicas.resize(n + ClusterState::K_max_replicas);
                                                    }

                                                    boost::sort::spreadsort::spreadsort(all_replicas.begin() + n, all_replicas.end());

                                                    if (trace) {
                                                            SLog("Got ", all_replicas.size(), " from [", base64_dec_buffer.as_s32(), "]\n");
                                                    }
                                            } else if (trace) {
                                                    SLog("Failed to decode replicas string\n");
                                            }
                                    } else if (trace) {
                                            SLog("No replicas string\n");
                                    }

                                    updates.emplace_back(partition_isr_info{
                                        .topic     = topic_name,
                                        .partition = static_cast<uint16_t>(_partition_id),
                                        .gen       = modify_index,
                                        .replicas  = range_base<uint16_t *, uint8_t>{reinterpret_cast<uint16_t *>(n), static_cast<uint8_t>(all_replicas.size() - n)}});
                            });
        // patch
        for (auto &it : updates) {
                it.replicas.offset = all_replicas.data() + reinterpret_cast<size_t>(it.replicas.offset);
        }

        reconcile_cluster_ISR(&updates);
        return true;
}

bool Service::handle_consul_resp_topology(const str_view32 content) {
        static constexpr bool trace{false};
        auto &                a            = reusable.json_a;
        auto &                new_topology = reusable.new_topology;
        auto &                all_replicas = reusable.all_replicas;
        auto &                intern_map   = reusable.intern_map;

        a.reuse();
        new_topology.clear();
        all_replicas.clear();
        intern_map.clear();

        iterate_consul_keys(cluster_state.tank_ns, cluster_state.name(),
                            content, /* consul_state.topology_monitor_modify_index */ 0, "topology"_s32, [&](const auto key, const uint64_t modify_index, const json &v) {
                                    const auto [first, second] = key.divided('/');

                                    if (!first || !second || !second.all_of_digits()) {
                                            SLog("Expected <token>/<partition> from ", key, "\n");
                                            return;
                                    }

                                    if (first.size() > 64) {
                                            IMPLEMENT_ME();
                                    }

                                    const auto _partition_id = second.as_uint64();

                                    if (_partition_id > std::numeric_limits<uint16_t>::max()) {
                                            IMPLEMENT_ME();
                                    }

                                    auto        topic_name = first.as_s8();
                                    const auto  res        = intern_map.emplace(topic_name, true);
                                    std::string v_str;

                                    if (res.second) {
                                            *const_cast<str_view8 *>(&res.first->first) = a.make_copy(topic_name);
                                    }
                                    topic_name = res.first->first;

                                    if (const auto it = v.find("Value"); it != v.end()) {
                                            if (it->is_string()) {
                                                    v_str = it->get<std::string>();
                                            } else if (it->is_null()) {
                                                    // allowed
                                            } else {
                                                    return;
                                            }
                                    }

                                    const auto n = all_replicas.size();

                                    if (!v_str.empty()) {
                                            base64_dec_buffer.clear();

                                            if (Base64::Decode(reinterpret_cast<const uint8_t *>(v_str.data()), v_str.size(), &base64_dec_buffer) > 0) {
                                                    decode_replica_ids(base64_dec_buffer.as_s32(), &all_replicas);

                                                    if (const auto cnt = all_replicas.size() - n; cnt > ClusterState::K_max_replicas) {
                                                            all_replicas.resize(n + ClusterState::K_max_replicas);
                                                    }

                                                    if (trace) {
                                                            SLog("Got [", values_repr(all_replicas.data() + n, all_replicas.size() - n), "] for ", topic_name, "/", _partition_id, "\n");
                                                    }

                                                    boost::sort::spreadsort::spreadsort(all_replicas.begin() + n, all_replicas.end());
                                                    all_replicas.resize(n + std::distance(all_replicas.begin() + n, std::unique(all_replicas.begin() + n, all_replicas.end())));

                                                    if (trace) {
                                                            SLog("Finalized to:", values_repr(all_replicas.data() + n, all_replicas.size() - n), "\n");
                                                    }

                                            } else if (trace) {
                                                    SLog("Failed to decode base64\n");
                                            }
                                    } else if (trace) {
                                            SLog("Value is unset for ", topic_name, "/", _partition_id, "\n");
                                    }

                                    new_topology.emplace_back(topology_partition{
                                        .topic     = topic_name,
                                        .partition = static_cast<uint16_t>(_partition_id),
                                        .gen       = modify_index,
                                        .replicas  = range_base<uint16_t *, uint8_t>{reinterpret_cast<uint16_t *>(n), static_cast<uint8_t>(all_replicas.size() - n)}});
                            });
        // patch
        for (auto &it : new_topology) {
                it.replicas.offset = all_replicas.data() + reinterpret_cast<size_t>(it.replicas.offset);
        }

        reconcile_cluster_topology(&new_topology);
        return true;
}

void Service::process_consul_cluster_configurations(const consul_request *const req, const str_view32 content) {
        simple_allocator a{64 * 1024};
        auto &           updates = reusable.json_conf_updates;

        updates.clear();

        iterate_consul_keys(cluster_state.tank_ns, cluster_state.name(),
                            content, 0, "configs"_s32, [&](const auto key, const uint64_t modify_index, const json &v) {
                                    uint64_t    rev;
                                    std::string v_str;

                                    if (const auto it = v.find("Value"); it != v.end()) {
                                            if (it->is_string()) {
                                                    v_str = it->get<std::string>();
                                            } else if (it->is_null()) {
                                                    // allowed
                                            } else {
                                                    SLog("Unexpected value\n");
                                                    return;
                                            }
                                    } else {
                                            SLog("Value is missing\n");
                                            return;
                                    }

                                    if (const auto it = v.find("Flags"); it != v.end() && it->is_number()) {
                                            rev = it->get<uint64_t>();
                                    } else {
                                            rev = 0;
                                    }

                                    updates.emplace_back(std::make_pair(a.make_copy(key.as_s8()),
                                                                        std::make_pair(a.make_copy(str_view32(v_str.data(), v_str.size())),
                                                                                       rev)));
                            });

        process_consul_configs(updates);
}

void Service::process_ready_consul_resp_impl(connection *c, const str_view32 content) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        const auto rc  = c->as.consul.cur.resp.rc;
        auto       req = c->as.consul.cur.req;
        TANK_EXPECT(req);
        TANK_EXPECT(!req->is_released());
        const bool succ = rc >= 200 && rc < 300;

        if (trace) {
                SLog("Got RC=", rc, " content=[", content, "] for request type ", unsigned(req->type), "\n");
        }

        // we are consul_req_over() as opposed to right before consul_state.put_req()
        // because if we don't and this is a TryBecomeClusterLeader()
        // we may invoke try_become_cluster_leader_timer() in TryBecomeClusterLeader case
        // which won't work because (consul_state.flags & unsigned(ConsulState::Flags::AttemptBecomeClusterLeader))
        consul_req_over(req);

        if (rc == 403) {
                // likely preovided the wrong credentials
                // abort immediately for now
                Print("Got unexpected access denied fault while attempting to register with the consul agent at ", consul_state.srv.endpoint, "\n");
                Print("This likely means that the API token in tank_consul.api is invalid or expired.\n");
                Print("Please verify that the API key is correct and restart this node\n");
                Print("Aborting Now\n");
                exit(1);
        }

        if (reactor_state != ReactorState::Active) {
                // if we are shutting down, we only care for DestroySession
                switch (req->type) {
                        case consul_request::Type::DestroySession:
                                unlink(Buffer{}.append(basePath_, "/.cluster_session_"_s32, cluster_state.local_node.id).c_str());
                                [[fallthrough]];
                        case consul_request::Type::ReleaseClusterLeadership:
                                if (trace) {
                                        SLog("Got RC=", rc, " content=[", content, "] for request type ", unsigned(req->type), "\n");
                                }
                                break;

                        default:
                                if (trace) {
                                        SLog("Ignoring request ", unsigned(req->type), " -- shutting down\n");
                                }
                                return;
                }
        }

        switch (req->type) {
                case consul_request::Type::CreatePartititons: {
                        if (rc == 200) {
                                gen_create_topic_succ(req);
                        } else {
                                gen_create_topic_fail(req);
                        }
                } break;

                case consul_request::Type::AcquireNodeID:
                        if (content.Eq(_S("true"))) {
                                cluster_node_id_acquired();
                        } else {
                                Print("** Unable to acquire node id. Is it already used by another cluster node?, got [", content, "]\n");
                                Print("Will Exit\n");
                                std::abort();
                        }
                        break;

                case consul_request::Type::CreateSession:
                        if (!succ) {

                        } else {
                                try {
                                        const auto doc = json::parse(content);
                                        const auto id  = doc["ID"].get<std::string>();

                                        TANK_EXPECT(id.size() <= sizeof(consul_state._session_id));
                                        consul_state._session_id_len = id.size();
                                        memcpy(consul_state._session_id, id.data(), id.size());

                                        if (trace) {
                                                SLog("Got session [", ansifmt::bold, consul_state.session_id(), ansifmt::reset, "]\n");
                                        }

                                        // we need to periodically renew the session
                                        schedule_consul_session_renewal();

                                        if (const auto bm = unsigned(ConsulState::Flags::ConfirmedSession); 0 == (consul_state.flags & bm)) {
                                                consul_state.flags |= bm;
                                                consul_sess_confirmed();
                                        }
                                } catch (const std::exception &e) {
                                        Print("Failed to get the ID:", e.what(), "\n");

                                        Print(content, "\n");
                                        IMPLEMENT_ME();
                                }
                        }
                        break;

                case consul_request::Type::RenewSession:
                        if (rc == 404 && content.BeginsWith(_S("Session id")) && content.EndsWith(_S("not found"))) {
                                // Attempted to renew a session that's no longer available
                                // we 'll create a new session
                                TANK_EXPECT(!consul_state.renew_timer.is_linked());

                                if (trace) {
                                        SLog("Will attempt to create a new session\n");
                                }

                                consul_state.flags &= ~unsigned(ConsulState::Flags::ConfirmedSession);
                                consul_state._session_id_len = 0;
                                schedule_consul_req(consul_state.get_req(consul_request::Type::CreateSession), true);
                        } else if (!succ) {
                                IMPLEMENT_ME();
                        } else {
                                if (const auto bm = unsigned(ConsulState::Flags::ConfirmedSession); 0 == (consul_state.flags & bm)) {
                                        consul_state.flags |= bm;
                                        consul_sess_confirmed();
                                }

                                schedule_consul_session_renewal();
                        }
                        break;

                case consul_request::Type::DestroySession:
                        session_released();
                        break;

                case consul_request::Type::RetrieveConfigs:
                        process_fetched_cluster_configurations(req, succ ? content : str_view32());
                        break;

                case consul_request::Type::RetrieveTopicConfig:
                        [[fallthrough]];
                case consul_request::Type::RetrieveClusterConfig:
                        process_consul_cluster_configurations(req, content);
                        break;

                case consul_request::Type::InitNS_Topology:
                case consul_request::Type::InitNS_Nodes:
                case consul_request::Type::InitNS_Leaders:
                case consul_request::Type::InitNS_ISR:
                case consul_request::Type::InitNS_ConfUpdates:
                        if (succ) {
                                if (++consul_state.ack_cnt == 5) {
                                        // We need to monitor first, and then proceed with registration
                                        monitor_consul_namespaces();
                                }
                        } else {
                                IMPLEMENT_ME();
                        }
                        break;

                case consul_request::Type::MonitorTopology:
                        // https://www.consul.io/api/kv.html
                        // we are recursively monitoring the whole TANK/clusters/<cluster> namespace
                        //
                        // Consul will _not_ send us a delta that includes changes only since last ModifyIndex
                        // If we monitor TANK/clusters/topics/foo/
                        // and we add or delete say TANK/clusters/foo/topics/partitions/10/isr
                        // then we 'll get back _all_ keys under TANK/clusters/topics/foo/ even if we only updated 1 key
                        // We can of course monitor all distinct topics, each with its own long polling connections
                        // but that seems like a bad idea. Also, even if we did that, we 'd still need to monitor clusters/foo/topics
                        // in case a new topic was created or deleted, which means that we 'll get the whole tree again if
                        // anything changed under that namespace.
                        //
                        // Topology will rarely change though. We won't be adding new topic or partitions often, nor do we plan
                        // to change the per-topic replication factor often.
                        // We are however going to likely be updating ISRs and partition leaders often which means
                        // we need to account for that
                        // To that end, the following namespace makes sense -- and it's fine if we need _3_ different long polling connections to monitor
                        // the keyspaces we need.
                        //
                        //	- TANK/clusters/<cluster>/ISR/topic/partition (value=isr)
                        //	- TANK/clusters/<cluster>/topology/topic/partition (value=whatever that includes repl.factor)
                        // 	- TANK/clusters/<cluster>/leaders/topic/partition (value=current leader)
                        //
                        // we will also have leaders/CLUSTER
                        // which will be the leader of the cluster, and we will be trying to to compete against it to become leaders ourselves
                        //
                        // Also, because partition configuration can be KBs in size, and we may have thousands of partitions, we don't want to
                        // monitor a namespace and get back _all_ partitions configurations if we update the configuration for one of those.
                        // We will instead need to monitor another namespace, where each key will be the <topic>/<partition> and it will
                        // have no value exception a version(Consul keys can be versioned). So whenever we want to udpate a configuration
                        // we will update the version(so that nodes that monitor that namespace can tell that a partition's configuation has been updated because it
                        // can check the version of all returned topics against the version we have locally). It will then pull those changes from another namespace we don't monitor.
                        //
                        // XXX: Another problem is that apparenly recursive does _not_ work with DELETE
                        // we should have to figure this out
                        if (!succ) {
                                auto new_req = consul_state.get_req(*req);

                                if (rc == 404) {
                                        handle_consul_resp_topology({});
                                }

                                new_req->tn.node.key = now_ms + 4 * 1000;
                                new_req->tn.type     = timer_node::ContainerType::SchedConsulReq;
                                register_timer(&new_req->tn.node);
                        } else {
                                if (handle_consul_resp_topology(content)) {
                                        consul_state.topology_monitor_modify_index = c->as.consul.cur.resp.consul_index;
                                        schedule_consul_req(consul_state.get_req(consul_request::Type::MonitorTopology), true);

                                        if (++consul_state.ack_cnt == 5) {
                                                consul_ns_retrieval_complete();
                                        }
                                }
                        }
                        break;

                case consul_request::Type::MonitorLeaders:
                        if (!succ) {
                                auto new_req = consul_state.get_req(*req);

                                if (rc == 404) {
                                        handle_consul_resp_leaders({});
                                }

                                new_req->tn.node.key = now_ms + 4 * 1000;
                                new_req->tn.type     = timer_node::ContainerType::SchedConsulReq;
                                register_timer(&new_req->tn.node);
                        } else {
                                if (handle_consul_resp_leaders(content)) {
                                        consul_state.leaders_monitor_modify_index = c->as.consul.cur.resp.consul_index;
                                        schedule_consul_req(consul_state.get_req(consul_request::Type::MonitorLeaders), true);

                                        if (++consul_state.ack_cnt == 5) {
                                                consul_ns_retrieval_complete();
                                        }
                                }
                        }
                        break;

                case consul_request::Type::MonitorConfUpdates:
                        [[fallthrough]];
                case consul_request::Type::MonitorConfUpdatesNoApply:
                        if (!succ) {
                                auto new_req = consul_state.get_req(*req);

                                if (rc == 404) {
                                        handle_consul_resp_conf_updates({}, req->type == consul_request::Type::MonitorConfUpdatesNoApply);
                                }

                                new_req->tn.node.key = now_ms + 4 * 1000;
                                new_req->tn.type     = timer_node::ContainerType::SchedConsulReq;
                                register_timer(&new_req->tn.node);
                        } else {
                                if (handle_consul_resp_conf_updates(content, req->type == consul_request::Type::MonitorConfUpdatesNoApply)) {
                                        consul_state.conf_updates_monitor_modify_index = c->as.consul.cur.resp.consul_index;
                                        schedule_consul_req(consul_state.get_req(consul_request::Type::MonitorConfUpdates), true);

                                        if (++consul_state.ack_cnt == 5) {
                                                consul_ns_retrieval_complete();
                                        }
                                }
                        }
                        break;

                case consul_request::Type::MonitorISRs:
                        if (!succ) {
                                auto new_req = consul_state.get_req(*req);

                                if (rc == 404) {
                                        handle_consul_resp_isr({});
                                }

                                new_req->tn.node.key = now_ms + 4 * 1000;
                                new_req->tn.type     = timer_node::ContainerType::SchedConsulReq;
                                register_timer(&new_req->tn.node);
                        } else {
                                if (handle_consul_resp_isr(content)) {
                                        consul_state.isrs_monitor_modify_index = c->as.consul.cur.resp.consul_index;
                                        schedule_consul_req(consul_state.get_req(consul_request::Type::MonitorISRs), true);

                                        if (++consul_state.ack_cnt == 5) {
                                                consul_ns_retrieval_complete();
                                        }
                                }
                        }
                        break;

                case consul_request::Type::MonitorNodes:
                        if (!succ) {
                                auto new_req = consul_state.get_req(*req);

                                if (rc == 404) {
                                        handle_consul_resp_nodes({});
                                }

                                new_req->tn.node.key = now_ms + 4 * 1000;
                                new_req->tn.type     = timer_node::ContainerType::SchedConsulReq;
                                register_timer(&new_req->tn.node);

                        } else {
                                if (handle_consul_resp_nodes(content)) {
                                        consul_state.nodes_monitor_modify_index = c->as.consul.cur.resp.consul_index;
                                        schedule_consul_req(consul_state.get_req(consul_request::Type::MonitorNodes), true);

                                        if (++consul_state.ack_cnt == 5) {
                                                consul_ns_retrieval_complete();
                                        }
                                }
                        }
                        break;

                case consul_request::Type::TryBecomeClusterLeader: {
                        // we will NOT invoke switch_cluster_leader_to() or resign_cluster_leadership() here
                        // we will wait for the matching response to MonitorLeaders
                        // see service_cluster.cpp comments(INVARIANTS)
                        // if (c->as.consul.cur.resp.rc == 200 && content.Eq(_S("true"))) { acquired) }
                        static constexpr bool trace{false};

                        if (trace) {
                                SLog("For TryBecomeClusterLeader we got ", rc, " [", content, "], cluster_state.leader_id = ", cluster_state.leader_id, "\n");
                        }

			cancel_timer(&try_become_cluster_leader_timer.node); // just in case

                        if (rc == 404 && content.BeginsWith(_S("Session id")) && content.EndsWith(_S("not found"))) {
                                // looks like our session has expired?
                                // we shouldn't have been able to reach this
                                // we should have acquired or renewed a session
                                IMPLEMENT_ME();
                        }

                        if (rc == 200 && content.Eq(_S("false"))) {
                                // this is where we are dealing with that stupid consul bug
                                // see initiate_tear_down() comments
                                // we will effectively try again in a while if need be.
                                // even if this is fixed, we 'll keep doing this because it's important that the cluster is always associated with a leader
                                if (trace) {
                                        SLog("Unable to acquire cluster leadership\n");
                                }

                                if (!cluster_state.leader_id) {
                                        if (trace) {
                                                SLog("But there is no current cluster leader\n");
                                        }

                                        if (!try_become_cluster_leader_timer.is_linked()) {
                                                if (trace) {
                                                        SLog("Will re-try to acquire cluster leadership in a while\n");
                                                }

                                                try_become_cluster_leader_timer.node.key = now_ms + 800;
                                                register_timer(&try_become_cluster_leader_timer.node);
                                        }
                                }
                        } 
                } break;

                case consul_request::Type::ReleaseClusterLeadership:
                case consul_request::Type::PartitionsTX:
                        break;

                default:
                        IMPLEMENT_ME();
        }
}

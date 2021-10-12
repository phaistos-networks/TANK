#include "service_common.h"
enum {
        trace = false,
};

bool Service::process_status(connection *const c, [[maybe_unused]] const uint8_t *p, [[maybe_unused]] const size_t len) {
        if (unlikely(len < sizeof(uint32_t))) {
                return shutdown(c);
        }

        auto       resp   = get_buf();
        const auto req_id = decode_pod<uint32_t>(p);

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::Status));
        const auto size_offset = resp->size();

        resp->RoomFor(sizeof(uint32_t)); // will patch length later
        resp->pack(req_id);

        resp->pack(static_cast<uint8_t>(0)); // global flags
        resp->pack(static_cast<bool>(cluster_aware()));
        resp->pack(static_cast<uint32_t>(topics.size()));
        resp->pack(static_cast<uint32_t>(partitions_v.size()));
        if (cluster_aware()) {
                resp->pack(static_cast<uint8_t>(0)); // cluster flags
                resp->pack(cluster_state._name_len)
                    .serialize(cluster_state._name, cluster_state._name_len);
                resp->pack(static_cast<uint32_t>(std::accumulate(cluster_state.all_nodes.begin(), cluster_state.all_nodes.end(), 0,
                                                                 [](const auto prev, const auto node) noexcept { return prev + node->available(); })));
        }

        resp->pack(static_cast<uint32_t>(total_open_partitions));
        resp->pack(static_cast<uint32_t>(open_partitions_time));
        resp->pack(static_cast<time32_t>(startup_ts));
        resp->pack(static_cast<uint32_t>(TANK_VERSION));

        *reinterpret_cast<uint32_t *>(resp->At(size_offset)) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();
        auto q       = c->outQ ?: (c->outQ = get_outgoing_queue());

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        return try_tx(c);
}

bool Service::process_create_topic(connection *const c, const uint8_t *p, const size_t len) {
        enum {
                trace = false,
        };

        if (unlikely(len < sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint8_t))) {
                if (trace) {
                        SLog("Unexpected len = ", len, "\n");
                }

                return shutdown(c);
        }

        auto             resp      = get_buf();
        const auto       requestId = decode_pod<uint32_t>(p);
        const strwlen8_t topicName(reinterpret_cast<const char *>(p) + 1, *p);

        p += topicName.size() + sizeof(uint8_t);

        partition_config partitionConfig;
        strwlen32_t      config;
        const auto       partitionsCnt = decode_pod<uint16_t>(p);

        if (trace) {
                SLog("Requested create_topic [", topicName, "] x ", partitionsCnt, "\n");
        }

        config.len = Compression::decode_varuint32(p);
        config.p   = reinterpret_cast<const char *>(p);
        p += config.size();

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::CreateTopic));
        const auto                                    sizeOffset = resp->size();
        std::vector<std::shared_ptr<topic_partition>> list;

        resp->RoomFor(sizeof(uint32_t));

        resp->pack(requestId, topicName);

        if (partitionsCnt == 0 || !is_valid_topic_name(topicName)) {
                // invalid request
                resp->pack(static_cast<uint8_t>(10));
        } else {
                config.TrimWS();
                if (config) {
                        try {
                                parse_partition_config(config, &partitionConfig);
                        } catch (...) {
                                if (trace) {
                                        SLog("Invalid configuration\n");
                                }

                                resp->Serialize<uint8_t>(4); // Invalid configuration
                                goto l1;
                        }
                }

                if (read_only) {
                        resp->pack(static_cast<uint8_t>(5));
                } else if (topic_by_name(topicName)) {
                        if (trace) {
                                SLog("Topic already exists\n");
                        }

                        resp->pack(static_cast<uint8_t>(1)); // already exists
                } else {
                        if (cluster_aware()) {
                                // TODO: chain requests so that we can support more than 64 partitions(limit for consul Tx values)
                                // we ignore configuration options provided here; they only matter for standalone operation mode
                                auto req = consul_state.get_req(consul_request::Type::CreatePartititons);

                                topicName.CopyTo(req->new_partitions.topic_name.data);
                                req->new_partitions.topic_name.size = topicName.size();

                                req->new_partitions.client_ch.set(c);
                                req->new_partitions.first_partition_index = 0;
                                req->new_partitions.partitions_cnt        = std::min<uint8_t>(64, partitionsCnt);
                                req->new_partitions.client_req_id         = requestId;

                                schedule_consul_req(req, true);
                                put_buf(resp);
                                // we will defer response
                                return true;
                        }

                        // we are going to create .<topic-name>
                        // and when we are done with it, we are going to rename it to <topic-name>
                        // this is important because for any reason we may not get the chance to create
                        // all partitions directories/files in there, and we don't want to leave a mess if that happens
                        char       topicPath[PATH_MAX];
                        const auto topicPathLen = Snprint(topicPath, sizeof(topicPath), basePath_, "/.", topicName, "/");

                        if (trace) {
                                SLog("Will try to create ", str_view32(topicPath, topicPathLen), "\n");
                        }

                        if (mkdir(topicPath, 0775) == -1) {
                                if (trace) {
                                        SLog("Failed to mkdir(", topicPath, "):", strerror(errno), "\n");
                                }

                                resp->pack(static_cast<uint8_t>(2));
                        } else {
                                const auto cleanup = [&]() {
                                        topicPath[topicPathLen] = '\0';
                                        rm_tankdir(topicPath);
                                };

                                try {
                                        auto t = std::make_shared<topic>(topicName, partitionConfig);

                                        // see Service::open_partition_log()
                                        t->flags |= unsigned(topic::Flags::under_construction);

                                        for (uint16_t i{0}; i < partitionsCnt; ++i) {
                                                try {
                                                        auto _p = init_local_partition(i, t.get(), partitionConfig, true);

                                                        list.emplace_back(std::move(_p));
                                                } catch (...) {
                                                        cleanup();
                                                        resp->pack(static_cast<uint8_t>(2));
                                                        goto l1;
                                                }
                                        }

                                        if (config) {
                                                int fd;

                                                strcpy(topicPath + topicPathLen, "config");
                                                fd = safe_open(topicPath, O_WRONLY | O_CREAT | O_LARGEFILE, 0775);
                                                if (fd == -1) {
                                                        if (trace) {
                                                                SLog("open(", topicPath, ") failed:", strerror(errno), "\n");
                                                        }

                                                        cleanup();
                                                        resp->pack(static_cast<uint8_t>(2));
                                                        goto l1;
                                                } else if (write(fd, config.p, config.len) != config.len) {
                                                        if (trace) {
                                                                SLog("write for (", topicPath, ") failed:", strerror(errno), "\n");
                                                        }

                                                        TANKUtil::safe_close(fd);
                                                        unlink(topicPath);
                                                        cleanup();
                                                        resp->pack(static_cast<uint8_t>(2));
                                                        goto l1;
                                                } else {
                                                        TANKUtil::safe_close(fd);
                                                }
                                        }

                                        topicPath[topicPathLen - 1] = '\0';
                                        if (rename(topicPath, Buffer{}.append(basePath_, "/", topicName).c_str()) == -1) {
                                                if (trace) {
                                                        SLog("Failed to commit topic, unable to rename ", topicPath, ": ", strerror(errno), "\n");
                                                }

                                                cleanup();
                                                resp->pack(static_cast<uint8_t>(2));
                                                goto l1;
                                        }

                                        if (trace) {
                                                SLog("Created topic\n");
                                        }

                                        TANK_EXPECT(t->flags & unsigned(topic::Flags::under_construction));
                                        t->flags ^= unsigned(topic::Flags::under_construction);
                                        t->register_partitions(list.data(), list.size());
                                        list.clear();

                                        register_topic(std::move(t));
                                        resp->pack(static_cast<uint8_t>(0));
                                } catch (const std::exception &e) {
                                        if (trace) {
                                                SLog("Exception caught:", e.what(), "\n");
                                        }

                                        cleanup();
                                        resp->pack(static_cast<uint8_t>(2));
                                }
                        }
                }
        }

l1:
        list.clear();

        *reinterpret_cast<uint32_t *>(resp->At(sizeOffset)) = resp->size() - sizeOffset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();
        auto q       = c->outQ ?: (c->outQ = get_outgoing_queue());

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        if (trace) {
                SLog("Responding\n");
        }

        return try_tx(c);
}

bool Service::process_load_conf(connection *const c, const uint8_t *p, const size_t len) {
        if (len < sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint16_t)) {
                track_shutdown(c, __LINE__, "Unexpected length ", len, "\n");
                return shutdown(c);
        }

        const auto e         = p + len;
        const auto req_id    = decode_pod<uint32_t>(p);
        const auto topic_len = decode_pod<uint8_t>(p);

        if (p + topic_len + sizeof(uint16_t) > e) {
                track_shutdown(c, __LINE__, "Unexpected length ", len, "\n");
                return shutdown(c);
        }
        const str_view8 topic_name(reinterpret_cast<const char *>(p), topic_len);

        p += topic_len;

        const auto partition_id = decode_pod<uint16_t>(p);
        auto       q            = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto       resp         = get_buf();
        const auto topic        = topic_by_name(topic_name);

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::ReloadConf));

        const auto size_offset = resp->size();

        resp->pack(static_cast<uint32_t>(0), req_id, topic_name.size());
        resp->serialize(topic_name.data(), topic_name.size());
        resp->pack(partition_id);

        if (cluster_aware()) {
                resp->pack(static_cast<uint8_t>(12));
        } else {
                if (!topic) {
                        // Nope
                        resp->pack(static_cast<uint8_t>(11));
                } else {
                        if (!topic->partitions_ || partition_id >= topic->partitions_->size()) {
                                resp->pack(static_cast<uint8_t>(10));
                        } else {
                                auto path = Buffer::build(basePath_.as_s32(), '/', topic->name_, '/', partition_id, "/config"_s32);
                                int  fd   = safe_open(path.c_str(), O_RDONLY);

                                if (-1 == fd) {
                                        resp->pack(static_cast<uint8_t>(2));
                                } else if (const auto file_size = lseek(fd, 0, SEEK_END); file_size > 0) {
                                        const auto vma_dtor = [file_size](auto ptr) {
                                                if (ptr && ptr != MAP_FAILED) {
                                                        munmap(ptr, file_size);
                                                }
                                        };
                                        std::unique_ptr<void, decltype(vma_dtor)> vma(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0), vma_dtor);
                                        auto                                      file_data = vma.get();

                                        TANKUtil::safe_close(fd);
                                        if (MAP_FAILED == file_data) {
                                                resp->pack(static_cast<uint8_t>(2));
                                        } else {
                                                madvise(file_data, file_size, MADV_SEQUENTIAL | MADV_DONTDUMP);

                                                try {
                                                        partition_config config;
                                                        auto             partition = topic->partitions_->at(partition_id);

                                                        parse_partition_config(str_view32(reinterpret_cast<const char *>(file_data), file_size), &config);
                                                        partition->config = config;
                                                        resp->pack(static_cast<uint8_t>(0));
                                                } catch (...) {
                                                        resp->pack(static_cast<uint8_t>(3));
                                                }
                                        }
                                } else {
                                        resp->pack(static_cast<uint8_t>(2));
                                        TANKUtil::safe_close(fd);
                                }
                        }
                }
        }

        *reinterpret_cast<uint32_t *>(resp->At(size_offset)) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        q->push_back(payload);

        return try_tx(c);
}

bool Service::process_discover_topology(connection *const c, const uint8_t *p, const size_t len) {
        if (len < sizeof(uint8_t)) [[unlikely]] {
                return shutdown(c);
        }
        const auto                  q      = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto                        resp   = get_buf();
        const auto                  req_id = decode_pod<uint32_t>(p);
        [[maybe_unused]] const auto flags  = decode_pod<uint8_t>(p);

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::DiscoverTopology));

        const auto size_offset = resp->size();

        resp->RoomFor(sizeof(uint32_t));
        resp->pack(req_id);

        if (not cluster_aware()) {
                resp->pack(static_cast<uint16_t>(std::numeric_limits<uint16_t>::max()));
        } else {
                const auto name = cluster_state.name();

                resp->pack(static_cast<uint16_t>(cluster_state.nodes.size()));
                resp->pack(static_cast<uint8_t>(name.size()));
                resp->serialize(name.data(), name.size());

                for (const auto &[id, it] : cluster_state.nodes) {
                        const auto n     = it.get();
                        uint8_t    flags = 0;

                        if (n->ep) {
                                flags |= 1 << 0;
                        }

                        if (n->available_) {
                                flags |= 1 << 1;
                        }
                        if (n->blocked) {
                                flags |= 1 << 2;
                        }

                        static_assert(std::is_same_v<std::remove_cv_t<decltype(n->id)>, uint16_t>);

                        resp->pack(n->id, flags);
                        if (n->ep) {
                                resp->pack(n->ep.addr4, n->ep.port);
                        }

                        resp->pack(static_cast<uint16_t>(n->replica_for.size()));
                }
        }

        *reinterpret_cast<uint32_t *>(resp->data() + size_offset) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        return try_tx(c);
}

bool Service::process_discover_topics(connection *const c, const uint8_t *p, const size_t len) {
        if (len < sizeof(uint32_t) + sizeof(uint8_t)) [[unlikely]] {
                return shutdown(c);
        }
        const auto                  q      = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto                        resp   = get_buf();
        const auto                  req_id = decode_pod<uint32_t>(p); // this identifies a broker_api_request in the context of the client
        [[maybe_unused]] const auto flags  = decode_pod<uint8_t>(p);

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::DiscoverTopics));

        const auto size_offset = resp->size();

        resp->RoomFor(sizeof(uint32_t));
        resp->pack(req_id);

        resp->pack(static_cast<uint32_t>(topics.size()));
        resp->pack(static_cast<bool>(cluster_aware()));

        for (const auto &[name, it] : topics) {
                const auto t = it.get();

                resp->pack(name.size());
                resp->serialize(name.data(), name.size());
                resp->pack(t->enabled);
                resp->pack(static_cast<uint16_t>(t->partitions_ ? t->partitions_->size() : 0));
                if (cluster_aware()) {
                        resp->pack(t->cluster.rf_);
                }
        }

        *reinterpret_cast<uint32_t *>(resp->data() + size_offset) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        return try_tx(c);
}

bool Service::process_discover_partitions(connection *const c, const uint8_t *p, const size_t len) {
        enum {
                trace = false,
        };

        if (unlikely(len < sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint8_t))) {
                return shutdown(c);
        }

        auto             q      = c->outQ ?: (c->outQ = get_outgoing_queue());
        const auto       end    = p + len;
        auto             resp   = get_buf();
        const auto       req_id = decode_pod<uint32_t>(p);
        const strwlen8_t topic_name(reinterpret_cast<const char *>(p) + 1, *p);

        p += topic_name.size() + sizeof(uint8_t);

        resp->pack(uint8_t(TankAPIMsgType::DiscoverPartitions));
        const auto size_offset = resp->size();

        resp->RoomFor(sizeof(uint32_t));
        resp->pack(req_id);

        auto topic = topic_by_name(topic_name);
        auto self  = cluster_state.local_node.ref;

        resp->pack(topic_name);

        if (not topic) {
                if (trace) {
                        SLog("Unknown topic\n");
                }

                resp->pack(uint16_t(0));
        } else {
                if (p == end) {
                        const auto n = topic->partitions_->size();

                        resp->pack(uint16_t(n));
                        resp->reserve(n * (sizeof(uint64_t) + sizeof(uint64_t)));

                        if (trace) {
                                SLog("partitions of ", topic_name, " ", n, "\n");
                        }

                        if (cluster_aware()) {
                                for (size_t i{0}; i < topic->total_enabled_partitions; ++i) {
                                        auto it               = topic->partitions_->at(i);
                                        auto partition_leader = it->cluster.leader.node;

                                        if (not partition_leader) {
                                                resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                resp->pack(static_cast<uint8_t>(0xfd));
                                        } else if (partition_leader != self) {
                                                resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                resp->pack(static_cast<uint8_t>(0xfc));
                                                resp->pack(partition_leader->ep.addr4, partition_leader->ep.port);
                                        } else {
                                                try {
                                                        auto log = partition_log(it.get());

                                                        resp->pack(log->firstAvailableSeqNum);
                                                        resp->pack(partition_hwmark(it.get()));
                                                } catch (const std::exception &e) {
                                                        // this *can* fail
                                                        // so we need to do something appropriate here
                                                        resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                        resp->pack(static_cast<uint8_t>(0xfb));
                                                }
                                        }
                                }
                        } else {
                                if (trace) {
                                        SLog("total_enabled_partitions:", topic->total_enabled_partitions, "\n");
                                }

                                for (size_t i{0}; i < topic->total_enabled_partitions; ++i) {
                                        auto it = topic->partitions_->at(i);

                                        try {
                                                auto log = partition_log(it.get());

                                                resp->pack(log->firstAvailableSeqNum);
                                                resp->pack(partition_hwmark(it.get()));
                                        } catch (const std::exception &e) {
                                                if (trace) {
                                                        SLog("Failed to access log:", e.what(), "\n");
                                                }

                                                resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                resp->pack(static_cast<uint8_t>(0xfb));
                                        }
                                }
                        }
                } else {
                        const auto n = std::distance(p, end) / sizeof(uint16_t);

                        resp->pack(static_cast<uint16_t>(n));
                        for (size_t i{0}; i < n; ++i) {
                                const auto pid = decode_pod<uint16_t>(p);

                                if (trace) {
                                        SLog("Request for ", pid, "\n");
                                }

                                if (auto part = topic->partition(pid)) {
                                        auto partition_leader = part->cluster.leader.node;

                                        if (!partition_leader) {
                                                if (trace) {
                                                        SLog("No Leader\n");
                                                }

                                                resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                resp->pack(static_cast<uint8_t>(0xfd));
                                        } else if (partition_leader != self) {
                                                if (trace) {
                                                        SLog("Different Leader\n");
                                                }

                                                resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                resp->pack(static_cast<uint8_t>(0xfc));
                                                resp->pack(partition_leader->ep.addr4, partition_leader->ep.port);
                                        } else {
                                                try {
                                                        auto log = partition_log(part);

                                                        if (trace) {
                                                                SLog("{", log->firstAvailableSeqNum, ", ", partition_hwmark(part), "}\n");
                                                        }

                                                        resp->pack(log->firstAvailableSeqNum);
                                                        resp->pack(partition_hwmark(part));
                                                } catch (const std::exception &e) {
                                                        resp->pack(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
                                                        resp->pack(static_cast<uint8_t>(0xfb));
                                                }
                                        }
                                } else {
                                        IMPLEMENT_ME();
                                }
                        }
                }
        }

        *reinterpret_cast<uint32_t *>(resp->data() + size_offset) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        return try_tx(c);
}

bool Service::process_peer_msg(connection *const c, const uint8_t msg, const uint8_t *data, const size_t len) {
        c->verify();

        switch (static_cast<TankAPIMsgType>(msg)) {
                case TankAPIMsgType::ConsumePeer:
                        return process_peer_consume_resp(c, data, len);

                case TankAPIMsgType::Ping:
                        return true;

                default:
                        SLog("Unexpected message ", unsigned(msg), "\n");
                        return shutdown(c);
        }
}

bool Service::process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len) {
	enum {
		trace = false,
	};

        TANK_EXPECT(c);
        TANK_EXPECT(c->fd > 2);

        if (trace) {
                SLog("New message  type ", msg, ", len ", len, "\n");
        }

        c->verify();

        switch (TankAPIMsgType(msg)) {
                case TankAPIMsgType::Produce:
                case TankAPIMsgType::ProduceWithSeqnum:
                        return process_produce(TankAPIMsgType(msg), c, data, len);

                case TankAPIMsgType::Consume:
                        [[fallthrough]];
                case TankAPIMsgType::ConsumePeer:
                        return process_consume(static_cast<TankAPIMsgType>(msg), c, data, len);

                case TankAPIMsgType::Ping:
                        return true;

                case TankAPIMsgType::DiscoverTopology:
                        return process_discover_topology(c, data, len);

                case TankAPIMsgType::DiscoverTopics:
                        return process_discover_topics(c, data, len);

                case TankAPIMsgType::DiscoverPartitions:
                        return process_discover_partitions(c, data, len);

                case TankAPIMsgType::ReloadConf:
                        return process_load_conf(c, data, len);

                case TankAPIMsgType::CreateTopic:
                        return process_create_topic(c, data, len);

                case TankAPIMsgType::Status:
                        return process_status(c, data, len);

                default:
                        return shutdown(c);
        }
}

void Service::gen_create_topic_succ(consul_request *req) {
        TANK_EXPECT(req);
        TANK_EXPECT(req->type == consul_request::Type::CreatePartititons);
        auto c = req->new_partitions.client_ch.get();

        if (!c) {
                // client has gone away
                return;
        }

        const auto      req_id = req->new_partitions.client_req_id;
        const str_view8 topic_name(req->new_partitions.topic_name.data, req->new_partitions.topic_name.size);
        auto            resp = get_buf();

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::CreateTopic));
        const auto size_offset = resp->size();

        resp->RoomFor(sizeof(uint32_t));
        resp->pack(req_id, topic_name);
        resp->pack(static_cast<uint8_t>(0));

        *reinterpret_cast<uint32_t *>(resp->At(size_offset)) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();
        auto q       = c->outQ ?: (c->outQ = get_outgoing_queue());

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        try_tx(c);
}

void Service::gen_create_topic_fail(consul_request *req) {
        TANK_EXPECT(req);
        TANK_EXPECT(req->type == consul_request::Type::CreatePartititons);
        auto c = req->new_partitions.client_ch.get();

        if (!c) {
                // client has gone away
                return;
        }

        const auto      req_id = req->new_partitions.client_req_id;
        const str_view8 topic_name(req->new_partitions.topic_name.data, req->new_partitions.topic_name.size);
        auto            resp = get_buf();

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::CreateTopic));
        const auto size_offset = resp->size();

        resp->RoomFor(sizeof(uint32_t));
        resp->pack(req_id, topic_name);
        resp->pack(static_cast<uint8_t>(2)); // system error

        *reinterpret_cast<uint32_t *>(resp->At(size_offset)) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = get_data_vector_payload();
        auto q       = c->outQ ?: (c->outQ = get_outgoing_queue());

        q->push_back(payload);

        payload->buf     = resp;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(resp->data()), resp->size()};

        try_tx(c);
}

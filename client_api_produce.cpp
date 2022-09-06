#include "client_common.h"

TankClient::broker_outgoing_payload *TankClient::build_produce_broker_req_payload(const broker_api_request *br_req) {
        enum {
                trace = false,
        };
        auto                  payload                    = new_req_payload(const_cast<broker_api_request *>(br_req));
        auto                  b                          = payload->b;
        [[maybe_unused]] auto api_req                    = br_req->api_req;
        const auto            req_is_produce_with_seqnum = api_req->type == api_request::Type::ProduceWithSeqnum;
        size_t                iovecs_cnt                 = 1;
        auto                 &iovecs                     = payload->iovecs.data;
        size_t                sum                        = 0, verified_sum{0};
        TANK_EXPECT(br_req);
        TANK_EXPECT(br_req->id);

        b->pack(static_cast<uint8_t>(api_req->type == api_request::Type::ProduceWithSeqnum ? TankAPIMsgType::ProduceWithSeqnum : TankAPIMsgType::Produce));
        b->pack(static_cast<uint32_t>(0)); // length, to be patched later

        b->pack(static_cast<uint16_t>(2)); // client version
        b->pack(static_cast<uint32_t>(br_req->id));
        b->pack("c++"_s8); // client ID

        b->pack(static_cast<uint8_t>(0));  // TODO: required acks
        b->pack(static_cast<uint32_t>(0)); // TODO: ack timeout

        const auto topics_cnt_offset = b->size();
        uint16_t   topics_cnt{0};

        b->pack(static_cast<uint8_t>(0)); // total topics, to be patched later

        iovecs[0].iov_base = 0;
        iovecs[0].iov_len  = b->size() | (1u << 30);

        sum += b->size() - sizeof(uint8_t) - sizeof(uint32_t);

        // partitions already grouped by topics in assign_req_partitions_to_api_req()
        for (auto it = br_req->partitions_list.next; it != &br_req->partitions_list;) {
                auto       req_part       = switch_list_entry(request_partition_ctx, partitions_list_ll, it);
                const auto topic          = req_part->topic;
                uint8_t    partitions_cnt = 0;

                iovecs[iovecs_cnt++] = iovec{
                    .iov_base = reinterpret_cast<void *>(b->size()),
                    .iov_len  = (sizeof(uint8_t) + sizeof(uint8_t) + topic.size()) | (1u << 30),
                };

                b->pack(topic.size());
                b->serialize(topic.data(), topic.size());

                const auto partitions_cnt_buf_offset = b->size();

                b->pack(static_cast<uint8_t>(0));

                sum += sizeof(uint8_t) + topic.size() + sizeof(uint8_t);

                do {
                        const auto _o = b->size();

                        b->pack(req_part->partition);                              // partition
                        b->encode_varuint32(req_part->as_op.produce.payload.size); // bundle length

                        const auto span = b->size() - _o;

                        sum += span + req_part->as_op.produce.payload.size;

                        iovecs[iovecs_cnt++] = iovec{
                            .iov_base = reinterpret_cast<void *>(_o),
                            .iov_len  = span | (1u << 30)};

                        if (req_is_produce_with_seqnum) {
                                // for ProduceWithSeqnum
                                // we are also encoding the sequence number of the
                                // first message in the first bundle
                                // for convenience(otherwise we would need to
                                // uncompress the bundle - potentially - to find it)
                                iovecs[iovecs_cnt++] = iovec{
                                    .iov_base = reinterpret_cast<void *>(&req_part->as_op.produce.first_msg_seqnum),
                                    .iov_len  = sizeof(uint64_t)};

                                sum += sizeof(uint64_t);
                        }

                        // bundle content
                        iovecs[iovecs_cnt++] = iovec{
                            .iov_base = req_part->as_op.produce.payload.data,
                            .iov_len  = req_part->as_op.produce.payload.size};

                        ++partitions_cnt;
                } while ((it = it->next) != &br_req->partitions_list && (req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, it))->topic == topic);

                *reinterpret_cast<uint8_t *>(b->data() + partitions_cnt_buf_offset) = partitions_cnt;
                ++topics_cnt;

                if (trace) {
                        SLog("Generated for ", partitions_cnt, " of ", topic, "\n");
                }
        }
        *reinterpret_cast<uint8_t *>(b->data() + topics_cnt_offset) = topics_cnt; // patch total topics
        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t))  = sum;        // patch request size

        // patch it
        for (size_t i{0}; i < iovecs_cnt; ++i) {
                auto &it = iovecs[i];

                if (it.iov_len & (1u << 30)) {
                        it.iov_len ^= (1u << 30);
                        it.iov_base = b->data() + reinterpret_cast<intptr_t>(it.iov_base);

                        if (trace) {
                                SLog("Patching ", i, "\n");
                        }
                }

                verified_sum += it.iov_len;
        }

        TANK_EXPECT(verified_sum - sizeof(uint8_t) - sizeof(uint32_t) == sum);
        TANK_EXPECT(iovecs_cnt <= sizeof_array(payload->iovecs.data));

        payload->iovecs.size = iovecs_cnt;
        return payload;
}

static uint8_t choose_compression_codec(const TankClient::msg *msgs, const size_t size) {
        if (size > 512) {
                return 1;
        }

        size_t sum{0};

        for (size_t i{0}; i < size; ++i) {
                sum += msgs[i].content.size() + msgs[i].key.size();

                if (sum > 1024) {
                        return 1;
                }
        }

        return 0;
}

bool TankClient::process_produce(connection *const c, const uint8_t *const content, const size_t len) {
        enum {
                trace = false,
        };
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        const auto                 *p      = content;
        [[maybe_unused]] const auto end    = p + len;
        const auto                  req_id = decode_pod<uint32_t>(p);
        const auto                  it     = pending_brokers_requests.find(req_id);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_cyan, ansifmt::inverse, "PRODUCE RESPONSE", ansifmt::reset, "\n");
        }

        if (it == pending_brokers_requests.end()) {
                if (trace) {
                        SLog("Not Found -- will ignore request(", req_id, ")\n");
                }

                return true;
        }

        auto                                 br_req  = it->second;
        switch_dlist                        *part_it = br_req->partitions_list.next, *next;
        auto                                 api_req = br_req->api_req;
        std::vector<request_partition_ctx *> retry, no_leader;
        bool                                 any_faults{false};
        Switch::endpoint                     new_leader;

        while (p < end) {
                // error, followed by topic name, followed by partitions count
                // and then for each partition, its ID
                auto err = decode_pod<uint8_t>(p);

                if (trace) {
                        SLog("err = ", err, "\n");
                }

                if (err == 0xff || err == 0xfe) {
                        TANK_EXPECT(part_it != &br_req->partitions_list);
                        auto       req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, part_it);
                        const auto topic    = req_part->topic;

                        if (err == 0xff) {
                                capture_unknown_topic_fault(api_req, topic);

                                if (trace) {
                                        SLog("Unknown topic ", topic, "\n");
                                }
                        } else if (err == 0xfe) {
                                capture_readonly_fault(api_req);

                                if (trace) {
                                        SLog("Read Only\n");
                                }
                        }

                        do {
                                next = part_it->next;

                                if (trace) {
                                        SLog("Skipping partition ", req_part->partition, "\n");
                                }

                                discard_request_partition_ctx(api_req, req_part);
                        } while ((part_it = next) != &br_req->partitions_list and
                                 (req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, part_it))->topic == topic);

                        any_faults = true;
                        continue;
                } else if (err == 0xfc) {
                        // new leader for the first partition
                        new_leader.addr4 = decode_pod<uint32_t>(p);
                        new_leader.port  = decode_pod<uint16_t>(p);

                        if (trace) {
                                SLog("new leader for first partition ", new_leader, "\n");
                        }
                }

                TANK_EXPECT(part_it != &br_req->partitions_list);

                for (const auto this_topic = switch_list_entry(request_partition_ctx, partitions_list_ll, part_it)->topic;;) {
                        TANK_EXPECT(part_it != &br_req->partitions_list);
                        auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, part_it);
                        auto next     = part_it->next;

                        if (trace) {
                                SLog("Considering ", ansifmt::bold, ansifmt::color_brown, req_part->topic, "/", req_part->partition, ansifmt::reset,
                                     " ", err, ", next = ", ptr_repr(next), "\n");
                        }

                        req_part->partitions_list_ll.detach_and_reset();

                        if (err == 0) {
                                // OK
                                req_part->partitions_list_ll.detach_and_reset();
                                set_partition_leader(req_part->topic, req_part->partition, br_req->br->ep);

                                api_req->ready_partitions_list.push_back(&req_part->partitions_list_ll);

                                if (trace) {
                                        SLog("SUCCESS\n");
                                }
                        } else if (err == 10) {
                                // invalid seq num
                                any_faults = true;
                                capture_invalid_req_fault(api_req, req_part->topic, req_part->partition);

                                discard_request_partition_ctx(api_req, req_part);

                                if (trace) {
                                        SLog("Attempted to publish to an explicit sequence number not allowed\n");
                                }
                        } else if (err == 0xfe) {
                                // read only
                                any_faults = true;
                                capture_readonly_fault(api_req);

                                discard_request_partition_ctx(api_req, req_part);

                                if (trace) {
                                        SLog("Read-Only system\n");
                                }
                        } else if (err == 0x1) {
                                // Unknown partition
                                any_faults = true;
                                capture_unknown_partition_fault(api_req, req_part->topic, req_part->partition);

                                discard_request_partition_ctx(api_req, req_part);

                                if (trace) {
                                        SLog("Unkown partition ", req_part->topic, "/", req_part->partition, "\n");
                                }
                        } else if (err == 0xfd) {
                                // No Leader
                                no_leader.emplace_back(req_part);
                        } else if (err == 0xfc) {
                                if (trace) {
                                        SLog("New Leader ", new_leader, "\n");
                                }

                                set_partition_leader(req_part->topic, req_part->partition, new_leader);
                                retry.emplace_back(req_part);
                        } else if (err == 0x2) {
                                // I/O
                                capture_system_fault(api_req, req_part->topic, req_part->partition);

                                discard_request_partition_ctx(api_req, req_part);

                                any_faults = true;
                        } else if (err == 0x3) {
                                // unable to get acks. from all other nodes
				// TODO: we should perhaps retry it
                                capture_system_fault(api_req, req_part->topic, req_part->partition);

                                discard_request_partition_ctx(api_req, req_part);

                                any_faults = true;
                        } else if (err == 0x4) {
                                // insufficient replicas - cannot service the produce request
                                capture_insuficient_replicas(api_req, req_part->topic, req_part->partition);

                                discard_request_partition_ctx(api_req, req_part);

                                any_faults = true;
                        } else {
                                IMPLEMENT_ME();
                        }

                        part_it = next;
                        if (part_it == &br_req->partitions_list || switch_list_entry(request_partition_ctx, partitions_list_ll, part_it)->topic != this_topic) {
                                if (trace) {
                                        SLog("Done with all partitions for this topic\n");
                                }

                                break;
                        } else {
                                err = decode_pod<uint8_t>(p);

                                if (trace) {
                                        SLog("err = ", err, "\n");
                                }

                                if (err == 0xfc) {
                                        new_leader.addr4 = decode_pod<uint32_t>(p);
                                        new_leader.port  = decode_pod<uint16_t>(p);
                                }
                        }
                }
        }

        unlink_broker_req(br_req, __LINE__);
        put_broker_api_request(br_req);

        update_api_req(api_req, any_faults, &no_leader, &retry);

        if (trace) {
                SLog("ready:", api_req->ready(), ", any_faults:", any_faults, "\n");
        }

        if (api_req->ready() || any_faults) {
                make_api_req_ready(api_req, __LINE__);
        }

        return true;
}

uint32_t TankClient::produce(const std::pair<topic_partition, std::vector<msg>> *const list, const size_t list_size) {
        enum {
                trace = false,
        };
        auto                                                      api_req = get_api_request(32 * 1000); // UPDATE: 2021-10-27 16s timeout now just in case
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;
        IOBuffer                                                  b, cb;

        api_req->type = api_request::Type::Produce;
        contexts.reserve(list_size);

        for (size_t _i{0}; _i < list_size; ++_i) {
                const auto &it         = list[_i];
                const auto  topic_name = intern_topic(it.first.first);
                const auto  partition  = it.first.second;
                const auto &msgs       = it.second;
                auto        req_part   = get_request_partition_ctx();
                auto        v          = std::make_unique<std::vector<msg>>();
                size_t      sum        = 0;
                auto        broker     = partition_leader(topic_name, partition) ?: any_broker();

                req_part->topic     = topic_name;
                req_part->partition = partition;

                TANK_EXPECT(not msgs.empty());

                for (const auto &it : msgs) {
                        sum += it.key.size() + it.content.size();
                }

                b.reserve(sum + 128);
                v->reserve(msgs.size());

                const auto codec        = choose_compression_codec(msgs.data(), msgs.size());
                uint8_t    bundle_flags = 0;
                const auto total_msgs   = msgs.size();

                if (trace) {
                        SLog("Bundle for ", topic_name, "/", partition, ", codec = ", codec, ", total_msgs = ", total_msgs, "\n");
                }

                // BEGIN: bundle header
                if (codec) {
                        TANK_EXPECT(codec < 3);
                        bundle_flags |= codec;
                }

                if (total_msgs < 16) {
                        // we can encode the message set size in flags, because it can fit
                        // in the 4bits we have reserved for that purpose
                        bundle_flags |= total_msgs << 2;
                        b.pack(bundle_flags);
                } else {
                        b.pack(bundle_flags);
                        b.encode_varuint32(total_msgs);
                }
                // END: bundle header

                // BEGIN: messages set
                if (!codec) {
                        for (size_t i{0}; i < total_msgs; ++i) {
                                const auto &m     = msgs[i];
                                uint8_t     flags = m.key ? static_cast<uint8_t>(TankFlags::BundleMsgFlags::HaveKey) : 0;

                                if (i && m.ts == msgs[i - 1].ts) {
                                        flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                        b.pack(flags);
                                } else {
                                        b.pack(flags, static_cast<uint64_t>(m.ts));
                                }

                                if (m.key) {
                                        b.pack(m.key.size());
                                        b.serialize(m.key.data(), m.key.size());
                                }

                                b.encode_varuint32(m.content.size());
                                b.serialize(m.content.data(), m.content.size());
                        }
                } else {
                        cb.clear();

                        for (size_t i{0}; i < total_msgs; ++i) {
                                const auto &m     = msgs[i];
                                uint8_t     flags = m.key ? static_cast<uint8_t>(TankFlags::BundleMsgFlags::HaveKey) : 0;

                                if (i && m.ts == msgs[i - 1].ts) {
                                        flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                        cb.pack(flags);
                                } else {
                                        cb.pack(flags, static_cast<uint64_t>(m.ts));
                                }

                                if (m.key) {
                                        cb.pack(m.key.size());
                                        cb.serialize(m.key.data(), m.key.size());
                                }

                                cb.encode_varuint32(m.content.size());
                                cb.serialize(m.content.data(), m.content.size());
                        }

                        if (not Compression::Compress(Compression::Algo::SNAPPY, cb.data(), cb.size(), &b)) [[unlikely]] {
                                IMPLEMENT_ME();
                        }

                        if (trace) {
                                SLog("Compressed from ", size_repr(cb.size()), " to ", size_repr(b.size()), "\n");
                        }
                }
                // END: messages set

                if (trace) {
                        SLog("Generated for ", topic_name, "/", partition, " ", b.size(), "\n");
                }

                req_part->as_op.produce.payload.size     = b.size();
                req_part->as_op.produce.payload.data     = reinterpret_cast<uint8_t *>(b.release());
                req_part->as_op.produce.first_msg_seqnum = 0;

                contexts.emplace_back(std::make_pair(broker, req_part));
        }

        assign_req_partitions_to_api_req(api_req.get(), &contexts, sizeof_array(broker_outgoing_payload::IOVECS::data) / 3 + 1);
        return schedule_new_api_req(std::move(api_req));
}

uint32_t TankClient::produce_with_seqnum(const std::pair<topic_partition, std::vector<consumed_msg>> *const list, const size_t list_size) {
        enum {
                trace = false,
        };
        auto                                                      api_req = get_api_request(32 * 1000);
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;
        IOBuffer                                                  b, cb;

        api_req->type = api_request::Type::ProduceWithSeqnum;
        contexts.reserve(list_size);

        for (size_t i{0}; i < list_size; ++i) {
                const auto &it        = list[i];
                const auto &msgs      = it.second;
                const auto  msgs_size = msgs.size();

                if (!msgs_size) {
                        continue;
                }

                const auto                  topic_name       = intern_topic(it.first.first);
                const auto                  partition        = it.first.second;
                auto                        req_part         = get_request_partition_ctx();
                auto                        v                = std::make_unique<std::vector<msg>>();
                auto                        broker           = partition_leader(topic_name, partition) ?: any_broker();
                const auto                  first_msg_seqnum = msgs.front().seqNum;
                const auto                  last_msg_seqnum  = msgs.back().seqNum;
                static constexpr const auto as_sparse        = true;

                req_part->topic     = topic_name;
                req_part->partition = partition;

                const auto *p = msgs.data(), *const e = p + msgs_size;
                size_t sum = p->key.size() + p->content.size();

                // also sanity check against sequence numbers
                for (++p; p < e; ++p) {
                        TANK_EXPECT(p->seqNum > p[-1].seqNum);
                        sum += p->key.size() + p->content.size();
                }

                const uint8_t codec        = msgs_size > 512 || sum > 1024 ? 1 : 0;
                uint8_t       bundle_flags = 0;

                b.reserve(sum + 128);
                v->reserve(msgs_size);

                // BEGIN: bundle header
                if (codec) {
                        TANK_EXPECT(codec < 3);
                        bundle_flags |= codec;
                }

                if (trace) {
                        SLog("Producing with seqnum: codec = ", codec, ", as_sparse = ", as_sparse, ", msgs_size = ", msgs_size, " for ", topic_name, "/", partition, "\n");
                }

                if (as_sparse) {
                        // set SPARSE bit
                        bundle_flags |= (1u << 6);
                }

                if (msgs_size < 16) {
                        // we can encode the message set size in flags, because it can fit
                        // in the 4bits we have reserved for that purpose
                        bundle_flags |= msgs_size << 2;
                        b.pack(bundle_flags);
                } else {
                        b.pack(bundle_flags);
                        b.encode_varuint32(msgs_size);
                }

                if (as_sparse) {
                        b.pack(first_msg_seqnum); // first message absolute seq.num

                        if (msgs_size != 1) {
                                b.encode_varuint32(last_msg_seqnum - first_msg_seqnum - 1);
                        }
                }
                // END: bundle header

                // BEGIN: messages set
                const auto penultimate = msgs_size - 1;

                // See: Service::persist_peer_partitions_content()
                if (!codec) {
                        for (size_t i{0}; i < msgs_size; ++i) {
                                const auto &m                     = msgs[i];
                                uint8_t     flags                 = m.key ? static_cast<uint8_t>(TankFlags::BundleMsgFlags::HaveKey) : 0;
                                const auto  use_last_specified_ts = i && m.ts == msgs[i - 1].ts;
                                uint32_t    delta;

                                if (use_last_specified_ts) {
                                        flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                }

                                if (as_sparse && i && i != penultimate) {
                                        delta = m.seqNum - msgs[i - 1].seqNum - 1;

                                        if (!delta) {
                                                flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                        }
                                } else {
                                        delta = 0;
                                }

                                b.pack(flags);

                                if (delta) {
                                        b.encode_varuint32(delta);
                                }

                                if (!use_last_specified_ts) {
                                        b.pack(m.ts);
                                }

                                if (m.key) {
                                        b.pack(m.key.size());
                                        b.serialize(m.key.data(), m.key.size());
                                }

                                b.encode_varuint32(m.content.size());
                                b.serialize(m.content.data(), m.content.size());
                        }
                } else {
                        cb.clear();

                        for (size_t i{0}; i < msgs_size; ++i) {
                                const auto &m                     = msgs[i];
                                uint8_t     flags                 = m.key ? static_cast<uint8_t>(TankFlags::BundleMsgFlags::HaveKey) : 0;
                                const auto  use_last_specified_ts = i && m.ts == msgs[i - 1].ts;
                                uint32_t    delta;

                                if (use_last_specified_ts) {
                                        flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                }

                                if (as_sparse && i && i != penultimate) {
                                        delta = m.seqNum - msgs[i - 1].seqNum - 1;

                                        if (!delta) {
                                                flags |= static_cast<uint8_t>(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                        }
                                } else {
                                        delta = 0;
                                }

                                cb.pack(flags);

                                if (delta) {
                                        cb.encode_varuint32(delta);
                                }

                                if (!use_last_specified_ts) {
                                        cb.pack(m.ts);
                                }

                                if (m.key) {
                                        cb.pack(m.key.size());
                                        cb.serialize(m.key.data(), m.key.size());
                                }

                                cb.encode_varuint32(m.content.size());
                                cb.serialize(m.content.data(), m.content.size());
                        }

                        if (!Compression::Compress(Compression::Algo::SNAPPY, cb.data(), cb.size(), &b)) {
                                IMPLEMENT_ME();
                        }
                }
                // END: messages set

                if (trace) {
                        SLog("Generated for ", topic_name, "/", partition, " ", b.size(), "\n");
                }

                req_part->as_op.produce.payload.size     = b.size();
                req_part->as_op.produce.payload.data     = reinterpret_cast<uint8_t *>(b.release());
                req_part->as_op.produce.first_msg_seqnum = first_msg_seqnum;

                contexts.emplace_back(std::make_pair(broker, req_part));
        }

        assign_req_partitions_to_api_req(api_req.get(), &contexts, sizeof_array(broker_outgoing_payload::IOVECS::data) / 3 + 1);
        return schedule_new_api_req(std::move(api_req));
}

uint32_t TankClient::produce_to(const topic_partition &to, const std::vector<msg> &msgs) {
        const std::vector<std::pair<topic_partition, std::vector<msg>>> v{
            std::make_pair(to, msgs)};

        return produce(v);
}

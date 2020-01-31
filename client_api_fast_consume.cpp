#include "client_common.h"

#ifdef TANK_CLIENT_FAST_CONSUME
void TankClient::clear_tank_resp(connection *const c) {
        static constexpr const bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);

	if (0 == (c->as.tank.flags & (1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly)))) {
		return;
	}

	if (trace) {
		SLog("Cleaning up tank response\n");
	}

        auto &resp = c->as.tank.cur_resp;
        auto &cctx = resp.cur_partition.capture_ctx;

        for (auto it = std::exchange(resp.no_leader_l, nullptr); it; it = std::exchange(it->_next, nullptr)) {
                //
        }

        for (auto it = std::exchange(resp.retry_l, nullptr); it; it = std::exchange(it->_next, nullptr)) {
                //
        }

        if (auto it = std::exchange(cctx.first_bucket, nullptr)) {
                auto last_bucket = std::exchange(cctx.last_bucket, nullptr);

                while (it != last_bucket) {
                        auto next = it->next;

                        put_msgs_bucket(it);
                        it = next;
                }

                put_msgs_bucket(last_bucket);
        }

        resp.reset();
}

bool TankClient::process_consume_content(connection *const c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
	TANK_EXPECT(c->as.tank.flags & (1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly)));
        auto b = c->in.b;
        TANK_EXPECT(b);

        if (c->as.tank.cur_resp.state == connection::As::Tank::Response::State::Drain) {
        drain:
                const auto size        = b->size();
                auto       next_offset = std::min(size, c->as.tank.cur_resp.resp_end_offset);

                if (trace) {
                        SLog("Will drain, next_offset = ", next_offset, "\n");
                }

                if (!b->is_locked()) {
                        if (next_offset == size) {
                                b->clear();
                                c->as.tank.cur_resp.resp_end_offset -= next_offset;
                                next_offset = 0;
                        } else if (next_offset > 4 * 1024 * 1024) {
                                c->as.tank.cur_resp.resp_end_offset -= next_offset;
                                b->erase_chunk(0, next_offset);
                                next_offset = 0;
                        }
                }

                b->set_offset(next_offset);

                if (const auto size = b->size(); c->as.tank.cur_resp.resp_end_offset >= size) {
                        // we are done
                        if (trace) {
                                SLog("Done Draining\n");
                        }

                        c->as.tank.cur_resp.state = connection::As::Tank::Response::State::Ready;
                        return true;
                }
        } else {
                if (!process_consume_content_impl(c)) {
                        return false;
                } else if (c->as.tank.cur_resp.state == connection::As::Tank::Response::State::Ready) {
                        if (trace) {
                                SLog("Now READY\n");
                        }

                        b->set_offset(c->as.tank.cur_resp.resp_end_offset);

                        if (trace) {
                                SLog("Now offset ", b->offset(), " / ", b->size(), "\n");
                        }

                        if (!b->is_locked()) {
                                if (const auto o = b->offset(); o == b->size()) {
                                        if (trace) {
                                                SLog("Can clear buffer\n");
                                        }

                                        b->clear();
                                        b->set_offset(static_cast<uint32_t>(0));
                                } else if (o > 4 * 1024 * 1024) {
                                        if (trace) {
                                                SLog("Will trim buffer\n");
                                        }

                                        b->erase_chunk(0, o);
                                        b->set_offset(static_cast<uint32_t>(0));
                                }
                        }

                        return true;
                } else if (c->as.tank.cur_resp.state == connection::As::Tank::Response::State::Drain) {
                        goto drain;
                }
        }

        return true;
}

bool TankClient::process_consume_content_impl(connection *const c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        TANK_EXPECT(c->as.tank.flags & (1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly)));
        auto b = c->in.b;
        TANK_EXPECT(b);
        const auto base = reinterpret_cast<const uint8_t *>(b->data());
        auto &     resp = c->as.tank.cur_resp;
        const auto e    = std::min(base + resp.resp_end_offset, base + b->size());
        using State     = connection::As::Tank::Response::State;
        auto br_req     = resp.breq;

        if (trace) {
                SLog("Now at state ", unsigned(resp.state), " resp.resp_end_offset = ", resp.resp_end_offset, "\n");
        }

// TODO: check if are past the rem bytes as well
// TODO: maybe set somewhere in resp the minimum number of bytes we need before we trampoline from process_consume_content() to this function
#define REQUIRE_BYTES(n)                                                                                         \
        if (p + (n) > e) {                                                                                       \
                SLog("Required more content at ", __LINE__, " needed ", n, " have ", std::distance(p, e), "\n"); \
                return true;                                                                                     \
        }

        switch (resp.state) {
                case State::ParseHeader: {
                        const auto *p = base + b->offset();
                        REQUIRE_BYTES(sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t));

                        resp.hdr_size = decode_pod<uint32_t>(p);
                        // we are going to wait until we got the whole header in
                        // right _past_ the header we expect to find the bundles content
                        // that way, we also won't need to check if we got enough content while parsing the header
                        REQUIRE_BYTES(resp.hdr_size);
                        resp.cur_partition.bundles_chunk.offset = std::distance(base, p + resp.hdr_size); // first partition's first bundle begins here

                        resp.req_id     = decode_pod<uint32_t>(p);
                        resp.topics_cnt = decode_pod<uint8_t>(p);

                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, "ParseHeader", ansifmt::reset, "\n");
                                SLog("hdr_size = ", resp.hdr_size, ", req_id = ", resp.req_id, ", topics_cnt = ", resp.topics_cnt, "\n");
                        }

                        const auto it = pending_brokers_requests.find(resp.req_id);

                        if (it == pending_brokers_requests.end()) {
                                // we will just silently consume the response content until we are done here
                                if (trace) {
                                        SLog("Unable to lookup pending broker request ", resp.req_id, "\n");
                                }

                                // drain
                                resp.breq  = nullptr;
                                resp.state = State::Drain;
                                b->set_offset(std::distance(base, e));
                                return true;
                        }

                        // we need to acquire exclusive ownership
                        // see also unlink_broker_req()
                        pending_brokers_requests.erase(it);

                        b->set_offset(std::distance(base, p));
                        br_req = resp.breq     = it->second;
                        resp.state             = State::ParseTopic;
                        resp.br_req_partctx_it = br_req->partitions_list.next;

                        if (trace) {
                                SLog("bundles_chunk.offset = ", resp.cur_partition.bundles_chunk.offset, "\n");
                        }
                }
                        [[fallthrough]];

                case State::ParseTopic:
                parse_topic : {
                        const auto *p = base + b->offset();

                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, "ParseTopic", ansifmt::reset, "\n");
                        }

                        if (resp.topics_cnt == 0) {
                                auto                                 api_req = br_req->api_req;
                                std::vector<request_partition_ctx *> no_leader, retry;

                                // we are done with all the topics
                                // we are expected to be done with the response too
                                // TODO:
                                if (trace) {
                                        SLog(ansifmt::color_magenta, ansifmt::bold, ansifmt::inverse, "Done with all topics in the request", ansifmt::reset, "\n");
                                }

                                // stop tracking it
                                unlink_broker_req(br_req, __LINE__);

                                // dispose of it
                                put_broker_api_request(br_req);

                                if (resp.retain_buf) {
                                        retain_conn_inbuf(c, api_req);

                                        TANK_EXPECT(c->in.b);
                                        TANK_EXPECT(c->in.b->locks_ > 0);
                                }

                                for (auto it = std::exchange(resp.no_leader_l, nullptr); it; it = std::exchange(it->_next, nullptr)) {
                                        no_leader.emplace_back(it);
                                }

                                for (auto it = std::exchange(resp.retry_l, nullptr); it; it = std::exchange(it->_next, nullptr)) {
                                        retry.emplace_back(it);
                                }

                                update_api_req(api_req, std::exchange(resp.any_faults, false), &no_leader, &retry);

				if (trace) {
					SLog("api_req->ready() = ", api_req->ready(), ", resp.any_faults = ", resp.any_faults, "\n");
				}

                                if (api_req->ready() || resp.any_faults) {
                                        make_api_req_ready(api_req, __LINE__);
                                }

                                resp.state = connection::As::Tank::Response::State::Ready;
                                return true;
                        }

                        const auto len = decode_pod<uint8_t>(p);

                        resp.topic_name.len = len;
                        memcpy(resp.topic_name.data_, p, len);

                        p += len;
                        --resp.topics_cnt;
                        resp.topic_partitions_cnt = decode_pod<uint8_t>(p);
                        resp.state                = State::ParseFirstTopicPartition;
                        b->set_offset(std::distance(base, p));

                        if (trace) {
                                SLog("Now at topic [", str_view32(resp.topic_name.data_, resp.topic_name.len), "] => ", resp.topic_partitions_cnt, "\n");
                        }
                }
                        [[fallthrough]];

                case State::ParseFirstTopicPartition: {
                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, "ParseFirstTopicPartition", ansifmt::reset, "\n");
                        }

                        const auto *    p = base + b->offset();
                        const str_view8 topic_name(resp.topic_name.data_, resp.topic_name.len);

                        if (p + sizeof(uint16_t) <= e && *reinterpret_cast<const uint16_t *>(p) == std::numeric_limits<uint16_t>::max()) {
                                auto api_req = br_req->api_req;

                                if (trace) {
                                        SLog("Unknown topic [", topic_name, "]\n");
                                }

                                resp.any_faults = true;

                                do {
                                        auto req_part = containerof(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it);
                                        auto next     = resp.br_req_partctx_it->next;

                                        clear_request_partition_ctx(api_req, req_part);
                                        put_request_partition_ctx(req_part);

                                        resp.br_req_partctx_it = next;
                                } while (resp.br_req_partctx_it != &br_req->partitions_list &&
                                         switch_list_entry(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it)->topic == topic_name);

                                p += sizeof(uint16_t);
                                b->set_offset(std::distance(base, p));
                                goto parse_topic;
                        } else if (resp.topic_partitions_cnt && p + sizeof(uint16_t) > e) {
                                return true;
                        }

                        resp.state = State::ParsePartition;
                        b->set_offset(std::distance(base, p));

                        if (trace) {
                                SLog("OK, ready to parse topic's first partition\n");
                        }
                }
                        [[fallthrough]];

                case State::ParsePartition:
                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, "ParsePartition", ansifmt::reset, "\n");
                        }

                parse_partition : {
                        if (0 == resp.topic_partitions_cnt) {
                                if (trace) {
                                        SLog("Done with topic's partitions\n");
                                }

                                goto parse_topic;
                        }

                        const auto *                p          = base + b->offset();
                        [[maybe_unused]] const auto p_id       = decode_pod<uint16_t>(p);
                        const auto                  err_flags  = decode_pod<uint8_t>(p);
                        auto &                      cur_part   = resp.cur_partition;
                        auto &                      cur_bundle = cur_part.cur_bundle;

                        if (trace) {
                                SLog("Parsing partition ", p_id, ", err_flags = ", err_flags, "\n");
                        }

                        resp.topic_partitions_cnt--;

                        if (err_flags == 0xff) {
                                // undefined partition
                                auto next    = resp.br_req_partctx_it->next;
                                auto api_req = br_req->api_req;

                                // TODO: capture_unknown_topic_fault()

                                TANK_EXPECT(resp.br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = containerof(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it);

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                resp.br_req_partctx_it = next;
                                resp.any_faults        = true;
                                goto parse_partition;
                        } else if (err_flags == 0xfd) {
                                // no leader
                                auto next    = resp.br_req_partctx_it->next;
                                auto api_req = br_req->api_req;

                                TANK_EXPECT(resp.br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = containerof(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it);

                                req_part->_next  = resp.no_leader_l;
                                resp.no_leader_l = req_part;

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                resp.br_req_partctx_it = next;
                                goto parse_partition;
                        } else if (err_flags == 0xfc) {
                                // different leader
                                auto                   next    = resp.br_req_partctx_it->next;
                                auto                   api_req = br_req->api_req;
                                const Switch::endpoint ep{decode_pod<uint32_t>(p), decode_pod<uint16_t>(p)};

                                TANK_EXPECT(resp.br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = containerof(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it);

                                set_leader(intern_topic(str_view8(resp.topic_name.data_, resp.topic_name.len)), p_id, ep);

                                req_part->_next = resp.retry_l;
                                resp.retry_l    = req_part;

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                resp.br_req_partctx_it = next;
                                goto parse_partition;
                        } else if (err_flags == 0xfe) {
                                // the first bundle in this bundles chunk is a sparse bundle
                                cur_part.log_base_seqnum = 0;
                        } else {
                                cur_part.log_base_seqnum = decode_pod<uint64_t>(p);

                                if (trace) {
                                        SLog("The first bundle isn't a sparse bundle, log_base_seqnum = ", cur_part.log_base_seqnum, "\n");
                                }
                        }

                        TANK_EXPECT(resp.br_req_partctx_it != &br_req->partitions_list);

                        // initialize cur_part, prepare for parsing its bundles
                        cur_part.highwater_mark    = decode_pod<uint64_t>(p);
                        cur_part.bundles_chunk_len = decode_pod<uint32_t>(p);
                        cur_part.bundles_chunk.end = cur_part.bundles_chunk.offset + cur_part.bundles_chunk_len;
                        cur_part.capture_ctx.reset();

                        if (trace) {
                                SLog("highwater_mark = ", cur_part.highwater_mark, ", bundles_chunk_len = ", cur_part.bundles_chunk_len, "\n");
                        }

                        if (err_flags == 0x1) {
                                // boundary check fault
                                auto                        next               = resp.br_req_partctx_it->next;
                                [[maybe_unused]] const auto first_avail_seqnum = decode_pod<uint64_t>(p);
                                auto                        api_req            = br_req->api_req;

                                //TODO: capture_boundary_access_fault()
				if (trace) {
					SLog("BOUNDARY check\n");
				}

                                TANK_EXPECT(resp.br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = containerof(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it);

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                resp.br_req_partctx_it = next;
                                resp.any_faults        = true;
                                goto parse_partition;
                        } else if (err_flags && err_flags < 0xfe) {
                                // TODO:
                                IMPLEMENT_ME();
                        }

                        cur_bundle.any_captured = false;
                        resp.state              = State::ParsePartitionBundle;
                        b->set_offset(std::distance(base, p));
                }
                        [[fallthrough]];

                case State::ParsePartitionBundle:
                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, "ParsePartitionBundle", ansifmt::reset, "\n");
                        }

                parse_partition_bundle : {
                        auto &cur_part = resp.cur_partition;

                        if (trace) {
                                SLog("Parsing next partititon bundle at ", cur_part.bundles_chunk.offset, "\n");
                        }

                        const auto *                bundles_chunk     = base + cur_part.bundles_chunk.offset;
                        const auto                  bundles_chunk_end = base + cur_part.bundles_chunk.end;
                        const auto                  partition_bundles = bundles_chunk;
                        [[maybe_unused]] const auto drained_partition = (cur_part.bundles_chunk_len == 0);
                        uint64_t                    first_msg_seqnum, last_msg_seqnum;
                        auto                        req_part         = containerof(request_partition_ctx, partitions_list_ll, resp.br_req_partctx_it);
                        const auto                  requested_seqnum = req_part->as_op.consume.seq_num;
                        auto                        log_base_seqnum  = cur_part.log_base_seqnum;
                        auto &                      cur_bundle       = cur_part.cur_bundle;
                        const auto *p = partition_bundles, *const chunk_end = std::min(e, bundles_chunk_end);
                        const auto consider_exhaustion = [&, drained_partition](const auto ref) {
                                if (trace) {
                                        SLog("consider_exhaustion(): resp.resp_end_offset = ", resp.resp_end_offset, ", b->size() = ", b->size(), "\n");
                                }

                                if (resp.resp_end_offset > b->size()) {
					if (trace) {
						SLog("We are expecting even more content resp.resp_end_offset = ", resp.resp_end_offset, ", b->size() = ", b->size(), "\n");
					}

					return false;
				} 

                                auto       api_req          = br_req->api_req;
                                auto &     req_part_resp    = req_part->as_op.consume.response;
                                auto &     cctx             = cur_part.capture_ctx;
                                const auto next_min_span    = cur_part.need_upto - cur_part.need_from;
                                const auto last_bucket      = cctx.last_bucket;
                                const auto last_bucket_size = cctx.last_bucket_size;
                                const auto consumed         = cctx.consumed;
                                const auto next_seqnum      = consumed
                                                             ? requested_seqnum == std::numeric_limits<uint64_t>::max()
                                                                   ? last_bucket->data[last_bucket_size - 1].seqNum + 1
                                                                   : std::max(requested_seqnum, last_bucket->data[last_bucket_size - 1].seqNum + 1)
                                                             : requested_seqnum == std::numeric_limits<uint64_t>::max()
                                                                   ? cur_part.highwater_mark + 1
                                                                   : requested_seqnum;

                                cur_part.bundles_chunk.offset = cur_part.bundles_chunk.end;
                                resp.br_req_partctx_it        = resp.br_req_partctx_it->next;

                                req_part_resp.next.seq_num  = next_seqnum;
                                req_part_resp.next.min_size = next_min_span;
                                req_part_resp.msgs.cnt      = consumed;
                                req_part_resp.drained       = drained_partition;

                                if (consumed) {
                                        auto out = consumed <= sizeof_array(req_part_resp.msgs.list.small)
                                                       ? req_part_resp.msgs.list.small + 0
                                                       : (req_part_resp.msgs.list.large = static_cast<consumed_msg *>(malloc(sizeof(consumed_msg) * consumed)));
                                        auto it          = cctx.first_bucket;
                                        auto last_bucket = cctx.last_bucket;

                                        while (it != last_bucket) {
                                                auto next = it->next;

                                                memcpy(out, it->data, sizeof(consumed_msg) * sizeof_array(msgs_bucket::data));
                                                out += sizeof_array(msgs_bucket::data);

                                                put_msgs_bucket(it);
                                                it = next;
                                        }

                                        memcpy(out, it->data, sizeof(consumed_msg) * last_bucket_size);
                                        put_msgs_bucket(it);
                                }

                                req_part->partitions_list_ll.detach_and_reset();
                                api_req->ready_partitions_list.push_back(&req_part->partitions_list_ll);

                                cctx.reset();

                                if (trace) {
                                        SLog(ansifmt::color_red, ansifmt::inverse, "Exhausted partition bundle at ", ref, ", next_min_span = ", next_min_span, ", next_seqnum = ", next_seqnum, ", consumed = ", consumed, ansifmt::reset, "\n");
                                }
                                return true;
                        };

                        // TODO: do something useful
                        // - do we expect more content, based on resp.resp_end_offset?
                        // - have we exhausted this bundles chunk (for this partition)?
#define PROCESS_EXHAUSTION()                         \
        do {                                         \
                if (consider_exhaustion(__LINE__)) { \
                        goto parse_partition;        \
                } else {                             \
                        return true;                 \
                }                                    \
        } while (0)

                next_bundle:
                        cur_part.bundles_chunk.offset = std::distance(base, p);
                        cur_part.need_from            = cur_part.bundles_chunk.offset;
                        cur_part.log_base_seqnum      = log_base_seqnum;

                        if (trace) {
                                SLog(ansifmt::color_brown, ansifmt::inverse, "Parsing next bundle log_base_seqnum = ", cur_part.log_base_seqnum, ansifmt::reset, "\n");
                        }

                        if (!Compression::check_decode_varuint32(p, chunk_end)) {
                                cur_part.need_upto = std::distance(base, p + 256);
                                PROCESS_EXHAUSTION();
                        }

                        const auto bundle_len = Compression::decode_varuint32(p);
                        const auto bundle_end = p + bundle_len;

                        if (trace) {
                                SLog("bundle_len = ", bundle_len, " next is at offset ", std::distance(base, bundle_end), "\n");
                        }

                        // we 'll adjust need_to later if we can
                        cur_part.need_upto = std::distance(base, bundle_end + 256);

                        if (p + sizeof(uint8_t) > chunk_end) {
                                PROCESS_EXHAUSTION();
                        }

                        // begin: bundle header
                        const auto bundle_hdr_flags = decode_pod<uint8_t>(p);
                        const auto codec            = bundle_hdr_flags & 3;
                        const auto sparse_bundle    = bundle_hdr_flags & (1u << 6);
                        uint32_t   msgset_size      = (bundle_hdr_flags >> 2) & 0xf;
                        uint64_t   msgset_end;

                        if (0 == msgset_size) {
                                if (!Compression::check_decode_varuint32(p, chunk_end)) {
                                        PROCESS_EXHAUSTION();
                                } else {
                                        msgset_size = Compression::decode_varuint32(p);
                                }
                        }

                        if (sparse_bundle) {
                                if (trace) {
                                        SLog("This is a SPARSE bundle\n");
                                }

                                if (p + sizeof(uint64_t) >= chunk_end) {
                                        PROCESS_EXHAUSTION();
                                }

                                first_msg_seqnum = decode_pod<uint64_t>(p);

                                if (msgset_size != 1) {
                                        if (!Compression::check_decode_varuint32(p, chunk_end)) {
                                                PROCESS_EXHAUSTION();
                                        }

                                        last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1;
                                } else {
                                        last_msg_seqnum = first_msg_seqnum;
                                }

                                if (trace) {
                                        SLog("first_msg_seqnum = ", first_msg_seqnum, ", last_msg_seqnum = ", last_msg_seqnum, "\n");
                                }

                                log_base_seqnum = first_msg_seqnum;
                                msgset_end      = last_msg_seqnum + 1;
                        } else {
                                msgset_end = log_base_seqnum + msgset_size;
                        }
                        // end: bundle header

                        if (trace) {
                                SLog("Parsed bundle header\n");
                        }

                        if (requested_seqnum < std::numeric_limits<uint64_t>::max() && requested_seqnum >= msgset_end) {
                                // fast-path: skip this bundle
                                if (trace) {
                                        SLog("Can skip bundle\n");
                                }

                                p               = bundle_end;
                                log_base_seqnum = msgset_end;
                                goto next_bundle;
                        }

                        cur_bundle.size             = bundle_len;
                        cur_bundle.codec            = codec;
                        cur_bundle.sparse           = sparse_bundle;
                        cur_bundle.first_msg_seqnum = first_msg_seqnum;
                        cur_bundle.last_msg_seqnum  = last_msg_seqnum;
                        cur_bundle.cur_msg_set.size = msgset_size;

                        if (codec) {
                                if (trace) {
                                        SLog("Need to decompress bundle msgs set\n");
                                }

                                if (bundle_end > chunk_end) {
                                        PROCESS_EXHAUSTION();
                                }

                                auto b = get_buffer();

                                switch (codec) {
                                        case 1:
                                                if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, std::distance(p, bundle_end), b)) {
                                                        IMPLEMENT_ME();
                                                }
                                                break;

                                        default:
                                                IMPLEMENT_ME();
                                }

                                cur_bundle.msgset_content.tmpbuf_range.p = reinterpret_cast<const uint8_t *>(b->data());
                                cur_bundle.msgset_content.tmpbuf_range.e = cur_bundle.msgset_content.tmpbuf_range.p + b->size();

                                b->reserve(sizeof(buf_llhdr) + 16);

                                auto ptr = reinterpret_cast<buf_llhdr *>(b->data() + b->size());

                                ptr->b         = b;
                                ptr->next      = resp.used_bufs;
                                resp.used_bufs = ptr;

                        } else {
                                resp.retain_buf = true;

                                cur_bundle.msgset_content.inb_range.o = std::distance(base, p);
                                cur_bundle.msgset_content.inb_range.e = std::distance(base, bundle_end);
                        }

                        cur_part.bundles_chunk.offset = std::distance(base, bundle_end);

                        // prepare for parsing the messages set
                        // we may need to read multiple packets before we have everything for the set
                        auto &cur_msgset = cur_bundle.cur_msg_set;

                        cur_msgset.ts                  = 0;
                        cur_msgset.msg_idx             = 0;
                        cur_msgset.min_accepted_seqnum = requested_seqnum == std::numeric_limits<uint64_t>::max() ? 0 : requested_seqnum;

                        resp.state               = State::ParsePartitionBundleMsgSet;
                        cur_part.log_base_seqnum = log_base_seqnum;

                        if (trace) {
                                SLog("Ready to parse bundle's msgs set size = ", msgset_size, ", next will be at ", cur_part.bundles_chunk.offset, "\n");
                        }
                }
                        [[fallthrough]];

                case State::ParsePartitionBundleMsgSet:
                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_brown, "ParsePartitionBundleMsgSet", ansifmt::reset, "\n");
                        }

                        {
                                // bundle's message set
                                auto &         cur_part        = resp.cur_partition;
                                auto &         cur_bundle      = cur_part.cur_bundle;
                                auto &         cur_msgset      = cur_bundle.cur_msg_set;
                                auto           any_captured    = cur_bundle.any_captured;
                                const auto     codec           = cur_bundle.codec;
                                auto &         log_base_seqnum = cur_part.log_base_seqnum;
                                auto &         msg_idx         = cur_msgset.msg_idx;
                                const uint8_t *p;
                                const uint8_t *msgset_end;
                                const auto     sparse_bundle       = cur_bundle.sparse;
                                const auto     msgset_size         = cur_msgset.size;
                                const auto     min_accepted_seqnum = cur_msgset.min_accepted_seqnum;
                                str_view8      key;
                                auto &         cctx = cur_part.capture_ctx;

                                if (codec) {
                                        p          = cur_bundle.msgset_content.tmpbuf_range.p;
                                        msgset_end = cur_bundle.msgset_content.tmpbuf_range.e;
                                } else {
                                        p          = base + cur_bundle.msgset_content.inb_range.o;
                                        msgset_end = std::min(e, base + cur_bundle.msgset_content.inb_range.e);
                                }

                                if (trace) {
                                        SLog("Attempting to parse messages set from data of size ", std::distance(p, msgset_end), "\n");
                                }

                                for (;; ++msg_idx, ++log_base_seqnum) {
                                        if (trace) {
                                                SLog("Parsing next message msg_idx = ", msg_idx, ", log_base_seqnum = ", log_base_seqnum, "\n");
                                        }

                                        if (!codec & any_captured) {
                                                // can optimize
                                                cur_part.need_upto = std::distance(base, p + 256);
                                        }

                                        if (p + sizeof(uint8_t) > msgset_end) {
                                                if (trace) {
                                                        SLog("OK, we hit msgs set EOF\n");
                                                }

                                                goto try_next_bundle;
                                        }

                                        const auto msg_flags = decode_pod<uint8_t>(p);

                                        if (sparse_bundle) {
                                                if (msg_flags & unsigned(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                        //
                                                } else if (0 == msg_idx) {
                                                        log_base_seqnum = cur_bundle.first_msg_seqnum;
                                                } else if (msg_idx == msgset_size - 1) {
                                                        log_base_seqnum = cur_bundle.last_msg_seqnum;
                                                } else {
                                                        if (!Compression::check_decode_varuint32(p, msgset_end)) {
                                                                if (trace) {
                                                                        SLog("Not enough data\n");
                                                                }
                                                                goto try_next_bundle;
                                                        }

                                                        const auto delta = Compression::decode_varuint32(p);

                                                        log_base_seqnum += delta;
                                                }
                                        }

                                        const auto msg_abs_seqnum = log_base_seqnum;

                                        if (0 == (msg_flags & unsigned(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                                if (p + sizeof(uint64_t) > msgset_end) {
                                                        if (trace) {
                                                                SLog("Not enough data\n");
                                                        }
                                                        goto try_next_bundle;
                                                }

                                                cur_msgset.ts = decode_pod<uint64_t>(p);
                                        }

                                        if (msg_flags & unsigned(TankFlags::BundleMsgFlags::HaveKey)) {
                                                if (p + sizeof(uint8_t) > msgset_end || (p + *p + sizeof(uint8_t) > msgset_end)) {
                                                        if (trace) {
                                                                SLog("Not enough data\n");
                                                        }
                                                        goto try_next_bundle;
                                                } else {
                                                        key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                                        p += sizeof(uint8_t) + key.size();
                                                }
                                        } else {
                                                key.reset();
                                        }

                                        if (!Compression::check_decode_varuint32(p, msgset_end)) {
                                                if (trace) {
                                                        SLog("Not enough data\n");
                                                }
                                                goto try_next_bundle;
                                        }

                                        const auto len = Compression::decode_varuint32(p);

                                        if (const auto e = p + len; e > msgset_end) {
                                                if (!codec && any_captured) {
                                                        // can optimize
                                                        cur_part.need_upto = std::distance(base, e + 256);
                                                }

                                                if (trace) {
                                                        SLog("Not enough data mssage content length = ", len, ", required ", size_repr(std::distance(msgset_end, e)), " more\n");
                                                }

                                                goto try_next_bundle;
                                        } else if (msg_abs_seqnum >= min_accepted_seqnum) {
                                                const str_view32 content(reinterpret_cast<const char *>(p), len);

                                                if (cctx.last_bucket_size == sizeof_array(msgs_bucket::data)) {
                                                        auto b = get_msgs_bucket();

                                                        b->next = nullptr;
                                                        if (cctx.last_bucket) {
                                                                cctx.last_bucket->next = b;
                                                        } else {
                                                                cctx.first_bucket = b;
                                                        }

                                                        cctx.last_bucket      = b;
                                                        cctx.last_bucket_size = 0;
                                                }

                                                auto m = cctx.last_bucket->data + cctx.last_bucket_size++;

                                                if (trace) {
                                                        SLog("Got key [", key, "] content [...] ", Date::ts_repr(Timings::Milliseconds::ToSeconds(cur_msgset.ts)), "\n");
                                                }

                                                cctx.consumed++;
                                                any_captured = cur_bundle.any_captured = true;
                                                m->seqNum                              = msg_abs_seqnum;
                                                m->content                             = content;
                                                m->ts                                  = cur_msgset.ts;
                                                m->key                                 = key;
                                        }

                                        p += len;

                                        // advance to the next message
                                        // in a new reentry we want to read the next message
                                        if (codec) {
                                                cur_bundle.msgset_content.tmpbuf_range.p = p;
                                        } else {
                                                cur_bundle.msgset_content.inb_range.o = std::distance(base, p);
                                        }
                                }

                        try_next_bundle:;
                                // we need to determine wether we have another bundle we can parse for this partition
                                // of we have exchausted all partition bundles
                                if (trace) {
                                        SLog("done with bundle, msg_idx = ", msg_idx, " / ", msgset_size, "\n");
                                }

                                resp.state = State::ParsePartitionBundle;
                                goto parse_partition_bundle;
                        }
                        break;

                default:
                        IMPLEMENT_ME();
        }
}
#endif

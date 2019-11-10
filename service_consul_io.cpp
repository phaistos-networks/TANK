#include "service_common.h"
#include <ext/tl/optional.hpp>

bool Service::try_recv_consul(connection *const c) {
        static constexpr bool trace{false};

        if (c->as.consul.flags & unsigned(connection::As::Consul::Flags::WaitShutdown)) {
                // we are just waiting until shutdown because
                // the last response didn't provide us with a content length
                // so we have no idea how much data to expect
                if (trace) {
                        SLog("Waiting\n");
                }

                return true;
        }

        switch (c->as.consul.state) {
                case connection::As::Consul::State::WaitRespFirstBytes:
                        //SLog("Got First Bytes from response\n");

                        c->as.consul.state = connection::As::Consul::State::ReadHeaders;
                        c->as.consul.cur.resp.reset();
                        [[fallthrough]];
                case connection::As::Consul::State::ReadHeaders:
                        return recv_consul_resp_headers(c);

                case connection::As::Consul::State::ReadContent:
                        return recv_consul_resp_content(c);

                case connection::As::Consul::State::ReadCompressedContent:
                        return recv_consul_resp_content_gzip(c);

                default: {
                        IMPLEMENT_ME();
                }
        }
}

static constexpr bool is_ws[256] = {
    [' ']  = true,
    ['\t'] = true,
    ['\r'] = true,
    ['\n'] = true,
    ['\v'] = true,
};

bool Service::recv_consul_resp_headers(connection *const c) {
        TANK_EXPECT(c);
        auto req = c->as.consul.cur.req;
        auto b   = c->inB;

        TANK_EXPECT(b);
        TANK_EXPECT(req);
        TANK_EXPECT(!req->is_released());
	auto b_data = b->data();
	TANK_EXPECT(b_data);
        str_view32 s;
        str_view32 name, value;

	// \0 terminated so that we won't need to check for (p < b->end())
	b_data[b->size()] = '\0';
        for (const auto *p = b_data + b->offset();;) {
                while (is_ws[static_cast<uint8_t>(*p)]) {
                        ++p;
                }

                if (*p == '\0') {
                        put_buf(b);
                        c->inB = nullptr;
                        return try_make_idle(c);
                }

                b->set_offset(std::distance(const_cast<const char *>(b_data), p));

                for (s.p = p;; ++p) {
                        if (*p == '\0') {
                                return consider_http_headers_size(c);
                        } else if (*p == '\n') {
                                s.set_end(p++);
                                break;
                        } else if (*p == '\r' && p[1] == '\n') {
                                s.set_end(p);
                                p += 2;
                                break;
                        }
                }

                if (!s.StripPrefix(_S("HTTP/"))) {
                        return shutdown(c, __LINE__);
                }

                // process the first line
                uint32_t major{0}, minor{0};

                {
                        const auto *p = s.data();

                        while (*p >= '0' && *p <= '9') {
                                major = major * 10 + (*(p++) - '0');
                        }

                        if (*p != '.' || major == 0) {
                                return shutdown(c, __LINE__);
                        }

                        for (++p; *p >= '0' && *p <= '9'; ++p) {
                                minor = minor * 10 + (*p - '0');
                        }

                        while (*p == ' ' || *p == '\t') {
                                ++p;
                        }

                        uint32_t rc{0};

                        while (*p >= '0' && *p <= '9') {
                                rc = rc * 10 + (*(p++) - '0');
                        }

                        while (*p == ' ' || *p == '\t') {
                                ++p;
                        }

                        c->as.consul.cur.resp.rc = rc;
                }

                // process all response headers
                tl::optional<uint64_t> content_length;
                uint64_t               consul_index{0};
                uint8_t                flags = (major > 1 || (major == 1 && minor >= 1)) ? unsigned(connection::As::Consul::Cur::Response::Flags::KeptAlive) : 0;

                for (;;) {
                        for (name.p = p;; ++p) {
                                if (*p == '\0') {
                                        return consider_http_headers_size(c);
                                } else if (*p == ':') {
                                        name.set_end(p);

                                        for (++p; *p == ' ' || *p == '\t'; ++p) {
                                                continue;
                                        }

                                        for (value.p = p;; ++p) {
                                                if (*p == '\0') {
                                                        return consider_http_headers_size(c);
                                                } else if (*p == '\n') {
                                                        value.set_end(p++);
                                                        break;
                                                } else if (*p == '\r' && p[1] == '\n') {
                                                        value.set_end(p);
                                                        p += 2;
                                                        break;
                                                }
                                        }
                                        break;
                                } else if (*p == '\n') {
                                        name.set_end(p++);
                                        value.reset();
                                        break;
                                } else if (*p == '\r' && p[1] == '\n') {
                                        name.set_end(p);
                                        value.reset();
                                        p += 2;
                                        break;
                                }
                        }

                        if (!name) {
                                // done with headers
                                const auto real_content_len = content_length ? content_length.value() : 0;
                                auto &     resp             = c->as.consul.cur.resp;

                                b->set_offset(std::distance(const_cast<const char *>(b_data), p));

                                if (flags & unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer)) {
                                        resp.decoded_content_bytes = 0;
                                        resp.content_length        = 0;
                                } else {
                                        resp.remaining_content_bytes = real_content_len;
                                        resp.content_length          = real_content_len;
                                }

                                resp.flags        = flags;
                                resp.consul_index = consul_index;
                                return consider_consul_resp_headers(c);
                        } else {
                                if (name.EqNoCase(_S("Content-Length"))) {
                                        content_length = value.as_uint64();
                                } else if (name.EqNoCase(_S("X-Consul-Index"))) {
                                        consul_index = value.as_uint64();
                                } else if (name.EqNoCase(_S("Connection"))) {
                                        if (value.EqNoCase(_S("keep-alive")) || value.EqNoCase(_S("keepalive"))) {
						flags|= unsigned(connection::As::Consul::Cur::Response::Flags::KeptAlive);
                                        } else {
						flags &= ~unsigned(connection::As::Consul::Cur::Response::Flags::KeptAlive);
                                        }
                                } else if (name.EqNoCase(_S("Transfer-Encoding"))) {
                                        if (value.EqNoCase(_S("chunked"))) {
						flags|= unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer);
                                        }
                                } else if (name.EqNoCase(_S("Content-Encoding"))) {
                                        if (value.EqNoCase(_S("gzip"))) {
						flags|= unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding);
                                        }
                                }
                        }
                }
        }
}

// this shouldn't be consused with LONG RUNNING requests
// such active consul requests are ones that we are not going to long-poll(e.g Monitor) and expect
// a response fairly quickly.
//
// TODO: maybe we should consider providing a duration in schedule_consul_req()
// where if we didn't get a response within that time, we 'd shut it down. That would mean
// that we couldn't rely on monotonically increasing expiration times (like we do now) and we would need
// to use use timers.
// Maybe we should do that later (an upside to this is that we wouldn't
// need to track long_polling_conns)
void Service::consider_long_running_active_consul_requests() {
        SLog("Considering long running requests:", consul_state.active_conns.size(), "\n");

        while (!consul_state.active_conns.empty()) {
                auto c = switch_list_entry(connection, as.consul.conns_ll, consul_state.active_conns.prev);

                c->verify();
                TANK_EXPECT(c->is_consul());
                TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Active);

                if (now_ms - c->classification.since < active_consul_connection_ttl) {
                        // this and all other idle consul connections haven't been idle for that long
                        break;
                }

                c->state.flags |= 1u << unsigned(connection::State::Flags::ShutdownReasonTimeout);
                shutdown(c, __LINE__);
        }
}

connection *Service::acq_new_consul_connection() {
        int fd;

	TANK_EXPECT(cluster_state.local_node.id);
	TANK_EXPECT(cluster_state._name);

        for (;;) {
                fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

                if (-1 == fd) {
                        if ((ENFILE == errno || EMFILE == errno) && try_shutdown_idle(1)) {
                                continue;
                        } else {
                                return nullptr;
                        }
                } else {
                        break;
                }
        }

        struct sockaddr_in sa;

        sa.sin_family      = AF_INET;
        sa.sin_addr.s_addr = consul_state.srv.endpoint.addr4;
        sa.sin_port        = htons(consul_state.srv.endpoint.port);

        if (-1 == connect(fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) && errno != EINPROGRESS) {
                TANKUtil::safe_close(fd);
                return nullptr;
        }

        auto c = get_connection();

        if (!c) {
                TANKUtil::safe_close(fd);
                return nullptr;
        }

        c->as.consul.reset();
        c->fd              = fd;
        c->type            = connection::Type::ConsulClient;
        c->as.consul.state = connection::As::Consul::State::Connecting;
        poller.insert(c->fd, EPOLLIN | EPOLLOUT, c);

        // we need to make sure we 'll be able to connect within a few seconds
        c->as.consul.attached_timer.type     = timer_node::ContainerType::ConnEstTimeout;
        c->as.consul.attached_timer.node.key = now_ms + 4 * 1000;
        register_timer(&c->as.consul.attached_timer.node);

        c->verify();
        return c;
}

bool Service::recv_consul_resp_content_chunks(connection *const c) {
	static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(c->as.consul.state == connection::As::Consul::State::ReadContent);
        TANK_EXPECT(c->as.consul.cur.resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer));
        auto b = c->inB;
        TANK_EXPECT(b);
        const auto b_data                = b->data();
        const auto offset                = b->offset();
        auto       decoded_content_bytes = c->as.consul.cur.resp.decoded_content_bytes;

        for (const auto *p = b_data + offset;;) {
                uint64_t chunk_len{0};

                b_data[b->size()] = '\0'; // IMPORTANT
                while (is_ws[static_cast<uint8_t>(*p)]) {
                        ++p;
                }

                // buffer is \0 terminated, so we don't need to guard against (p < e)
                for (;; ++p) {
                        const auto C = *p;

                        if (C == '\0') {
                                c->as.consul.cur.resp.decoded_content_bytes = decoded_content_bytes;
                                return true;
                        } else if (C >= 'a' && C <= 'f') {
                                chunk_len = chunk_len * 16 + (10 + C - 'a');
                        } else if (C >= 'A' && C <= 'F') {
                                chunk_len = chunk_len * 16 + (10 + C - 'A');
                        } else if (C >= '0' && C <= '9') {
                                chunk_len = chunk_len * 16 + (C - '0');
                        } else if (C == ' ' || C == '\t') {
                                for (++p; *p == ' ' || *p == '\t'; ++p) {
                                        //
                                }
                                if (*p == '\r' && p[1] == '\n') {
                                        p += 2;
                                } else if (*p == '\n') {
                                        ++p;
                                } else {
                                        c->as.consul.cur.resp.decoded_content_bytes = decoded_content_bytes;
                                        return true;
                                }

                                break;
                        } else if (*p == '\r' && p[1] == '\n') {
                                p += 2;
                                break;
                        } else if (*p == '\n') {
                                ++p;
                                break;
                        } else {
                                // what's this?
                                // Unexpected input
                                std::abort();
                                return shutdown(c, __LINE__);
                        }
                }

		if (trace) {
			SLog("chunk:", chunk_len, "\n");
		}

                const auto upto = p + chunk_len;

                if (upto > b->end()) {
                        // need more content for this chunk
			if (trace) {
				SLog("Need More Content\n");
			}

                        c->as.consul.cur.resp.decoded_content_bytes = decoded_content_bytes;
                        return true;
                }
                const auto start      = decoded_content_bytes;
                const auto strip_span = std::distance(const_cast<const char *>(b_data) + decoded_content_bytes, p);

		if (trace) {
			SLog("start:", start, ", strip_span:", strip_span, "\n");
		}

                b->erase(start, strip_span);
                decoded_content_bytes += chunk_len;

                const auto new_offset = decoded_content_bytes;

                p = b_data + new_offset;
                b->set_offset(new_offset);

                if (0 == chunk_len) {
                        // we are done

                        b->set_offset(static_cast<uint32_t>(0));
                        b->resize(decoded_content_bytes);

                        return process_ready_consul_resp(c, c->inB);
                }
        }

        return true;
}

const uint8_t *Service::parse_compressed_http_resp_content(connection *const c, const uint8_t *p, const uint8_t *const e, const int _flush) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        auto &resp = c->as.consul.cur.resp;
        TANK_EXPECT(c->as.consul.state == connection::As::Consul::State::ReadCompressedContent);
        TANK_EXPECT(resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding));
        auto &     cc   = resp.comp_ctx;
        const auto base = p;

        if (cc.state == CompressionContext::State::Headers) {
                if (p + 15 > e) {
                        return base;
                }

                if (p[0] != 0x1f || p[1] != 0x8b) {
                        return nullptr;
                }

                if (p[2] != Z_DEFLATED) {
                        return nullptr;
                }

                p += 3;

                const auto                  fmt   = decode_pod<uint8_t>(p);
                [[maybe_unused]] const auto ts    = decode_pod<uint32_t>(p);
                [[maybe_unused]] const auto level = decode_pod<uint8_t>(p);
                [[maybe_unused]] const auto os    = decode_pod<uint8_t>(p);

                if (fmt & (1u << 2)) {
                        // extra
                        p += decode_pod<uint16_t>(p);
                }

                if (fmt & (1u << 3)) {
                        // filename
                        do {
                                if (unlikely(p == e)) {
                                        return nullptr;
                                }
                        } while (*(p++));
                        ++p; // skip \0
                }

                if (fmt & (1u << 4)) {
                        // comment
                        do {
                                if (unlikely(p == e)) {
                                        return nullptr;
                                }
                        } while (*(p++));
                        ++p; // skip \0
                }

                if (fmt & (1u << 5)) {
                        // encryption header
                        p += 12;
                }

                if (fmt & 1) {
                        // CRC16 for gzip
                        p += sizeof(uint16_t);
                }

                if (unlikely(p > e)) {
                        return nullptr;
                }

                memset(&cc.stream, 0, sizeof(cc.stream));
                if (inflateInit2(&cc.stream, -MAX_WBITS) != Z_OK) {
                        IMPLEMENT_ME();
                }

                // we will decompress into cc.b
                cc.b     = get_buf();
                cc.state = CompressionContext::State::Content;
        } else if (cc.state == CompressionContext::State::Footer) {
        footer:
                if (std::distance(p, e) < sizeof(uint32_t) + sizeof(uint32_t)) {
                        // need more content for the footer
                } else {
                        [[maybe_unused]] const auto crc32         = decode_pod<uint32_t>(p);
                        [[maybe_unused]] const auto original_size = decode_pod<uint32_t>(p);

                        // TODO: if (original_size != cc.b->size() { }
                        inflateEnd(&cc.stream);

                        cc.state = CompressionContext::State::Fin;
                }

                return p;
        }

        auto ob = cc.b;
        TANK_EXPECT(ob);
        auto  ob_size = ob->size();
        auto &stream  = cc.stream;

        stream.next_in  = reinterpret_cast<Bytef *>(const_cast<uint8_t *>(p));
        stream.avail_in = std::distance(p, e);

        while (stream.avail_in) {
                ob->reserve(stream.avail_in * 2);

                stream.next_out  = reinterpret_cast<Bytef *>(ob->data() + ob_size);
                stream.avail_out = ob->capacity();
                stream.total_out = 0;
                stream.total_in  = 0;

                TANK_EXPECT(stream.avail_in);
                TANK_EXPECT(stream.avail_out);

                const auto r = inflate(&stream, _flush);

                if (stream.total_in > 0) {
                        p += stream.total_in;
                }

                ob_size += stream.total_out;
                ob->resize(ob_size);

                if (r == Z_STREAM_END) {
                        cc.state = CompressionContext::State::Footer;

                        goto footer;
                } else if (r == Z_BUF_ERROR) {
                        if (stream.avail_out == 0 && stream.avail_in) {
                                continue;
                        } else {
                                return p;
                        }
                } else if (r != Z_OK) {
                        IMPLEMENT_ME();
                }
        }

        return p;
}

bool Service::recv_consul_resp_content_gzip_chunks(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(c->as.consul.state == connection::As::Consul::State::ReadCompressedContent);
        TANK_EXPECT(c->as.consul.cur.resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding));
        auto &resp = c->as.consul.cur.resp;
        TANK_EXPECT(resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding));
        auto b = c->inB;
        TANK_EXPECT(b);
        auto *     b_data   = reinterpret_cast<uint8_t *>(b->data());
        auto blen     = b->size();
        const auto offset   = b->offset();
        auto &     cc       = resp.comp_ctx;
        const bool draining = resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::Draining);

        for (const auto *p = b_data + offset;;) {
                uint64_t chunk_len{0};

                // IMPORTANT, need to set \0 here in the loop
                b_data[b->size()] = '\0'; // IMPORTANT

                while (is_ws[static_cast<uint8_t>(*p)]) {
                        ++p;
                }

                // buffer is \0 terminated, so we don't need to guard against (p < e)
                for (;; ++p) {
                        const auto C = *p;

                        if (C == '\0') {
                                return true;
                        } else if (C >= 'a' && C <= 'f') {
                                chunk_len = chunk_len * 16 + (10 + C - 'a');
                        } else if (C >= 'A' && C <= 'F') {
                                chunk_len = chunk_len * 16 + (10 + C - 'A');
                        } else if (C >= '0' && C <= '9') {
                                chunk_len = chunk_len * 16 + (C - '0');
                        } else if (C == ' ' || C == '\t') {
                                for (++p; *p == ' ' || *p == '\t'; ++p) {
                                        //
                                }
                                if (*p == '\r' && p[1] == '\n') {
                                        p += 2;
                                } else if (*p == '\n') {
                                        ++p;
                                } else {
                                        return true;
                                }

                                break;
                        } else if (*p == '\r' && p[1] == '\n') {
                                p += 2;
                                break;
                        } else if (*p == '\n') {
                                ++p;
                                break;
                        } else {
                                std::abort();
                                return shutdown(c, __LINE__);
                        }
                }

                const auto upto = p + chunk_len;

                if (upto > reinterpret_cast<uint8_t *>(b->end())) {
                        // need more content for this chunk
                        return true;
                }

                if (0 == chunk_len) {
                        return process_ready_consul_resp(c, cc.b);
                }

                if (draining) {
                        // we are not interested in the response content
                        // we 'll consume and silently drop whatever content we read
                        const auto new_offset = std::distance(const_cast<const uint8_t *>(b_data), p);

                        if (new_offset == blen) {
                                put_buf(std::exchange(c->inB, nullptr));
                                return true;
                        } else if (new_offset > 2 * 1024 * 1024) {
                                b->erase(0, new_offset);
                                b->set_offset(static_cast<uint32_t>(0));
                                p = b_data;
                        } else {
                                p = upto;
                        }
                } else {
                        const auto consume_it = parse_compressed_http_resp_content(c, p, upto, Z_FINISH);

                        if (consume_it != upto) {
                                // we expected to have consumed the _whole_ chunk here
                                return shutdown(c, __LINE__);
                        } else if (cc.state != CompressionContext::State::Fin) {
                                // This makes no sense
                                // we consumed the whole gzip chunk but didn't get to the end?
                                // likely corrupt
                                return shutdown(c, __LINE__);
                        } else {
                                TANK_EXPECT(cc.b);

                                // OK, transition Fin->Headers again
                                cc.state = CompressionContext::State::Headers;
                        }

                        const auto new_offset = std::distance(const_cast<const uint8_t *>(b_data), p);

                        if (new_offset == blen) {
                                put_buf(std::exchange(c->inB, nullptr));
                                return true;
                        } else if (new_offset > 2 * 1024 * 1024) {
                                b->erase(0, new_offset);
                                b->set_offset(static_cast<uint32_t>(0));
                                p = b_data;
				blen = b->size();
                        } else {
                                p = upto;
                        }
                }
        }
}

void Service::tear_down_consul_resp(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());

        if (c->as.consul.state == connection::As::Consul::State::ReadContent || c->as.consul.state == connection::As::Consul::State::ReadCompressedContent) {
                auto &resp = c->as.consul.cur.resp;

                if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding)) {
                        auto &ctx = resp.comp_ctx;

                        switch (ctx.state) {
                                case CompressionContext::State::Headers:
                                case CompressionContext::State::Fin:
                                        break;

                                default:
                                        if (auto b = std::exchange(ctx.b, nullptr)) {
                                                put_buf(b);
                                        }

                                        inflateEnd(&ctx.stream);
                                        ctx.state = CompressionContext::State::Headers;
                                        break;
                        }
                }
        }

        if (auto b = std::exchange(c->inB, nullptr)) {
                put_buf(b);
        }
}

bool Service::recv_consul_resp_content_gzip(connection *const c) {
	static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(c->as.consul.state == connection::As::Consul::State::ReadCompressedContent);
        TANK_EXPECT(c->as.consul.cur.resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding));
        auto b = c->inB;
        TANK_EXPECT(b);
        auto &     resp                    = c->as.consul.cur.resp;
        const auto blen                    = b->size();
        const auto offset                  = b->offset();
        const auto available_content_bytes = blen - offset; // how much extra content do we have available in the read buffer now

        if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer)) {
		if (trace) {
			SLog("Going to reach compresed chunks\n");
		}

                return recv_consul_resp_content_gzip_chunks(c);
        }

        if (available_content_bytes > resp.remaining_content_bytes) {
                // we got more data in the HTTP response than we were told
		if (trace) {
			SLog("Got more data than expected\n");
		}

                return shutdown(c, __LINE__);
        }

        const auto  b_data = reinterpret_cast<const uint8_t *>(b->data());
        const auto *p = b_data + offset, *const e = b_data + b->size();

        if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::Draining)) {
		if (trace) {
			SLog("Now Draining\n");
		}

                if (available_content_bytes == resp.remaining_content_bytes) {
                        return process_ready_consul_resp(c, b);
                }

                if (available_content_bytes > resp.remaining_content_bytes) {
                        return shutdown(c, __LINE__);
                }

                // we don't care about the response so we 'll just silently consume and ignore it
                resp.remaining_content_bytes -= available_content_bytes;
                put_buf(std::exchange(c->inB, nullptr));
                return true;
        } else {
		if (trace) {
			SLog("Will attempt to decompress content\n");
		}

                const auto consume_it = parse_compressed_http_resp_content(c, p, e, 0);

                if (consume_it == nullptr) {
			if (trace) {
				SLog("Need to shut down the con\n");
			}

                        return shutdown(c, __LINE__);
                } else if (consume_it == p) {
			if (trace) {
				SLog("Need more content\n");
			}
				
                        return true;
                }

                // compute new offset into c->inB based
                // on how much was consumed in parse_compressed_http_resp_content()
                const auto new_offset = std::distance(b_data, consume_it);

                b->set_offset(new_offset);

		if (trace) {
			SLog("new_offset = ", new_offset, "\n");
		}

                if (available_content_bytes == resp.remaining_content_bytes) {
                        // we must be done by now
                        // we have cosumed the entirety of the HTTP response content
			if (trace) {
				SLog("We are done\n");
			}

			auto &&cc = resp.comp_ctx;

                        if (cc.state != CompressionContext::State::Fin) {
                                IMPLEMENT_ME();
                        }

                        // we are done here
                        auto ob = cc.b;

                        TANK_EXPECT(ob);
                        TANK_EXPECT(ob->offset() == 0);

                        return process_ready_consul_resp(c, ob);
                }

                resp.remaining_content_bytes -= available_content_bytes;

                if (new_offset == b->size()) {
			if (trace) {
				SLog("consumed all data from input buffer\n");
			}

                        put_buf(std::exchange(c->inB, nullptr));
                } else if (new_offset > 2 * 1024 * 1024) {
			if (trace) {
				SLog("Stripping prefix\n");
			}

                        b->erase(0, new_offset);
                        b->set_offset(static_cast<uint32_t>(0));
                }

                return true;
        }
}

bool Service::recv_consul_resp_content(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(c->as.consul.state == connection::As::Consul::State::ReadContent);
        auto &resp = c->as.consul.cur.resp;

        if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer)) {
                return recv_consul_resp_content_chunks(c);
        }

        auto b = c->inB;
        TANK_EXPECT(b);
        const auto blen                    = b->size();
        const auto offset                  = b->offset();
        const auto available_content_bytes = blen - offset; // how much extra content do we have available in the read buffer now

        if (available_content_bytes == resp.remaining_content_bytes) {
                return process_ready_consul_resp(c, c->inB);
        }

        if (available_content_bytes > resp.remaining_content_bytes) {
                // Got more than we expected to get
                return shutdown(c, __LINE__);
        }

        // expecting more data
        if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::Draining)) {
		// we don't care for any response content
                resp.remaining_content_bytes -= available_content_bytes;
                put_buf(std::exchange(c->inB, nullptr));
        }

        return true;
}

bool Service::consider_http_headers_size(connection *const c) {
        auto       b           = c->inB;
        const auto headers_len = b->size() - b->offset();

        if (headers_len > 2 * 1024 * 1024) {
                // We can't allow clients to send a request with too many headers
                // nor a peer to provide a response with a large heade otherwise
                // they could easily force inB to allocate too much memory
                return shutdown(c, __LINE__);
        }

        return true;
}

bool Service::complete_consul_req(connection *const c) {
        static constexpr bool trace{false};

        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(!c->inB);
        auto req = c->as.consul.cur.req;

        TANK_EXPECT(req);
        TANK_EXPECT(!req->is_released());

        if (trace) {
                SLog("Done with ", ptr_repr(req), " ", ptr_repr(c), "\n");
        }

        auto next = req->then;

#ifdef TANK_RUNTIME_CHECKS
        if (next) {
                TANK_EXPECT(!next->is_released());

                for (auto it = next->then; it; it = it->then) {
                        TANK_EXPECT(!it->is_released());
                }
        }
#endif

        c->as.consul.cur.req = nullptr;

        consul_req_over(req);
        consul_state.put_req(req, __LINE__);
        consul_state.srv.consequtive_faults = 0;

        // Looks like we got a request in the pending request _AND_ assigned to connections's request
        // and now that we are reusing the request and set its flags to Released, verify() determines
        // that that pending request is now released

        if (-1 == c->fd) {
                // was invoked in the context of cleanup_connection()
                TANK_EXPECT(nullptr == next);
                return false;
        }

        if (next) {
                TANK_EXPECT(!next->is_released());

                // a request that depended on this completed request
                // should be scheduled now
                set_consul_conn_cur_req(c, next);
        }

        return consider_idle_consul_connection(c);
}

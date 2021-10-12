#include "client_common.h"

#ifdef HAVE_NETIO_THROTTLE
void TankClient::throttle_read(connection *c, const uint64_t expiration) {
        static constexpr bool trace{false};

        if (!c->throttler.read.ll.empty()) {
                c->throttler.read.ll.detach_and_reset();
        }

        if (c->state.flags & (1u << static_cast<uint8_t>(connection::State::Flags::NeedOutAvail))) {
                if (trace) {
                        SLog(ansifmt::color_blue, ansifmt::bgcolor_red, "Throttle read: EPOLLOUT", ansifmt::reset, "\n");
                }
                poller.set_data_events(c->fd, c, EPOLLOUT);
        } else {
                if (trace) {
                        SLog(ansifmt::color_blue, ansifmt::bgcolor_red, "Throttle read: 0 for ", expiration - now_ms, ansifmt::reset, "\n");
                }
                poller.set_data_events(c->fd, c, 0);
        }

        c->throttler.read.until              = expiration;
        throttled_connections_read_list_next = std::min(c->throttler.read.until, throttled_connections_read_list_next);
        throttled_connections_read_list.push_back(&c->throttler.read.ll);

        TANK_EXPECT(throttled_connections_read_list.empty() == false);
}

void TankClient::throttle_write(connection *c, const uint64_t expiration) {
        static constexpr bool trace{false};

        if (!c->throttler.write.ll.empty()) {
                c->throttler.write.ll.detach_and_reset();
        }

        if (trace) {
                SLog(ansifmt::color_blue, ansifmt::bgcolor_red, "Throttle write", ansifmt::reset, "\n");
        }

        stop_poll_outavail(c);

        c->throttler.write.until              = expiration;
        throttled_connections_write_list_next = std::min(c->throttler.write.until, throttled_connections_write_list_next);
        throttled_connections_write_list.push_back(&c->throttler.write.ll);
}

void TankClient::manage_throttled_connections() {
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::inverse, ansifmt::color_blue, "Managing:", throttled_connections_read_list.size(), " ", throttled_connections_write_list.size(), ansifmt::reset, "\n");
        }

        throttled_connections_read_list_next = std::numeric_limits<uint64_t>::max();

        while (!throttled_connections_read_list.empty()) {
                auto it   = throttled_connections_read_list.prev;
                auto c    = switch_list_entry(connection, throttler.read.ll, it);
                auto prev = it->prev;

                if (const auto when = c->throttler.read.until; when > now_ms) {
                        throttled_connections_read_list_next = when;
                        goto l1;
                }

                if (c->state.flags & (1u << static_cast<uint8_t>(connection::State::Flags::NeedOutAvail))) {
                        if (trace) {
                                SLog("No longer throttling read (EPOLLIN|EPOLLOUT)\n");
                        }

                        poller.set_data_events(c->fd, c, EPOLLIN | EPOLLOUT);
                } else {
                        if (trace) {
                                SLog("No longer throttling read (EPOLLIN)\n");
                        }

                        poller.set_data_events(c->fd, c, EPOLLIN);
                }

                c->throttler.read.ll.detach_and_reset();
                rcv(c);

                it = prev;
        }

l1:

        throttled_connections_write_list_next = std::numeric_limits<uint64_t>::max();

        while (!throttled_connections_write_list.empty()) {
                auto it   = throttled_connections_write_list.prev;
                auto c    = switch_list_entry(connection, throttler.write.ll, it);
                auto prev = it->prev;

                if (const auto when = c->throttler.write.until; when > now_ms) {
                        throttled_connections_write_list_next = when;
                        return;
                }

                if (trace) {
                        SLog("No longer throttling write\n");
                }

                poll_outavail(c);

                c->throttler.write.ll.detach_and_reset();
                it = prev;
        }
}
#endif

void TankClient::poll_outavail(connection *const c) {
        const auto new_flags = c->state.flags | (1u << static_cast<uint8_t>(connection::State::Flags::NeedOutAvail));

        if (c->state.flags != new_flags) {
                c->state.flags = new_flags;

#ifdef HAVE_NETIO_THROTTLE
                if (c->throttler.read.ll.empty()) {
                        poller.set_data_events(c->fd, c, EPOLLIN | EPOLLOUT);
                } else {
                        poller.set_data_events(c->fd, c, EPOLLOUT);
                }
#else
                poller.set_data_events(c->fd, c, EPOLLIN | EPOLLOUT);
#endif
        }
}

void TankClient::stop_poll_outavail(connection *const c) {
        const auto new_flags = c->state.flags & ~(1u << static_cast<uint8_t>(connection::State::Flags::NeedOutAvail));

        if (c->state.flags != new_flags) {
                c->state.flags = new_flags;

#ifdef HAVE_NETIO_THROTTLE
                if (c->throttler.read.ll.empty()) {
                        poller.set_data_events(c->fd, c, EPOLLIN);
                } else {
                        poller.set_data_events(c->fd, c, 0);
                }
#else
                poller.set_data_events(c->fd, c, EPOLLIN);
#endif
        }
}

bool TankClient::tx(connection *c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->fd != -1);

        switch (c->type) {
                case connection::Type::Tank:
                        return tx_tank(c);

                default:
                        IMPLEMENT_ME();
        }
}

bool TankClient::materialize_next_broker_req_payload([[maybe_unused]] broker *br) {
        // we no longer lazilily materialize broker_api_request's
        return false;
}

#ifdef HAVE_NETIO_THROTTLE
constexpr size_t throttle_write_size() {
        return 8;
}

constexpr size_t throttle_read_size() {
        return 256;
}

constexpr uint32_t throttle_span() {
        return 500;
}
#endif

bool TankClient::tx_tank(connection *const c) {
	enum {
		trace = false,
	};
        auto                  fd = c->fd;
        auto                  br = c->as.tank.br; // associated broker

	// TODO: https://github.com/phaistos-networks/TANK/issues/76
	// - fast-path for a single outgoing content payload
	// - track payload size so that we can avoid iterating the iovecs

        TANK_EXPECT(br);
        TANK_EXPECT(c->fd != -1);

        struct iovec iov[128], *const out_end = iov + sizeof_array(iov);
        auto index = br->outgoing_content.front_payload_iovecs_index;

        if (trace) {
                SLog("Attempting to send, index = ", index,
                     ", any:", br->outgoing_content.front() != nullptr, "\n");
        }

        for (;;) {
                auto   out = iov;
                auto   _i  = index;
                size_t sum{0};

                for (auto it = br->outgoing_content.front(); out < out_end; it = it->next, _i = 0) {
                        if (!it) {
                                // we may still have requests pending materialization
                                // lazy materialization offers some important benefits
                                // also, notice how we use set_any_transferred(), so that
                                // even if a single byte of broker request payload has been scheduled for transmission
                                // we will know
                                if (!materialize_next_broker_req_payload(br)) {
                                        if (trace) {
                                                SLog("No more broker requests to materialize\n");
                                        }

                                        break;
                                }

                                if (trace) {
                                        SLog("Materialized broker request\n");
                                }

                                it = br->outgoing_content.front();
                        }

			TANK_EXPECT(it);
#ifdef HAVE_NETIO_THROTTLE
                        const auto n = std::min<size_t>(std::distance(out, out_end), it->iovecs.size - _i);

                        for (const auto *p = it->iovecs.data + _i, *const e = p + n; p < e; ++p) {
                                const auto l = p->iov_len;

                                if (sum + l <= throttle_write_size()) {
                                        sum += l;
                                        *out++ = *p;
                                } else {
                                        *out++ = {p->iov_base, throttle_write_size() - sum};
                                        break;
                                }
                        }
#else
                        const auto n = std::min<size_t>(std::distance(out, out_end), it->iovecs.size - _i);

                        if (trace) {
                                SLog("For payload size = ", it->iovecs.size, "\n");
                        }

                        for (const auto *p = it->iovecs.data + _i, *const e = p + n; p < e; ++p) {
                                sum += p->iov_len;
                        }

                        memcpy(out, it->iovecs.data + _i, n * sizeof(struct iovec));
                        out += n;
#endif
                }

                if (out == iov) {
                        if (trace) {
                                SLog("Nothing to send\n");
                        }

                        return true;
                }

        l10:
                auto r = writev(fd, iov, std::distance(iov, out));

                if (trace) {
                        SLog("writev() for ", std::distance(iov, out), " ", r, " c = ", ptr_repr(c), "\n");
                }

                if (-1 == r) {
                        if (EINTR == errno) {
                                goto l10;
                        } else if (EAGAIN == errno) {
                                br->outgoing_content.front_payload_iovecs_index = index;
                                poll_outavail(c);
                                return true;
                        } else {
                                switch (errno) {
                                        case EIO:
                                                break;

                                        default:
						Print("Unexpected, writev() failed with ", strerror(errno), "\n");
                                                std::abort();
                                }

                                if (trace) {
                                        SLog("Got:", strerror(errno), "\n");
                                }

                                return shutdown(c, __LINE__);
                        }
                }

                // if we wrote as much as we needed, then
                // it's likely the socket buffer's not full, otherwise if it only
                // could keep some of the data it's almost certainly going to be full
                // if we writev() again
#ifdef HAVE_NETIO_THROTTLE
                constexpr bool try_again = false;
#else
                const auto try_again = r == sum;
#endif
                auto   it = br->outgoing_content.front();
                iovec *ptr;
                size_t len;

                if (trace) {
                        SLog("Got r(", r, ") out of sum(", sum, ") try_again(", try_again, ")\n");
                }

        next_payload:
                while (r >= (len = (ptr = it->iovecs.data + index)->iov_len)) {
                        r -= len;

                        if (++index == it->iovecs.size) {
                                const auto next       = it->next;
                                auto       broker_req = it->broker_req;

                                TANK_EXPECT(broker_req->have_payload);
                                broker_req->have_payload = false;

                                // we need to track the dispatched requests
                                // so that flush_broker() can deal with those requests as opposed
                                // to only relying on them to time out
                                br->pending_responses_list.push_back(&broker_req->pending_responses_list_ll);

                                TANK_EXPECT(false == br->pending_responses_list.empty());

                                if (trace) {
                                        SLog(ansifmt::color_brown, ansifmt::bgcolor_blue, 
						"Payload dispatched for ID:", broker_req->id, ")", ansifmt::reset, "\n");
                                }

                                br->outgoing_content.pop_front();

                                // we used to retain payloads in case we wanted to retry
                                // a request. We no longer do that for simplicity reasons.
                                // it's cheap to rebuild a payload anyway
                                put_payload(it, __LINE__);

                                br->outgoing_content.first_payload_partially_transferred = false;

                                if (next) {
                                        it    = next;
                                        index = 0;
                                        goto next_payload;
                                } else {
                                        // done
                                        if (trace) {
                                                SLog("Done for fd ", fd, " for connection ", ptr_repr(c), "\n");
                                        }

                                        br->outgoing_content.front_payload_iovecs_index = 0;
                                        stop_poll_outavail(c);
                                        return true;
                                }
                        }
                }

                TANK_EXPECT(it->broker_req->have_payload);

                br->outgoing_content.first_payload_partially_transferred = true;

                ptr->iov_len -= r;
                ptr->iov_base = static_cast<char *>(ptr->iov_base) + r;

                if (try_again) {
                        TANK_EXPECT(false == br->outgoing_content.empty());
                        continue;
                } else {
                        br->outgoing_content.front_payload_iovecs_index = index;
#ifdef HAVE_NETIO_THROTTLE
                        throttle_write(c, now_ms + throttle_span());
#else
                        poll_outavail(c);
#endif
                        return true;
                }
        }
}

bool TankClient::any_requests_pending_delivery() const noexcept {
	// XXX: this is not optimal, but given how and when it's meant to be used, that's OK
        for (auto &it : brokers) {
                auto br = it.second.get();

                if (!br->outgoing_content.empty()) {
                        return true;
                }
        }

#if 0 // turns out, this is not necessary
        for (auto it = all_conns_list.next; it != &all_conns_list; it = it->next) {
                const auto c = containerof(connection, all_conns_list_ll, it);

                if (c->state.flags & (1u << unsigned(connection::State::Flags::ConnectionAttempt))) {
                        return true;
                }
        }
#endif

        return false;
}

bool TankClient::process_srv_in(connection *const c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        auto b = c->in.b;
        TANK_EXPECT(b);
        TANK_EXPECT(b->use_count());

#ifdef TANK_CLIENT_FAST_CONSUME
        if (c->as.tank.flags & (1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly))) {
                if (!process_consume_content(c)) {
                        return false;
                } else if (c->as.tank.cur_resp.state == connection::As::Tank::Response::State::Ready) {
                        c->as.tank.flags &= ~(1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly));
                } else {
                        return true;
                }
	}
#endif

        auto  base   = reinterpret_cast<const uint8_t *>(b->data());
        const auto  b_size = b->size();
        const auto *p      = base + b->offset();
        const auto *e      = base + b_size;

        for (;;) {
                TANK_EXPECT(p <= e);

                if (unlikely(p + sizeof(uint8_t) + sizeof(uint32_t) > e)) {
			break;
                }

                const auto msg = decode_pod<uint8_t>(p);
                const auto len = decode_pod<uint32_t>(p);

                if (trace) {
                        SLog(ansifmt::color_brown, ansifmt::inverse, "msg = ", msg, ", len = ", len, ansifmt::reset, "\n");
                }

                if (0 == (c->as.tank.flags & (1u << static_cast<uint8_t>(connection::As::Tank::Flags::ConsideredReqHeader)))) {
                        // haven't considered the request header yet
                        TANK_EXPECT(p <= e);
                        const auto offset = std::distance(base, p);

                        if (trace) {
                                SLog("Not Considered Req Yet\n");
                        }

                        if (!b->is_locked()) {
                                // well, we can't do jack shit here
                                b->reserve(len + offset + 16);

                                base = reinterpret_cast<const uint8_t *>(b->data());
                                p    = base + offset;
                                e    = base + b_size;

                                TANK_EXPECT(p <= e);
                        }

                        c->as.tank.flags |= (1u << static_cast<uint8_t>(connection::As::Tank::Flags::ConsideredReqHeader));
                }

#ifdef TANK_CLIENT_FAST_CONSUME
                // we 'll try to
                if (msg == uint8_t(TankAPIMsgType::Consume)) {
                        static constexpr bool trace{false};
                        const auto            cur_offset = std::distance(const_cast<const char *>(b->data()), reinterpret_cast<const char *>(p));

                        b->set_offset(cur_offset);
                        c->as.tank.cur_resp.reset();
                        c->as.tank.cur_resp.resp_end_offset = cur_offset + len;
                        c->as.tank.flags |= 1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly);

                        if (trace) {
                                SLog("************ CONSUME *******************:", c->as.tank.cur_resp.resp_end_offset, " (cur_offset = ", cur_offset, ", b->size() = ", b->size(), ")\n");
                        }

                        if (!process_consume_content(c)) {
                                return false;
                        } else if (c->as.tank.cur_resp.state == connection::As::Tank::Response::State::Ready) {
                                // we are done, see if we have more responses we can process
                                p = base + b->offset();
                                e = base + b->size();

                                c->as.tank.flags &= ~(1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly));

                                if (trace) {
                                        SLog("We are done (READY) remaining ", std::distance(p, e), "\n");
                                }
                                continue;
                        } else {
                                if (trace) {
                                        SLog("Need more content\n");
                                }

                                return true;
                        }
                }
#endif

                TANK_EXPECT(p <= e);
                if (p + len > e) {
                        if (trace) {
                                SLog("Need more content, msg.size = ", len, ", have now ", std::distance(p, e), "\n");

                                if (c->type == connection::Type::Tank && c->as.tank.br) {
                                        SLog("Broker ", ptr_repr(c->as.tank.br), " ", c->as.tank.br->ep, " pending_responses ", c->as.tank.br->pending_responses_list.size(), "\n");
                                }
                        }

                        return true;
                }

                c->as.tank.flags &= ~(1u << static_cast<uint8_t>(connection::As::Tank::Flags::ConsideredReqHeader));

                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_red, "PROCESSING ", len, ansifmt::reset, "\n");
                }

                if (!process_msg(c, msg, p, len)) {
                        return false;
                }

                p += len;
                TANK_EXPECT(p <= e);

                b->set_offset(reinterpret_cast<const char *>(p));
        }

        if (auto b = c->in.b) {
                const auto offset = b->offset();
                const auto size   = b->size();

                if (offset == size) {
                        if (trace) {
                                SLog("Can release b (", ptr_repr(p), ", rc = ", b->use_count(), ")\n");
                        }

                        release_mb(b);
                        c->in.b = nullptr;
                } else if (!b->is_locked()) {
                        if (trace) {
                                SLog("Not Locked, will delete prefix of length ", offset, "\n");
                        }

                        b->erase_chunk(0, offset);
                        b->set_offset(static_cast<uint32_t>(0));
                } else if (trace) {
                        SLog("Locked\n");
                }
        }

        return true;
}

bool TankClient::rcv(connection *const c) {
        enum {
                trace = false,
        };
        TANK_EXPECT(c);
        int          fd = c->fd, n;
        managed_buf *b;

        TANK_EXPECT(fd != -1);

        if (unlikely(-1 == ioctl(fd, FIONREAD, &n))) {
		Print("ioctl() failed:", strerror(errno), " for ", fd, " c ", ptr_repr(c), "\n");
                std::abort();
        }

        if ((b = c->in.b)) {
                if (const auto available = b->capacity() - b->size(); available < n && b->is_locked()) {
                        // we can't modfy this because other users depend on it
                        // so geta new managed buffer and copy whatever extra we had
                        // in the previous buffer to it, and then
                        // assign that new buffer to this connection
                        auto nb = get_managed_buffer();

			TANK_EXPECT(nb);
                        if (const auto rem = b->size() - b->offset()) {
				TANK_EXPECT(b->data());

                                nb->serialize(reinterpret_cast<int8_t *>(b->data() + b->offset()), rem);
                                b->length -= rem;
                        }

                        release_mb(b);

                        b = c->in.b = nb;
                        TANK_EXPECT(b->is_locked() == false);
                        TANK_EXPECT(b->use_count() == 1);
                }
        } else {
                b = c->in.b = get_managed_buffer();
        }

#ifdef HAVE_NETIO_THROTTLE
        n = std::min<int>(n, throttle_read_size());
#endif
        // this is really mostly about HAVE_NETIO_THROTTLE
        // but regardless, we don't want to pass 0 to read()
        const auto actual = n + 1;

        b->reserve(b->size() + actual);

        if (const auto r = read(fd, b->data() + b->size(), actual); - 1 == r) {
                if (EINTR == errno or EAGAIN == errno) {
                        return true;
                } else {
                        return shutdown(c, __LINE__);
                }
        } else if (0 == r) {
                // peer went away
                return shutdown(c, __LINE__);
        } else {
                if (trace) {
                        SLog("Read:", r, " from ", fd, "\n");
                }

                b->length += r;
        }

#ifdef HAVE_NETIO_THROTTLE
        throttle_read(c, throttle_span() + now_ms);
#endif

        switch (c->type) {
                case connection::Type::Tank:
                        return process_srv_in(c);

                default:
                        IMPLEMENT_ME();
        }
}

void TankClient::begin_reactor_loop_iteration() {
        // Reset state / buffers used for tracking collected content and responses in last poll() call
        [[maybe_unused]] static constexpr bool trace{false};

        gc_ready_responses();

        all_captured_faults.clear();
        all_discovered_partitions.clear();
        _discovered_topologies.clear();
        all_discovered_topics.clear();
        reload_conf_results_v.clear();
        consumed_content.clear();
        produce_acks_v.clear();
        created_topics_v.clear();
        collected_cluster_status_v.clear();
}

void TankClient::drain_pipe(int fd) {
        int  n;
        char buf[512];

        if (unlikely(ioctl(fd, FIONREAD, &n) == -1)) {
                throw Switch::system_error("ioctl() failed:", strerror(errno));
        } else if (unlikely(!n)) {
                // Just in case we get 0; if we do, read() will fail with EAGAIN anyway
                // We shouldn't get n == 0 but let's be on the safe side
                n = sizeof(buf);
        }

        do {
                const auto r = read(fd, buf, sizeof(buf));

                if (r == -1) {
                        if (errno != EAGAIN && errno != EINTR) {
                                throw Switch::system_error("Unable to drain pipe:", strerror(errno));
                        } else {
                                break;
                        }
                } else if (!r) {
                        break;
                } else {
                        n -= r;
                }

        } while (n);
}

bool TankClient::process_io(const size_t events_count) {
        static constexpr bool trace{false};
        bool                  interrupted = false;

        if (trace) {
                SLog("Events ", events_count, "\n");
        }

        for (const auto it : poller.new_events(events_count)) {
                const auto  events = it->events;
                auto *const c      = static_cast<connection *>(it->data.ptr);

                if (c->fd == interrupt_fd) {
                        // someone woke us up
                        uint64_t one;

                        read(c->fd, &one, sizeof(one));
                        interrupted = true;
                        continue;
                }

                // POLLHUP: no longer connected; in TCP, FIN has been received
                // POLLERR: socket got an async. error. In TCP, typically means an RST has been received or sent.
                if (events & (EPOLLERR | EPOLLHUP)) {
                        shutdown(c, __LINE__);
                        continue;
                }

                if (events & EPOLLOUT) {
                        if (c->state.flags & (1u << unsigned(connection::State::Flags::ConnectionAttempt))) {
                                if (trace) {
                                        SLog("Connection was established\n");
                                }

                                if (c->type == connection::Type::Tank) {
                                        // whenever we manage to connect, we
                                        // decrement the tracked consequtive connection failures by 2(as opposed to resetting it to 0)
                                        auto br = c->as.tank.br;

                                        TANK_EXPECT(br);
                                        br->set_reachability(broker::Reachability::LikelyReachable, __LINE__);
                                        br->consequtive_connection_failures -= std::min<size_t>(2, br->consequtive_connection_failures);

                                        if (br->unreachable_brokers_tree_node.node.leaf_p) {
                                                eb64_delete(&br->unreachable_brokers_tree_node);
                                                unreachable_brokers_tree_next = eb_is_empty(&unreachable_brokers_tree)
                                                                                    ? std::numeric_limits<uint64_t>::max()
                                                                                    : eb64_first(&unreachable_brokers_tree)->key;

                                                TANK_EXPECT(br->unreachable_brokers_tree_node.node.leaf_p == nullptr);
                                        }
                                }

                                c->list.detach_and_reset();
                                conns_pend_est_next_expiration = conns_pend_est_list.empty()
                                                                     ? std::numeric_limits<uint64_t>::max()
                                                                     : switch_list_entry(connection, list, conns_pend_est_list.prev)->expiration;
                                c->state.flags &= ~((1u << uint8_t(connection::State::Flags::ConnectionAttempt)) | (1u << uint8_t(connection::State::Flags::NeedOutAvail)));
                                poller.set_data_events(c->fd, c, EPOLLIN);
                        }

                        if (!tx(c)) {
                                continue;
                        }
                }

                // it's important that we check for EPOLLOUT before we check for EPOLLIN
                // see Service::process_io()
                if (events & EPOLLIN) {
                        rcv(c);
                }
        }

        return interrupted;
}

void TankClient::check_conns_pending_est() {
        static constexpr bool trace{false};

        while (!conns_pend_est_list.empty()) {
                auto       c   = switch_list_entry(connection, list, conns_pend_est_list.prev);
                const auto exp = c->expiration;

                if (exp > now_ms) {
                        conns_pend_est_next_expiration = exp;
                        break;
                }

                if (trace) {
                        SLog("Too long to establish connection\n");
                }

                shutdown(c, __LINE__);
        }

        conns_pend_est_next_expiration = std::numeric_limits<uint64_t>::max();
}

void TankClient::wakeup_unreachable_broker(broker *br) {
        static constexpr bool trace{false};
        TANK_EXPECT(br);
        TANK_EXPECT(br->unreachable_brokers_tree_node.node.leaf_p == nullptr);

        br->consequtive_connection_failures =
            std::min<uint8_t>(br->consequtive_connection_failures,
                              broker::max_consequtive_connection_failures - 1);

        if (trace) {
                SLog("Waking up unreachable broker ", br->ep, "\n");
        }

        if (br->outgoing_content.empty()) {
                // this could happen e.g if
                // all requests were timed out by the time we got to wake this up
                if (trace) {
                        SLog("No outgoing content\n");
                }

                return;
        }

        if (!try_transmit(br)) {
                // XXX: it's OK
        }
}

void TankClient::check_unreachable_brokers() {
        eb64_node *it;

        goto l1;
        for (;;) {
                if (!it) {
                l1:
                        it = eb64_first(&unreachable_brokers_tree);
                        if (!it) {
                                unreachable_brokers_tree_next = std::numeric_limits<uint64_t>::max();
                                return;
                        }
                }

                if (const auto key = it->key; key > now_ms) {
                        unreachable_brokers_tree_next = key;
                        return;
                }

                auto br   = eb64_entry(it, broker, unreachable_brokers_tree_node);
                auto next = eb64_next(it);

                eb64_delete(it);

                wakeup_unreachable_broker(br);

                it = next;
        }
}

uint64_t TankClient::reactor_next_wakeup() const noexcept {
	enum {
		trace = false,
	};

        uint64_t until = TANKUtil::minimum(
            unreachable_brokers_tree_next,
            retry_bundles_next,
            conns_pend_est_next_expiration,
            api_reqs_expirations_tree_next);

#ifdef HAVE_NETIO_THROTTLE
        until = TANKUtil::minimum(until,
                                  throttled_connections_read_list_next,
                                  throttled_connections_write_list_next);
#endif

	

        return until;
}

void TankClient::reactor_step(uint32_t timeout_ms) {
        now_ms = Timings::Milliseconds::Tick();

        begin_reactor_loop_iteration();

        const auto step_end = now_ms + timeout_ms;

        // instead of polling once, we are now going
        // to poll (and thus, consume incoming packets)
        // until we either timeout or we have any_responses()
        //
        // this makes a lot of sense, and also avoid excessive calls to
        // TankClient::poll()
        for (;;) {
                const auto until = std::min(step_end, reactor_next_wakeup());

                sleeping.store(true, std::memory_order_relaxed);

                const auto r          = poller.poll(likely(until >= now_ms) ? until - now_ms : 0);
                const auto saved_erro  = errno;
                bool       interrupted = false;

                sleeping.store(false, std::memory_order_relaxed);

                now_ms = Timings::Milliseconds::Tick();
                if (now_ms > next_curtime_update) {
                        const auto _now = time(nullptr);

                        if (unlikely(_now == ((time_t)-1))) {
                                IMPLEMENT_ME();
                        }

                        cur_time            = _now;
                        next_curtime_update = now_ms + 500;
                }

                if (unlikely(-1 == r)) {
                        if (saved_erro == EINTR) {
                                interrupted = true;
                        } else if (saved_erro != EAGAIN) {
                                throw Switch::system_error("epoll_wait()");
                        }
                } else if (r) {
                        interrupted = process_io(r);
                }

                if (now_ms >= conns_pend_est_next_expiration) {
                        check_conns_pending_est();
                }

                if (now_ms >= api_reqs_expirations_tree_next) {
                        check_pending_api_responses();
                }

                if (now_ms >= retry_bundles_next) {
                        check_pending_retries();
                }

                if (now_ms >= unreachable_brokers_tree_next) {
                        check_unreachable_brokers();
                }

#ifdef HAVE_NETIO_THROTTLE
                if (now_ms >= throttled_connections_read_list_next ||
                    now_ms >= throttled_connections_write_list_next) {
                        manage_throttled_connections();
                }
#endif

		if (interrupted) {
			break;
		}

                if (!r) {
                        if (now_ms >= step_end) {
                                // OK, waited too long
                                break;
                        } else {
                                // no I/O, but likely a timer fired
                                // we haven't exceeded our allowed time to poll for I/O though so we 'll poll again
                        }
                } else if (any_responses()) {
                        // we got something
                        break;
                }
        }

        end_reactor_loop_iteration();
}

void TankClient::check_pending_retries() {
        eb64_node *it;

        goto l1;
        for (;;) {
                if (!it) {
                l1:
                        it = eb64_first(&retry_bundles_ebt_root);

                        if (!it) {
                                retry_bundles_next = std::numeric_limits<uint64_t>::max();
                                return;
                        }
                }

                if (const auto key = it->key; key > now_ms) {
                        retry_bundles_next = key;
                        return;
                }

                auto rb   = eb64_entry(it, retry_bundle, node);
                auto next = eb64_next(it);

                eb64_delete(it);

                retry_bundle_impl(rb);

                it = next;
        }
}

void TankClient::end_reactor_loop_iteration() {
        // for now, no-op
}

void TankClient::check_pending_api_responses() {
        eb64_node *it;

        goto l1;
        for (;;) {
                if (!it) {
                l1:
                        it = eb64_first(&api_reqs_expirations_tree);
                        if (!it) {
                                api_reqs_expirations_tree_next = std::numeric_limits<uint64_t>::max();
                                return;
                        }
                }

                if (const auto key = it->key; key > now_ms) {
                        api_reqs_expirations_tree_next = key;
                        return;
                }

                auto api_req = eb64_entry(it, api_request, api_reqs_expirations_tree_node);
                auto next    = eb64_next(it);

                // it's important that we eb64_delete() before we abort_broker_req()
                // otherwise abort_broker_req() may eb64_delete() as well. See service comments
                eb64_delete(it);

                abort_api_request(api_req);

                it = next;
        }
}

bool TankClient::should_poll() const noexcept {
        return not (pending_responses.empty() and conns_pend_est_list.empty());
}

void TankClient::make_unreachable(broker *br) {
        static constexpr bool trace{false};
        TANK_EXPECT(br);

        br->set_reachability(broker::Reachability::LikelyUnreachable, __LINE__);

        if (br->unreachable_brokers_tree_node.node.leaf_p) {
                if (trace) {
                        SLog("Detaching from unreachable_brokers_tree\n");
                }

                eb64_delete(&br->unreachable_brokers_tree_node);
        }

        const auto n      = std::pow(2, br->consequtive_connection_failures);
        const auto delay1 = n * 128;
        const auto delay2 = n * 16;
        const auto delay  = delay1 + delay2;

        br->unreachable_brokers_tree_node.key = now_ms + delay;
        eb64_insert(&unreachable_brokers_tree, &br->unreachable_brokers_tree_node);
        unreachable_brokers_tree_next = std::min<uint64_t>(unreachable_brokers_tree_next, br->unreachable_brokers_tree_node.key);

        if (trace) {
                SLog("Will block for a while, consequtive_connection_failures = ",
                     br->consequtive_connection_failures,
                     ", delay = ", delay, "\n");
        }
}

void TankClient::abort_broker_connection(connection *c, broker *br) {
        enum {
                trace = false,
        };
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        TANK_EXPECT(br);
        TANK_EXPECT(c->as.tank.br == br);
        bool consider_fault;

        if (trace) {
                SLog("abort_broker_connection(), ",
                     ptr_repr(br), " ", br->ep,
                     " br->pending_responses_list.size = ", br->pending_responses_list.size(), "\n");
        }

        if (c->state.flags & (1u << unsigned(connection::State::Flags::ConnectionAttempt))) {
                if (trace) {
                        SLog(ansifmt::inverse, "Connection to ", br->ep,
                             " shut down while attempting to connect", ansifmt::reset,
                             ", consequtive_connection_failures = ", br->consequtive_connection_failures, "\n");
                }

                consider_fault = true;
        } else if (not br->outgoing_content.empty() and br->outgoing_content.first_payload_partially_transferred) {
                if (trace) {
                        SLog("Failed to transfer payload\n");
                }

                consider_fault = true;
        } else if (c->in.b) {
                if (trace) {
                        SLog("Failed: in.b != nullptr\n");
                }

                consider_fault = true;
        } else {
                consider_fault = false;
        }

        if (consider_fault) {
                shift_failed_broker(br);

                if (++(br->consequtive_connection_failures) >= broker::max_consequtive_connection_failures) {
                        if (trace) {
                                SLog("Now BLOCKED\n");
                        }

                        br->blocked_until = now_ms + 8 * 1000;
                        br->set_reachability(broker::Reachability::Blocked, __LINE__);
                } else {
                        make_unreachable(br);
                }
        } else if (trace) {
                SLog("Aborting connection -- not considered fault\n");
        }

        flush_broker(br);
}

bool TankClient::shutdown(connection *const c, const uint32_t ref, const bool fault) {
	enum {
		trace = false,
	};
        TANK_EXPECT(c);
        TANK_EXPECT(c->fd != -1);

        if (trace) {
                SLog("Shutting down connection ", ptr_repr(c), ", ref = ", ref, ", type = ", unsigned(c->type), "\n");
        }

        poller.erase(c->fd);
        close(c->fd);
        c->fd = -1;

#ifdef HAVE_NETIO_THROTTLE
        c->throttler.read.ll.try_detach_and_reset();
        c->throttler.write.ll.try_detach_and_reset();
#endif

        c->all_conns_list_ll.try_detach_and_reset();

#ifdef TANK_CLIENT_FAST_CONSUME
        if (c->type == connection::Type::Tank && (c->as.tank.flags & (1u << unsigned(connection::As::Tank::Flags::InterleavedRespAssembly)))) {
                clear_tank_resp(c);
        }
#endif

        // important
        c->gen = 0;

        if (auto b = std::exchange(c->in.b, nullptr)) {
                release_mb(b);
        }

        if (fault) {
                if (c->state.flags & (1u << uint8_t(connection::State::Flags::ConnectionAttempt))) {
                        TANK_EXPECT(!c->list.empty());
                        c->list.detach_and_reset();

                        if (trace) {
                                SLog("Was attempting to connect, removing connection from conns_pend_est_list\n");
                        }

                        conns_pend_est_next_expiration = conns_pend_est_list.empty()
                                                             ? std::numeric_limits<uint64_t>::max()
                                                             : switch_list_entry(connection, list, conns_pend_est_list.prev)->expiration;
                }

                if (c->type == connection::Type::Tank) {
                        abort_broker_connection(c, c->as.tank.br);
                }
        }

        put_connection(c);
        return false;
}

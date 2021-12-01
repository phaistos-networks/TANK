#include "service_common.h"
#include <sched.h>

static constexpr bool trace_idle{false};
static constexpr bool trace_timers{false};
static constexpr bool trace_classification{false};

// From SENDFILE(2): The original Linux sendfile() system call was not designed to handle large file offsets.
// Consequently, Linux 2.4 added sendfile64(), with a wider type for the offset argument.
// The glibc sendfile() wrapper function transparently deals with the kernel differences.
#define HAVE_SENDFILE64 1

void Service::wakeup_reactor() {
        uint64_t one{1};

        ::write(_interrupt_efd, &one, sizeof(one));
}

void Service::maybe_wakeup_reactor() {
        std::atomic_signal_fence(std::memory_order_seq_cst);
        if (sleeping.load(std::memory_order_relaxed)) {
                sleeping.store(false, std::memory_order_relaxed);
                wakeup_reactor();
        }
}

// it is important that we defer work by marking a resource as dirty and placing it in a queue if it was just marked as dirty
// as opposed to imediately processing the resource op. so that we can avoid doing this more than once in the same reactor loop iteration
// because it doesn't make much sense to do so, and because batching may provide us with other benefits
void Service::begin_reactor_loop_iteration() {
        reusable_conns.insert(reusable_conns.end(), pending_reusable_conns.begin(), pending_reusable_conns.end());
        pending_reusable_conns.clear();

        gc_waitctx_deferred();

        if (next_cluster_state_apply == std::numeric_limits<uint64_t>::max()) {
                // only if not postponed
                apply_deferred_updates();
        }

        while (auto req = consul_state.deferred_reqs_head) {
                auto n = req->next;

                req->flags &= ~unsigned(consul_request::Flags::Deferred);
                schedule_consul_req_impl(req, req->flags & unsigned(consul_request::Flags::Urgent));
                consul_state.deferred_reqs_head = n;
        }

        // it is important that we drain the queue
        // and that we process any pending signals here before
        // we block in epoll_pwait()
        // see progADMANatic/service_reactor::begin_loop_iteration() for rational
        drain_pubsub_queue();

        if (const auto mask = pending_signals.load(std::memory_order_relaxed)) {
                pending_signals.fetch_and(~mask, std::memory_order_relaxed);

                if (!process_pending_signals(mask)) {
                        initiate_tear_down();
                }
        }
}

void Service::initiate_tear_down() {
        static constexpr bool trace{false};

        if (reactor_state == ReactorState::Active) {
                if constexpr (trace) {
                        SLog("Switching to TearDown\n");
                }

                reactor_state = ReactorState::TearDown;
        } else {
                if constexpr (trace) {
                        SLog("Already tearing down reactor\n");
                }

                return;
        }

        disable_listener();

        // we don't want to wait too long for that
        set_reactor_state_idle_timer.node.key = now_ms + 2 * 1000;
        register_timer(&set_reactor_state_idle_timer.node);

        if (cluster_aware()) {
                if (consul_state._session_id_len) {
                        // we need to release it first, _then_ we will switch to ReactorState::WaitAllConnsIdle
                        if constexpr (trace) {
                                SLog("Will need to release session\n");
                        }

                        reactor_state = ReactorState::ReleaseSess;
                        if (cluster_state.leader_self()) {
                                // XXX:
                                // this appears to be a consul bug. If we destroy a session, then other nodes
                                // long-polling for /TANK/clusters/<cluster/leaders/ will be provided with a response, where e.g
                                // key CLUSTER will not be associated with a "Session"(which is what we expect to happen)
                                // If howevwe we try_become_cluster_leader() the consul HTTP request fail with 200 "false".
                                //
                                // If we keep trying(see try_become_cluster_leader_timer), it will eventually succeed after about 4 seconds
                                //
                                // If we release the session lock for that key, before we destroy the session, then one
                                // of the other nodes will succeed in try_become_cluster_leader()
                                //
                                // I don't know why this is so, but it is definitely not ideal.
                                // This is also affecting node locks, where if we don't release the lock of an acquired cluster node on shutdown,
                                // if we immediately restart the service where it attempts to re-acquire its node ID, it fails
                                //
                                // As of 1.4.0 it's not resolved.
                                // Tracking issue here: https://github.com/hashicorp/consul/issues/5205
                                //
                                // UPDATE: apparently, this is the expected behavior (I still maintain this is a bug)
                                // so we need to work around it, which means we need to release the node lock as well in addition to the cluster lock
                                auto first = consul_state.get_req(consul_request::Type::ReleaseClusterLeadership);
                                auto req   = first;

                                if constexpr (trace) {
                                        SLog("Self is the cluster leader, will ReleaseClusterLeadership before DestroySession\n");
                                }

                                req = (req->then = consul_state.get_req(consul_request::Type::ReleaseNodeLock));
                                req = (req->then = consul_state.get_req(consul_request::Type::DestroySession));
                                schedule_consul_req(first, true);
                        } else {
                                // Destroying the session will also release all locks or leases assoc. with it
                                if constexpr (trace) {
                                        SLog("Self is not cluster leader, will DestroySession\n");
                                }
                                auto req = consul_state.get_req(consul_request::Type::ReleaseNodeLock);

                                req->then = consul_state.get_req(consul_request::Type::DestroySession);
                                schedule_consul_req(req, true);
                        }
                        return;
                }
        }

        if (all_conns_idle()) {
                if constexpr (trace) {
                        SLog("All connections idle already\n");
                }

                reactor_state = ReactorState::Idle;
        } else {
                if constexpr (trace) {
                        SLog("Waill wait for all connections to become idle\n");
                }

                reactor_state = ReactorState::WaitAllConnsIdle;
        }
}

void Service::consider_deferred_produce_responses() {
        static constexpr bool trace{false};

        while (!deferred_produce_responses_expiration_list.empty()) {
                auto dpr = switch_list_entry(produce_response, deferred.expiration.ll, deferred_produce_responses_expiration_list.prev);

                // how can that possibly be?
                TANK_EXPECT(false == dpr->deferred.expiration.ll.empty());

                if (const auto when = dpr->deferred.expiration.ts; when > now_ms) {
                        deferred_produce_responses_next_expiration = when;
                        return;
                }

                if (trace) {
                        SLog("Timing out DPR\n");
                }

                // this is really a trampoline to try_generate_produce_response()
                complete_deferred_produce_response(dpr);
        }
}

int Service::reactor_main() {
        unsigned iterations{0};

#define _TRACE_EXPENSIVE 1
        now_ms = Timings::Milliseconds::Tick();

#ifdef _TRACE_EXPENSIVE
        uint64_t _start;
#endif

        reactor_state = ReactorState::Active;
        curTime       = time(nullptr);
        for (uint64_t next_curtime_update{0}; reactor_state != ReactorState::Idle;) {
                if (now_ms > next_cluster_state_apply) {
                        next_cluster_state_apply = std::numeric_limits<uint64_t>::max();
                }

#ifdef _TRACE_EXPENSIVE
                _start = Timings::Microseconds::Tick();
#endif

                begin_reactor_loop_iteration();

#ifdef _TRACE_EXPENSIVE
                if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                        SLog("Took ", duration_repr(delta), " for op\n");
                }
#endif

                // determine how long to wait, account for the next timer/event timestamp
                const auto wait_until = TANKUtil::minimum(
                    scheduled_consume_retries_list_next,
                    deferred_produce_responses_next_expiration,
                    isr_pending_ack_list_next,
                    consul_state.active_conns_next_process_ts,
                    next_idle_check_ts,
                    timers_ebtree_next,
                    consul_state.active_conns_next_process_ts,
                    next_cluster_state_apply,
                    next_active_partitions_check,
                    now_ms + 30 * 1000);

                sleeping.store(true, std::memory_order_relaxed);

                const auto max_conngen_process = next_connection_generation;
                const auto r                   = poller.poll(wait_until - now_ms);
                const auto saved_errno         = errno; // in case it's updated before we use it
                sleeping.store(false, std::memory_order_relaxed);

                // CPU relax -- this is not a spin-lock wait loop
                // but we may as well be kind to the CPU
                asm volatile("pause\n"
                             :
                             :
                             : "memory");

                now_ms = Timings::Milliseconds::Tick();
                if (++iterations == 10) {
                        // be kind
                        sched_yield();
                        iterations = 0;
                }

                if (now_ms > next_curtime_update) {
                        // we can avoid excessive time() calls by
                        // checking updating every 500ms. The chance that we 'll get the time wrong (by 1 second) is an acceptable tradeoff
                        const auto _now = time(nullptr);

                        if (unlikely(_now == ((time_t)-1))) {
                                Print("time() failed:", strerror(errno), "\n");
                                std::abort();
                        }

                        curTime             = _now;
                        next_curtime_update = now_ms + 500;
                }

                if (unlikely(-1 == r)) {
                        if (saved_errno != EINTR && saved_errno != EAGAIN) {
                                IMPLEMENT_ME();
                        }
                } else if (r) {
#ifdef _TRACE_EXPENSIVE
                        _start = Timings::Microseconds::Tick();
#endif
                        if (const auto res = process_io(r, max_conngen_process)) {
                                return res;
                        }

#ifdef _TRACE_EXPENSIVE
                        if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                                SLog("Took ", duration_repr(delta), " for op\n");
                        }
#endif
                }

                if (now_ms >= timers_ebtree_next) {
#ifdef _TRACE_EXPENSIVE
                        _start = Timings::Microseconds::Tick();
#endif

                        process_timers();

#ifdef _TRACE_EXPENSIVE
                        if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                                SLog("Took ", duration_repr(delta), " for op\n");
                        }
#endif
                }

                if (now_ms >= next_idle_check_ts) {
                        consider_idle_conns();
                }

                if (now_ms >= next_active_partitions_check) {
                        consider_active_partitions();
                }

                if (now_ms >= consul_state.active_conns_next_process_ts) {
#ifdef _TRACE_EXPENSIVE
                        _start = Timings::Microseconds::Tick();
#endif

                        consider_long_running_active_consul_requests();

#ifdef _TRACE_EXPENSIVE
                        if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                                SLog("Took ", duration_repr(delta), " for op\n");
                        }
#endif
                }

                if (now_ms >= isr_pending_ack_list_next) {
#ifdef _TRACE_EXPENSIVE
                        _start = Timings::Microseconds::Tick();
#endif

                        consider_isr_pending_ack();

#ifdef _TRACE_EXPENSIVE
                        if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                                SLog("Took ", duration_repr(delta), " for op\n");
                        }
#endif
                }

                if (now_ms >= deferred_produce_responses_next_expiration) {
#ifdef _TRACE_EXPENSIVE
                        _start = Timings::Microseconds::Tick();
#endif

                        consider_deferred_produce_responses();

#ifdef _TRACE_EXPENSIVE
                        if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                                SLog("Took ", duration_repr(delta), " for op\n");
                        }
#endif
                }

                if (now_ms >= scheduled_consume_retries_list_next) {
#ifdef _TRACE_EXPENSIVE
                        _start = Timings::Microseconds::Tick();
#endif

                        consider_scheduled_consume_retries();

#ifdef _TRACE_EXPENSIVE
                        if (const auto delta = Timings::Microseconds::Since(_start); delta > Timings::Seconds::ToMicros(1)) {
                                SLog("Took ", duration_repr(delta), " for op\n");
                        }
#endif
                }
        }

        tear_down();

#ifdef _TRACE_EXPENSIVE
#undef _TRACE_EXPENSIVE
#endif

        Print("TANK terminated\n");
        return 0;
}

void Service::put_connection(connection *const c) {
        TANK_EXPECT(c->connectionsList.empty());
        TANK_EXPECT(c->classification.ll.empty());
        TANK_EXPECT(-1 == c->fd);

        if (c->inB) {
                put_buf(c->inB);
                c->inB = nullptr;
        }

        pending_reusable_conns.emplace_back(c);
}

connection *Service::get_connection() {
        connection *c;

        if (!reusable_conns.empty()) {
                c = reusable_conns.back();
                reusable_conns.pop_back();
        } else {
                c           = static_cast<connection *>(connections_allocators.Alloc(sizeof(connection)));
                c->sentinel = reinterpret_cast<uintptr_t>(c);
        }

        c->gen         = ++next_connection_generation; // it is important to pre-incement, NOT post-increment
        c->fd          = -1;
        c->state.flags = 0;
        c->type        = connection::Type::UNKNOWN;
        c->state.set_classsification(connection::ClassificationTracker::Type::NotClassified);
        c->outQ = nullptr;
        c->inB  = nullptr;

        c->classification.ll.reset();
        c->connectionsList.reset();

        allConnections.push_back(&c->connectionsList);
        return c;
}

bool Service::expected_connest(connection *c) {
        static constexpr bool trace{false};

        if (c->is_consul() && c->as.consul.state == connection::As::Consul::State::Connecting) {
                if (trace) {
                        SLog("Established new consul connnection\n");
                }

                c->state.flags &= ~(1u << unsigned(connection::State::Flags::NeedOutAvail)); // in case.
                consul_state.srv.state = ConsulState::Srv::State::Available;
                poller.set_data_events(c->fd, c, EPOLLIN);
                cancel_timer(&c->as.consul.attached_timer.node);

                consider_idle_consul_connection(c);
                return true;
        } else if (c->type == connection::Type::Consumer && c->as.consumer.state == connection::As::Consumer::State::Connecting) {
                if (trace) {
                        SLog("Established new connection with a peer\n");
                }

                c->state.flags &= ~(1u << unsigned(connection::State::Flags::NeedOutAvail));
                c->as.consumer.state = connection::As::Consumer::State::Idle;
                poller.set_data_events(c->fd, c, EPOLLIN);
                cancel_timer(&c->as.consumer.attached_timer.node);

                consider_connected_consumer(c);
                return true;
        } else {
                // Likely polling for socket send buffer availability
                if (c->state.flags & (1u << unsigned(connection::State::Flags::NeedOutAvail))) {
                        if (trace) {
                                SLog("Got EPOLLOUT, was waiting for socket send buffer availability\n");
                        }

                        c->state.flags ^= (1u << unsigned(connection::State::Flags::NeedOutAvail));
                } else {
                        if (trace) {
                                SLog("ODD: why are we here?\n");
                        }
                }

                poller.set_data_events(c->fd, c, EPOLLIN);
                return false;
        }
}

int Service::process_io(const int r, const uint64_t max_conn_gen) {
        for (const auto it : poller.new_events(r)) {
                auto ptr = it->data.ptr;
                int  fd  = *reinterpret_cast<int *>(ptr);

                if (ptr == &_interrupt_efd) {
                        uint64_t _c;

                        read(fd, &_c, sizeof(_c));
                        continue;
                }

                if (unlikely(-1 == fd)) {
                        continue;
                }

                if (ptr == &listenFd) {
                        if (const auto res = try_accept(fd)) {
                                return r;
                        }
                        continue;
                }

                if (ptr == &prom_listen_fd) {
                        if (const auto res = try_accept_prom(fd)) {
                                return r;
                        }
                        continue;
                }

                auto *const c = static_cast<connection *>(ptr);

                if (unlikely(c->gen > max_conn_gen)) {
                        // this connection was created after epoll_wait() was invoked
                        // so the fact that we are processing I/O events about it here means that
                        // the connection was shut down after epoll_wait() (and possibly that
                        // this struct connection has been reused for another connection).
                        // Whatever the case, we must ignore whatever epoll events here for that connection
                        // because it's not the same connection as the one that was registered with epoll
                        continue;
                }

                if (unlikely(c->fd <= 2)) {
                        SLog("Unexpected fd ", c->fd, " for ", ptr_repr(c), "\n");
                        std::abort();
                }

                const auto events = it->events;

                if (events & (EPOLLHUP | EPOLLERR)) {
                        shutdown(c);
                        continue;
                }

                c->verify();

                // we are checking for EPOLLOUT first as opposed to checking events for
                // EPOLLIN first, because this helps with connection::Type::Consumer connections
                // where we need to check if the connection is established, and we do that in
                // our impl. of expected_connest(), where we check if as.consumer.state == connection::As::Consumer::State::Connecting
                // The problem with that is that expected_connest() checks state == connection::As::Consumer::State::Connecting, however
                // once we connect to the peer it immediately pings us, so most of the time we get
                // a EPOLLIN(because that ping arrived together with the handshake packets) and EPOLLOUT
                // and when we check for EPOLLIN first, and wind up processing the peer response (ping)
                // we then set the state to State::Idle because we have drained the input buffer
                // so when, after processing EPOLLIN in the same loop iteration, we check for EPOLLOUT, the state
                // is not Conecting so we don't ever think we connected.
                if (events & EPOLLOUT) {
                        if (false == expected_connest(c)) {
                                if (!tx(c)) {
                                        continue;
                                }
                        }
                }

                if (events & EPOLLIN) {
                        TANK_EXPECT(c->fd > 2);

                        try_recv(c);
                }
        }

        return 0;
}

void Service::consider_connected_consumer(connection *c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Consumer);
        auto                               peer = c->as.consumer.node;
        std::unordered_set<cluster_node *> set{peer};

        TANK_EXPECT(peer);
        TANK_EXPECT(peer->consume_conn.ch.get() == c);

        if (trace) {
                SLog("Connection to peer ", peer->id, "@", peer->ep, " for replication has been established\n");
        }

        try_replicate_from(set);
}

void Service::consider_scheduled_consume_retries() {
        static constexpr bool trace{false};

        if (trace) {
                SLog("Scheduled:", scheduled_consume_retries_list.size(), "\n");
        }

        while (!scheduled_consume_retries_list.empty()) {
                auto n = switch_list_entry(cluster_node, consume_retry_ctx.ll, scheduled_consume_retries_list.prev);

                if (const auto when = n->consume_retry_ctx.when; now_ms < when) {
                        scheduled_consume_retries_list_next = when;
                        return;
                }

                n->consume_retry_ctx.ll.detach_and_reset();

                if (trace) {
                        SLog("Can now attempt to replicate from ", n->ep, ", remaining:", scheduled_consume_retries_list.size(), "\n");
                }

                try_replicate_from(n);
        }

        scheduled_consume_retries_list_next = std::numeric_limits<uint64_t>::max();
}

void Service::consider_idle_conns() {
        // in reverse so that we consider the oldest idle connections before we consider the most recent ones
        if (trace_idle) {
                SLog("Considering ", idle_connections_count, " idle connections\n");
        }

        while (!idle_connections.empty()) {
                auto c = switch_list_entry(connection, classification.ll, idle_connections.prev);

                TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Idle);

                if (trace_idle) {
                        SLog("Connection ", ptr_repr(c), " idle for ", now_ms - c->classification.since, "\n");
                }

                if (now_ms - c->classification.since < idle_connection_ttl) {
                        // this and all other idle connections haven't been idle long enough
                        break;
                }

                shutdown(c);
        }
}

bool Service::try_make_idle(connection *const c) {
        if (c->outQ || c->inB) {
                // pending data to transmit or data to process
                // TODO: what do we do here? maybe we need to shutdown()
                // otherwise, if we return false what will happen?
                return false;
        }

        switch (c->type) {
                case connection::Type::ConsulClient: {
                        switch (c->as.consul.state) {
                                case connection::As::Consul::State::Idle:
                                        return true;

                                default:
                                        return false;
                        }

                } break;

                default:
                        return true;
        }
}

void Service::classify_active(connection *const c) {
        TANK_EXPECT(c);
        c->verify();

        if (trace_classification) {
                SLog(ansifmt::bold, ansifmt::color_blue, "ACTIVE:", ptr_repr(c), ansifmt::reset, "\n");
        }

        if (c->is_consul()) {
                TANK_EXPECT(c->as.consul.conns_ll.empty());
                c->classification.since = now_ms & (-128);

                if (consul_state.active_conns.empty()) {
                        consul_state.active_conns_next_process_ts = c->classification.since + active_consul_connection_ttl;
                }

                consul_state.active_conns.push_back(&c->as.consul.conns_ll);
        }
}

void Service::declassify_active(connection *const c) {
        TANK_EXPECT(c);
        c->verify();
        TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Active);

        if (trace_classification) {
                SLog(ansifmt::bold, ansifmt::color_blue, "-ACTIVE:", ptr_repr(c), ansifmt::reset, "\n");
        }

        if (c->is_consul()) {
                TANK_EXPECT(!c->as.consul.conns_ll.empty());
                c->as.consul.conns_ll.detach_and_reset();

                if (consul_state.active_conns.empty()) {
                        consul_state.active_conns_next_process_ts = std::numeric_limits<uint64_t>::max();
                } else {
                        auto       c         = switch_list_entry(connection, as.consul.conns_ll, consul_state.active_conns.prev);
                        const auto idle_time = now_ms - c->classification.since;

                        consul_state.active_conns_next_process_ts = now_ms + (active_consul_connection_ttl - idle_time);
                }
        }
}

bool Service::all_conns_idle() const {
        if (false == consul_state.active_conns.empty()) {
                // TODO: need a simple counter for all active tank client connections
                return false;
        }

        return true;
}

void Service::declassify_long_polling(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        c->verify();
        TANK_EXPECT(false == c->classification.ll.empty());
        TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::LongPolling);

        if (trace_classification) {
                SLog(ansifmt::bold, ansifmt::color_blue, "-LONG_POLLING:", ptr_repr(c), ansifmt::reset, "\n");
        }

        c->classification.ll.detach_and_reset();
}

void Service::classify_long_polling(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(c->classification.ll.empty());
        c->verify();

        if (trace_classification) {
                SLog(ansifmt::bold, ansifmt::color_blue, "-+LONG_POLLING:", ptr_repr(c), ansifmt::reset, "\n");
        }

        long_polling_conns.push_back(&c->classification.ll);
}

void Service::classify_idle(connection *const c, const size_t ref) {
        TANK_EXPECT(c);
        c->verify();
        TANK_EXPECT(c->state.classification() != connection::ClassificationTracker::Type::Idle);

        if (trace_classification) {
                SLog(ansifmt::bold, ansifmt::color_blue, "IDLE:", ptr_repr(c), ansifmt::reset, " ", ref, "\n");
        }

        if (c->is_consul()) {
                TANK_EXPECT(c->as.consul.conns_ll.empty());
                consul_state.idle_conns.push_back(&c->as.consul.conns_ll);
        }

        // we will align down to 128ms
        // so that we be checking for idle connections less frequently, even if we potentially
        // need to shutdown a connection ~100ms earlier
        c->classification.since = now_ms & (-128);

        if (idle_connections.empty()) {
                // first idle connection
                next_idle_check_ts = c->classification.since + idle_connection_ttl;

                if (trace_idle) {
                        SLog("Set next_idle_check_ts to ", next_idle_check_ts, "\n");
                }
        }

        ++idle_connections_count;
        idle_connections.push_back(&c->classification.ll);

        while (idle_connections_count > 64) {
                auto c = switch_list_entry(connection, classification.ll, idle_connections.prev);

                shutdown(c, __LINE__);
        }

        if (reactor_state == ReactorState::WaitAllConnsIdle && all_conns_idle()) {
                reactor_state = ReactorState::Idle;
        }
}
void Service::declassify_idle(connection *const c) {
        TANK_EXPECT(c);
        c->verify();
        TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Idle);

        if (trace_classification) {
                SLog(ansifmt::bold, ansifmt::color_blue, "-IDLE:", ptr_repr(c), ansifmt::reset, "\n");
        }

        if (c->is_consul()) {
                // should have been tracked in consul_state.idle_conns
                TANK_EXPECT(false == c->as.consul.conns_ll.empty());
                c->as.consul.conns_ll.detach_and_reset();
        }

        TANK_EXPECT(false == c->classification.ll.empty());
        TANK_EXPECT(idle_connections_count);

        c->classification.ll.detach_and_reset();
        --idle_connections_count;

        if (idle_connections.empty()) {
                next_idle_check_ts = std::numeric_limits<uint64_t>::max();
        } else {
                auto       c         = switch_list_entry(connection, classification.ll, idle_connections.prev);
                const auto idle_time = now_ms - c->classification.since;

                next_idle_check_ts = now_ms + (idle_connection_ttl - idle_time);
        }
}

void Service::make_idle(connection *const c, const size_t ref) {
        c->verify();

        if (const auto cl = c->state.classification(); cl == connection::ClassificationTracker::Type::Idle) {
                return;
        } else if (cl == connection::ClassificationTracker::Type::LongPolling) {
                declassify_long_polling(c);
        } else if (cl == connection::ClassificationTracker::Type::Active) {
                declassify_active(c);
        }

        classify_idle(c, ref);
        c->state.set_classsification(connection::ClassificationTracker::Type::Idle);
}

void Service::make_active(connection *const c) {
        c->verify();

        if (const auto cl = c->state.classification(); cl == connection::ClassificationTracker::Type::Active) {
                return;
        } else if (cl == connection::ClassificationTracker::Type::LongPolling) {
                declassify_long_polling(c);
        } else if (cl == connection::ClassificationTracker::Type::Idle) {
                declassify_idle(c);
        }

        classify_active(c);
        c->state.set_classsification(connection::ClassificationTracker::Type::Active);
}

void Service::make_long_polling(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        c->verify();

        if (const auto cl = c->state.classification(); cl == connection::ClassificationTracker::Type::LongPolling) {
                return;
        } else if (cl == connection::ClassificationTracker::Type::Active) {
                declassify_active(c);
        } else if (cl == connection::ClassificationTracker::Type::Idle) {
                declassify_idle(c);
        }

        classify_long_polling(c);
        c->state.set_classsification(connection::ClassificationTracker::Type::LongPolling);
}

// attempt to shutdown upto `n` connections in order
// to reclaim resources, especially FDs
size_t Service::try_shutdown_idle(size_t n) {
        static constexpr const bool trace{false};
        const auto                  b = n;

        if (trace) {
                SLog("Require at least ", n, " more FDs, will try to shutdown among ", idle_connections.size(), " idle connections\n");
        }

        while (n && !idle_connections.empty()) {
                auto c = switch_list_entry(connection, classification.ll, idle_connections.prev);

                TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Idle);
                shutdown(c);
                --n;
        }

        return b - n;
}

void Service::disable_listener() {
        if (-1 == listenFd) {
                return;
        }

        poller.erase(listenFd);
        TANKUtil::safe_close(listenFd);
        listenFd = -1;
}

bool Service::enable_listener() {
        static constexpr bool trace{false};
        struct sockaddr_in    sa;

        if (listenFd != -1) {
                return true;
        }

        for (;;) {
                listenFd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

                if (-1 == listenFd) {
                        if ((ENFILE == errno || EMFILE == errno) && try_shutdown_idle(1)) {
                                continue;
                        } else {
                                if (trace) {
                                        SLog("Unable to enable listener:", strerror(errno), "\n");
                                }
                                return false;
                        }
                } else {
                        break;
                }
        }

        memset(&sa, 0, sizeof(sa));
        sa.sin_addr.s_addr = tank_listen_ep.addr4;
        sa.sin_port        = htons(tank_listen_ep.port);
        sa.sin_family      = AF_INET;

        if (Switch::SetReuseAddr(listenFd, 1) == -1) {
                Print("SO_REUSEADDR: ", strerror(errno), "\n");

                TANKUtil::safe_close(listenFd);
                listenFd = -1;
                return false;
        } else if (bind(listenFd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa))) {
                Print("bind() failed:", strerror(errno), "\n");
                Print("Is there another TANK instance already running?\n");

                TANKUtil::safe_close(listenFd);
                listenFd = -1;
                return false;
        } else if (listen(listenFd, 512)) {
                Print("listen() failed:", strerror(errno), "\n");

                TANKUtil::safe_close(listenFd);
                listenFd = -1;
                return false;
        }

        if (trace) {
                SLog("Enabled listener\n");
        }

        poller.insert(listenFd, EPOLLIN, &listenFd);
        return true;
}

bool Service::process_pending_signals(uint64_t mask) {
        do {
                const auto bit = mask & -mask;

                if (bit == (1ull << SIGINT)) {
                        return false;
                }

                mask ^= bit;
        } while (mask);

        return true;
}

void Service::drain_pubsub_queue() {
        if (auto it = mainThreadClosures.drain()) {
                // we don't care about the order those closures are executed
                // but we may as well reverse the list anyway
                mainthread_closure *rh{nullptr};

                while (it) {
                        auto t{it};

                        it      = t->next;
                        t->next = rh;
                        rh      = t;
                }

                do {
                        auto next = rh->next;

                        (*rh)();
                        delete rh;
                        rh = next;
                } while (rh);
        }
}

void Service::fire_timer(timer_node *const ctx) {
        static constexpr bool trace = trace_timers;
        //static constexpr bool trace = false;

        if (trace) {
                SLog(ansifmt::color_brown, "firing timer ", ptr_repr(&ctx->node), " ", unsigned(ctx->type), ansifmt::reset, "\n");
        }

        switch (ctx->type) {
                case timer_node::ContainerType::TryBecomeClusterLeader: {
                        try_become_cluster_leader(__LINE__);
                } break;

                case timer_node::ContainerType::ShutdownConsumerConn: {
                        auto c = containerof(connection, as.consumer.attached_timer, ctx);

                        TANK_EXPECT(c);
                        TANK_EXPECT(c->type == connection::Type::Consumer);
                        c->verify();

                        SLog("Shutting down consumer connection\n");
                        shutdown(c);
                } break;

                case timer_node::ContainerType::ForceSetReactorStateIdle: {
                        reactor_state = ReactorState::Idle;
                } break;

                case timer_node::ContainerType::SchedConsulReq: {
                        auto req = containerof(consul_request, tn, ctx);

                        enqueue_consul_req(req);
                } break;

                case timer_node::ContainerType::TryConsulClusterReg: {
                        if (trace) {
                                SLog("Will re-try registerion with consul agent\n");
                        }

                        consul_state.srv.state = ConsulState::Srv::State::Unknown;
                        register_with_cluster();
                } break;

                case timer_node::ContainerType::ScheduleRenewConsulSess: {
                        if (trace) {
                                SLog("About to reschedule renewal\n");
                        }

                        auto req = consul_state.get_req();

                        req->type = consul_request::Type::RenewSession;
                        schedule_consul_req(req, true);
                } break;

                case timer_node::ContainerType::PeerConnEstTimeout: {
                        auto c = containerof(connection, as.consumer.attached_timer, ctx);

                        SLog("TOO long to connect to peer ", ptr_repr(c), "\n");

                        TANK_EXPECT(c);
                        TANK_EXPECT(c->type == connection::Type::Consumer);
                        c->verify();

                        shutdown(c);
                } break;

                case timer_node::ContainerType::ConnEstTimeout: {
                        const auto c = containerof(connection, as.consul.attached_timer, ctx);

                        TANK_EXPECT(c);
                        TANK_EXPECT(c->type == connection::Type::ConsulClient);
                        c->verify();

                        if (trace) {
                                SLog("Too long to connect to consul ", ptr_repr(c), "\n");
                        }

                        shutdown(c);
                } break;

                case timer_node::ContainerType::WaitCtx: {
                        static constexpr bool trace{false};
                        [[maybe_unused]] auto wc = containerof(wait_ctx, exp_tree_node, ctx);

                        if (trace) {
                                SLog("Timed out wait ctx\n");
                        }

                        abort_wait_ctx(wc);
                } break;

                case timer_node::ContainerType::CleanupTracker:
                        cleanup_scheduled_logs();
                        break;

                default:
                        IMPLEMENT_ME();
                        break;
        }
}

void Service::process_timers() {
        eb64_node *it;

        if (trace_timers) {
                SLog(ansifmt::color_brown, "Processing timers, empty root:", eb_is_empty(&timers_ebtree_root), ansifmt::reset, "\n");
        }

        goto l1;
        for (;;) {
                if (!it) {
                        // we may have another registered so try again
                l1:
                        it = eb64_first(&timers_ebtree_root);
                        if (!it) {
                                timers_ebtree_next = std::numeric_limits<uint64_t>::max();
                                return;
                        }
                }

                if (const auto key = it->key; key > now_ms) {
                        // not expired yet, try again later
                        // we are done
                        timers_ebtree_next = it->key;
                        return;
                }

                auto ctx  = eb64_entry(it, timer_node, node);
                auto next = eb64_next(it);

                // unlink before we get to fire_timer()
                // because within fire_timer() the node may be inserted again
                eb64_delete(it);

                fire_timer(ctx);
                it = next;
        }
}

void Service::abort_retry_consume_from(cluster_node *n) {
        static constexpr bool trace{false};

        if (n->consume_retry_ctx.ll.try_detach_and_reset()) {
                if (trace) {
                        SLog("Aborted retry consume from ", n->ep, "\n");
                }

                scheduled_consume_retries_list_next = scheduled_consume_retries_list.empty()
                                                          ? std::numeric_limits<uint64_t>::max()
                                                          : switch_list_entry(cluster_node, consume_retry_ctx.ll,
                                                                              scheduled_consume_retries_list.prev)
                                                                ->consume_retry_ctx.when;
        }
}

// A replication (this node consumes/drains from another node that's currently the leader of a partition)
// connection has been severed, and we need to retry soon again in case it's possible to reconnect
void Service::schedule_retry_consume_from(cluster_node *n) {
        static constexpr bool trace{false};
        TANK_EXPECT(n);

        if (!any_partitions_to_replicate_from(n)) {
                // don't bother
                if (trace) {
                        SLog("No need to schedule retry consume from ", n->ep, ", this node is not going to replicate any partitions from that node\n");
                }

                return;
        }

        if (!n->consume_retry_ctx.ll.empty()) {
                // already scheduled
                if (trace) {
                        SLog("Node ", n->ep, " already scheduled for consumption retry\n");
                }

                return;
        }

        if (trace) {
                SLog("Node ", n->ep, " scheduled for drain retry\n");
        }

        scheduled_consume_retries_list.push_back(&n->consume_retry_ctx.ll);
        n->consume_retry_ctx.when           = now_ms + 800;
        scheduled_consume_retries_list_next = std::min(scheduled_consume_retries_list_next, n->consume_retry_ctx.when);
}

void Service::cleanup_connection(connection *const c, [[maybe_unused]] const uint32_t ref) {
	enum {
		trace = false,
	};
        TANK_EXPECT(c);
        TANK_EXPECT(c->fd != -1);

        c->verify();

        poller.erase(c->fd);
        TANKUtil::safe_close(c->fd);
        c->fd = -1;

        switch (c->type) {
                case connection::Type::UNKNOWN:
                        std::abort();

                case connection::Type::Consumer:
                        // if you stop e.g send SIGSTOP or ctrl-z a tank node process
                        // then the connection is going to shut down because of inactivity or whatever
                        //
                        // if we SIGCONT the process, we should be able to resume draining, but how
                        // do we know that the node is back up? Currently, we don't, but maybe we need
                        // to track all nodes which we tried to drain and connection was severed, so that we
                        // can try again every a few seconds, if (any_partitions_to_replicate_from(node))
                        if (trace) {
                                SLog("Consumer Connection shut down at ", ref, "\n");
                        }

                        if (auto n = c->as.consumer.node) {
                                if (trace) {
                                        SLog("Consumer connection was for node ", n->ep, "\n");
                                }

                                n->consume_conn.ch.reset();
                                n->consume_conn.repl_streams_cnt = 0;

                                if (any_partitions_to_replicate_from(n)) {
                                        if (trace) {
                                                SLog("Still more partitons to replicate from node; will retry consume in a while\n");
                                        }

                                        abort_retry_consume_from(n);
                                        schedule_retry_consume_from(n);
                                } else if (trace) {
                                        SLog("No partitions to replicate content from node\n");
                                }
                        }

                        cancel_timer(&c->as.consumer.attached_timer.node);

                        if (c->as.consumer.state == connection::As::Consumer::State::Idle) {
                                // that's OK
                        } else if (c->as.consumer.state == connection::As::Consumer::State::Busy) {
                                // this is also OK, no harm, no foul
                        }
                        break;

                // depending on the type, we may have more work to do
                case connection::Type::Prometheus: {
                        //
                } break;

                case connection::Type::ConsulClient: {
                        cancel_timer(&c->as.consul.attached_timer.node);

                        tear_down_consul_resp(c);

                        if (c->as.consul.flags & unsigned(connection::As::Consul::Flags::WaitShutdown)) {
                                // response wasn't a keep-alive connection
                                // so we are have been reading in the response
                                // until the connection has been shutdown
                                switch (c->as.consul.state) {
                                        case connection::As::Consul::State::ReadContent:
                                        case connection::As::Consul::State::ReadCompressedContent:
                                                if (trace) {
                                                        SLog("Was waiting for shutdown before processing content\n");
                                                }

                                                process_ready_consul_resp(c, c->inB);
                                                break;

                                        default:
                                                std::abort();
                                }
                        } else {
                                switch (c->as.consul.state) {
                                        case connection::As::Consul::State::Idle:
                                                if (trace) {
                                                        SLog("Shut down an idle consul client\n");
                                                }

                                                TANK_EXPECT(nullptr == c->as.consul.cur.req);
                                                break;

                                        default: {
                                                auto req = c->as.consul.cur.req;
                                                TANK_EXPECT(req);

                                                if (trace) {
                                                        SLog("Failed to transmit consul request\n");
                                                }

                                                enqueue_consul_req(req);

                                                if (c->as.consul.state == connection::As::Consul::State::WaitRespFirstBytes and req->will_long_poll()) {
                                                        // this is fine, likely timed out by consul because polling for far too long
                                                        // enqueing it for retry as soon as possible is sufficient
                                                        if (trace) {
                                                                SLog("Request was a long polling request and we have been waiting for the first byte\n");
                                                        }
                                                } else {
                                                        if (c->as.consul.state == connection::As::Consul::State::Connecting) {
                                                                if (trace) {
                                                                        SLog("Failed to connect to consul agent\n");
                                                                }

                                                                consul_state.srv.state = ConsulState::Srv::State::Unreachable;
                                                        } else {
                                                                if (trace) {
                                                                        SLog("Consul Agent is unreliable\n");
                                                                }

                                                                consul_state.srv.state = ConsulState::Srv::State::Unreliable;
                                                        }

                                                        if (trace) {
                                                                SLog("Consequtive consul faults:", consul_state.srv.consequtive_faults, "\n");
                                                        }

                                                        static constexpr size_t max_consequtive_faults = 4;

                                                        if (++consul_state.srv.consequtive_faults > max_consequtive_faults) {
                                                                if (0 == (consul_state.flags & unsigned(ConsulState::Flags::LastRegFailed))) {
                                                                        Print(ansifmt::color_red, "Failed to establish a connection to the consul agent or to complete registration. \n");
                                                                        Print("Will retry in a while. Please make sure endpoint is correct, and that the consul agent is running\n");
                                                                        Print(ansifmt::reset);

                                                                        consul_state.flags |= unsigned(ConsulState::Flags::LastRegFailed);
                                                                }

                                                                if (trace) {
                                                                        SLog("Too many consequtive faults, will need to disable consul service\n");
                                                                }

                                                                consul_state.srv.consequtive_faults = max_consequtive_faults;
                                                                disable_consul_srv();
                                                        } else {
                                                                try_reschedule_one_consul_req();
                                                        }
                                                }
                                        } break;
                                }
                        }

                } break;

                case connection::Type::TankClient: {
                        [[maybe_unused]] static constexpr bool trace{false};

                        if (trace) {
                                SLog("Client connection shutdown waitCtxList.size = ", c->as.tank.waitCtxList.size(), "\n");
                        }

                        while (!c->as.tank.produce_responses_list.empty()) {
                                auto pr = switch_list_entry(produce_response, client_ctx.connection_ll, c->as.tank.produce_responses_list.next);

                                complete_deferred_produce_response(pr);
                        }

                        // For simplicity, place in a vector and drain it instead
                        expiredCtxList.clear();
                        for (auto it = c->as.tank.waitCtxList.next; it != &c->as.tank.waitCtxList; it = it->next) {
                                expiredCtxList.push_back(switch_list_entry(wait_ctx, list, it));
                        }

                        while (not expiredCtxList.empty()) {
                                auto ctx = expiredCtxList.back();

                                expiredCtxList.pop_back();
                                destroy_wait_ctx(ctx);
                        }
                } break;
        }

        // in case we track it anywhere
        switch (c->state.classification()) {
                case connection::ClassificationTracker::Type::Active:
                        declassify_active(c);
                        break;

                case connection::ClassificationTracker::Type::Idle:
                        declassify_idle(c);
                        break;

                case connection::ClassificationTracker::Type::LongPolling:
                        declassify_long_polling(c);
                        break;

                case connection::ClassificationTracker::Type::NotClassified:
                        break;
        }

        // NOW flag it as destroyed
        c->gen = std::numeric_limits<uint64_t>::max();

        // no longer tracked in allConnections
        c->connectionsList.detach_and_reset();

        // Release input/output resources
        if (auto b = std::exchange(c->inB, nullptr)) {
                put_buf(b);
        }

        if (auto q = std::exchange(c->outQ, nullptr)) {
                put_outgoing_queue(q);
        }

        // release the connection -- will be reused
        // as of the next reactor loop iteration
        put_connection(c);
}

bool Service::shutdown(connection *const c, const uint32_t ref) {
	enum {
		trace = false,
	};

        if (-1 == c->fd) {
                // already earlier
                return false;
        }

        if (trace) {
                SLog(
                    "SHUTDOWN connection of type ", [t = c->type]() {
                            switch (t) {
                                    case connection::Type::TankClient:
                                            return "client"_s32;
                                    case connection::Type::Prometheus:
                                            return "prometheus"_s32;
                                    case connection::Type::ConsulClient:
                                            return "consul"_s32;
                                    case connection::Type::Consumer:
                                            return "consumer"_s32;
                                    default:
                                            return "other"_s32;
                            }
                    }(),
                    " at ", ref, " ", time(nullptr), " ", ptr_repr(c), " addr4 ", ip4addr_repr(c->addr4), "\n");
        }

        cleanup_connection(c, ref);
        return false;
}

void Service::stop_poll_outavail(connection *c) {
        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))) {
                poller.set_data_events(c->fd, c, EPOLLIN);
                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::NeedOutAvail));
        }
}

void Service::poll_outavail(connection *const c) {
        if (0 == (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))) {
                c->state.flags |= (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                poller.set_data_events(c->fd, c, EPOLLIN | EPOLLOUT);
        }
}

void Service::release_payload(payload *const p) {
        TANK_EXPECT(p);

        switch (p->src) {
                case payload::Source::DataVector: {
                        auto dv_p = static_cast<data_vector_payload *>(p);

                        if (auto b = dv_p->buf) {
                                put_buf(b);
                        }

                        put_data_vector_payload(dv_p);
                } break;

                case payload::Source::FileContents: {
                        auto fh_p = static_cast<file_contents_payload *>(p);

			fh_p->file_range.fdhandle.reset();

                        put_file_contents_payload(fh_p);
                } break;
        }
}

// https://github.com/phaistos-networks/TANK/issues/14
//
// If only FreeBSD's great sendfile() syscall was available on Linux, with support for the
// extra flags based on NGINX's and Netflix's work, that'd make everything so much simpler.
// We 'd just use the SF_NODISKIO flag and the SF_READAHEAD macro, and check for EBUSY
// and optionally use readahead() and try again later(we could also mmap() the log and use mincore() to determine if
// all pages are cached)
Service::flushop_res Service::flush_file_contents(connection *const c,
                                                  payload *const    payload,
                                                  const bool        have_cork) {
        // TODO(markp): transmit_threshold should probably be computed dynamically based on current approximate load and other signals
        static constexpr bool        trace{false};
        static constexpr std::size_t transmit_threshold{24 * 1024 * 1024};
        TANK_EXPECT(c);
        TANK_EXPECT(payload);
        TANK_EXPECT(payload->src == payload::Source::FileContents);

        auto &      it = *static_cast<file_contents_payload *>(payload);
        int         fd = c->fd;
        auto        q  = c->outQ;
        std::size_t transmitted{0};

        q->verify();

        // any chance this is not the front?
        TANK_EXPECT(q->front() == payload);

        assert(it.file_range.fdhandle);
	assert(it.file_range.fdhandle->fd > 2);
        assert(it.file_range.range.size());

        if (trace) {
                SLog("About to transfer ", it.file_range.range, "\n");
        }

        for (size_t maxSpan{128 * 1024}, sum{0};; maxSpan = 1024 * 1024) {
                auto &         range  = it.file_range.range;
                const uint64_t before = Timings::Microseconds::Tick();
                // This is probably a good idea; break this down into multiple requests; syscall overhead should be low.
                // Give readahead() a chance to page-in data
                // in order to reduce the likelihood of blocking here waiting for that
                //
                // We are also now paying attention to how long we have spent in this function so that we can abort early
                // if we have neglected other connections for too long.
                //
                // https://github.com/phaistos-networks/TANK/issues/14#issuecomment-301000261
                const auto outLen = std::min<size_t>(range.len, maxSpan);

#ifdef HAVE_SENDFILE64
                off64_t    offset = range.offset;
                const auto r      = sendfile64(fd, it.file_range.fdhandle->fd, &offset, outLen);
#else
                off_t      offset = range.offset;
                const auto r      = sendfile(fd, it.file_range.fdhandle->fd, &offset, outLen);
#endif

                sum += Timings::Microseconds::Since(before);

                if (trace && 0) {
                        // See https://github.com/phaistos-networks/TANK/issues/14 for measurements
                        SLog("Sending contents ", range_base<uint64_t, size_t>(range.offset, outLen), " => r=", size_repr(r), " range.size=", size_repr(range.size()),
                             " ", duration_repr(Timings::Microseconds::Since(before)), "\n");
                }

                if (-1 == r) {
                        if (EINTR == errno) {
                                continue;
                        } else if (EAGAIN == errno) {
                                if (trace) {
                                        SLog("EAGAIN (have_cork = ", have_cork, ", needOutAvail = ", (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))), ")\n");
                                }

                                return flushop_res::NeedOutAvail;
                        } else {
                                track_shutdown(c, __LINE__, "sendfile() failed: ", strerror(errno), " for ", outLen, "\n");
                                shutdown(c);
                                return flushop_res::Shutdown;
                        }
                } else {
                        range.len -= r;
                        range.offset += r;

                        if (it.tracker.since) {
                                it.tracker.src_topic->metrics.bytes_out += r;
                        }

                        if (0 == range.len) {
                                // done streaming the file chunk
                                if (trace) {
                                        SLog("SENT-releasing ", ansifmt::bold, ansifmt::color_blue,
                                             ptr_repr(it.file_range.fdhandle.get()), " ", it.file_range.fdhandle.use_count(), ansifmt::reset, "\n");
                                }

                                if (const auto since = it.tracker.since) {
                                        const auto delta = Timings::Microseconds::ToMillis(Timings::Microseconds::Since(since));
                                        auto       topic{it.tracker.src_topic};

                                        if (trace) {
                                                SLog("Took ", duration_repr(delta), " to send\n");
                                        }

                                        TANK_EXPECT(topic);
                                        topic->metrics.latency.reg_sample(delta);
                                }

				it.file_range.fdhandle.reset();
				assert(not it.file_range.fdhandle);

                                q->pop_front_expected(payload);
                                release_payload(payload);
                                return flushop_res::Flush;
                        }

                        if (r < outLen) {
                                // if we didn't get to sendfile() all the data we wanted to write
                                // it means that the socket buffer is now full, and it will almost definitely
                                // not be drained by the time we call sendfile() again in this loop; we 'd insted
                                // get EAGAIN.
                                // So we 'll just consider the socket buffer full now and execute the EAGAIN logic
                                if (trace) {
                                        SLog("Socket buffer likely full (r = ", r, ", outLen = ", outLen, ")\n");
                                }

                                return flushop_res::NeedOutAvail;
                        }

                        transmitted += r;
#if 1
                        if (sum > 2'000)
#else
                        // for verifying client flush_broker() semantics
                        if (sum > 10)
#endif
                        {
                                //SLog("exiting\n"); std::abort();

                                // over 0.002s?
                                // Spent too long here, looks like we are reading data not currently in the kernel VM cache
                                // so, give readhead() a fair chance to work for us, and return control to the loop so that other
                                // clients and their connections can be served.
                                // https://github.com/phaistos-networks/TANK/issues/14#issuecomment-301442619
                                if (trace) {
                                        SLog("Bailing, transmitted ", transmitted, " but spent ", sum, " ", duration_repr(sum), "\n");
                                }

                                return flushop_res::NeedOutAvail;
                        }

                        if (unlikely(transmitted > transmit_threshold)) {
                                // Be fair to all other connections, and see if we have any connections in the listen queue for accept4()
                                // We tansferred too much data already.
                                //
                                // The only downside is the overhead epoll_ctl() call(see poll_outavail() impl.)
                                // and that if there are no other consumers/producers this is not going to produce any benefits.
                                // https://github.com/phaistos-networks/TANK/issues/14#issuecomment-301000261
                                if (trace) {
                                        SLog("Transmitted too much data\n");
                                }

                                return flushop_res::NeedOutAvail;
                        }
                }
        }
}

Service::flushop_res Service::flush_iov_impl(connection *const   c,
                                             struct iovec *const iov,
                                             const uint32_t      iovCnt,
                                             const bool          have_cork) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        c->verify();

        const auto fd = c->fd;
        auto       q  = c->outQ;

        TANK_EXPECT(q);
        TANK_EXPECT(fd > 2);
        TANK_EXPECT(iovCnt);
        q->verify();

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_magenta, ansifmt::inverse, "FLUSHING ", iovCnt,
                     " ivecs", ansifmt::reset, " to ", fd, "\n");
        }

        for (;;) {
                if (trace) {
                        SLog("Now attempting to writev() ", iovCnt, "\n");
                }

                TANK_EXPECT(iovCnt);

                auto r = writev(fd, iov, iovCnt);

                if (trace) {
                        SLog("writev() => ", r, "\n");
                }

                if (-1 == r) {
                        if (EINTR == errno) {
                                continue;
                        } else if (EAGAIN == errno) {
                                if (trace) {
                                        SLog("Return NeedOutAvail\n");
                                }

                                return flushop_res::NeedOutAvail;
                        } else {
                                if (trace) {
                                        SLog("writev() failed:", strerror(errno), "\n");
                                }

                                track_shutdown(c,
                                               __LINE__,
                                               "writev() failed for ", iovCnt,
                                               ": ", strerror(errno), "\n");

                                shutdown(c);

                                if (trace) {
                                        SLog("Return Shutdown\n");
                                }
                                return flushop_res::Shutdown;
                        }
                }

                if (trace) {
                        SLog("starting at ", q->dv_iov_index, "\n");
                }

                for (auto dv_iov_index = q->dv_iov_index;;) {
                next_gather:
                        TANK_EXPECT(q->front()->src == payload::Source::DataVector);

                        auto it  = static_cast<data_vector_payload *>(q->front());
                        auto ptr = it->iov + dv_iov_index;

                        TANK_EXPECT(dv_iov_index < it->iov_cnt);
                        if (trace) {
                                SLog("dv_iov_index = ", dv_iov_index, " / ", it->iov_cnt, "\n");
                        }

                        while (r >= ptr->iov_len) {
                                r -= ptr->iov_len;

                                TANK_EXPECT(dv_iov_index + 1 <= it->iov_cnt);

                                if (++dv_iov_index == it->iov_cnt) {
                                        if (trace) {
                                                SLog("OK, dropping front now, size before ", q->size(), "\n");
                                        }

                                        q->pop_front_expected(it);
                                        dv_iov_index = 0;

                                        q->verify();

                                        release_payload(it);

                                        q->verify();

                                        if (!r) {
                                                if (c->state.flags & (1 << unsigned(connection::State::Flags::DrainingForShutdown))) {
                                                        if (have_cork) {
                                                                Switch::SetTCPCork(fd, 0);
                                                        }

                                                        shutdown(c);

                                                        if (trace) {
                                                                SLog("Return Shutdown\n");
                                                        }
                                                        return flushop_res::Shutdown;

                                                } else {
                                                        // callee may want to deal with the cork
                                                        q->dv_iov_index = dv_iov_index;

                                                        if (trace) {
                                                                SLog("Return Flush\n");
                                                        }
                                                        return flushop_res::Flush;
                                                }

                                        } else {
                                                // flushed data for next payload
                                                goto next_gather;
                                        }
                                } else {
                                        ++ptr;
                                }
                        }

                        q->dv_iov_index = dv_iov_index;
                        ptr->iov_base   = reinterpret_cast<char *>(ptr->iov_base) + r;
                        ptr->iov_len -= r;

                        if (trace) {
                                SLog("Return NeedOutAvail\n");
                        }

                        return flushop_res::NeedOutAvail;
                }
        }
}

bool Service::handle_flush(connection *const c) {
        TANK_EXPECT(c);

        switch (c->type) {
                case connection::Type::ConsulClient:
                        return handle_consul_flush(c);

                default:
                        try_make_idle(c);
                        return true;
        }
}

bool Service::try_tx(connection *const c) {
	assert(c);

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))) {
                return true;
        } else {
                return tx(c);
        }
}

// This method's implementation is somewhat more complex that it perhaps ought to be, but we need to
// keep the syscalls count down to minimum and coallesce data to write
//
// Related: https://github.com/phaistos-networks/TANK/issues/41
bool Service::tx(connection *const c) {
        static constexpr bool trace{false};

        TANK_EXPECT(c);
        TANK_EXPECT(c->fd > 2);
        c->verify();

        const auto   fd{c->fd};
        bool         have_cork{false};
        struct iovec iov[256];
        const auto   verify_local_q = []() noexcept {
                //
        };

        if (c->type == connection::Type::TankClient) {
                if (c->as.tank.flags & unsigned(connection::As::Tank::Flags::PendingIntro)) {
                        if (trace) {
                                SLog("Introducing Self\n");
                        }

                        introduce_self(c, have_cork);
                }
        }

        auto q{c->outQ};

        if (!q) {
                if (trace) {
                        SLog("Nothing to send\n");
                }

                stop_poll_outavail(c);
                try_make_idle(c);
                verify_local_q();
                return true;
        }

        q->verify();

        if (trace) {
                SLog("Attempting to send ", q->size(), " payloads\n");

                for (auto it = q->front(); it; it = it->next) {
                        SLog("Payload of type ", it->src == payload::Source::DataVector ? "DV" : "FD", "\n");
                }
        }

        q->verify();

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, "*********** TX()", ansifmt::reset, "\n");
        }

	// TODO: https://github.com/phaistos-networks/TANK/issues/76

        while (auto it_ = q->front()) {
                if (it_->src == payload::Source::DataVector) {
                        if (const auto next = it_->next; nullptr == next || next->src != payload::Source::DataVector) {
                                // fast-path: just one DV to stream
                                const auto it    = static_cast<data_vector_payload *>(it_);
                                const auto index = q->dv_iov_index;
                                const auto cnt   = it->iov_cnt;

                                if (trace) {
                                        SLog("Fast-Path for DV: Will send a single DV\n");
                                }

                                if (next && false == have_cork) {
                                        have_cork = true;
                                        Switch::SetTCPCork(fd, 1);

                                        if (trace) {
                                                SLog("Enabling CORK (have next)\n");
                                        }
                                }

                                TANK_EXPECT(index < cnt);
                                TANK_EXPECT(cnt <= sizeof_array(data_vector_payload::iov));

                                switch (flush_iov_impl(c, it->iov + index, cnt - index, have_cork)) {
                                        case flushop_res::Shutdown:
                                                return false;

                                        case flushop_res::NeedOutAvail:
                                                if (have_cork) {
                                                        Switch::SetTCPCork(c->fd, 0);
                                                }

                                                poll_outavail(c);
                                                verify_local_q();
                                                return true;

                                        case flushop_res::Flush:
                                                break;
                                }
                        } else {
                                auto     dv_iov_index = q->dv_iov_index;
                                uint32_t collected_iovs_cnt{0};

                                do {
                                        TANK_EXPECT(it_->src == payload::Source::DataVector);
                                        const auto it = static_cast<data_vector_payload *>(it_);
                                        const auto n  = std::min<decltype(collected_iovs_cnt)>(it->iov_cnt - dv_iov_index,
                                                                                              sizeof_array(iov) - collected_iovs_cnt);

                                        memcpy(iov + collected_iovs_cnt, it->iov + dv_iov_index, sizeof(struct iovec) * n);
                                        collected_iovs_cnt += n;

                                        if (collected_iovs_cnt == sizeof_array(iov)) {
                                                break;
                                        }

                                        dv_iov_index = 0;
                                } while ((it_ = it_->next) && it_->src == payload::Source::DataVector);

                                if (trace) {
                                        SLog("OK, collected ", collected_iovs_cnt,
                                             " / ", sizeof_array(iov),
                                             " have_cork = ", have_cork,
                                             " it = ", ptr_repr(it_), "\n");
                                }

                                if (false == have_cork && it_) {
                                        if (trace) {
                                                SLog("Activating Cork\n");
                                        }

                                        have_cork = true;
                                        Switch::SetTCPCork(fd, 1);
                                }

                                switch (flush_iov_impl(c, iov, collected_iovs_cnt, have_cork)) {
                                        case flushop_res::Shutdown:
                                                return false;

                                        case flushop_res::NeedOutAvail:
                                                if (have_cork) {
                                                        Switch::SetTCPCork(c->fd, 0);
                                                }

                                                poll_outavail(c);
                                                verify_local_q();
                                                return true;

                                        case flushop_res::Flush:
                                                break;
                                }
                        }
                } else if (it_->src == payload::Source::FileContents) {
                        if (trace) {
                                SLog("payload holds content range\n");
                        }

                        TANK_EXPECT(static_cast<const file_contents_payload *>(it_)->file_range.fdhandle);
                        TANK_EXPECT(static_cast<const file_contents_payload *>(it_)->file_range.range.size());

                        switch (flush_file_contents(c, it_, have_cork)) {
                                case flushop_res::Shutdown:
                                        return false;

                                case flushop_res::NeedOutAvail:
                                        if (trace) {
                                                SLog("Required out availability, have_cork = ", have_cork, "\n");
                                        }

                                        if (have_cork) {
                                                Switch::SetTCPCork(fd, 0);
                                        }

                                        poll_outavail(c);
                                        verify_local_q();
                                        return true;

                                case flushop_res::Flush:
                                        break;
                        }

                } else {
                        IMPLEMENT_ME();
                }
        }

        if (have_cork) {
                if (trace) {
                        SLog("Deactivating Cork\n");
                }

                Switch::SetTCPCork(fd, 0);
        }

        if (trace) {
                SLog("Releasing queue\n");
        }

        q->verify();
        put_outgoing_queue(q);
        c->outQ = nullptr;

        stop_poll_outavail(c);
        return handle_flush(c);
}

connection *Service::new_peer_connection(cluster_node *const peer) {
        TANK_EXPECT(peer);
        static constexpr bool trace{false};
        auto                  c = get_connection();

        if (!c) {
                return nullptr;
        }

        int fd;

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
        sa.sin_addr.s_addr = peer->ep.addr4;
        sa.sin_port        = htons(peer->ep.port);

        if (trace) {
                SLog("Attempting to connect to ", peer->ep, "\n");
        }

        if (-1 == connect(fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) && errno != EINPROGRESS) {
                if (trace) {
                        SLog("Failed to connect() to peer:", strerror(errno), "\n");
                }

                put_connection(c);
                TANKUtil::safe_close(fd);
                return nullptr;
        }

        c->as.consumer.reset();
        c->fd                = fd;
        c->type              = connection::Type::Consumer;
        c->as.consumer.state = connection::As::Consumer::State::Connecting;
        c->as.consumer.node  = peer;
        c->state.flags       = 1u << unsigned(connection::State::Flags::NeedOutAvail);
        poller.insert(c->fd, EPOLLIN | EPOLLOUT, c);

        c->as.consumer.attached_timer.type     = timer_node::ContainerType::PeerConnEstTimeout;
        c->as.consumer.attached_timer.node.key = now_ms + 4 * 1000;
        register_timer(&c->as.consumer.attached_timer.node);

        peer->consume_conn.ch.set(c);
        peer->consume_conn.repl_streams_cnt++;

        return c;
}

// Stopped consuming/replicating for one partition from `peer`
// If there are no more partitions we are replicating from that node, we can safely
// shut down the connection
void Service::did_abort_repl_stream(cluster_node *peer) {
        static constexpr bool trace{false};
        TANK_EXPECT(peer);
        TANK_EXPECT(peer->consume_conn.repl_streams_cnt);

        if (0 == --(peer->consume_conn.repl_streams_cnt)) {
                // we no longer need this connection
                auto c = peer->consume_conn.ch.get();

                TANK_EXPECT(c);
                TANK_EXPECT(c->type == connection::Type::Consumer);
                c->as.consumer.state = connection::As::Consumer::State::Idle;

                if (trace) {
                        SLog("Replication stream to ", peer->ep, " aborted, and we no longer need to replicate for any partition from that node. Will severe conection\n");
                }

                shutdown(c);
        } else if (trace) {
                SLog("Replication stream to ", peer->ep, " aborted, but there are other streams for other partitons with that node; will retain connection\n");
        }
}

connection *Service::peer_connection(cluster_node *peer) {
        static constexpr bool trace{false};
        TANK_EXPECT(peer);
        TANK_EXPECT(peer->ep);

        if (auto c = peer->consume_conn.ch.get()) {
                if (c->as.consumer.state == connection::As::Consumer::State::ScheduledShutdown) {
                        if (trace) {
                                SLog("Connection was scheduled for shutdown -- will use it\n");
                        }

                        TANK_EXPECT(c->as.consumer.attached_timer.is_linked());
                        cancel_timer(&c->as.consumer.attached_timer.node);
                }
                return c;
        } else {
                return new_peer_connection(peer);
        }
}

int Service::try_accept(int fd) {
        static constexpr bool trace{false};
        sockaddr_in           sa;
        socklen_t             saLen = sizeof(sa);
        int                   newFd;

_accept:
        newFd = accept4(fd, reinterpret_cast<sockaddr *>(&sa), &saLen, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (newFd == -1) {
                SLog("accept4() failed:", strerror(errno), "\n");

                if (errno == EMFILE || errno == ENFILE) {
                        if (trace) {
                                SLog("Will try_shutdown_idle()\n");
                        }

                        if (try_shutdown_idle(1)) {
                                goto _accept;
                        } else {
                                // TODO(markp): Ideally, we 'd poller.erase(listenFd);
                                // set listening = false; and if a connection's closed, check
                                // if (listening == false)  { poller.insert(listenFd, EPOLLIN, &listenFd); listening  = true; }
                                // so that we won't waste any cycles attempting to accept4() only to fail because of this here reason
                                // This would probably be useful in case someone puprosely tried to harm the service, or some badly written client(s)
                                // are acting up.
                                //
                                // For now, just do nothing
                                // Reported @by @rkrambovitis
                                return 0;
                        }
                } else if (errno != EINTR && errno != EAGAIN) {
			Print("accept5() failed:", strerror(errno), "\n");
			return 1;
                }
        } else {
                TANK_EXPECT(newFd > 2);
                TANK_EXPECT(saLen == sizeof(sockaddr_in));
                static const auto rcvBufSize = str_view32::make_with_cstr(getenv("TANK_BROKER_SOCKBUF_RCV_SIZE") ?: "0").as_uint32();
                static const auto sndBufSize = str_view32::make_with_cstr(getenv("TANK_BROKER_SOCKBUF_SND_SIZE") ?: "0").as_uint32();

                auto c = get_connection();

                TANK_EXPECT(c);

                // Kafka's default is 1mb for both buffers
                if (rcvBufSize) {
                        if (setsockopt(newFd, SOL_SOCKET, SO_RCVBUF, (char *)&rcvBufSize, sizeof(rcvBufSize)) == -1) {
                                Print("WARNING: unable to set socket receive buffer size:", strerror(errno), "\n");
                        }
                }

                if (sndBufSize) {
                        if (setsockopt(newFd, SOL_SOCKET, SO_SNDBUF, (char *)&sndBufSize, sizeof(sndBufSize)) == -1) {
                                Print("WARNING: unable to set socket send buffer size:", strerror(errno), "\n");
                        }
                }

                c->as.tank.reset();
                c->type  = connection::Type::TankClient;
                c->addr4 = *reinterpret_cast<const uint32_t *>(&sa.sin_addr.s_addr);
                c->fd    = newFd;

                TANK_EXPECT(c->fd > 2);

                Switch::SetNoDelay(c->fd, 1);

                // We are going to ping as soon as we can so that the client will know we have been accepted
                // but we can't write(fd, ..) now; so we 'll need to wait for EPOLLOUT and then ping
                c->as.tank.flags = unsigned(connection::As::Tank::Flags::PendingIntro);
                c->state.flags   = 1u << unsigned(connection::State::Flags::NeedOutAvail);
                poller.insert(c->fd, EPOLLIN | EPOLLOUT, c);

                // start as idle
                make_idle(c, __LINE__);

                if (trace) {
                        SLog("Accepted new connection\n");
                }
        }

        return 0;
}

int Service::try_accept_prom(int fd) {
        sockaddr_in sa;
        socklen_t   len = sizeof(sa);
        int         new_fd;

_accept:
        new_fd = accept4(fd, reinterpret_cast<sockaddr *>(&sa), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (new_fd == -1) {
                if (errno == EMFILE || errno == ENFILE) {
                        if (try_shutdown_idle(1)) {
                                goto _accept;
                        } else {
                                // ran out of FDs and we can't do much about it
                                return 0;
                        }
                } else if (errno != EINTR && errno != EAGAIN) {
                        Print("accept4() failed:", strerror(errno), "\n");
                        return 1;
                }
        } else {
                auto c = get_connection();

                c->as.prometheus.reset();
                c->fd   = new_fd;
                c->type = connection::Type::Prometheus;

                poller.insert(c->fd, EPOLLIN, c);

                // start as idle
                make_idle(c, __LINE__);
        }

        return 0;
}

bool Service::try_recv_prom(connection *const c) {
        static constexpr const bool trace{false};
        auto *const                 b = c->inB;
        const auto *p = b->data() + b->offset(), *const e = b->end();

        if (trace) {
                SLog("From prometheus [", b->as_s32(), "]\n");
        }

        for (;;) {
                // process next HTTP request, if we have one
                while (p != e && isspace(*p)) {
                        ++p;
                }

                if (p == e) {
                        if (trace) {
                                SLog("No other HTTP requests to process\n");
                        }

                        put_buf(b);
                        c->inB = nullptr;
                        try_make_idle(c);
                        return true;
                }

                b->set_offset(p - b->data());

                strwlen32_t method;

                for (method.p = p; p != e && !isblank(*p); ++p) {
                        continue;
                }

                if (p == e) {
                        return true;
                }

                method.SetEnd(p);

                if (trace) {
                        SLog("Method [", method, "]\n");
                }

                for (++p; p != e && isblank(*p); ++p) {
                        continue;
                }

                strwlen32_t path;

                for (path.p = p;; ++p) {
                        if (p == e) {
                                return true;
                        } else if (*p == '\n') {
                                path.SetEnd(p);
                                ++p;
                                break;
                        } else if (*p == '\r' && p + 1 < e && p[1] == '\n') {
                                path.SetEnd(p);
                                p += 2;
                                break;
                        }
                }

                if (trace) {
                        SLog("Path [", path, "]\n");
                }

                // parse headers
                strwlen32_t n, v;
                uint64_t    content_length{std::numeric_limits<uint64_t>::max()};

                for (;;) {
                        for (n.p = p;; ++p) {
                                if (p == e) {
                                        return true;
                                } else if (*p == ':') {
                                        n.SetEnd(p);

                                        for (++p; p != e && isblank(*p); ++p) {
                                                //
                                        }

                                        v.p = p;
                                        for (;; ++p) {
                                                if (p == e) {
                                                        return true;
                                                } else if (*p == '\n') {
                                                        v.SetEnd(p);
                                                        ++p;
                                                        break;
                                                } else if (*p == '\r' && p + 1 < e && p[1] == '\n') {
                                                        v.SetEnd(p);
                                                        p += 2;
                                                        break;
                                                }
                                        }

                                        break;
                                } else if (*p == '\n') {
                                        n.SetEnd(p);
                                        ++p;
                                        v.reset();
                                        break;
                                } else if (*p == '\r' && p + 1 < e && p[1] == '\n') {
                                        n.SetEnd(p);
                                        p += 2;
                                        v.reset();
                                        break;
                                }
                        }

                        if (trace) {
                                SLog("[", n, "] [", v, "]\n");
                        }

                        if (!n) {
                                // done with headers
                                auto q       = c->outQ ?: (c->outQ = get_outgoing_queue());
                                auto payload = get_data_vector_payload();

                                payload->iov_cnt = 0;

                                const auto http_error = [&](auto c, const auto resp, const bool close_on_flush = false) {
                                        if (trace) {
                                                SLog("Failed:", resp, "\n");
                                        }

                                        payload->append("HTTP/1.1 "_s32);
                                        payload->append(resp);
                                        payload->append("\r\nServer: TANK\r\nContent-Length: 0\r\n\r\n"_s32);
                                        q->push_back(payload);
                                };

                                b->set_offset(p - b->data());

                                if (content_length != std::numeric_limits<uint64_t>::max()) {
                                        http_error(c, "413 Payload Too Large"_s32, true);
                                        // shutdown immediately
                                        return true;
                                }
                                if (!method.Eq(_S("GET"))) {
                                        http_error(c, "405 Method Not Allowed"_s32);
                                } else if (!path.BeginsWith(_S("/metrics"))) {
                                        http_error(c, "404 Not Found"_s32);
                                } else {
                                        auto b = get_buf();

                                        payload->buf = b;
                                        payload->append("HTTP/1.1 200 OK\r\nServer: TANK\r\nContent-Type: text/plain\r\nContent-Length: "_s32);
                                        const auto content_len_o{b->size()};
                                        b->append("                     \r\n\r\n"_s32);

                                        const auto body_o{b->size()};
#pragma mark Prometheus Metrics Response
                                        b->append("# HELP tanksrv_topic_latency Consumer latency\n"_s32);
                                        b->append("# TYPE tanksrv_topic_latency histogram\n"_s32);
                                        b->append("# HELP tanksrv_topic_produced_bytes Total bytes of all accepted new messages\n"_s32);
                                        b->append("# TYPE tanksrv_topic_produced_bytes counter\n"_s32);
                                        b->append("# HELP tanksrv_topic_produced_msgs Total accepted messages\n"_s32);
                                        b->append("# TYPE tanksrv_topic_produced_msgs counter\n"_s32);
                                        b->append("# HELP tanksrv_topic_consumed_bytes Total bytes of all outgoing messages\n"_s32);
                                        b->append("# TYPE tanksrv_topic_consumed_bytes counter\n"_s32);

                                        for (const auto &it : topics) {
                                                const auto [name, topic] = it;

                                                if (const auto v = topic->metrics.bytes_in) {
                                                        b->append(R"(tanksrv_topic_produced_bytes{m=")", name, R"("} )", v, "\n");
                                                }
                                                if (const auto v = topic->metrics.msgs_in) {
                                                        b->append(R"(tanksrv_topic_produced_msgs{m=")", name, R"("} )", v, "\n");
                                                }
                                                if (const auto v = topic->metrics.bytes_out) {
                                                        b->append(R"(tanksrv_topic_consumed_bytes{m=")", name, R"("} )", v, "\n");
                                                }

                                                if (const auto cnt = topic->metrics.latency.cnt) {
                                                        uint64_t total{0};

                                                        for (uint32_t i{0}; i != sizeof_array(topic::metrics_struct::latency_struct::histogram_scale); ++i) {
                                                                total += topic->metrics.latency.hist_buckets[i];

                                                                if (total) {
                                                                        b->append(R"(tanksrv_topic_latency_bucket{le=")", 
										topic::metrics_struct::latency_struct::histogram_scale[i], R"(", n=")", name, R"("} )", total, "\n"_s32);
                                                                }
                                                        }

                                                        b->append(R"(tanksrv_topic_latency_bucket{le="+Inf", n=")", name, R"("} )", cnt, "\n"_s32);
                                                        b->append(R"(tanksrv_topic_latency_sum{n=")", name, R"("} )", topic->metrics.latency.sum, "\n"_s32);
                                                        b->append(R"(tanksrv_topic_latency_count{n=")", name, R"("} )", cnt, "\n\n"_s32);
                                                }
                                        }

                                        b->data()[content_len_o + sprintf(b->data() + content_len_o, "%u", b->size() - body_o)] = ' ';
                                        payload->append(b->as_s32());
                                        q->push_back(payload);
                                }

                                if (!try_tx(c)) {
                                        return false;
                                }

                                break;
                        } else {
                                if (n.EqNoCase(_S("content-length"))) {
                                        content_length = v.as_uint64();
                                }
                        }
                }
        }

        return true;
}

bool Service::try_recv_consumer(connection *const c) {
	enum {
		trace = false,
	};
        auto *const           b = c->inB;

        for (const auto *e = reinterpret_cast<uint8_t *>(b->end());;) {
                const auto *p = reinterpret_cast<uint8_t *>(b->data_at_offset());

                if (e - p >= sizeof(uint8_t) + sizeof(uint32_t)) {
                        const auto msg     = *p++;
                        const auto msg_len = decode_pod<uint32_t>(p);

                        if (unlikely(msg_len > 256 * 1024 * 1024)) {
                                Print("** TOO large incoming packet of length ", size_repr(msg_len), "\n");
                                return shutdown(c);
                        }

                        if (0 == (c->as.tank.flags & unsigned(connection::As::Tank::Flags::ConsideredReqHeader))) {
                                // So that ingestion of future incoming data will not require buffer reallocations
                                const auto o = std::distance(b->data(), reinterpret_cast<char *>(const_cast<uint8_t *>(p)));

                                if (trace) {
                                        SLog("Need to reserve(", msg_len, ")\n");
                                }

                                b->reserve(msg_len);

                                p = reinterpret_cast<uint8_t *>(b->At(o));
                                e = reinterpret_cast<uint8_t *>(b->end());

                                c->as.tank.flags |= unsigned(connection::As::Tank::Flags::ConsideredReqHeader);
                        }

                        if (p + msg_len > e) {
                                if (trace) {
                                        SLog("Need more data for ", msg, "\n");
                                }

                                return true;
                        }

                        c->as.tank.flags &= ~unsigned(connection::As::Tank::Flags::ConsideredReqHeader);

                        if (not process_peer_msg(c, msg, reinterpret_cast<const uint8_t *>(p), msg_len)) {
                                return false;
                        }

                        p += msg_len;
                        if (p == e) {
                                b->clear();
                                c->inB = nullptr;
                                put_buf(b);

                                try_make_idle(c);
                                break;
                        } else {
                                b->set_offset(reinterpret_cast<const char *>(p));
                        }

                } else {
                        break;
                }
        }

        return true;
}

bool Service::try_recv_tank(connection *const c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->fd > 2);
        static constexpr bool trace{false};
        auto *const           b = c->inB;

        for (const auto *e = reinterpret_cast<uint8_t *>(b->end());;) {
                const auto *p = reinterpret_cast<uint8_t *>(b->data_at_offset());

                if (e - p >= sizeof(uint8_t) + sizeof(uint32_t)) {
                        const auto msg        = *p++;
                        const auto msg_length = decode_pod<uint32_t>(p);

                        if (unlikely(msg_length > 256 * 1024 * 1024)) {
                                Print("** TOO large incoming packet of length ", size_repr(msg_length), "\n");
                                track_shutdown(c, __LINE__, "Too large incoming packet of length ", msg_length, "\n");

                                return shutdown(c);
                        }

                        if (0 == (c->as.tank.flags & unsigned(connection::As::Tank::Flags::ConsideredReqHeader))) {
                                // So that ingestion of future incoming data will not require buffer reallocations
                                const auto o = std::distance(b->data(), reinterpret_cast<char *>(const_cast<uint8_t *>(p)));

                                if (trace) {
                                        SLog("Need to reserve(", msg_length, ")\n");
                                }

                                b->reserve(msg_length);

                                p = reinterpret_cast<uint8_t *>(b->At(o));
                                e = reinterpret_cast<uint8_t *>(b->end());

                                c->as.tank.flags |= unsigned(connection::As::Tank::Flags::ConsideredReqHeader);
                        }

                        if (p + msg_length > e) {
                                if (trace) {
                                        SLog("Need more data for ", msg, "\n");
                                }

                                return true;
                        }

                        c->as.tank.flags &= ~unsigned(connection::As::Tank::Flags::ConsideredReqHeader);
                        if (not process_msg(c, msg, reinterpret_cast<const uint8_t *>(p), msg_length)) {
                                return false;
                        }

                        p += msg_length;
                        if (p == e) {
                                b->clear();
                                c->inB = nullptr;
                                put_buf(b);

                                try_make_idle(c);
                                break;
                        } else {
                                b->set_offset(reinterpret_cast<const char *>(p));
                        }

                } else {
                        break;
                }
        }

        return true;
}

bool Service::try_recv(connection *const c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->fd > 2);
        c->verify();

        int  fd = c->fd, n;
        auto b  = c->inB;

        if (unlikely(-1 == ioctl(fd, FIONREAD, &n))) {
                std::abort();
        }

        if (!b) {
                // in case it wasn't active to begin with
                b = (c->inB = get_buf());
                make_active(c);
        }

        b->reserve(n);

        if (const auto r = read(fd, b->end(), n); - 1 == r) {
                if (EINTR == errno || EAGAIN == errno) {
                        return true;
                } else {
                        if (trace) {
                                SLog("Failed:", strerror(errno), "\n");
                        }

                        return shutdown(c);
                }
        } else if (0 == r) {
                return shutdown(c);
        } else {
                b->advance_size(r);
        }

        switch (c->type) {
                case connection::Type::UNKNOWN:
                        std::abort();

                case connection::Type::Consumer:
                        if (not try_recv_consumer(c)) {
                                return false;
                        } else if (auto b = c->inB; b and b->offset() == b->size()) {
                                put_buf(b);
                                c->inB = nullptr;
                                try_make_idle(c);
                        }
                        break;

                case connection::Type::Prometheus:
                        return try_recv_prom(c);

                case connection::Type::TankClient:
                        if (not try_recv_tank(c)) {
                                return false;
                        } else if (auto b = c->inB) {
                                if (b->offset() == b->size()) {
                                        put_buf(b);
                                        c->inB = nullptr;
                                        try_make_idle(c);
                                } else if (b->offset() > 4 * 1024 * 1024) {
                                        b->DeleteChunk(0, b->offset());
                                        b->SetOffset(uint64_t(0));
                                }
                        }
                        break;

                case connection::Type::ConsulClient:
                        return try_recv_consul(c);
        }

        return true;
}

void Service::register_timer(eb64_node *const node) {
        timers_ebtree_next = std::min<uint64_t>(node->key, timers_ebtree_next);
        eb64_insert(&timers_ebtree_root, node);

        if (trace_timers) {
                SLog(ansifmt::color_brown, "Registered timer ", ptr_repr(node),
                     " now key ", node->key, " ebtree_next ", timers_ebtree_next,
                     " ebtree next - tick", timers_ebtree_next - Timings::Milliseconds::Tick(),
                     " tick ", Timings::Milliseconds::Tick(), ansifmt::reset, "\n");
        }
}

bool Service::cancel_timer(eb64_node *const node) {
        if (trace_timers) {
                SLog(ansifmt::color_brown, "Attempting to cancel timer ", ptr_repr(node),
                     " (is linked?:", node->node.leaf_p != nullptr, ")", ansifmt::reset, "\n");
        }

        if (!node->node.leaf_p) {
                // already unlinked
                return false;
        }

        const auto when = node->key;

        eb64_delete(node);
        TANK_EXPECT(!node->node.leaf_p);

        if (when <= timers_ebtree_next) {
                // update timers_ebtree_next
                if (const auto eb = eb64_first(&timers_ebtree_root)) {
                        timers_ebtree_next = eb->key;
                } else {
                        timers_ebtree_next = std::numeric_limits<uint64_t>::max();
                }
        }

        return true;
}

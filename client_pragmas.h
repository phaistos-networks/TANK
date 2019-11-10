protected:
void gc_ready_responses();

#ifdef TANK_CLIENT_FAST_CONSUME
bool process_consume_content(connection *);

bool process_consume_content_impl(connection *);

void clear_tank_resp(connection *);
#endif

#ifdef HAVE_NETIO_THROTTLE
void throttle_read(connection *, const uint64_t);

void throttle_write(connection *, const uint64_t);

void manage_throttled_connections();
#endif

Switch::endpoint leader_for(const str_view8, const uint16_t);

void shift_failed_broker(broker *);

void update_api_req(api_request *, const bool , std::vector<request_partition_ctx *> *, std::vector<request_partition_ctx *> *);

broker *any_broker();

void fail_api_request(std::unique_ptr<api_request>);

void abort_api_request(api_request *);

bool materialize_srv_request(api_request *);

bool materialize_reload_config_request(api_request *);

bool materialize_create_topic_requet(api_request *);

bool materialize_produce_request(api_request *);

bool materialize_next_broker_req_payload(broker *);

void try_stop_track_unreachable(broker *);

void abort_broker_connection(connection *, broker *);

void make_unreachable(broker *);

void wakeup_unreachable_broker(broker *);

void check_unreachable_brokers();

void try_make_api_req_ready(api_request *, const uint32_t);

void clear_request_partition_ctx(api_request *, request_partition_ctx *);

void abort_api_request_brokers_reqs(api_request *, std::vector<request_partition_ctx *> *, const uint32_t);

void abort_api_request_retry_bundles(api_request *, std::vector<request_partition_ctx *> *);

void retry_bundle_impl(retry_bundle *);

void schedule_retry(api_request *, request_partition_ctx **, const uint16_t , const uint64_t);

void check_pending_retries();

bool schedule_req_partitions(api_request *, std::vector<request_partition_ctx *> *);

bool schedule_req_partitions(api_request *, std::vector<std::pair<broker *, request_partition_ctx *>> *);

bool schedule_req_partitions(api_request *, request_partition_ctx **, const size_t);

void set_leader(const str_view8, const uint16_t, const Switch::endpoint);

void set_leader(const str_view8, const uint16_t, const str_view32);

broker *partition_leader(const str_view8, const uint16_t);

void capture_unsupported_request(api_request *);

void capture_topic_already_exists(api_request *, const str_view8);

void capture_unknown_topic_fault(api_request *, const str_view8);

void capture_unknown_partition_fault(api_request *, const str_view8, const uint16_t);

void capture_boundary_access_fault(api_request *, const str_view8, const uint16_t, const uint64_t, const uint64_t);

void capture_timeout(api_request *, const str_view8, const uint16_t, const uint32_t);

void capture_network_fault(api_request *, const str_view8, const uint16_t);

void capture_system_fault(api_request *, const str_view8, const uint16_t);

void capture_invalid_req_fault(api_request *, const str_view8, const uint16_t);

void capture_insuficient_replicas(api_request *, const str_view8, const uint16_t);

void capture_readonly_fault(api_request *);

void fail_api_req(api_request *);

void gc_api_request(std::unique_ptr<api_request>);

bool materialize_consume_api_request(api_request *);

bool materialize_discover_partitions_requests(api_request *);

bool materialize_api_response(api_request *);

msgs_bucket *get_msgs_bucket();

void put_msgs_bucket(msgs_bucket *);

void end_reactor_loop_iteration();

void make_api_req_ready(api_request *, const uint32_t);

void retain_mb(managed_buf *b) {
	TANK_EXPECT(b);

	b->retain();
}

void release_mb(managed_buf *b) {
        TANK_EXPECT(b);

	if (!b->use_count()) {
		SLog("Attempting to release ", ptr_repr(b), " with rc == 0\n");
		std::abort();
	}

        TANK_EXPECT(b->use_count());

        if (b->release()) {
                put_managed_buffer(b);
        }
}

managed_buf *get_managed_buffer();

void put_managed_buffer(managed_buf *);

void retain_conn_inbuf(connection *, api_request *);

void abort_broker_req(broker_api_request *);

void check_pending_api_responses();

void unlink_broker_req(broker_api_request *, const size_t);

void check_conns_pending_est();

broker_outgoing_payload *build_broker_req_payload(broker_api_request *);

switch_dlist *assign_req_partitions_to_api_req(api_request *, std::vector<std::pair<broker *, request_partition_ctx *>> *, const uint32_t limit = std::numeric_limits<uint32_t>::max());

broker *broker_by_endpoint(const Switch::endpoint);

str_view8 intern_topic(const str_view8);

void process_undeliverable_broker_req(broker_api_request *, const bool, const uint32_t ref = 0);

broker_outgoing_payload *new_req_payload(broker_api_request *);

broker_outgoing_payload *build_reload_partition_conf_broker_req_payload(const broker_api_request *);

broker_outgoing_payload *build_create_topic_broker_req_payload(const broker_api_request *);

broker_outgoing_payload *build_produce_broker_req_payload(const broker_api_request *);

broker_outgoing_payload *build_consume_broker_req_payload(const broker_api_request *);

broker_outgoing_payload *build_discover_partitions_broker_req_payload(const broker_api_request *);

uint32_t schedule_new_api_req(std::unique_ptr<api_request>);

uint32_t track_pending_resp(std::unique_ptr<api_request>);

bool schedule_broker_payload(broker_api_request *, broker_outgoing_payload *);

bool schedule_broker_req(broker_api_request *);

std::unique_ptr<api_request> get_api_request(const uint64_t expiration);

void put_api_request(std::unique_ptr<api_request> );

broker_api_request *get_broker_api_request();

void put_broker_api_request(broker_api_request *);

request_partition_ctx *get_request_partition_ctx();

void put_request_partition_ctx(request_partition_ctx *);

void poll_outavail(connection *);

void stop_poll_outavail(connection *);

bool process_srv_in(connection *);

void drain_pipe(int);

void process_io(const size_t);

uint64_t reactor_next_wakeup() const noexcept;

void reactor_step(uint32_t);

void begin_reactor_loop_iteration();

bool is_unreachable(const Switch::endpoint) const;

bool shutdown(connection *const c, const uint32_t ref, const bool fault = true);

bool process_produce(connection *const c, const uint8_t *const content, const size_t len);

bool process_consume(connection *const c, const uint8_t *const content, const size_t len);

bool process_discover_partitions(connection *const c, const uint8_t *const content, const size_t len);

bool process_reload_partition_conf(connection *, const uint8_t *, const size_t);

bool process_create_topic(connection *const c, const uint8_t *const content, const size_t len);

bool process_srv_status(connection *const, const uint8_t *, const size_t);

broker_outgoing_payload *build_srv_status_broker_req_payload(const broker_api_request *);

bool process_msg(connection *const c, const uint8_t msg, const uint8_t *const content, const size_t len);


bool rcv(connection *const c);

bool tx_tank(connection *c);

bool tx(connection *const c);

bool try_tx(connection *const c) {
        return tx(c);
}

IOBuffer *get_buffer();

void put_buffer(IOBuffer *const b);

auto get_payload() {
        broker_outgoing_payload *p;

	++rsrc_tracker.payload;
        if (false == reusable_payloads.empty()) {
                p = reusable_payloads.back();

                reusable_payloads.pop_back();
        } else {
                p = static_cast<broker_outgoing_payload *>(core_allocator.Alloc(sizeof(broker_outgoing_payload)));
        }

        p->iovecs.size = 0;
        p->next        = nullptr;
        p->b           = get_buffer();

        return p;
}

broker_outgoing_payload *get_payload_for(connection *, const size_t);

void put_payload(broker_outgoing_payload *p, [[maybe_unused]] const uint32_t ref) {
	TANK_EXPECT(rsrc_tracker.payload);
	--rsrc_tracker.payload;

        if (auto b = std::exchange(p->b, nullptr)) {
                put_buffer(b);
        }

        reusable_payloads.emplace_back(p);
}

auto get_connection() {
        connection *c;

	++rsrc_tracker.connection;

        if (!reusable_connections.empty()) {
                c = reusable_connections.back().release();
                reusable_connections.pop_back();
        } else {
                c = new connection();
        }

        c->gen = next_conns_gen++;
        c->reset();
        return c;
}

void put_connection(connection *const c) {
	TANK_EXPECT(rsrc_tracker.connection);
	--rsrc_tracker.connection;

        c->state.flags = 0;
        reusable_connections.emplace_back(std::move(c));
}

bool try_transmit(broker *);

bool init_broker_connection(broker *);

void flush_broker(broker *bs);

public:
TankClient(const strwlen32_t endpoints = {});

~TankClient();

const auto &consumed() const noexcept {
        return consumed_content;
}

const auto &faults() const noexcept {
        return all_captured_faults;
}

const auto &produce_acks() const noexcept {
        return produce_acks_v;
}

const auto &discovered_partitions() const noexcept {
        return all_discovered_partitions;
}

const auto &reloaded_partition_configs() const noexcept {
        return reload_conf_results_v;
}

const auto &created_topics() const noexcept {
        return created_topics_v;
}

const auto &statuses() const noexcept {
	return collected_cluster_status_v;
}

inline void poll(const uint32_t timeout_ms) {
        reactor_step(timeout_ms);
}

// Maybe you want to use it after poll() has returned
// TODO: check if (!vector.empty()) instead; should be faster for std::vector<>
bool any_responses() const noexcept {
        return consumed().size() ||
               faults().size() ||
               produce_acks().size() ||
               discovered_partitions().size() ||
               reloaded_partition_configs().size() ||
               created_topics().size() ||
               statuses().size();
}

[[gnu::warn_unused_result, nodiscard]] uint32_t produce(const std::pair<topic_partition, std::vector<msg>> *, const size_t);

[[gnu::warn_unused_result, nodiscard]] inline uint32_t produce(const std::vector<std::pair<topic_partition, std::vector<msg>>> &req) {
	return produce(req.data(), req.size());
}

[[gnu::warn_unused_result, nodiscard]] uint32_t produce_to(const topic_partition &to, const std::vector<msg> &msgs);

// This is needed for Tank system tools. Applications should never need to use this method
// e.g tank-ctl mirroring functionality
//
// Right now, it's only required for implementing the mirroring functionality

[[gnu::warn_unused_result, nodiscard]] uint32_t produce_with_seqnum(const std::pair<topic_partition, std::vector<consumed_msg>> *, const size_t);

[[gnu::warn_unused_result, nodiscard]] inline uint32_t produce_with_seqnum(const std::vector<std::pair<topic_partition, std::vector<consumed_msg>>> &req) {
	return produce_with_seqnum(req.data(), req.size());
}

[[gnu::warn_unused_result, nodiscard]] uint32_t consume(const std::pair<topic_partition, std::pair<uint64_t, uint32_t>> *, const std::size_t, const uint64_t maxWait, const uint32_t minSize);

[[gnu::warn_unused_result, nodiscard]] uint32_t consume(const std::vector<std::pair<topic_partition, std::pair<uint64_t, uint32_t>>> &req, const uint64_t maxWait, const uint32_t minSize);

[[gnu::warn_unused_result, nodiscard]] uint32_t consume_from(const topic_partition &from, const uint64_t seqNum, const uint32_t minFetchSize, const uint64_t maxWait, const uint32_t minSize);

[[gnu::warn_unused_result, nodiscard]] uint32_t discover_partitions(const strwlen8_t topic);

[[gnu::warn_unused_result, nodiscard]] uint32_t reload_partition_conf(const strwlen8_t topic, const uint16_t partition);

[[gnu::warn_unused_result, nodiscard]] uint32_t create_topic(const strwlen8_t topic, const uint16_t numPartitions, const strwlen32_t configuration);

[[gnu::warn_unused_result, nodiscard]] uint32_t service_status();

bool any_requests_pending_delivery() const noexcept;

void reset(const bool dtor_context = false);

void set_client_id(const char *const p, const uint32_t len) {
        clientId.Set(p, len);
}

void set_retry_strategy(const RetryStrategy) noexcept {
	// no-op
}

void set_compression_strategy(const CompressionStrategy c) noexcept {
        compressionStrategy = c;
}

void set_sock_sndbuf_size(const int v) noexcept {
        sndBufSize = v;
}

void set_sock_rcvbuf_size(const int v) noexcept {
        rcvBufSize = v;
}

void set_default_leader(const Switch::endpoint e);

void set_allow_streaming_consume_responses(const bool v) noexcept {
        allowStreamingConsumeResponses = v;
}

void set_default_leader(const strwlen32_t e) {
        set_default_leader(Switch::ParseSrvEndpoint(e, {_S("tank")}, 11011));
}

void interrupt_poll();

bool should_poll() const noexcept;

// A handy utility method
// Will check that reqID is valid, and then will wait until it gets an ack. for this request
// or any fault - and if it fails, it will throw an exception..
// This is mostly useful for produce responses
void wait_scheduled(const uint32_t reqID);

// Another utility method
// It attempts to determine the sequence number of the message in (topic_partition) that's closest to event_time, by using binary search among the messages space
// cut_off_threshold is time, in milliseconds, that determines how far from the target event time the event, identified by the returned sequence number, can be
// The lower that value, the fewer binary search probles it will take. Depending on the size of your messages, you may want to
// set this very high or very low -- and then just consume messages until you have reached or exceeded that target event_time
uint64_t sequence_number_by_event_time(const topic_partition &, const uint64_t event_time, const uint64_t cut_off_threshold = Timings::Minutes::ToMillis(5));

inline size_t count_brokers() const noexcept {
	return all_brokers.size();
}

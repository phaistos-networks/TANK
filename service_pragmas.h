#ifndef HWM_UPDATE_BASED_ON_ACKS
void rebuild_partition_tracked_isrs(topic_partition *);

void isr_touch(isr_entry *, const uint64_t);
#endif

void schedule_compaction(std::unique_ptr<pending_compaction> &&);

void schedule_compaction(const char *, topic_partition_log *);

void track_accessed_partition(topic_partition *, const time_t);

void consider_active_partitions();

void gen_create_topic_succ(consul_request *);

void gen_create_topic_fail(consul_request *);

bool can_accept_any_messages() const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

void invalidate_replicated_partitions_from_peer_cache(cluster_node *n) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

void invalidate_replicated_partitions_from_peer_cache_by_partition(topic_partition *p) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

bool can_accept_messages(const topic_partition *) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

void track_io_fail(topic_partition *);

void track_io_success(topic_partition *);

void consider_isr_pending_ack();

void consul_req_over(consul_request *);

void conclude_bootstrap_updates();

void force_cluster_updates_apply();

void schedule_cluster_updates_apply(const char *);

void apply_deferred_updates();

void apply_cluster_state_updates();

void verify_cluster_invariants(topic_partition *p);

void consider_isr(topic_partition *);

bool handle_consul_resp_conf_updates(const str_view32, const bool);

bool handle_consul_resp_topology(const str_view32);

bool handle_consul_resp_leaders(const str_view32);

bool handle_consul_resp_isr(const str_view32);

bool handle_consul_resp_nodes(const str_view32);

void cleanup_scheduled_logs();

void apply_nodes_updates(const std::vector<std::pair<cluster_node *, bool>> &, const std::vector<cluster_node *> &);

void reconcile_cluster_nodes(const std::vector<cluster_nodeid_update> &);

Switch::endpoint decode_endpointb64(const std::string &);

void process_fetched_cluster_configurations(const consul_request *, const str_view32);

void process_consul_configs(const std::vector<std::pair<str_view8, std::pair<str_view32, uint64_t>>> &);

void process_cluster_config(const str_view32, const uint64_t);

void process_topic_config(const str_view8, const str_view32, const uint64_t);

void consider_updated_consul_configs(const std::vector<std::pair<str_view8, uint64_t>> &, const bool);

void monitor_consul_namespaces();

void cluster_node_id_acquired();

void consul_sess_confirmed();

void reconcile_leadership(std::vector<partition_leader_update> *);

void reconcile_cluster_topology(std::vector<topology_partition> *);

void reconcile_ISR(std::vector<partition_isr_info> *);

void try_become_cluster_leader(const uint32_t);

void process_consul_cluster_configurations(const consul_request *, const str_view32);

void consul_ns_retrieval_complete();

void begin_reactor_loop_iteration();

void enqueue_consul_req(consul_request *);

void disable_tank_srv();

void disable_consul_srv();

void try_reschedule_one_consul_req();

void schedule_consul_session_renewal();

bool schedule_consul_req_impl(consul_request *, const bool);

bool schedule_consul_req(consul_request *, const bool);

bool initiate_consul_req(connection *);

void set_consul_conn_cur_req(connection *, consul_request *);

bool bind_consul_req(consul_request *, connection *);

connection *acq_new_consul_connection();

bool enable_listener();

void disable_listener();

bool consider_idle_consul_connection(connection *);

bool register_with_cluster();

static void rm_tankdir(const char *);

static void parse_partition_config(const char *, partition_config *);

static void parse_partition_config(const strwlen32_t, partition_config *);

inline bool cluster_aware() const noexcept {
        return cluster_state._name_len;
}

auto get_outgoing_queue() {
        outgoing_queue *res;

        if (!outgoingQueuesPool.empty()) {
                res = outgoingQueuesPool.back();
                outgoingQueuesPool.pop_back();
        } else {
                res = new outgoing_queue();
        }

        res->reset();
        return res;
}

topic *topic_by_name(const strwlen8_t) const;

void release_payload(payload *);

void put_outgoing_queue(outgoing_queue *);

auto get_buf() {
	IOBuffer *res;

	if (!bufs.empty()) {
		res = bufs.back();
		bufs.pop_back();
	}  else{
		res = new IOBuffer();
	}

	return res;
}


repl_stream *get_repl_stream() {
	repl_stream *s;

	if (!reusable_replication_streams.empty())  {
		s = reusable_replication_streams.back();
		reusable_replication_streams.pop_back();
	} else {
		s = static_cast<repl_stream *>(repl_streams_allocator.Alloc(sizeof(repl_stream)));
	}
	s->reset();
	return s;
}

void put_repl_stream(repl_stream *s) {
	reusable_replication_streams.emplace_back(s);
}

void put_buf(IOBuffer *b) {
        if (b) {
                if (b->Reserved() > 800 * 1024 || bufs.size() > 16)
                        delete b;
                else {
                        b->clear();
                        bufs.push_back(b);
                }
        }
}

file_contents_payload *get_file_contents_payload() {
        file_contents_payload *p;

        if (!reusable_file_contents_payloads.empty()) {
                p = reusable_file_contents_payloads.back();
                reusable_file_contents_payloads.pop_back();
        } else {
                p      = static_cast<file_contents_payload *>(payloads_allocator.Alloc(sizeof(file_contents_payload)));
                p->src = payload::Source::FileContents;
        }

        p->reset();
        return p;
}

void put_file_contents_payload(file_contents_payload *p) {
	TANK_EXPECT(p);
        reusable_file_contents_payloads.emplace_back(p);
}

data_vector_payload *get_data_vector_payload() {
        data_vector_payload *p;

        if (!reusable_data_vector_payloads.empty()) {
                p = reusable_data_vector_payloads.back();
                reusable_data_vector_payloads.pop_back();
        } else {
                p      = static_cast<data_vector_payload *>(payloads_allocator.Alloc(sizeof(data_vector_payload)));
                p->src = payload::Source::DataVector;
        }

        p->reset();
        return p;
}

void put_data_vector_payload(data_vector_payload *p) {
        reusable_data_vector_payloads.emplace_back(p);
}

connection *get_connection();

void put_connection(connection *const c);

void register_topic(topic *const t) {
        if (false == topics.insert({t->name(), t}).second) {
                throw Switch::exception("Topic ", t->name(), " already registered");
        }
}

Switch::shared_refptr<topic_partition> init_local_partition(const uint16_t idx, topic *, const partition_config &, const bool);

uint32_t partitionLeader(const topic_partition *const p) {
        return 1;
}

const topic_partition *getPartition(const strwlen8_t topic, const uint16_t partitionIdx) const {
        if (const auto it = topics.find(topic); it != topics.end())
                return it->second->partition(partitionIdx);
        else
                return nullptr;
}

topic_partition *getPartition(const strwlen8_t topic, const uint16_t partitionIdx) {
        if (const auto it = topics.find(topic); it != topics.end())
                return it->second->partition(partitionIdx);
        else
                return nullptr;

        return nullptr;
}

void gc_waitctx_deferred();

void consider_long_running_active_consul_requests();

void consider_idle_conns();

size_t try_shutdown_idle(size_t);

void tear_down();

void make_idle(connection *, const size_t);

void make_long_polling(connection *);

bool consider_consul_resp_headers(connection *);

bool consider_http_headers_size(connection *);

bool try_make_idle(connection *);

void declassify_active(connection *);

void classify_active(connection *);

void classify_idle(connection *, const size_t);

void declassify_idle(connection *);

void classify_long_polling(connection *);

void declassify_long_polling(connection *);

void make_active(connection *);

bool process_load_conf(connection *, const uint8_t *, const size_t);

bool process_consume(const TankAPIMsgType, connection *const c, const uint8_t *p, const size_t len);

bool process_discover_partitions(connection *const c, const uint8_t *p, const size_t len);

bool process_create_topic(connection *const c, const uint8_t *p, const size_t len);

bool process_status(connection *const c, const uint8_t *p, const size_t len);

wait_ctx *get_waitctx(const uint32_t totalPartitions) {
        TANK_EXPECT(totalPartitions <= sizeof_array(waitCtxPool));
        auto &v = waitCtxPool[totalPartitions];

        if (!v.empty()) {
                auto res = v.back();

                v.pop_back();
                return res;
        } else {
                return reinterpret_cast<wait_ctx *>(malloc(sizeof(wait_ctx) + totalPartitions * sizeof(wait_ctx_partition)));
        }
}

void put_waitctx(wait_ctx *const ctx) {
        waitCtxPool[ctx->total_partitions].push_back(ctx);
}

bool process_pending_signals(uint64_t);

void drain_pubsub_queue();

void fire_timer(timer_node *);

bool cancel_timer(eb64_node *);

void register_timer(eb64_node *);

void process_timers();

void wakeup_all_consumers(topic_partition *);

bool register_consumer_wait(const TankAPIMsgType, connection *const c, const uint32_t requestId, const uint64_t maxWait, const uint32_t minBytes, topic_partition **const partitions, const uint32_t totalPartitions);

bool process_produce(const TankAPIMsgType, connection *const c, const uint8_t *p, const size_t len);

bool process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len);

bool process_peer_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len);

void wakeup_wait_ctx(wait_ctx *const wctx, connection *);

void abort_wait_ctx(wait_ctx *const wctx);

void destroy_wait_ctx(wait_ctx *const wctx);

void cleanup_connection(connection *, const uint32_t);

bool shutdown(connection *const c, const uint32_t ref);

enum class flushop_res : uint8_t {
        NeedOutAvail,
        Shutdown,
        Flush
};

flushop_res flush_file_contents(connection *, payload *, const bool);

flushop_res flush_iov_impl(connection *, struct iovec *, const uint32_t, const bool);

bool try_tx(connection *);

void poll_outavail(connection *);

void stop_poll_outavail(connection *);

void introduce_self(connection *, bool &);

bool handle_consul_flush(connection *const c);

bool handle_flush(connection *const c);

bool tx(connection *const c);

int try_accept(int);

int try_accept_prom(int);

void tear_down_consul_resp(connection *);

bool process_ready_consul_resp(connection *, IOBuffer *);

void  process_ready_consul_resp_impl(connection *, const str_view32);

bool recv_consul_resp_headers(connection *);

bool recv_consul_resp_content_impl(connection *);

bool complete_consul_req(connection *);

bool recv_consul_resp_content(connection *);

bool recv_consul_resp_content_gzip(connection *);

bool recv_consul_resp_content_gzip_chunks(connection *);

const uint8_t *parse_compressed_http_resp_content(connection *, const uint8_t *, const uint8_t *, const int);

bool recv_consul_resp_content_chunks(connection *);

bool recv_consul_resp_ccompressed_content(connection *);

bool try_recv_consul(connection *);

bool try_recv_prom(connection *);

bool try_recv_tank(connection *);

bool try_recv_consumer(connection *);

bool try_recv(connection *);

int process_io(const int, const uint64_t);

void wakeup_reactor();

int reactor_main();

int verify(char **, const int);

static bool is_valid_topic_name(const str_view8) noexcept;

bool should_manage_topic_partition_replicas(const topic *) const noexcept;

consul_request *schedule_topology_update_tx(const std::vector<NodesPartitionsUpdates::reduced_rs> &,
                                 const std::vector<NodesPartitionsUpdates::expanded_rs> &,
                                 const std::vector<NodesPartitionsUpdates::leadership_promotion> &,
				 topic_partition **, const size_t);

void gen_partition_nodes_updates(topic_partition **, const size_t,
                                  std::vector<NodesPartitionsUpdates::reduced_rs> *,
                                  std::vector<NodesPartitionsUpdates::expanded_rs> *,
                                  std::vector<NodesPartitionsUpdates::leadership_promotion> *);

void request_cluster_config();

void request_topic_config(topic *);

void process_fetched_cluster_configurations(consul_request *, const str_view32);

void reconcile_cluster_ISR(std::vector<partition_isr_info> *);

void apply_isr(topic_partition *, const range_base<nodeid_t *, uint8_t>);

auto get_isr_entry() {
        isr_entry *e;

        if (!reusable_isr_entries.empty()) {
                e = reusable_isr_entries.back();
                reusable_isr_entries.pop_back();
        } else {
                e = static_cast<isr_entry *>(isr_entries_allocator.Alloc(sizeof(isr_entry)));
        }

        e->reset();
        return e;
}

void put_isr_entry(isr_entry *e) {
        reusable_isr_entries.emplace_back(e);
}

void track_consume_req(topic_partition *p, const nodeid_t);

bool all_conns_idle() const;

void session_released();

void initiate_tear_down();

void replicate_partitions(std::vector<std::pair<topic_partition *, cluster_node *>> *, std::vector<std::pair<topic_partition *, cluster_node *>> *);

connection *peer_connection(cluster_node *);

connection *new_peer_connection(cluster_node *);

void abort_peer_connection_local(cluster_node *, connection *);

void replicate_from(cluster_node *, topic_partition *const*, const std::size_t);

void try_replicate_from(const std::unordered_set<cluster_node *> &);

void did_abort_repl_stream(cluster_node *);

void consider_connected_consumer(connection *);

bool process_peer_consume_resp(connection *, const uint8_t, const size_t);

bool process_peer_consume_resp(connection *, const uint8_t *, const size_t);

void open_partition_log(topic_partition *, const partition_config &);

bool close_partition_log(topic_partition *);

int reset_partition_log(topic_partition *);

Switch::shared_refptr<topic_partition>  define_partition(const uint16_t, topic *);

void persist_peer_partitions_content(topic_partition *, const std::vector<topic_partition::msg> &, const bool);

void peer_consumed_local_partition(topic_partition *, cluster_node *, const uint64_t);

void expire_isr_ack(isr_entry *);

topic_partition_log  *partition_log(topic_partition *);

void try_replicate_from(cluster_node *);

void isr_dispose(isr_entry *);

#ifdef HWM_UPDATE_BASED_ON_ACKS
isr_entry *isr_bind(topic_partition *, cluster_node *, const uint8_t, const uint32_t);

inline isr_entry *isr_bind(topic_partition *p, cluster_node *n, const uint32_t ref) {
        return isr_bind(p, n, p->cluster.isr.next_partition_isr_node_id(), ref);
}
#else
isr_entry *isr_bind(topic_partition *, cluster_node *, const uint32_t);
#endif

void persist_isr(topic_partition *, const uint32_t);

bool any_partitions_to_replicate_from(cluster_node *) const;

void abort_retry_consume_from(cluster_node *);

void schedule_retry_consume_from(cluster_node *);

const std::vector<topic_partition *> *partitions_to_replicate_from(cluster_node *);

void try_abort_replication(topic_partition *, cluster_node *, const uint32_t);

bool expected_connest(connection *);

#ifdef HWM_UPDATE_BASED_ON_ACKS
void consider_acknowledged_product_reqs(topic_partition *);
#endif

void consider_pending_client_produce_responses(topic_partition *, cluster_node *, const uint64_t);

void consider_pending_client_produce_responses(isr_entry *, topic_partition *, cluster_node *, const uint64_t);

void consider_pending_client_produce_responses(topic_partition *);

void complete_deferred_produce_response(produce_response *);

void confirm_deferred_produce_resp_partition(produce_response *, topic_partition *, const uint8_t);

void consider_scheduled_consume_retries();

void consider_deferred_produce_responses();

bool generate_produce_response(produce_response *);

bool try_generate_produce_response(produce_response *);

auto get_produce_response() {
	produce_response *r;

	if (!reusable_produce_responses.empty()) {
		r = reusable_produce_responses.back().release();
		reusable_produce_responses.pop_back();
	} else {
		r = new produce_response();
	}

	r->gen = ++next_produce_response_gen;
	r->reset();
	return r;
}

void put_produce_response(produce_response *r) {
	TANK_EXPECT(r);

	// need to clear here as well as in produce_response::reset()
	// because we don't get to do that in ~Service::reset()
	for (auto ptr : r->unknown_topic_names) {
		std::free(ptr);
	}
	r->unknown_topic_names.clear();


        r->gen = std::numeric_limits<uint64_t>::max();
	reusable_produce_responses.emplace_back(r);
}

void set_hwmark(topic_partition *, const uint64_t);

void set_hwmark(topic_partition *, const uint64_t, fd_handle *, const uint32_t);

uint64_t partition_hwmark(topic_partition *) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

void update_hwmark(topic_partition *, const topic_partition::Cluster::pending_ack_bundle_desc);

void update_hwmark(topic_partition *, const uint64_t);


#ifdef TRACK_ISR_MAX_ACKS
void update_isr_max_ack(topic_partition *, isr_entry *);
#endif

// we need to keep this sane, we don't want to wake up too often
// so we are just going to align this to 64ms
static inline uint64_t normalized_max_wait(uint64_t v) noexcept {
        if (!v) {
                // if 0 is explicitly requested, that's OK, wait it out
		// because likely the client needs to know immediately(very latency sensitive)
                return 0;
        }

        static const uint64_t alignment = 64;

        // we don't want to wait forever
        v = std::min<uint64_t>(v, Timings::Hours::ToMillis(1));

        static_assert(0 == (alignment & 1));
        static_assert(std::is_same<decltype(alignment), const uint64_t>::value);

        return std::max<uint64_t>(256 + 64, (v + alignment - 1) & -alignment);
}

void consider_highwatermark_update(topic_partition *, const uint64_t);

void consider_append_res(topic_partition *, append_res &, std::vector<wait_ctx *> *);

void consider_highwatermark_update(topic_partition *, const uint64_t, std::vector<wait_ctx *> *);

void consider_isr_pending_ack(topic_partition *);

void consider_isr_max_ack(topic_partition *);

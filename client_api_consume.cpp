#include "client_common.h"

// build a consume payload for a broker API request
TankClient::broker_outgoing_payload *TankClient::build_consume_broker_req_payload(const broker_api_request *broker_req) {
        static constexpr bool trace{false};
        auto                  payload = new_req_payload(const_cast<broker_api_request *>(broker_req));
        [[maybe_unused]] auto broker  = broker_req->br;
        auto                  b       = payload->b;
        auto                  api_req = broker_req->api_req;

        if (trace) {
                SLog("Building CONSUME request payload for ", broker_req->partitions_list.size(), " partitions\n");
        }

        TANK_EXPECT(b);

        b->pack(static_cast<uint8_t>(TankAPIMsgType::Consume)); // request type
        b->pack(static_cast<uint32_t>(0));                      // size (will patch)

        b->pack(static_cast<uint16_t>(2)); // client version
        b->pack(broker_req->id);
        b->pack(""_s8); // client identifier
        b->pack(api_req->as.consume.max_wait);
        b->pack(static_cast<uint32_t>(api_req->as.consume.min_size));

        auto       it = broker_req->partitions_list.next;
        uint8_t    topics_cnt{0};
        const auto topics_cnt_buf_offset = b->size();

        b->pack(static_cast<uint8_t>(0));

        // partitions are already grouped by topic in assign_req_partitions_to_api_req()
        while (it != &broker_req->partitions_list) {
                auto       partition_req = containerof(request_partition_ctx, partitions_list_ll, it);
                const auto topic         = partition_req->topic;

                b->pack(topic);

                uint8_t    partitions_cnt{0};
                const auto partitions_cnt_buf_offset = b->size();

                b->pack(static_cast<uint8_t>(0));
                do {
                        b->pack(partition_req->partition);
                        b->pack(partition_req->as_op.consume.seq_num);
                        b->pack(static_cast<uint32_t>(partition_req->as_op.consume.min_fetch_size));

                        if (trace) {
                                SLog("Requesting ", topic, "/", partition_req->partition, ", from ", partition_req->as_op.consume.seq_num, "\n");
                        }

                        ++partitions_cnt;
                } while ((it = it->next) != &broker_req->partitions_list && (partition_req = switch_list_entry(request_partition_ctx, partitions_list_ll, it))->topic == topic);

                *reinterpret_cast<uint8_t *>(b->data() + partitions_cnt_buf_offset) = partitions_cnt;
                ++topics_cnt;
        }

        *reinterpret_cast<uint8_t *>(b->data() + topics_cnt_buf_offset) = topics_cnt;
        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t))      = b->size() - sizeof(uint8_t) - sizeof(uint32_t); // patch

        payload->iovecs.data[0].iov_base = b->data();
        payload->iovecs.data[0].iov_len  = b->size();
        payload->iovecs.size             = 1;

        return payload;
}

uint32_t TankClient::consume(const std::vector<std::pair<topic_partition,
                                                         std::pair<uint64_t, uint32_t>>> &sources,
                             const uint64_t                                               max_wait,
                             const uint32_t                                               min_size) {

	return consume(sources.data(), sources.size(), max_wait, min_size);
}

uint32_t TankClient::consume(const std::pair<topic_partition, std::pair<uint64_t, uint32_t>> *sources, const size_t total_sources,
                             const uint64_t max_wait,
                             const uint32_t min_size) {
        static constexpr bool trace{false};
        // fan-out: api_req will coordinate broker api requests
        // NOTE: get_api_request() will update now_ms

        // We can have the client reactor track the api request for expiration (i.e
        // after max_wait or so, and if the API request hasn't been processed yet/is still around,
        // then reactor will explicitly abort it with a timeout error), or not. We can do that
        // by specifying a timeout in get_api_request() > 0.
        //
        // The problem with that otherwise good idea is that there are TANK client applications that may
        // block for a substantial amount of time between invoking an consume method and invoking poll() - which
        // is when the reactor runs and gets to check timeouts, network I/O etc - and that can result in
        // the reactor aborting the API request before TANK (service) even got to process it, just because
        // it's been too long since it was submitted via get_api_request()
        // Example:
        // {
        //	const auto req_id = tank_client.consume_from(, .. max_wait = 1000, ...);
        // 	slow_bootstrap_function();
        //	for (;;)  {   // application main event loop
        //	 	if (!req_id) { req_id = tank_client.consume_from(); }
        //		tank_client.poll(1000);
        //	}
        // }
        // In this example, the application first issues a consume request, then intializes state via slow_bootstrap_function()
        // which is the recommended way to initialize state that may be persisted locally and process all events that
        // may affect that state since boostrap commenced.
        // The problem is, if slow_bootstrap_function() takes say over 1s then as soon as tank_client.poll() is executed,
        // the reactor will likely abort the api request via check_pending_api_responses() because it's been too long
        // and the request hasn't been ready yet.
        //
        // Example:
        // {
        //	for (;;) {// application main event loop
        //		if (!req_id) { req_id = tank_client.consume_from(.., max_wait = 1000, ...); }
        //
        //		tank_client.poll(8000);
        //
        //		if (!tank_client.faults.empty()) {
        //			process_faults(tank_client);
        //			req_id = 0;
        // 		}
        //
        //		if (tank_client.consumed().empty()) { continue; }
        //
        // 		for (const auto &part : tank_client.consumed()) {
        //			for (const auto m : part.msgs()) {
        //				process_message(m);
        //			}
        //		}
        //		req_id = 0;
        //	}
        //
        // In this example, process_message() may take 1s, so next time poll() is called
        // the api request will likely be aborted as well.
        //
        // So it's probably a good idea to not have the client runtime abort the api request
        // i.e use get_api_request() with timeout set to 0
        // The problem with that though is that if the TANK node takes too long to respond (see
        // below for reasons why that may happen), the client will never know i.e
        // the api request will never become ready or timeout
        // Reasons include:
        // - node stuck for some reason (e.g SIGSTOP signal sent)
        // - network packets relay problems between the client and the node
        // - other reasons
        //
        // For now, we are not setting an explicitl timeout but this not ideal
        // We need something better. Perhaps we need to consider how long it's been since
        // the last call to poll() and adjust the timeout of all pending api requests by that time?
        // Not sure what's the right way to deal with it, but we need something more there.
#if 0
        auto api_req = get_api_request(max_wait ? max_wait + 1000 : 20 * 1000);
#else
        auto api_req = get_api_request(0);
#endif
        std::vector<std::pair<broker *, request_partition_ctx *>> contexts;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, ansifmt::inverse, "Will consume from ", total_sources, " sources", ansifmt::reset, "\n");
        }

        // initialize the api request for consume
        api_req->as.consume.max_wait = max_wait;
        api_req->as.consume.min_size = min_size;
        api_req->type                = api_request::Type::Consume;

        // we could have a generic method that returns
        // a decltype(contexts) and then we would
        // initialize the respective request partition context, but it's not currently necessary
        for (unsigned i = 0; i < total_sources; ++i) {
                const auto &it        = sources[i];
                auto        topic     = intern_topic(it.first.first);
                auto        partition = it.first.second;
                auto        req_part  = get_request_partition_ctx();
                // if we have a leader for this topic, choose it, explicitly
                // otherwise, select any broker and we 'll get to update assignments later
                // and potentially retry the request on another broker
                //
                // When we get back a response for a partition, we can use set_leader() to assign
                // that broker as the leader for it(in case it's not paired with that already).
                //
                // As an optimization, we could have tracked in request_partition_ctx wether
                // the broker is provided by partition_leader() or any_broker()
                // so that we would only set_leader() later if we got the broker from any_broker(), which
                // would save us an unordered_map<>::emplace.
                //
                // We are not doing this for now though.
                auto broker = partition_leader(topic, partition) ?: any_broker();

                // initialize the broker partition request context
                req_part->topic     = topic;
                req_part->partition = partition;

                req_part->as_op.consume.seq_num        = it.second.first;
                req_part->as_op.consume.min_fetch_size = it.second.second;

                contexts.emplace_back(std::make_pair(broker, req_part));

                if (trace) {
                        SLog("Fetch from ", req_part->topic, "/", req_part->partition, " min_fetch_size = ", it.second.second, " (", size_repr(it.second.second), ")\n");
                }

                TANK_EXPECT(req_part->topic == topic);
                TANK_EXPECT(req_part->partition == partition);
        }

        assign_req_partitions_to_api_req(api_req.get(), &contexts);

        TANK_EXPECT(api_req->type == api_request::Type::Consume);
        return schedule_new_api_req(std::move(api_req));
}

// TODO:
// There is an obvious optimization that we need to implement once we have the baseline functionality(i.e whatever's implemented currently) right
// so that we can both profile and measure against it, but also verify against it for correctness.
// Instead of waiting until we have received the whole response (say 64MBs worth of bundles), we can instead _progressively_. We would maintain
// a state machine, and we would progressively decode/parse bundles and message sets as we read in incoming data, so by the time we have received
// the whole response, we would have effecitvely also parsed everything, because the data are coming in in batches(i.e we don't get the whole
// response in one packet, unless its a tiny response).
// This provides for all kinds of interesting benefits:
// - when processing the whole request(all bundles), it will take a 'long' time to do it, which means, the threads will block until that happens, thereby
// 	slowing processing of all other connections/requests/tasks. By interleaving/doing this concurrently, we
// 	only block the thread for far shorter amounts of time, and because the data arrive slowly, we can process the new data, and by the time we
// 	are done parsing new bundles, new data would have arrived, thereby doing this in an alternating fashion.
// - this could potentially provide for much faster consume requests, which is especially important for clients/consumers
// - reducing blocking of the main thread because of the incremental processing semantics would not only benefit TANK (see Service::process_peer_consume_resp() impl.)
// 	but also the client because e.g AppServers may use a single client to multiple requests
bool TankClient::process_consume(connection *const c, const uint8_t *const content, const size_t len) {
        static constexpr bool trace{false};
        static constexpr bool trace_faults{true};
        TANK_EXPECT(c);
        TANK_EXPECT(c->type == connection::Type::Tank);
        const auto *p   = content;
        const auto  end = p + len;
        // the response header length is encoded in the response so that we can quickly jump to
        // the beginning of the bundles.
        // IMPORTANT: the bundles content of all involved partitions is streamed right AFTER the response header
        if (unlikely(p + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint8_t) > end)) {
                if (trace) {
                        SLog("Unable to decode header\n");
                }

                return false;
        }

        const auto                           hdr_size      = decode_pod<uint32_t>(p);
        const auto *                         bundles_chunk = p + hdr_size; // first partition's bundles chunk (see ^^ about partitions chunks)
        const auto                           req_id        = decode_pod<uint32_t>(p);
        const auto                           it            = pending_brokers_requests.find(req_id);
        str_view8                            key;
        std::vector<request_partition_ctx *> no_leader, retry; // TODO: reuse

#if 0
	{
		int fd = open("/tmp/consume.resp", O_WRONLY|O_TRUNC|O_CREAT, 0775);

		TANK_EXPECT(fd != -1);
		TANK_EXPECT(write(fd, content, len) == len);
		close(fd);
	}
#endif

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, ansifmt::inverse, "CONSUME response  len = ", len, " (", size_repr(len), ")", ansifmt::reset, "\n");
        }

        if (it == pending_brokers_requests.end()) {
                // this is probably fine; timed-out or whatever else
                // we are just going to ignore it
                if (trace) {
                        SLog("Ignoring, unknown broker request ", req_id, "(no longer around)\n");
                }

                return true;
        }

        auto                        br_req            = it->second;
        auto                        api_req           = br_req->api_req;
        auto                        br_req_partctx_it = br_req->partitions_list.next;
        bool                        retain_buffer     = false;
        const auto                  topics_cnt        = decode_pod<uint8_t>(p);
        bool                        any_faults        = false;
        [[maybe_unused]] const auto before            = Timings::Microseconds::Tick();
        std::vector<IOBuffer *>     used_buffers;

        DEFER({
                for (auto b : used_buffers) {
                        put_buffer(b);
                }
        });

        if (trace) {
                SLog("Response for topics ", topics_cnt, "\n");
        }

        for (size_t i{0}; i < topics_cnt; ++i) {
                const auto len = decode_pod<uint8_t>(p);

                if (unlikely(p + len + sizeof(uint8_t) > end)) {
                        if (trace) {
                                SLog("Unable to decode topic name and partitions count\n");
                        }

                        return false;
                }

                const str_view8 topic_name(reinterpret_cast<const char *>(p), len);
                p += len;
                const auto partitions_cnt = decode_pod<uint8_t>(p);
                uint64_t   log_base_seqnum;

                if (trace) {
                        SLog("Topic [", topic_name, "], for ", partitions_cnt, " partitions from ", c->as.tank.br->ep, " for request ", api_req->request_id, "\n");
                }

                if (unlikely(p + sizeof(uint16_t) * partitions_cnt > end)) {
                        if (trace) {
                                SLog("Not enough content for partitions\n");
                        }

                        return false;
                }

                if (*reinterpret_cast<const uint16_t *>(p) == std::numeric_limits<uint16_t>::max()) {
                        if (trace || trace_faults) {
                                SLog("Unknown topic [", topic_name, "]\n");
                        }

                        capture_unknown_topic_fault(api_req, topic_name);
                        any_faults = true;

                        // get rid of all partition contexts associated with this topic
                        do {
                                auto req_part = switch_list_entry(request_partition_ctx,
                                                                  partitions_list_ll,
                                                                  br_req_partctx_it);
                                auto next     = br_req_partctx_it->next;

                                if (trace) {
                                        SLog("Ignoring partition ", req_part->partition, "\n");
                                }

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                br_req_partctx_it = next;
                        } while (br_req_partctx_it != &br_req->partitions_list &&
                                 switch_list_entry(request_partition_ctx, partitions_list_ll, br_req_partctx_it)->topic == topic_name);

                        p += sizeof(uint16_t);
                        continue;
                }

                for (size_t k{0}; k < partitions_cnt; ++k) {
                        if (unlikely(p + sizeof(uint16_t) + sizeof(uint8_t) > end)) {
                                if (trace) {
                                        SLog("Unable to decode (partition id, err_flags)\n");
                                }

                                return false;
                        }

                        const auto partition_id = decode_pod<uint16_t>(p);
                        const auto err_flags    = decode_pod<uint8_t>(p);

                        if (trace) {
                                SLog(ansifmt::color_green, "For partition ", partition_id, ", err_flags ", err_flags, ansifmt::reset, "\n");
                        }

                        if (err_flags == 0xff) {
                                auto next = br_req_partctx_it->next;

                                if (trace || trace_faults) {
                                        SLog("Undefined partition ", topic_name, "/", partition_id, "\n");
                                }

                                capture_unknown_partition_fault(api_req, topic_name, partition_id);
                                any_faults = true;

                                TANK_EXPECT(br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req_partctx_it);

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                br_req_partctx_it = next;
                                continue;
                        } else if (err_flags == 0xfd) {
                                // no leader
                                auto next = br_req_partctx_it->next;

                                if (trace) {
                                        SLog("No current leader for ", topic_name, "/", partition_id, "\n");
                                }

                                TANK_EXPECT(br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req_partctx_it);

                                no_leader.emplace_back(req_part);

                                req_part->partitions_list_ll.detach_and_reset();
                                br_req_partctx_it = next;
                                continue;
                        } else if (err_flags == (0xfc)) {
                                // different leader
                                auto next = br_req_partctx_it->next;

                                if (unlikely(p + sizeof(uint32_t) + sizeof(uint16_t) > end)) {
                                        if (trace) {
                                                SLog("Unable to decode new leader\n");
                                        }

                                        return false;
                                }

                                const Switch::endpoint ep{decode_pod<uint32_t>(p), decode_pod<uint16_t>(p)};

                                TANK_EXPECT(br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = switch_list_entry(request_partition_ctx,
                                                                  partitions_list_ll,
                                                                  br_req_partctx_it);

                                if (trace) {
                                        SLog(ansifmt::color_cyan, "Leader for ", topic_name, "/", partition_id, " is now ", ep, ansifmt::reset, "\n");
                                }

                                set_leader(intern_topic(topic_name), partition_id, ep);
                                retry.emplace_back(req_part);

                                req_part->partitions_list_ll.detach_and_reset();
                                br_req_partctx_it = next;
                                continue;
                        } else if (err_flags == 0xfe) {
                                // the first bundle in the bundles chunk is a sparse bundle
                                // it encodes the first and last message seq.number in that bundle header
                                // so there is no base seq.number here
                                if (trace) {
                                        SLog("SPARSE bundle\n");
                                }

                                log_base_seqnum = 0;
                        } else {
                                // base abs. seq.num of the first message in the first bundle of this chunk
                                if (unlikely(p + sizeof(uint64_t) > end)) {
                                        if (trace) {
                                                SLog("Unable to decode log_base_seqnum\n");
                                        }

                                        return false;
                                }

                                log_base_seqnum = decode_pod<uint64_t>(p);

                                if (trace) {
                                        SLog("Not a sparse bundle, log_base_seqnum = ", log_base_seqnum, "\n");
                                }
                        }

                        TANK_EXPECT(br_req_partctx_it != &br_req->partitions_list);

                        auto req_part = switch_list_entry(request_partition_ctx,
                                                          partitions_list_ll, br_req_partctx_it);

                        // OK, we know that this broker is the current leader for this partition
                        // see comment sin TankClient::consume() about an optimization
                        set_leader(req_part->topic, req_part->partition, br_req->br->ep);

                        if (unlikely(p + sizeof(uint64_t) + sizeof(uint32_t) > end)) {
                                if (trace) {
                                        SLog("Unable to decode hw mark and bundles_chunk_len\n");
                                }

                                return false;
                        }

                        const auto highwater_mark    = decode_pod<uint64_t>(p);
                        const auto bundles_chunk_len = decode_pod<uint32_t>(p); // length of this particion's chunk that contains 0+ bundles
                        const auto requested_seqnum  = req_part->as_op.consume.seq_num;

                        if (trace) {
                                SLog("err_flags = ", err_flags,
                                     ", log_base_seqnum = ", log_base_seqnum,
                                     ", highwater_mark = ", highwater_mark,
                                     ", bundles_chunk_len = ", bundles_chunk_len, " (", size_repr(bundles_chunk_len), ")",
                                     ", requested_seqnum = ", requested_seqnum, "\n");
                        }

                        if (err_flags == 0x1) {
                                // boundary check fault
                                auto next = br_req_partctx_it->next;

                                if (unlikely(p + sizeof(uint64_t) > end)) {
                                        if (trace) {
                                                SLog("Unable to decode first_avail_seqnum\n");
                                        }

                                        return false;
                                }

                                const auto first_avail_seqnum = decode_pod<uint64_t>(p);

                                if (trace || trace_faults) {
                                        SLog("Boundary check failed first_avail_seqnum = ", first_avail_seqnum, ", highwater_mark = ", highwater_mark, ", requested_seqnum = ", requested_seqnum, "\n");
                                }

                                capture_boundary_access_fault(api_req, topic_name, partition_id, first_avail_seqnum, highwater_mark);
                                any_faults = true;

                                TANK_EXPECT(br_req_partctx_it != &br_req->partitions_list);
                                auto req_part = switch_list_entry(request_partition_ctx, partitions_list_ll, br_req_partctx_it);

                                clear_request_partition_ctx(api_req, req_part);
                                put_request_partition_ctx(req_part);

                                br_req_partctx_it = next;
                                continue;
                        } else if (err_flags && err_flags < 0xfe) {
                                // TODO: captured_faults, get rid of partition
                                IMPLEMENT_ME();
                                continue;
                        }

                        // this is the reliable to detect if we have drained a partition
                        // as opposed to e.g checking if no messages were captured and next.minFetchSize <= used_min_fetch_size
                        const auto drained_partition = (bundles_chunk_len == 0);
                        const auto partition_bundles = bundles_chunk;
                        uint64_t   first_msg_seqnum, last_msg_seqnum;

                        bundles_chunk += bundles_chunk_len; // skip bundles for this partition

                        // we need to know how much data we are going to need for
                        // the next request. Initially assume we 'll need the whole bundle, but
                        // we are going to adjust need_upto if we can depending on wether the message set of the bundle is compressed, etc
                        // This makes a lot more sense than the previous design
                        const uint8_t *need_from, *need_upto;
                        bool           any_captured{false};

                        // consider all bundles in this chunk
                        // last bundle in this chunk may be partial
                        msgs_bucket *first_bucket{nullptr}, *last_bucket{nullptr};
                        size_t       consumed         = 0;
                        uint32_t     last_bucket_size = sizeof_array(msgs_bucket::data);

                        if (trace) {
                                SLog("partition bundles_chunk_len = ", bundles_chunk_len, " (", size_repr(bundles_chunk_len), "), drained_partition = ", drained_partition, "\n");
                        }

                        used_buffers.clear();
                        // process all bundles in this partition's bundles chunk
                        for (const auto *p = partition_bundles, *const chunk_end = std::min(end, bundles_chunk);;) {
                                need_from = p;

                                if (unlikely(!Compression::check_decode_varuint32(p, chunk_end))) {
                                        if (trace) {
                                                SLog("Unable to decode bundle_len:", std::distance(need_from, chunk_end), "\n");
                                        }

                                        need_upto = p + 256;
                                        break;
                                }

                                const auto bundle_len = Compression::decode_varuint32(p);
                                const auto bundle_end = p + bundle_len;

                                // assume we will need until the end of the bundle at least
                                // we will adjust this as we understand better the response structure
                                need_upto = bundle_end + 256;

                                if (trace) {
                                        SLog("bundle_len = ", bundle_len, "(", size_repr(bundle_len), ") at ", std::distance(content, p), "\n");
                                }

                                if (unlikely(p >= chunk_end)) {
                                        if (trace) {
                                                SLog("Unable to decode bundle_hdr_flags\n");
                                        }

                                        break;
                                }

                                // BEGIN: bundle header
                                const auto                          bundle_hdr_flags = decode_pod<uint8_t>(p);
                                const auto                          codec            = bundle_hdr_flags & 3;
                                const auto                          sparse_bundle    = bundle_hdr_flags & (1u << 6);
                                uint32_t                            msgset_size      = (bundle_hdr_flags >> 2) & 0xf;
                                uint64_t                            msgset_end;
                                range_base<const uint8_t *, size_t> msgset_content;

                                if (0 == msgset_size) {
                                        if (unlikely(!Compression::check_decode_varuint32(p, chunk_end))) {
                                                break;
                                        } else {
                                                msgset_size = Compression::decode_varuint32(p);
                                        }
                                }

                                if (trace) {
                                        SLog("codec = ", codec, ", msgset_size = ", msgset_size, ", sparse_bundle = ", sparse_bundle, "\n");
                                }

                                if (sparse_bundle) {
                                        if (unlikely(p + sizeof(uint64_t) >= chunk_end)) {
                                                break;
                                        }

                                        first_msg_seqnum = decode_pod<uint64_t>(p);

                                        if (msgset_size != 1) {
                                                if (unlikely(!Compression::check_decode_varuint32(p, chunk_end))) {
                                                        break;
                                                }

                                                last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1;
                                        } else {
                                                last_msg_seqnum = first_msg_seqnum;
                                        }

                                        log_base_seqnum = first_msg_seqnum;
                                        msgset_end      = last_msg_seqnum + 1;

                                        if (trace) {
                                                SLog("sparse: first_msg_seqnum = ", first_msg_seqnum, ", msgset_end = ", msgset_end, "\n");
                                        }
                                } else {
                                        msgset_end = log_base_seqnum + msgset_size;
                                }
                                // END: bundle header

                                if (requested_seqnum < std::numeric_limits<uint64_t>::max() && requested_seqnum >= msgset_end) {
                                        // fast path: skip this bundle
                                        p               = bundle_end;
                                        log_base_seqnum = msgset_end;

                                        if (trace) {
                                                SLog("Skipping content bundle because requested_seqnum(", requested_seqnum, ") >= msgset_end(", msgset_end, ")\n");
                                        }

                                        continue;
                                }

                                if (codec) {
                                        if (trace) {
                                                SLog("Need to decompress for ", codec, "\n");
                                        }

                                        if (bundle_end > chunk_end) {
                                                if (trace) {
                                                        SLog("Not enough bytes: ", std::distance(chunk_end, bundle_end), " (", size_repr(std::distance(chunk_end, bundle_end)), ") more required\n");
                                                }

                                                break;
                                        }

                                        auto b = get_buffer();

                                        used_buffers.emplace_back(b);
                                        switch (codec) {
                                                case 1:
                                                        if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, std::distance(p, bundle_end), b)) {
                                                                IMPLEMENT_ME();
                                                        }
                                                        break;

                                                default:
                                                        IMPLEMENT_ME();
                                        }

                                        msgset_content.set(reinterpret_cast<const uint8_t *>(b->data()), b->size());
                                } else {
                                        retain_buffer = true;
                                        msgset_content.set(p, std::min(std::distance(p, chunk_end), std::distance(p, bundle_end)));
                                }

                                // advance p past this bundle
                                p = bundle_end;

                                // scan this bundle's message set
                                uint64_t   ts                  = 0;
                                uint32_t   msg_idx             = 0;
                                const auto min_accepted_seqnum = requested_seqnum == std::numeric_limits<uint64_t>::max() ? 0 : requested_seqnum;

                                if (trace) {
                                        SLog("Scanning message set of length ", size_repr(msgset_content.size()), "\n");
                                }

                                for (const auto *p = msgset_content.offset, *const msgset_end = p + msgset_content.size();; ++msg_idx, ++log_base_seqnum) {
                                        if (!codec && any_captured) {
                                                // this makes sense because we didn't need to decompress the bundle
                                                need_upto = p + 256;
                                        }

                                        if (unlikely(p + sizeof(uint8_t) > msgset_end)) {
                                                // likely hit end of the message set
                                                // i.e (p == msgset_end)
                                                if (trace && p != msgset_end) {
                                                        SLog("Unable to decode msg_flags\n");
                                                }

                                                // important; don't goto next_partition here
                                                // (technically, you should jmp next_partition if (p == msgset_end)
                                                // and break otherwise
                                                break;
                                        }

                                        const auto msg_flags = decode_pod<uint8_t>(p);

                                        if (sparse_bundle) {
                                                if (trace) {
                                                        SLog("Yes, sparse_bundle\n");
                                                }

                                                if (msg_flags & unsigned(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                        // (prev + 1)
                                                        // no need to advance log_base_seqnum, was advanced in for()
                                                        if (trace) {
                                                                SLog("seqnum++\n");
                                                        }
                                                } else if (0 == msg_idx) {
                                                        log_base_seqnum = first_msg_seqnum;

                                                        if (trace) {
                                                                SLog("First, setting to ", first_msg_seqnum, "\n");
                                                        }
                                                } else if (msg_idx == msgset_size - 1) {
                                                        log_base_seqnum = last_msg_seqnum;

                                                        if (trace) {
                                                                SLog("Last, setting to ", last_msg_seqnum, "\n");
                                                        }
                                                } else {
                                                        if (unlikely(!Compression::check_decode_varuint32(p, msgset_end))) {
                                                                if (trace) {
                                                                        SLog("Can't decode delta\n");
                                                                }

                                                                goto next_partition;
                                                        }

                                                        const auto delta = Compression::decode_varuint32(p);

                                                        if (trace) {
                                                                SLog("Advance by ", delta + 1, "\n");
                                                        }

                                                        // not going to set log_base_seqnum to (delta + 1)
                                                        // because we 'll (++log_base_seqnum) at the end of the iteration anyway
                                                        log_base_seqnum += delta;
                                                }
                                        }

                                        const auto msg_abs_seqnum = log_base_seqnum;

                                        if (trace) {
                                                SLog("abs.seqnum = ", msg_abs_seqnum, "\n");
                                        }

                                        if (0 == (msg_flags & unsigned(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                                if (unlikely(p + sizeof(uint64_t) > msgset_end)) {
                                                        if (trace) {
                                                                SLog("Not Enough Content Available\n");
                                                        }

                                                        goto next_partition;
                                                }

                                                ts = decode_pod<uint64_t>(p);

                                                if (trace) {
                                                        SLog("New TS", ts, "\n");
                                                }
                                        } else if (trace) {
                                                SLog("Using last TS\n");
                                        }

                                        if (msg_flags & unsigned(TankFlags::BundleMsgFlags::HaveKey)) {
                                                if (unlikely(p + sizeof(uint8_t) > msgset_end || (p + *p + sizeof(uint8_t) > msgset_end))) {
                                                        if (trace) {
                                                                SLog("Not Enough Content Available\n");
                                                        }

                                                        goto next_partition;
                                                } else {
                                                        key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                                        p += sizeof(uint8_t) + key.size();
                                                }
                                        } else {
                                                key.reset();
                                        }

                                        if (unlikely(!Compression::check_decode_varuint32(p, msgset_end))) {
                                                if (trace) {
                                                        SLog("Not Enough Content Available\n");
                                                }

                                                goto next_partition;
                                        }

                                        const auto len = Compression::decode_varuint32(p);

                                        if (trace) {
                                                SLog("Message content length:", len, "\n");
                                        }

                                        if (const auto e = p + len; e > msgset_end) {
                                                if (!codec && any_captured) {
                                                        // see above
                                                        need_upto = e + 256;
                                                }

                                                if (trace) {
                                                        SLog("Past msgset_end, needed additional:", e - msgset_end, " bytes to accept content\n");
                                                }

                                                goto next_partition;
                                        }

                                        if (msg_abs_seqnum > highwater_mark) {
                                                // abort immediately
                                                // we need to respect the semantics
                                                if (trace) {
                                                        SLog("Past HW mark\n");
                                                }

                                                goto next_partition;
                                        } else if (msg_abs_seqnum >= min_accepted_seqnum) {
                                                const str_view32 content(reinterpret_cast<const char *>(p), len);

                                                if (last_bucket_size == sizeof_array(msgs_bucket::data)) {
                                                        auto b = get_msgs_bucket();

                                                        b->next = nullptr;
                                                        if (last_bucket) {
                                                                consumed += last_bucket_size;
                                                                last_bucket->next = b;
                                                        } else {
                                                                first_bucket = b;
                                                        }

                                                        last_bucket      = b;
                                                        last_bucket_size = 0;
                                                }

                                                auto m = last_bucket->data + last_bucket_size++;

                                                any_captured = true;
                                                m->seqNum    = msg_abs_seqnum;
                                                m->content   = content;
                                                m->ts        = ts;
                                                m->key       = key;

                                                if (trace && false) {
                                                        SLog("Got key = [", key, "] ts = ",
                                                             Date::ts_repr(ts / 1000),
                                                             ", content.len = ", size_repr(content.size()), "\n");
                                                }
                                        }

                                        p += len; // to next bundle for this partition
                                }
                        }

                next_partition:
                        TANK_EXPECT(need_from <= need_upto);

                        if (last_bucket) {
                                consumed += last_bucket_size;
                        }

                        auto       next          = br_req_partctx_it->next;
                        const auto next_min_span = std::distance(need_from, need_upto); // TODO: + 256
                        const auto next_seqnum   = consumed
                                                     ? requested_seqnum == std::numeric_limits<uint64_t>::max()
                                                           ? last_bucket->data[last_bucket_size - 1].seqNum + 1
                                                           : std::max(requested_seqnum, last_bucket->data[last_bucket_size - 1].seqNum + 1)
                                                     : requested_seqnum == std::numeric_limits<uint64_t>::max() ? highwater_mark + 1 : requested_seqnum;
                        auto &req_part_resp = req_part->as_op.consume.response;

                        if (trace) {
                                SLog(ansifmt::bgcolor_red, "consumed = ", consumed,
                                     ", used_buffers = ", used_buffers.size(),
                                     ", next_min_span = ", next_min_span, " (", size_repr(next_min_span), " )",
                                     ", next_seqnum = ", next_seqnum, ansifmt::reset, "\n");
                        }

                        req_part_resp.next.seq_num  = next_seqnum;
                        req_part_resp.next.min_size = next_min_span;
                        req_part_resp.msgs.cnt      = consumed;
                        req_part_resp.drained       = drained_partition;

                        if (const auto n = used_buffers.size()) {
                                req_part_resp.used_buffers.size = n;
                                req_part_resp.used_buffers.data = static_cast<IOBuffer **>(malloc(sizeof(IOBuffer *) * n));

                                memcpy(req_part_resp.used_buffers.data, used_buffers.data(), sizeof(IOBuffer *) * n);
                        } else {
                                req_part_resp.used_buffers.size = 0;
                        }
                        used_buffers.clear();

                        if (consumed) {
                                auto out = consumed <= sizeof_array(req_part_resp.msgs.list.small)
                                               ? req_part_resp.msgs.list.small + 0
                                               : (req_part_resp.msgs.list.large = static_cast<consumed_msg *>(malloc(sizeof(consumed_msg) * consumed)));
                                auto it = first_bucket;

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
                        br_req_partctx_it = next;
                }
        }

        // stop tracking this
        unlink_broker_req(br_req, __LINE__);

        // dispose of it
        put_broker_api_request(br_req);

        if (retain_buffer) {
                retain_conn_inbuf(c, api_req);
        }

        update_api_req(api_req, any_faults, &no_leader, &retry);

        // 1.362s for 128MBs
        // when streaming overseer messages from 10.5.5.45
        // and about 6ms for 500k
        // once the implementation of the optimization described earlier is ready
        // it should take maybe an order of magnitude less because we will interleave processing with transfer
        // ~25% of the time is spent on snappy decompression
        if (trace) {
                SLog("any_faults = ", any_faults,
                     ", retry.size = ", retry.size(),
                     ", no_leader.size = ", no_leader.size(),
                     ", ready = ", api_req->ready(),
                     ", broker_requests_list.size = ", api_req->broker_requests_list.size(), "\n");
        }

        if (trace) {
                SLog(duration_repr(Timings::Microseconds::Since(before)), " for ", size_repr(len), "\n");
        }

        if (api_req->ready() || any_faults) {
                make_api_req_ready(api_req, __LINE__);
        }

        return true;
}

uint32_t TankClient::consume_from(const topic_partition &from,
                                  const uint64_t         seqNum,
                                  const uint32_t         minFetchSize,
                                  const uint64_t         maxWait,
                                  const uint32_t         minSize) {
        const std::vector<std::pair<topic_partition, std::pair<uint64_t, uint32_t>>> sources{
            std::make_pair(from, std::make_pair(seqNum, minFetchSize))};

        return consume(sources, maxWait, minSize);
}

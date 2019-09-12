#include "service_common.h"

bool adjust_range_start(lookup_res &res, const uint64_t abs_seq_num, robin_hood::unordered_map<uint64_t, adjust_range_start_cache_value> *const cache);

// we need to discriminate between requests originating from peers and clients
// requests originating from clients are subject to a partitions' high water mark, that is the committed offset
// When TANK is cluster_aware(), the committed offset is partition_hwmark(), otherwise it's partition_log()->lastAssignedSeqNum
// For client requests, there is no such explict limit.
//
// will guard against malformed requests
bool Service::process_consume(const TankAPIMsgType _msg, connection *const c, const uint8_t *p, const size_t _len) {
        static constexpr bool trace{false};
	static constexpr bool trace_faults{true};
        const bool            consume_peer_req = _msg == TankAPIMsgType::ConsumePeer;
        const auto            consume_req      = !consume_peer_req;
        const auto            ca               = cluster_aware();
        auto                  self             = cluster_state.local_node.ref;
        const auto            end              = p + _len;

	if (trace) {
		SLog("====================================| Processing CONSUME request for ", _len, "\n");
	}
	

        try {
                bool          respond_now{false};
                uint16_t      replica_id;
                uint16_t      client_version;
                uint32_t      request_id;
                cluster_node *peer;

                if (_msg == TankAPIMsgType::Consume) {
                        if (unlikely(p + sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint8_t) > end)) {
                                return shutdown(c, __LINE__);
                        }

                        client_version = decode_pod<uint16_t>(p);
                        request_id     = decode_pod<uint32_t>(p);

                        const strwlen8_t client_id(reinterpret_cast<const char *>(p) + 1, *p);

                        p += client_id.size() + sizeof(uint8_t);

                        replica_id = 0;
                        peer       = nullptr;

			if (trace) {
				SLog("Consume request from client [", client_id, "] version ", client_version, ", request_id ", request_id, "\n");
			}
                } else {
                        if (unlikely(p + sizeof(uint16_t) > end)) {
                                return shutdown(c, __LINE__);
                        }

                        replica_id     = decode_pod<uint16_t>(p);
                        client_version = 0;
                        request_id     = 0;
                        peer           = cluster_state.find_node(replica_id);
                }

		if (trace) {
			SLog("**New CONSUME request of type [", _msg == TankAPIMsgType::Consume ? "Consume":"ConsumePeer", "]\n");
		}


                if (unlikely(p + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint16_t) > end)) {
			if (trace) {
				SLog("Shutting down CONSUME request connection because missing ", std::distance(end, p + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint16_t)), " for header\n");
			}

                        return shutdown(c, __LINE__);
                }

#if 1
                const auto max_wait = normalized_max_wait(decode_pod<uint64_t>(p));
#else
                auto max_wait = normalized_max_wait(decode_pod<uint64_t>(p));

                max_wait = 120 * 1000;
#endif
                const auto     min_bytes  = std::min<uint32_t>(decode_pod<uint32_t>(p), 64 * 1024 * 1024); // keep it sane
                const uint16_t topics_cnt = consume_peer_req ? decode_pod<uint16_t>(p) : decode_pod<uint8_t>(p);
                auto           resp_hdr   = get_buf();
                uint32_t       patch_list_size{0};
                uint8_t        partitions_requested_eof_patch_list_indices[256];
                const auto     track_partition_eof = [&, this](auto partition) {
                        const auto l = resp_hdr->size();

                        // commit current range
                        patch_list[patch_list_size++].SetEnd(l);

                        // reserve a new range here for this partition
                        partitions_requested_eof_patch_list_indices[partitions_requested_eof.size()] = patch_list_size++;

                        // start a new range
                        patch_list[patch_list_size].offset = l;

                        // track the partition
                        partitions_requested_eof.push_back(partition);

                        if (trace) {
                                SLog("Into partitions_requested_eof\n");
                        }
                };

                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_magenta, "New CONSUME request for topicsCnt = ", topics_cnt, ansifmt::reset, "\n");
                }

                resp_hdr->pack(static_cast<uint8_t>(_msg));
                const auto size_offset = resp_hdr->size();
                resp_hdr->RoomFor(sizeof(uint32_t)); // will patch later

                // This is an optimization for the client
                // we 'll store the response length header
                const auto hdrsize_offset = resp_hdr->size();
                resp_hdr->RoomFor(sizeof(uint32_t)); // will patch later

                if (consume_req) {
                        resp_hdr->pack(request_id);
                        resp_hdr->pack(static_cast<uint8_t>(topics_cnt));
                } else {
                        resp_hdr->pack(static_cast<uint16_t>(topics_cnt));
                }

                size_t      sum{0};
                auto        header_payload = get_data_vector_payload();
                auto *const q              = c->outQ ?: (c->outQ = get_outgoing_queue());
                auto *const saved_back     = q->back();

                header_payload->buf = resp_hdr;
                q->push_back(header_payload);

                patch_list[0].offset = 0;
                partitions_requested_eof.clear();

                if (unlikely(p + topics_cnt * sizeof(uint8_t) > end)) {
                        put_buf(resp_hdr);
                        return shutdown(c, __LINE__);
                }

                for (uint32_t i{0}; i < topics_cnt; ++i) {
                        if (unlikely(p + (*p) + sizeof(uint16_t) > end)) {
                                put_buf(resp_hdr);
                                return shutdown(c, __LINE__);
                        }

                        const str_view8 topic_name(reinterpret_cast<const char *>(p) + 1, *p);
                        p += topic_name.size() + sizeof(uint8_t);

                        const uint16_t partitions_cnt = consume_req ? decode_pod<uint8_t>(p) : decode_pod<uint16_t>(p);
                        auto           topic          = topic_by_name(topic_name);

                        if (trace) {
                                SLog("topic [", topic_name, "]\n");
                        }

                        resp_hdr->pack(topic_name.size());
                        resp_hdr->serialize(topic_name.data(), topic_name.size());
                        if (consume_req) {
                                resp_hdr->pack(static_cast<uint8_t>(partitions_cnt));
                        } else {
                                resp_hdr->pack(static_cast<uint16_t>(partitions_cnt));
                        }

                        if (!topic) {
                                if (trace) {
                                        SLog("Unknown topic [", topic_name, "]\n");
                                }

                                p += (sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t)) * partitions_cnt;

                                if (unlikely(p > end)) {
                                        put_buf(resp_hdr);
                                        return shutdown(c, __LINE__);
                                }

                                // Absuse scheme so that we won't have another field for this fault
                                // Set next/first partition id to UINT16_MAX
                                resp_hdr->pack<uint16_t>(std::numeric_limits<uint16_t>::max());
                                respond_now = true;
                                continue;
                        }

                        if (trace) {
                                SLog(partitions_cnt, " for topic [", topic_name, "]\n");
                        }

                        if (unlikely(p + (sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t)) * partitions_cnt > end)) {
                                put_buf(resp_hdr);
                                return shutdown(c, __LINE__);
                        }

                        for (uint32_t k{0}; k < partitions_cnt; ++k) {
                                const auto partition_id = decode_pod<uint16_t>(p);
                                auto       abs_seq_num  = decode_pod<uint64_t>(p);
                                const auto fetch_size   = decode_pod<uint32_t>(p);
                                auto       partition    = topic->partition(partition_id);

                                // TODO(markp): we should probably limit this to say a few MBs at most
                                // so that clients won't be able to abuse/miuse tank
                                //
                                // We just need to make sure no single event is larger than whatever threshold we choose
                                resp_hdr->pack(partition_id);

                                if (!partition || !partition->enabled()) {
                                        if (trace) {
                                                SLog("Undefined partition ", partition_id, "\n");
                                        }

                                        resp_hdr->pack(static_cast<uint8_t>(0xff));
                                        respond_now = true;
                                        continue;
                                }

                                if (ca) {
                                        auto partition_leader = partition->cluster.leader.node;

                                        if (!partition_leader) {
                                                // no leader? this makes no sense
						// likely a transient issue
                                                if (trace || true) {
                                                        SLog("No Leader for ", partition->owner->name(), "/", partition->idx, "\n");
                                                }

                                                resp_hdr->pack(static_cast<uint8_t>(0xfd));
                                                respond_now = true;
                                                continue;
                                        } else if (partition_leader != self) {
                                                // ask to redirect for this partition
                                                if (trace) {
                                                        SLog("Different leader\n");
                                                }

                                                resp_hdr->pack(static_cast<uint8_t>(0xfc));
                                                resp_hdr->pack(partition_leader->ep.addr4, partition_leader->ep.port);
                                                respond_now = true;
                                                continue;
                                        }
                                }

                                auto log = partition_log(partition);

                                if (trace) {
                                        SLog(ansifmt::color_green, ansifmt::inverse, "> REQUEST FOR partition ", partition_id, ", absSeqNum ", abs_seq_num,
                                             ", fetchSize ", fetch_size, " firstAvailableSeqNum = ", log->firstAvailableSeqNum, ", lastAssignedSeqNum = ", log->lastAssignedSeqNum, ansifmt::reset, "\n");
                                }

                                if (abs_seq_num == UINT64_MAX) {
                                        if (cluster_aware()) {
                                                abs_seq_num = partition_hwmark(partition) + 1;

                                                if (trace) {
                                                        SLog("Requested EOF, set to (hwmark+1) => ", abs_seq_num, "\n");
                                                }
                                                goto l100;
                                        } else {
                                                track_partition_eof(partition);
                                        }
                                } else {
                                l100:
                                        range32_t  range;
                                        bool       first_bundle_is_sparse;
                                        uint64_t   start;
                                        const bool fetch_only_committed = consume_req;
                                        auto       res                  = partition->read_from_local(fetch_only_committed, abs_seq_num, fetch_size);
                                        const auto hwmark               = partition_hwmark(partition);
                                        const auto ceil_seqnum          = (false == cluster_aware() || _msg != TankAPIMsgType::Consume)
                                                                     ? partition_log(partition)->lastAssignedSeqNum
                                                                     : hwmark;

                                        switch (res.fault) {
                                                case lookup_res::Fault::PastMax: {
                                                        // we attempted to read past the highwater mark(i.e last committed message seq.num)
                                                        const auto hwmark = partition->highwater_mark.seq_num;

                                                        if (trace) {
                                                                SLog("Fault::PastMax ", abs_seq_num, ", hwmark = ", hwmark, "\n");
                                                        }

                                                        if (abs_seq_num == hwmark + 1) {
                                                                if (trace) {
                                                                        SLog("Attempted to fetch starting from hwmark(", hwmark, ") + 1\n");
                                                                }

                                                                // register_consumer_wait()
                                                                // will see that we do the right thing here
                                                                track_partition_eof(partition);
                                                        } else {
                                                                resp_hdr->pack(static_cast<uint8_t>(1));
                                                                resp_hdr->pack(static_cast<uint64_t>(0));
                                                                resp_hdr->pack(ceil_seqnum);
                                                                resp_hdr->pack(static_cast<uint32_t>(0));
                                                                {
                                                                        // Only for this specific fault
                                                                        resp_hdr->Serialize<uint64_t>(partition_log(partition)->firstAvailableSeqNum);
                                                                }

                                                                respond_now = true;

                                                                if (trace || trace_faults) {
                                                                        SLog(ansifmt::color_green, ansifmt::inverse, "> REQUEST FOR partition ", partition_id, ", absSeqNum ", abs_seq_num, " (request:", _msg == TankAPIMsgType::Consume ? "CONSUME" : "CONSUME PEER", ")", 
                                                                             ", fetchSize ", fetch_size, " firstAvailableSeqNum = ", log->firstAvailableSeqNum, ", lastAssignedSeqNum = ", log->lastAssignedSeqNum, ", hwmark = ", hwmark, ansifmt::reset, "\n");
                                                                        SLog("Treating as BOUNDARY CHECK fault\n");
                                                                }


								// XXX:
                                                                //  if (trace_faults) { SLog("EXITING\n"); std::abort(); }
                                                        }
                                                } break;

                                                case lookup_res::Fault::NoFault:
                                                        // for promethus metrics
                                                        start = Timings::Microseconds::Tick();

                                                        if (log && abs_seq_num >= log->cur.baseSeqNum) {
                                                                // we can use the cache
                                                                first_bundle_is_sparse = adjust_range_start(res, abs_seq_num, &log->cur.triangulation_cache);
                                                        } else {
                                                                first_bundle_is_sparse = adjust_range_start(res, abs_seq_num, nullptr);
                                                        }

                                                        range.Set(res.fileOffset, fetch_size);

                                                        if (trace) {
                                                                SLog(ansifmt::bold, ansifmt::color_green, "Initial range ", range, ansifmt::reset, " for ", abs_seq_num, "\n");
                                                        }

                                                        if (range.stop() > res.fileOffsetCeiling) {
                                                                range.SetEnd(res.fileOffsetCeiling);

                                                                if (trace) {
                                                                        SLog("Adjuted to ", range, "\n");
                                                                }
                                                        }

                                                        if (trace) {
                                                                SLog(ansifmt::bold, "Response:(base_seqnum = ",
                                                                     res.absBaseSeqNum, ", range ", range, ", first_bundle_is_sparse = ", first_bundle_is_sparse, ")", ansifmt::reset, "\n");
                                                        }

                                                        if (first_bundle_is_sparse) {
                                                                // Set special errorOrFlags to let the client know that we are not going to encode here the seq.num of the first msg of the first bundle, because
                                                                // the first bundle we are streaming is a 'sparse bundle', which means it encodes the absolute sequence number of its first message
                                                                // in the bundle header anyway
                                                                resp_hdr->pack(static_cast<uint8_t>(0xfe)); // errorOrFlags

                                                                if (trace) {
                                                                        SLog("Setting errorOrFlags to 0xfe (first bundle is sparse)\n");
                                                                }

                                                        } else {
                                                                resp_hdr->pack(static_cast<uint8_t>(0)); // errorOrFlags
                                                                resp_hdr->pack(res.absBaseSeqNum);       // absolute first seq.num of the first message of the first bundle in the streamed chunk
                                                        }

                                                        resp_hdr->pack(ceil_seqnum);
                                                        resp_hdr->pack(range.len);

#ifdef __linux__
                                                        // Initiate readahead on that range so that our subsequent sendfile() from that file will be satisfied from the cache, and will not block on disk I/O
                                                        // (assuming we have initiated readahead early enough and other activity on the system did not in the meantime flush pages from cache)
                                                        //
                                                        // This syscall attempts to schedule the read in the background and return immediately.
                                                        // However, it may block while it reads the FS metadata needed to locate the requested blocks. This occurs frquently with ext[234] on large files
                                                        // using indirect blocks instead of extents, giving the appearance that the call blocks until the requested data have been read.
                                                        //
                                                        // XXX: I need to find out if readahead() will only read pages not already paged-in, or will re-read pages even if already resident in memory.
                                                        // XXX: I am not sure if this is a good idea - need to further measure the impact and gains
                                                        //
                                                        // UPDATE:
                                                        // http://lxr.free-electrons.com/source/mm/readahead.c
                                                        // 	Looks like it will inly deal with pages not mapped yet. The cost should be mininal, though
                                                        // 	the kernel does have to iterate all pages in the range and look each of those in a RBT.

                                                        if (true) {
                                                                // See https://github.com/phaistos-networks/TANK/issues/14 for measurements
                                                                const uint64_t b = trace ? Timings::Microseconds::Tick() : 0;

                                                                readahead(res.fdh->fd, range.offset, range.len);

                                                                if (trace) {
                                                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(b)), " for readahead(", range, ") ", size_repr(range.len), "\n");
                                                                }
                                                        }
#endif

                                                        sum += range.len;

                                                        {
                                                                auto p = get_file_contents_payload();

                                                                p->init(res.fdh.get(), range, start, topic);
                                                                q->push_back(p);

                                                                TANK_EXPECT(p->file_range.fdhandle);
                                                                TANK_EXPECT(p->file_range.fdhandle->use_count() > 1);
                                                        }

                                                        respond_now = true;
                                                        break;

                                                case lookup_res::Fault::AtEOF: {
                                                        if (trace) {
                                                                SLog("Got AtEOF; will wait\n");
                                                        }

                                                        track_partition_eof(partition);
                                                        break;
                                                }

                                                case lookup_res::Fault::Empty:
                                                        resp_hdr->pack(uint8_t(0));
                                                        resp_hdr->pack(res.absBaseSeqNum);
                                                        resp_hdr->pack(ceil_seqnum);
                                                        resp_hdr->pack(uint32_t(0));

                                                        if (trace) {
                                                                SLog("**EMPTY**\n");
                                                        }

                                                        respond_now = true;
                                                        break;

                                                case lookup_res::Fault::BoundaryCheck:
							if (trace) {
								SLog("RETURNING boundary check\n");
							}

                                                        resp_hdr->pack(uint8_t(1));
                                                        resp_hdr->pack(uint64_t(0));
                                                        resp_hdr->pack(ceil_seqnum);
                                                        resp_hdr->pack(uint32_t(0));

                                                        // Only for this specific fault
                                                        resp_hdr->Serialize<uint64_t>(partition_log(partition)->firstAvailableSeqNum);

                                                        if (trace) {
                                                                SLog("Boundary Check\n");
                                                        }

                                                        respond_now = true;
                                                        break;
                                        }
                                }

                                if (consume_peer_req && peer) {
                                        peer_consumed_local_partition(partition, peer, abs_seq_num);
                                }
                        }
                }

                if (trace) {
                        SLog("respond_now = ", respond_now, ", maxWait = ", max_wait, "\n");
                }

                // TODO(markp): https://github.com/phaistos-networks/TANK/issues/17#issuecomment-236106945
                // (don't respond even if we have any data, amount >= minBytes)
                if (respond_now || 0 == max_wait) {
                        uint32_t   extra = 0;
                        const auto n     = partitions_requested_eof.size();

                        if (trace) {
                                SLog("Responding now, partitions_requested_eof.size() = ", partitions_requested_eof.size(), "\n");
                        }

                        // terminate current range
                        patch_list[patch_list_size++].SetEnd(resp_hdr->size());

                        for (uint32_t i{0}; i < n; ++i) {
                                const auto idx = partitions_requested_eof_patch_list_indices[i];
                                const auto o   = resp_hdr->size();
                                auto       p   = partitions_requested_eof[i];
                                auto       log = partition_log(p);

                                resp_hdr->pack(static_cast<uint8_t>(0));
                                resp_hdr->pack(log->firstAvailableSeqNum);
                                resp_hdr->pack(p->hwmark());
                                resp_hdr->pack(static_cast<uint32_t>(0));

                                // we reserved this range in track_partition_eof()
                                // initialize it now to this new segement
                                patch_list[idx] = {o, resp_hdr->size() - o};
                        }

                        // patch response size
                        // and header size. The header which includes information about all involved
                        // topcis and partitions preceeds all payloads.  header{topic/partitions....} payloads{payload...}
                        *reinterpret_cast<uint32_t *>(resp_hdr->At(size_offset))    = resp_hdr->size() - size_offset - sizeof(uint32_t) + sum + extra;
                        *reinterpret_cast<uint32_t *>(resp_hdr->At(hdrsize_offset)) = resp_hdr->size() - hdrsize_offset - sizeof(uint32_t) + extra;

                        if (trace) {
                                SLog("resp_hdr.length = ", resp_hdr->size(), " ", *(uint32_t *)resp_hdr->At(size_offset), "\n");
                        }

                        header_payload->set_iov(patch_list, patch_list_size);
                        return try_tx(c);
                } else {
                        // Can't respond; we 'll need to wait until we have any data for any of those
                        // topic/partitions first

                        if (trace) {
                                SLog("Cannot respond yet (", q->size(), ")\n");
                        }

                        // erase any payloads appended to the outgoing queue in this method
			// XXX: this crashed once, why ? figure it out
                        if (saved_back) {
                                for (auto it = saved_back->next; it;) {
                                        auto next = it->next;

                                        release_payload(it);
                                        it = next;
                                }
                                saved_back->next = nullptr;
                        } else {
                                for (auto it = q->front(); it;) {
                                        auto next = it->next;

                                        release_payload(it);
                                        it = next;
                                }

                                q->front_ = q->back_ = nullptr;
                        }

                        if (q->empty()) {
                                if (trace) {
                                        SLog("No longer needed outQ\n");
                                }

                                put_outgoing_queue(q);
                                c->outQ = nullptr;
                        }

                        if (trace) {
                                SLog("Registering wait for ", unsigned(_msg), "\n");
                        }

                        return register_consumer_wait(_msg, c, request_id, max_wait, min_bytes, partitions_requested_eof.data(), partitions_requested_eof.size());
                }
        } catch (const std::exception &e) {
                if (trace) {
                        SLog("Cought exception:", e.what(), "\n");
                }

                return shutdown(c, __LINE__);
        }
}

#include "service_common.h"

bool adjust_range_start(lookup_res &res, const uint64_t abs_seq_num, std::unordered_map<uint64_t, adjust_range_start_cache_value> *const cache);


// whenever the leader of a partition changes, we need to wake up
// everyone so that they will connect to the right node
//
// if we don't do that, then the consumer will not fetch any content until the registered wait context times out
void Service::wakeup_all_consumers(topic_partition *const p) {
        TANK_EXPECT(p);
        static constexpr bool   trace{false};
        auto &                  waiting_list = p->waiting_list;
        auto &                  woken_up     = reusable.woken_up;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, "Will FORCE wake up all consumers of ",
                     p->owner->name(), "/", p->idx, ", waiting_list.size() = ",
                     p->waiting_list.size(), ": different leader?", ansifmt::reset, "\n");
        }

        woken_up.clear();
	woken_up.reserve(waiting_list.size());

	// TODO: why not just iterate from [waiting_list.size() - 1, 0] so that
	// erase_from_waiting_list() will be faster?
        while (!waiting_list.empty()) {
                auto [it, index] = waiting_list.front();
                auto ctx         = it->partitions + index;

                TANK_EXPECT(ctx->partition == p);
                p->erase_from_waiting_list(0);
                woken_up.emplace_back(it);
        }

        if (trace) {
                SLog("WAKEUP: Will wake up ", woken_up.size(), "\n");
        }

        for (auto ctx : woken_up) {
                wakeup_wait_ctx(ctx, nullptr);
        }
}

// A consumer(or peer who is replicating partition content from this node -- acting as a partition leader)'s response
// cannot be generated now, so we need to defer it until we can either produce it, or it times out
bool Service::register_consumer_wait(const TankAPIMsgType _msg, connection *const c,
                                     const uint32_t          request_id,
                                     const uint64_t          max_wait,
                                     const uint32_t          min_bytes,
                                     topic_partition **const partitions, const uint32_t total_partitions) {
        static constexpr bool trace{false};
        auto                  ctx = get_waitctx(total_partitions);
        const auto            ca  = cluster_aware();

	TANK_EXPECT(ctx);

        if (trace) {
                SLog(ansifmt::color_brown, ansifmt::inverse, "REGISTERING consumer wait for ",
                     total_partitions,
                     " partitions, max_wait = ", max_wait,
		     ", request_id = ", request_id,
                     ", min_bytes = ", min_bytes, ansifmt::reset, "\n");
        }

        if (max_wait) {
                // setup and register the timer for context expiration
                ctx->exp_tree_node.node.key = now_ms + max_wait;
                ctx->exp_tree_node.type     = timer_node::ContainerType::WaitCtx;
                register_timer(&ctx->exp_tree_node.node);
        } else {
                // not linked
                memset(&ctx->exp_tree_node.node, 0, sizeof(ctx->exp_tree_node.node));
        }

        // An alternative implementation would simply track the hwmark_threshold and nothing more
        // (i.e no range, no fdh) so that when a new message was committed(depending on hwmark_threshold)
        // we 'd use read_from_local() like we do in process_consume()
        // to determine what to serve and from where
        // this should simplify things, to the expense of higher overhead (we can avoid that completely by doing what we do now
        // where we track the file and the range needed) and would also make it hard to support min_bytes semantics
        ctx->_msg             = _msg;
        ctx->requestId        = request_id;
        ctx->c                = c;
        ctx->total_partitions = total_partitions;
        ctx->minBytes         = min_bytes;
        ctx->capturedSize     = 0;

        // register wait_ctx with the client's connection
        ctx->list.reset();
        c->as.tank.waitCtxList.push_back(&ctx->list);

        for (size_t i{0}; i < total_partitions; ++i) {
                auto p   = partitions[i];
                auto out = ctx->partitions + i;

                // initialize wait_ctx_partition
                out->partition = p;
                out->fdh       = nullptr;
                out->seqNum    = 0;

                // we can't set hwmark_threshold to 0 
                // because 0 is a valid(in this context) sequence number
                // so instead we 'll use magic value std::numeric_limits<uint64_t>::max()
                out->hwmark_threshold = (false == ca || _msg == TankAPIMsgType::ConsumePeer)
                                            ? std::numeric_limits<uint64_t>::max()
                                            : partition_hwmark(p);
                out->range.reset();

                if (trace) {
                        SLog("Partition ", ptr_repr(p), ", hwmark_threshold = ", out->hwmark_threshold, ", waiting_list.size() before append = ", p->waiting_list.size(), "\n");
                }

                if (out->hwmark_threshold != std::numeric_limits<uint64_t>::max()) {
                        // Yes, we require a HWM bump
                        auto       log          = partition_log(p);
                        const auto cur_filesize = log->cur.fileSize;
                        auto       cur_fh       = log->cur.fdh.get();
                        auto       hwmark_fh    = p->highwater_mark.file.handle;

                        // we wish to commence reading starting from
                        // the next message past the HWMark
                        out->seqNum = out->hwmark_threshold + 1;

                        if (nullptr == hwmark_fh) {
                                // this should happen if partition is empty(no messages in any segment)
                                // or simply no messages were committed.
                                //
                                // we 'll fallback to read_from_local()
                                // this should be very rare
                                if (trace) {
                                        SLog("No hwmark_fh\n");
                                }
                        } else if (hwmark_fh != cur_fh) {
                                // current segment changed since we checkpointed the highwater mark
                                //
                                // we 'll fallback to read_from_local()
                                // this should be very rare
                                if (trace) {
                                        SLog("hwmark_fh(", ptr_repr(hwmark_fh), ") != cur_fh(", ptr_repr(cur_fh), ")\n");
                                }
                        } else {
                                // most likely path
                                out->fdh = hwmark_fh;
                                out->fdh->Retain();

                                out->range.offset = p->highwater_mark.file.size;
                                out->range.len    = cur_filesize != std::numeric_limits<uint32_t>::max()
                                                     ? cur_filesize - p->highwater_mark.file.size
                                                     : 0;

                                ctx->capturedSize += out->range.size();

                                if (trace) {
                                        SLog("Require HW commit, initial range ", out->range, "\n");
                                }
                        }

                        if (trace) {
                                SLog("cur_filesize = ", cur_filesize,
                                     ", highwater_mark.file.size = ", p->highwater_mark.file.size,
                                     ", range =", out->range,
                                     ", fdh = ", ptr_repr(out->fdh), "\n");
                        }
                }

                // register wait ctx with partition
                p->waiting_list.emplace_back(ctx, i);
        }

        return true;
}

// requirements were met
// generate the deferred consume response
void Service::wakeup_wait_ctx(wait_ctx *const wctx, connection *const produceConnection) {
        static constexpr bool trace{false};
        auto                  response_hdr = get_buf();
        uint16_t              topicsCnt{0};
        auto                  c = wctx->c;
        auto *const           q = c->outQ ?: (c->outQ = get_outgoing_queue());
        size_t                sum{0};
        const auto            msg = wctx->_msg;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_green, "Waking up wait ctx ", ptr_repr(wctx), ", request_id = ", wctx->requestId,
                     ansifmt::reset, " msg=", unsigned(msg), "\n");
        }

        auto payload = get_data_vector_payload();

        q->push_back(payload); // it's important that we push this payload in `q` before we will push any file_contents_payloads
        payload->buf = response_hdr;

        response_hdr->pack(static_cast<uint8_t>(msg));
        const auto sizeOffset = response_hdr->size();
        response_hdr->RoomFor(sizeof(uint32_t));
        const auto headerSizeOffset = response_hdr->size();
        response_hdr->RoomFor(sizeof(uint32_t));

        if (msg == TankAPIMsgType::Consume) {
                response_hdr->Serialize(wctx->requestId);
        }

        const auto topics_cnt_offset = response_hdr->size();
        const auto pcnt              = wctx->total_partitions;

        if (msg == TankAPIMsgType::ConsumePeer) {
                response_hdr->RoomFor(sizeof(uint16_t));
        } else {
                response_hdr->RoomFor(sizeof(uint8_t));
        }

        if (trace) {
                SLog("For this wait context, ", pcnt, " partitions, request type = ", msg == TankAPIMsgType::ConsumePeer ? "ConsumePeer" : "Consume", "\n");
        }

        for (uint32_t i{0}; i < pcnt;) {
                auto        it        = wctx->partitions + i;
                const auto *p         = it->partition;
                const auto  t         = p->owner;
                const auto  topicName = t->name();
                const auto  saved_i   = i;

                ++topicsCnt;
                response_hdr->pack(topicName.size());
                response_hdr->serialize(topicName.data(), topicName.size());

                if (trace) {
                        SLog("Topic [", topicName, "]\n");
                }

                const auto partitions_cnt_offset = response_hdr->size();
                if (msg == TankAPIMsgType::ConsumePeer) {
                        response_hdr->RoomFor(sizeof(uint16_t));
                } else {
                        response_hdr->RoomFor(sizeof(uint8_t));
                }

                do {
                        if (trace) {
                                SLog("partition ", p->idx, "\n");
                        }

                        response_hdr->pack(static_cast<uint16_t>(p->idx)); // partition
                        response_hdr->pack(static_cast<uint8_t>(0));       // error_flags

                        if (it->fdh) {
                                if (trace) {
                                        SLog("HAVE data for ", topicName, ".", p->idx,
                                             ", seqNum = ", it->seqNum,
                                             ", hwmark = ", p->hwmark(),
                                             ", range = ", it->range,
                                             " (", it->range.len, ")", ptr_repr(it->fdh),
                                             " ", it->fdh->use_count(), "\n");
                                }

                                static_assert(sizeof(it->seqNum) == sizeof(uint64_t));

                                response_hdr->pack(it->seqNum);
                                response_hdr->pack(p->hwmark());
                                response_hdr->pack(static_cast<uint32_t>(it->range.size()));

                                sum += it->range.len;

                                const auto __v = it->fdh->use_count();
                                auto       p   = get_file_contents_payload();

                                p->init(it->fdh, it->range, Timings::Microseconds::Tick(), t);
                                q->push_back(p);

                                TANK_EXPECT(it->fdh->use_count() == __v + 1);

                                it->fdh->Release(); // was retained in consider_append_res()
                                it->fdh = nullptr;
                        } else {
                                response_hdr->pack(static_cast<uint64_t>(0));
                                response_hdr->pack(p->hwmark());
                                response_hdr->pack(static_cast<uint32_t>(0));
                        }

                } while (++i < pcnt && (p = (it = wctx->partitions + i)->partition)->owner == t);

                if (msg == TankAPIMsgType::ConsumePeer) {
                        *reinterpret_cast<uint16_t *>(response_hdr->At(partitions_cnt_offset)) = i - saved_i;
                } else {
                        *reinterpret_cast<uint8_t *>(response_hdr->At(partitions_cnt_offset)) = i - saved_i;
                }
        }

        if (msg == TankAPIMsgType::ConsumePeer) {
                *reinterpret_cast<uint16_t *>(response_hdr->At(topics_cnt_offset)) = topicsCnt;
        } else {
                *reinterpret_cast<uint8_t *>(response_hdr->At(topics_cnt_offset)) = topicsCnt;
        }

        *reinterpret_cast<uint32_t *>(response_hdr->At(sizeOffset))       = response_hdr->size() - sizeOffset - sizeof(uint32_t) + sum;
        *reinterpret_cast<uint32_t *>(response_hdr->At(headerSizeOffset)) = response_hdr->size() - headerSizeOffset - sizeof(uint32_t);

        destroy_wait_ctx(wctx);

        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(response_hdr->data()), response_hdr->size()};

        if (c != produceConnection) {
                // see process_produce()
                try_tx(c);
        }
}

// timed out, generate a response
void Service::abort_wait_ctx(wait_ctx *const wctx) {
        static constexpr bool trace{false};

        if (wctx->list.empty()) {
                // was detached earlier
                return;
        }

        auto       response_hdr = get_buf();
        uint16_t   topics_cnt{0};
        auto       c   = wctx->c;
        const auto msg = wctx->_msg;

        TANK_EXPECT(c && c->fd != -1 && c->gen != std::numeric_limits<uint64_t>::max());

        if (trace) {
                SLog("Aborting wait ctx ", ptr_repr(wctx), ", ", wctx->requestId, "\n");
        }

        response_hdr->pack(static_cast<uint8_t>(msg));
        const auto sizeOffset = response_hdr->size();
        response_hdr->RoomFor(sizeof(uint32_t));
        const auto headerSizeOffset = response_hdr->size();
        response_hdr->RoomFor(sizeof(uint32_t));
        response_hdr->Serialize(wctx->requestId);
        const auto topics_cnt_offset = response_hdr->size();
        const auto pcnt              = wctx->total_partitions;

        if (msg == TankAPIMsgType::ConsumePeer) {
                response_hdr->RoomFor(sizeof(uint16_t));
        } else {
                response_hdr->RoomFor(sizeof(uint8_t));
        }

        for (uint32_t i{0}; i < pcnt;) {
                auto        it        = wctx->partitions + i;
                const auto *p         = it->partition;
                const auto  t         = p->owner;
                const auto  topicName = t->name();
                const auto  saved_i   = i;

                ++topics_cnt;
                response_hdr->pack(topicName.size());
                response_hdr->serialize(topicName.data(), topicName.size());

                if (trace) {
                        SLog("Topic [", topicName, "]\n");
                }

                const auto partitions_cnt_offset = response_hdr->size();

                if (msg == TankAPIMsgType::ConsumePeer) {
                        response_hdr->RoomFor(sizeof(uint16_t));
                } else {
                        response_hdr->RoomFor(sizeof(uint8_t));
                }

                do {
                        if (trace) {
                                SLog("partition ", p->idx, "\n");
                        }

                        if (it->fdh) {
                                it->fdh->Release();
                                it->fdh = nullptr;
                        }

                        response_hdr->pack(static_cast<uint16_t>(p->idx));
                        response_hdr->pack(static_cast<uint8_t>(0));
                        response_hdr->pack(static_cast<uint64_t>(0)); // first available sequence number
                        response_hdr->pack(static_cast<uint64_t>(p->hwmark()));
                        response_hdr->pack(static_cast<uint32_t>((0)));
                } while (++i < pcnt && (p = (it = wctx->partitions + i)->partition)->owner == t);

                if (msg == TankAPIMsgType::ConsumePeer) {
                        *reinterpret_cast<uint16_t *>(response_hdr->At(partitions_cnt_offset)) = i - saved_i;
                } else {
                        *reinterpret_cast<uint8_t *>(response_hdr->At(partitions_cnt_offset)) = i - saved_i;
                }
        }

        if (msg == TankAPIMsgType::ConsumePeer) {
                *reinterpret_cast<uint16_t *>(response_hdr->At(topics_cnt_offset)) = topics_cnt;
        } else {
                *reinterpret_cast<uint8_t *>(response_hdr->At(topics_cnt_offset)) = topics_cnt;
        }
        *reinterpret_cast<uint32_t *>(response_hdr->At(sizeOffset))       = response_hdr->size() - sizeOffset - sizeof(uint32_t);
        *reinterpret_cast<uint32_t *>(response_hdr->At(headerSizeOffset)) = response_hdr->size() - headerSizeOffset - sizeof(uint32_t);

        auto q       = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto payload = get_data_vector_payload();

        q->push_back(payload);

        payload->buf     = response_hdr;
        payload->iov_cnt = 1;
        payload->iov[0]  = {static_cast<void *>(response_hdr->data()), response_hdr->size()};

        // XXX: hypotethetical scenario where this could fail
        // 1. client sets wait time for consume request e.g 10ms
        // 2. tank accepts the connection, sets NeedOutAvail and add with (POLLIN|POLLOUT)
        // 3. tank reads the request, processes it, and because there is no content available
        // 	register_consumer_wait() to be fired in about 10ms
        // 4. (now_ms > nextExpWaitCtxCheck) is true, and wait context is now aborted
        //  but this try_tx() won't schedule data immediately
        // because NeedOutAvail is still set, because POLLOUT hasn't yet become available.
        //
        // If for some reason we don't get POLLOUT, and thus, invoke tx()
        // then we 'll never get to respond to the client

        destroy_wait_ctx(wctx);
        try_tx(c);
}

// dispose of a wait context
void Service::destroy_wait_ctx(wait_ctx *const wctx) {
        static constexpr bool trace{false};
        TANK_EXPECT(wctx);

        if (wctx->list.empty()) {
                return;
        }

        wctx->list.detach_and_reset();

        if (trace) {
                SLog("destroying ", ptr_repr(wctx), " ", wctx->total_partitions, "\n");
        }

        const auto pcnt = wctx->total_partitions;

        for (uint32_t i{0}; i < pcnt; ++i) {
                auto &     it = wctx->partitions[i];
                auto       p  = it.partition;
                const auto n  = p->waiting_list.size();

                if (it.fdh) {
                        // release strong ref to the segment
                        if (trace) {
                                SLog("Releasing ", ptr_repr(it.fdh), " ", it.fdh->use_count(), "\n");
                        }

                        it.fdh->Release();
                        it.fdh = nullptr;
                }

                // erase this wait_ctx from partition's waiting list
                // TODO: optimize me?
                for (size_t i{0}; i < n; ++i) {
                        if (auto &it = p->waiting_list[i]; it.first == wctx) {
                                p->erase_from_waiting_list(i);
                                break;
                        }
                }
        }

        cancel_timer(&wctx->exp_tree_node.node);

        // Defer put_waitctx() until the next iteration
        waitctx_deferred_gc.emplace_back(wctx);
}

void Service::gc_waitctx_deferred() {
        // Deferred, see waitctx_deferred_gc decl. comments
        for (auto wctx : waitctx_deferred_gc) {
                TANK_EXPECT(wctx->list.empty());

                put_waitctx(wctx);
        }
        waitctx_deferred_gc.clear();
}

// invoked whenever the highwater mark of a partition is updated
// we may wake up any sleeping consumers
void Service::consider_highwatermark_update(topic_partition *partition, const uint64_t hwmark, std::vector<wait_ctx *> *woken_up_ctx) {
        static constexpr bool trace{false};
        TANK_EXPECT(partition);
        TANK_EXPECT(woken_up_ctx);
        auto &waiting_list = partition->waiting_list;

        if (trace) {
                SLog(ansifmt::color_magenta, "Will consider HWM update for ",
                     partition->owner->name(), "/", partition->idx,
                     " hwmark = ", hwmark, ", waiting_list.size() = ", waiting_list.size(), ansifmt::reset, "\n");
        }

        for (uint32_t i{0}; i < waiting_list.size();) {
                auto [it, index] = waiting_list[i];
                auto ctx         = it->partitions + index; // context for this partition in the wait_ctx(it)

                TANK_EXPECT(ctx->partition == partition);

                if (trace) {
                        SLog("Considering partition wctx, hwmark_threshold = ", ctx->hwmark_threshold, ", fdh = ", ptr_repr(ctx->fdh), "\n");
                }

                if (ctx->hwmark_threshold == std::numeric_limits<uint64_t>::max()) {
                        // handled by consider_append_res()
                        if (trace) {
                                SLog("Not relevant -- like a consumer(peer) connection\n");
                        }

                        ++i;
                        continue;
                }

                if (hwmark <= ctx->hwmark_threshold) {
			if (trace) {
				SLog("Ignoring because hwmark(", hwmark, ") <= hwmark_threshold(", ctx->hwmark_threshold, ")\n");
			}

                        ++i;
                        continue;
                }

                auto log = partition_log(partition);

                TANK_EXPECT(log);

                if (trace) {
                        SLog("GOT hwmark_threshold = ", ctx->hwmark_threshold, 
				", minBytes = ", it->minBytes, 
				", log->cur.fileSize = ", log->cur.fileSize, 
				", capturedSize(", it->capturedSize, "), seqNum = ", ctx->seqNum, "\n");
                }

                // reset hwmark_threshold so that
                // any subsequent consider_highwatermark_update or consider_append_res will use it
                ctx->hwmark_threshold = 0;

                if (!ctx->fdh) {
                        // it's OK if we don't respect minBytes here
                        // this onyl happens very rarely and consumers are expected to retry
                        auto res = partition->read_from_local(false, ctx->seqNum, it->minBytes);

                        adjust_range_start(res, ctx->seqNum, nullptr);

                        if (trace) {
                                SLog("Not bound to file, used read_from_local(): Got res{.absBaseSeqNum = ",
                                     res.absBaseSeqNum, ", .fileOffset = ",
                                     res.fileOffset, ", .fileOffsetCeiling = ",
                                     res.fileOffsetCeiling, "}\n");
                        }

                        ctx->fdh = res.fdh.get();
                        ctx->range.set(res.fileOffset, res.fileOffsetCeiling - res.fileOffset);
                        ctx->fdh->Retain();

                        if (trace) {
                                SLog("Updated range to ", ctx->range, "\n");
                        }

                        partition->erase_from_waiting_list(i);
                        woken_up_ctx->emplace_back(it);
                        continue;
                } else if (it->capturedSize >= it->minBytes) {
                        partition->erase_from_waiting_list(i);

                        if (trace) {
                                SLog("Should wake up now, capturedSize = ", it->capturedSize, "\n");
                        }

                        // XXX: we should only wake up if it:wait_ctx has no other partitions pending woke up
                        woken_up_ctx->emplace_back(it);
                        continue;
                } else if (trace) {
                        SLog("Not captured enough content yet capturedSize(", it->capturedSize, ") < minBytes(", it->minBytes, ")\n");
                }

                ++i;
        }
}

void Service::consider_highwatermark_update(topic_partition *partition, const uint64_t hwmark) {
	auto &woken_up = reusable.woken_up;

        woken_up.clear();
        consider_highwatermark_update(partition, hwmark, &woken_up);

        for (auto ctx : woken_up) {
                wakeup_wait_ctx(ctx, nullptr);
        }
}

// invoked whenever a partition leader (i.e local node) persists new messages for a partition
void Service::consider_append_res(topic_partition *partition, append_res &res, std::vector<wait_ctx *> *woken_up_ctx) {
        static constexpr bool trace{false};
        TANK_EXPECT(partition);
        TANK_EXPECT(woken_up_ctx);
        bool  have_switched_segment{false};
        auto &waiting_list = partition->waiting_list;

        if (trace) {
                SLog(ansifmt::color_blue, ansifmt::bold, ansifmt::inverse, " waitingList.size() = ", waiting_list.size(), ansifmt::reset, "\n");
        }

        // for all wait contexts this partition is registered with
        for (uint32_t i{0}; i < waiting_list.size();) {
                auto [it, index] = waiting_list[i];
                auto ctx         = it->partitions + index;

                TANK_EXPECT(ctx->partition == partition);

                if (ctx->hwmark_threshold != std::numeric_limits<uint64_t>::max() && !ctx->fdh) {
                        if (trace) {
                                SLog("Will ignore because requires HWM update(no fdh)\n");
                        }

                        ++i;
                        continue;
                }

                if (!ctx->fdh) {
                        // not bound to a log segment yet
                        // bind it now
                        const auto __n = res.fdh->use_count();

                        ctx->fdh = res.fdh.get();
                        ctx->fdh->Retain();

                        TANK_EXPECT(ctx->fdh->use_count() == __n + 1);

                        ctx->range       = res.dataRange;
                        ctx->seqNum      = res.msgSeqNumRange.offset;
                        it->capturedSize = res.dataRange.len;

                        if (trace) {
                                SLog("Just registered fdh for wait ctx, capturedSize(", it->capturedSize,
                                     "), range = ", ctx->range, " ptr(fdh)=", ptr_repr(ctx->fdh), " rc=", ctx->fdh->use_count(), ", minBytes = ", it->minBytes, "\n");
                        }

                } else if (res.fdh.get() != ctx->fdh) {
                        // was bound to a log segment, but that changed because
                        // e.g siwtched to another one
                        have_switched_segment = true;

                        if (trace) {
                                SLog("Switched to a new fdh for wait ctx\n");
                        }

                } else {
                        // extend the range
                        ctx->range.len += res.dataRange.size();
                        it->capturedSize += res.dataRange.size();

                        if (trace)
                                SLog("Extending range, capturedSize(", it->capturedSize, "), range ", ctx->range, "\n");
                }

                if (have_switched_segment) {
                        // explicitly when we switch to a new segment
			if (trace) {
				SLog("Will FORCE wake up because we switched to a new segment\n");
			}

                        woken_up_ctx->emplace_back(it);
                        partition->erase_from_waiting_list(i);
                        continue;
                } 
	
		if (ctx->hwmark_threshold != std::numeric_limits<uint64_t>::max()) {
			if (trace) {
				SLog("Will not bother waking up, because depends on HWM (hwmark_threshold = ", ctx->hwmark_threshold, ")\n");
			}
		} else {
                        if (it->capturedSize >= it->minBytes) {
                                if (trace) {
                                        SLog("either have_switched_segment(", have_switched_segment, "), or capturedSize(",
                                             it->capturedSize, ") >= minBytes(", it->minBytes, "), hwmark_threshold(", ctx->hwmark_threshold, "), hwmark(", partition_hwmark(partition), ")\n");
                                }

                                woken_up_ctx->emplace_back(it);
                                partition->erase_from_waiting_list(i);
                                continue;
                        } else if (trace) {
                                SLog("Will not wake up because capturedSize(", it->capturedSize, ") < minBytes(", it->minBytes, ")\n");
                        }
                }

                ++i;
        }
}

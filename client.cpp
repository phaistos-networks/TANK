#include "tank_client.h"
#include <switch_algorithms.h>
#include <sys/uio.h>
#include <text.h>
#include <unistd.h>
#include <date.h>

static constexpr bool trace{false};

TankClient::broker *TankClient::broker_state(const Switch::endpoint e)
{
#ifdef LEAN_SWITCH
        auto it = bsMap.find(e);

        if (it != bsMap.end())
                return it->second;
        else
        {
                auto state = new broker(e);

                bsMap.insert({e, state});
                return state;
        }
#else
        broker **p;

        if (bsMap.Add(e, nullptr, &p))
                *p = new broker(e);
        return *p;
#endif
}

void TankClient::bind_fd(connection *const c, int fd)
{
        c->fd = fd;

        // we can't write() anything until we got POLLOUT after we connect()
        c->state.flags = (1u << uint8_t(connection::State::Flags::ConnectionAttempt)) | (1u << uint8_t(connection::State::Flags::NeedOutAvail));
        poller.AddFd(fd, POLLIN | POLLOUT, c);
        c->state.lastInputTS = Timings::Milliseconds::Tick();
        c->state.lastOutputTS = c->state.lastInputTS;
        connectionAttempts.push_back(c);
}

TankClient::TankClient(const strwlen32_t defaultLeader)
{
        switch_dlist_init(&connections);
        if (pipe2(pipeFd, O_CLOEXEC) == -1)
                throw Switch::system_error("pipe() failed:", strerror(errno));

        poller.AddFd(pipeFd[0], POLLIN, &pipeFd[0]);

	if (defaultLeader)
		set_default_leader(defaultLeader);
}

TankClient::~TankClient()
{
        while (switch_dlist_any(&connections))
        {
                auto c = switch_list_entry(connection, list, connections.next);

                if (c->fd != -1)
                {
                        poller.DelFd(c->fd);
                        close(c->fd);
                }

                if (auto b = c->inB)
                        put_buffer(b);

                switch_dlist_del(&c->list);
                delete c;
        }

        for (auto &it : bsMap)
        {
#ifdef LEAN_SWITCH
                auto bs = it.second;
#else
                auto bs = it.value();
#endif

                while (auto p = bs->outgoing_content.front())
                {
                        bs->outgoing_content.pop_front();
			if (!p->tracked_by_reqs_tracker())
                                put_payload(p, __LINE__);
                }

                for (auto it = bs->retainedPayloadsList.next; it != &bs->retainedPayloadsList;)
                {
                        auto next = it->next;
                        auto payload = switch_list_entry(outgoing_payload, pendingRespList, it);

			if (!payload->tracked_by_reqs_tracker())
                        	put_payload(payload, __LINE__);

                        it = next;
                }

                for (const auto id : bs->reqs_tracker.pendingConsume)
                {
                        const auto res = pendingConsumeReqs.detach(id);
                        auto info = res.value();

			put_payload(info.reqPayload, __LINE__);
                        free(info.seqNums);
                }

                for (const auto id : bs->reqs_tracker.pendingProduce)
                {
                        const auto res = pendingProduceReqs.detach(id);
                        const auto info = res.value();

			put_payload(info.reqPayload, __LINE__);
                        free(info.ctx);
                }

		for (const auto id : bs->reqs_tracker.pendingDiscover)
		{
			const auto res = pendingDiscoverReqs.detach(id);
			const auto info = res.value();

			put_payload(info.reqPayload, __LINE__);
		}

                delete bs;
        }

        while (connectionsPool.size())
                delete connectionsPool.Pop();

        while (payloadsPool.size())
                delete payloadsPool.Pop();

        while (usedBufs.size())
                delete usedBufs.Pop();

        while (buffersPool.size())
                delete buffersPool.Pop();

        if (pipeFd[0] != -1)
                close(pipeFd[0]);
        if (pipeFd[1] != -1)
                close(pipeFd[1]);
}

uint8_t TankClient::choose_compression_codec(const msg *const msgs, const size_t msgsCnt)
{
        // arbitrary selection heuristic
        if (msgsCnt > 64)
                return 1;
        else
        {
                size_t sum{0};

                for (size_t i{0}; i != msgsCnt; ++i)
                {
                        sum += msgs[i].content.len;
                        if (sum > 1024)
                                return 1;
                }
        }

        return 0;
}

bool TankClient::produce_to_leader_with_base(const uint32_t clientReqId, const Switch::endpoint leader, const produce_ctx *const produce, const size_t cnt)
{
        const uint64_t start = trace ? Timings::Microseconds::Tick() : 0;
        auto bs = broker_state(leader);
        auto payload = get_payload();
        auto &b = *payload->b;
        auto &b2 = *payload->b2;
        uint32_t base{0};
        uint8_t topicsCnt{0};
        const auto reqId = ids_tracker.leader_reqs.next++;

	require(cnt); // can't publish an empty messages set
        require(payload->iovCnt == 0);
        ranges.clear();
        produceCtx.clear();

        if (trace)
                SLog("Producing to leader ", leader, ", reqId = ", reqId, "\n");

        // req(msg, size) not serialized here, we 'll use payload->iov[] to make sure they are transmitted before anything else later

        b.reserve(256);
        b.Serialize<uint16_t>(1); // client version
        b.Serialize<uint32_t>(reqId);
        b.Serialize(clientId.len);
        b.Serialize(clientId.p, clientId.len);
        b.Serialize(uint8_t(0));  // required acks
        b.Serialize(uint32_t(0)); // ack timeout

        const auto topicsCntOffset = b.length();
        b.MakeSpace(sizeof(uint8_t));

        for (uint32_t i{0}; i != cnt;)
        {
                const auto *it = produce + i;
                const auto topic = it->topic;
                const uint8_t compressionCodec = compressionStrategy == CompressionStrategy::CompressIntelligently ? choose_compression_codec(it->msgs, it->msgsCnt)
                                                                                                                   : compressionStrategy == CompressionStrategy::CompressNever ? 0 : 1;
                uint8_t partitionsCnt{0};

                b.Serialize(topic.len);
                b.Serialize(topic.p, topic.len);

                const auto partitionsCntOffset = b.length();
                b.MakeSpace(sizeof(uint8_t));

                produceCtx.Serialize(topic.len);
                produceCtx.Serialize(topic.p, topic.len);
                const auto produceCtxPartitionsCntOffset = produceCtx.length();
                produceCtx.MakeSpace(sizeof(uint8_t));

                if (trace)
                        SLog("For topic [", topic, "]\n");

                ++topicsCnt;
                do
                {
			const auto baseSeqNum = it->baseSeqNum;
                        uint8_t bundleFlags{0};

                        ++partitionsCnt;
                        b.Serialize<uint16_t>(it->partitionId);

                        const auto bundleOffset = b.length();
                        ranges.push_back({base, bundleOffset - base});

			if (baseSeqNum)
			{
				// Set SPARSE bit
				bundleFlags|=(1u << 6);
			}

                        // BEGIN:bundle header
                        if (compressionCodec)
                        {
                                Drequire(compressionCodec < 3);
                                bundleFlags |= compressionCodec;
                        }

			expect(it->msgsCnt);

                        if (it->msgsCnt < 16)
                        {
                                // can encode message set total messages in flags, because it will fit in
                                // the 4 bits we have reserved for that
                                bundleFlags |= (it->msgsCnt << 2);
                        }

                        b.Serialize(bundleFlags);

                        if (it->msgsCnt >= 16)
                                b.SerializeVarUInt32(it->msgsCnt);

			if (baseSeqNum)
			{
				b.Serialize<uint64_t>(baseSeqNum);	 //base seqnum

				if (it->msgsCnt != 1)
				{
					// last message seqNum delta/offset from baseSeqNum
					// we encode msgsCnt - 2 because
					// we want to encode (lastMsg - firstMsg) - 1
					// so if baseSeqNum = 10 and msgsCnt = 2, the last would be 11, so we want to encode
					// 11 - 10 - 1 = 0
					//
					// see tank_encoding.md
					b.SerializeVarUInt32(it->msgsCnt - 2); 	
				}
			}

			if (trace)
				SLog("bundleFlags = ", bundleFlags, "\n");


                        // END:bundle header; serialize messages set

                        produceCtx.Serialize<uint16_t>(it->partitionId);

                        const auto msgSetOffset = b.length();
                        uint64_t lastTS{0};
                        size_t required{0};

                        if (trace)
                                SLog("Packing ", it->msgsCnt, " for bundle msg set, for partition ", it->partitionId, "\n");

                        // This helps a lot; down from 0.84 to 0.226 for 1,000,000 messages, of 100bytes each
                        for (uint32_t i{0}; i != it->msgsCnt; ++i)
                        {
                                const auto &m = it->msgs[i];

				if (unlikely(!m.content.len))
				{
					put_payload(payload, __LINE__);
					throw Switch::data_error("Unexpected message with no content");
				}

                                required += m.key.len;
                                required += m.content.len;
                        }
                        required += it->msgsCnt * (24);

                        b.reserve(required);

                        for (uint32_t i{0}; i != it->msgsCnt; ++i)
                        {
                                const auto &m = it->msgs[i];
                                uint8_t flags = m.key ? uint8_t(TankFlags::BundleMsgFlags::HaveKey) : 0;
				bool encodeTS;

				if (trace)
					SLog("message ", i, " / ", it->msgsCnt, "\n");



                                if (m.ts == lastTS && i)
                                {
                                        // This message's timestamp == last message timestamp we serialized
                                        flags |= uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                        b.Serialize(flags);
					encodeTS= false;

					if (trace)
						SLog("Using last specified TS\n");
                                }
                                else
                                {
                                        lastTS = m.ts;
                                        b.Serialize(flags);
					encodeTS= true;

					if (trace)
						SLog("Specifying TS\n");
                                }

				if (baseSeqNum)
                                {
                                        // bundle sparse bit is set
                                        if (i && i != it->msgsCnt - 1)
                                        {
						// they are ordered in this bundle, and distance from last is always 1
						// and we encode distance - 1 so always encode 0 here
						if (trace)
							SLog("Serializing delta = 0\n");

						b.SerializeVarUInt32(0);
                                        }
                                }
				
				if (encodeTS)
				{
	                                b.Serialize<uint64_t>(m.ts);
				}

                                if (m.key)
                                {
                                        b.Serialize(m.key.len);
                                        b.Serialize(m.key.p, m.key.len);

					if (trace)
						SLog("Specifying key\n");
                                }

				if (trace)
					SLog("msg.content.len = ", m.content.len, "\n");

                                b.SerializeVarUInt32(m.content.len);
                                b.Serialize(m.content.p, m.content.len);
                        }

                        if (!compressionCodec)
                        {
                                const auto offset = b.length();
                                const auto bundleSize = offset - bundleOffset;
                                const range32_t r(bundleOffset, bundleSize);

                                b.SerializeVarUInt32(bundleSize);
                                b.Serialize<uint64_t>(it->baseSeqNum);
                                base = b.length();
                                ranges.push_back({offset, base - offset});
                                ranges.push_back(r);
                        }
                        else
                        {
                                const auto msgSetLen = b.length() - msgSetOffset, bundleHeaderLength = msgSetOffset - bundleOffset;
                                auto &cmpBuf = b2;
                                const auto o = cmpBuf.length();
                                const uint64_t start = trace ? Timings::Microseconds::Tick() : 0;

                                if (unlikely(!Compression::Compress(Compression::Algo::SNAPPY, b.At(msgSetOffset), msgSetLen, &cmpBuf)))
                                {
                                        put_payload(payload, __LINE__);
                                        throw Switch::exception("Failed to compress content");
                                }

                                if (trace)
                                        SLog(size_repr(msgSetLen), " => ", size_repr(cmpBuf.length()), ", in ", duration_repr(Timings::Microseconds::Since(start)), "\n");

                                // TODO: if cmpBuf.length() > msgSetLen, don't use compressed content(not worth it) - also unset flags compression codec bits

                                const auto compressedMsgSetLen = cmpBuf.length() - o;

                                b.SetLength(msgSetOffset);
                                const auto offset = b.length();
                                b.SerializeVarUInt32(compressedMsgSetLen + bundleHeaderLength);
                                b.Serialize<uint64_t>(it->baseSeqNum);

                                base = b.length();
                                ranges.push_back({offset, b.length() - offset});         // (bundle length:varint + maybe startSeqNum:u64)
                                ranges.push_back({bundleOffset, bundleHeaderLength});    // bundle header
                                ranges.push_back({o | (1u << 30), compressedMsgSetLen}); // compressed bundle messages set
                        }

                } while (++i != cnt && (it = produce + i)->topic == topic);

                *(uint8_t *)b.At(partitionsCntOffset) = partitionsCnt;
                *(uint8_t *)produceCtx.At(produceCtxPartitionsCntOffset) = partitionsCnt;
        }
        *(uint8_t *)b.At(topicsCntOffset) = topicsCnt;

        const auto reqSize = b.length() + b2.length();

        if (trace)
                SLog("b.length = ", b.length(), ", b2.length = ", b2.length(), "\n");

        require(payload->iovCnt + 1 + ranges.size() <= sizeof_array(payload->iov));

        b.reserve(sizeof(uint8_t) + sizeof(uint32_t));
        payload->iov[payload->iovCnt++] = {b.End(), sizeof(uint8_t) + sizeof(uint32_t)};
        b.Serialize(uint8_t(TankAPIMsgType::ProduceWithBaseSeqNum)); 	// req.msgtype
        b.Serialize<uint32_t>(reqSize);        				// req.size

        for (const auto &r : ranges)
        {
                const auto o = r.offset & (~(1u << 30));

                if (o == r.offset)
                        payload->iov[payload->iovCnt++] = {(void *)b.At(r.offset), r.len};
                else
                        payload->iov[payload->iovCnt++] = {(void *)b2.At(o), r.len};
        }

        auto ctx = (uint8_t *)malloc(produceCtx.length());

        if (unlikely(!ctx))
	{
		put_payload(payload, __LINE__);
                throw Switch::system_error("out of memory");
	}

	// it is _not_ an idempotent request, but we 'll track it anyway in pendingProduceReqs, because if the broker tells us that is no longer the
	// leader for the (topic, partition) and we need to connect to another broker and reschedule the payload to it, we can do so by looking up
	// the payload in pendingProduceReqs
	payload->flags = 1u << uint8_t(outgoing_payload::Flags::ReqMaybeRetried);
	Drequire(payload->tracked_by_reqs_tracker());
        memcpy(ctx, produceCtx.data(), produceCtx.length());

        bs->reqs_tracker.pendingProduce.insert(reqId);
        bs->outgoing_content.push_back(payload);
        pendingProduceReqs.Add(reqId, {clientReqId, payload, nowMS, ctx, produceCtx.length()});
        track_inflight_req(reqId, nowMS, TankAPIMsgType::Produce);

        if (trace)
                SLog("Took ", duration_repr(Timings::Microseconds::Since(start)), " to generate produce request\n");

        return try_transmit(bs);
}

bool TankClient::produce_to_leader(const uint32_t clientReqId, const Switch::endpoint leader, const produce_ctx *const produce, const size_t cnt)
{
        const uint64_t start = trace ? Timings::Microseconds::Tick() : 0;
        auto bs = broker_state(leader);
        auto payload = get_payload();
        auto &b = *payload->b;
        auto &b2 = *payload->b2;
        uint32_t base{0};
        uint8_t topicsCnt{0};
        const auto reqId = ids_tracker.leader_reqs.next++;

	require(cnt); // can't publish an empty messages set
        require(payload->iovCnt == 0);
        ranges.clear();
        produceCtx.clear();

        if (trace)
                SLog("Producing to leader ", leader, ", reqId = ", reqId, "\n");

        // req(msg, size) not serialized here, we 'll use payload->iov[] to make sure they are transmitted before anything else later

        b.reserve(256);
        b.Serialize<uint16_t>(1); // client version
        b.Serialize<uint32_t>(reqId);
        b.Serialize(clientId.len);
        b.Serialize(clientId.p, clientId.len);
        b.Serialize(uint8_t(0));  // required acks
        b.Serialize(uint32_t(0)); // ack timeout

        const auto topicsCntOffset = b.length();
        b.MakeSpace(sizeof(uint8_t));

        for (uint32_t i{0}; i != cnt;)
        {
                const auto *it = produce + i;
                const auto topic = it->topic;
                const uint8_t compressionCodec = compressionStrategy == CompressionStrategy::CompressIntelligently ? choose_compression_codec(it->msgs, it->msgsCnt)
                                                                                                                   : compressionStrategy == CompressionStrategy::CompressNever ? 0 : 1;
                uint8_t partitionsCnt{0};

                b.Serialize(topic.len);
                b.Serialize(topic.p, topic.len);

                const auto partitionsCntOffset = b.length();
                b.MakeSpace(sizeof(uint8_t));

                produceCtx.Serialize(topic.len);
                produceCtx.Serialize(topic.p, topic.len);
                const auto produceCtxPartitionsCntOffset = produceCtx.length();
                produceCtx.MakeSpace(sizeof(uint8_t));

                if (trace)
                        SLog("For topic [", topic, "]\n");

                ++topicsCnt;
                do
                {
                        uint8_t bundleFlags{0};

                        ++partitionsCnt;
                        b.Serialize<uint16_t>(it->partitionId);

                        const auto bundleOffset = b.length();
                        ranges.push_back({base, bundleOffset - base});

                        // BEGIN:bundle header
                        if (compressionCodec)
                        {
                                Drequire(compressionCodec < 3);
                                bundleFlags |= compressionCodec;
                        }

			expect(it->msgsCnt);

                        if (it->msgsCnt < 16)
                        {
                                // can encode message set total messages in flags, because it will fit in
                                // the 4 bits we have reserved for that
                                bundleFlags |= (it->msgsCnt << 2);
                        }

                        b.Serialize(bundleFlags);

                        if (it->msgsCnt >= 16)
                                b.SerializeVarUInt32(it->msgsCnt);
                        // END:bundle header; serialize messages set

                        produceCtx.Serialize<uint16_t>(it->partitionId);

                        const auto msgSetOffset = b.length();
                        uint64_t lastTS{0};
                        size_t required{0};

                        if (trace)
                                SLog("Packing ", it->msgsCnt, " for bundle msg set, for partition ", it->partitionId, "\n");

                        // This helps a lot; down from 0.84 to 0.226 for 1,000,000 messages, of 100bytes each
                        for (uint32_t i{0}; i != it->msgsCnt; ++i)
                        {
                                const auto &m = it->msgs[i];

                                required += m.key.len;
                                required += m.content.len;
                        }
                        required += it->msgsCnt * (16);

                        b.reserve(required);

                        for (uint32_t i{0}; i != it->msgsCnt; ++i)
                        {
                                const auto &m = it->msgs[i];
                                uint8_t flags = m.key ? uint8_t(TankFlags::BundleMsgFlags::HaveKey) : 0;

                                if (m.ts == lastTS && i)
                                {
                                        // This message's timestamp == last message timestamp we serialized
                                        flags |= uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                        b.Serialize(flags);
                                }
                                else
                                {
                                        lastTS = m.ts;
                                        b.Serialize(flags);
                                        b.Serialize<uint64_t>(m.ts);
                                }

                                if (m.key)
                                {
                                        b.Serialize(m.key.len);
                                        b.Serialize(m.key.p, m.key.len);
                                }

                                b.SerializeVarUInt32(m.content.len);
                                b.Serialize(m.content.p, m.content.len);
                        }

                        if (!compressionCodec)
                        {
                                const auto offset = b.length();
                                const auto bundleSize = offset - bundleOffset;
                                const range32_t r(bundleOffset, bundleSize);

                                b.SerializeVarUInt32(bundleSize);
                                base = b.length();
                                ranges.push_back({offset, base - offset});
                                ranges.push_back(r);
                        }
                        else
                        {
                                const auto msgSetLen = b.length() - msgSetOffset, bundleHeaderLength = msgSetOffset - bundleOffset;
                                auto &cmpBuf = b2;
                                const auto o = cmpBuf.length();
                                const uint64_t start = trace ? Timings::Microseconds::Tick() : 0;

                                if (unlikely(!Compression::Compress(Compression::Algo::SNAPPY, b.At(msgSetOffset), msgSetLen, &cmpBuf)))
                                {
                                        put_payload(payload, __LINE__);
                                        throw Switch::exception("Failed to compress content");
                                }

                                if (trace)
                                        SLog(size_repr(msgSetLen), " => ", size_repr(cmpBuf.length()), ", in ", duration_repr(Timings::Microseconds::Since(start)), "\n");

                                // TODO: if cmpBuf.length() > msgSetLen, don't use compressed content(not worth it) - also unset flags compression codec bits

                                const auto compressedMsgSetLen = cmpBuf.length() - o;

                                b.SetLength(msgSetOffset);
                                const auto offset = b.length();
                                b.SerializeVarUInt32(compressedMsgSetLen + bundleHeaderLength);


                                base = b.length();
                                ranges.push_back({offset, b.length() - offset});         // (bundle length:varint)
                                ranges.push_back({bundleOffset, bundleHeaderLength});    // bundle header
                                ranges.push_back({o | (1u << 30), compressedMsgSetLen}); // compressed bundle messages set
                        }

                } while (++i != cnt && (it = produce + i)->topic == topic);

                *(uint8_t *)b.At(partitionsCntOffset) = partitionsCnt;
                *(uint8_t *)produceCtx.At(produceCtxPartitionsCntOffset) = partitionsCnt;
        }
        *(uint8_t *)b.At(topicsCntOffset) = topicsCnt;

        const auto reqSize = b.length() + b2.length();

        if (trace)
                SLog("b.length = ", b.length(), ", b2.length = ", b2.length(), "\n");

        require(payload->iovCnt + 1 + ranges.size() <= sizeof_array(payload->iov));

        b.reserve(sizeof(uint8_t) + sizeof(uint32_t));
        payload->iov[payload->iovCnt++] = {b.End(), sizeof(uint8_t) + sizeof(uint32_t)};
        b.Serialize(uint8_t(TankAPIMsgType::Produce)); 	// req.msgtype
        b.Serialize<uint32_t>(reqSize);        		// req.size

        for (const auto &r : ranges)
        {
                const auto o = r.offset & (~(1u << 30));

                if (o == r.offset)
                        payload->iov[payload->iovCnt++] = {(void *)b.At(r.offset), r.len};
                else
                        payload->iov[payload->iovCnt++] = {(void *)b2.At(o), r.len};
        }

        auto ctx = (uint8_t *)malloc(produceCtx.length());

        if (unlikely(!ctx))
	{
		put_payload(payload, __LINE__);
                throw Switch::system_error("out of memory");
	}

	// it is _not_ an idempotent request, but we 'll track it anyway in pendingProduceReqs, because if the broker tells us that is no longer the
	// leader for the (topic, partition) and we need to connect to another broker and reschedule the payload to it, we can do so by looking up
	// the payload in pendingProduceReqs
	payload->flags = 1u << uint8_t(outgoing_payload::Flags::ReqMaybeRetried);
	Drequire(payload->tracked_by_reqs_tracker());
        memcpy(ctx, produceCtx.data(), produceCtx.length());

        bs->reqs_tracker.pendingProduce.insert(reqId);
        bs->outgoing_content.push_back(payload);
        pendingProduceReqs.Add(reqId, {clientReqId, payload, nowMS, ctx, produceCtx.length()});
        track_inflight_req(reqId, nowMS, TankAPIMsgType::Produce);

        if (trace)
                SLog("Took ", duration_repr(Timings::Microseconds::Since(start)), " to generate produce request\n");

        return try_transmit(bs);
}

void TankClient::track_inflight_req(const uint32_t reqId, const uint64_t ts, const TankAPIMsgType reqMsg)
{
        // TODO: https://github.com/phaistos-networks/TANK/issues/21
        if (trace)
                SLog("Registering ", reqId, " of type ", uint8_t(reqMsg), "\n");
}

void TankClient::consider_inflight_reqs(const uint64_t ts)
{
        // TODO: https://github.com/phaistos-networks/TANK/issues/21
}

void TankClient::forget_inflight_req(const uint32_t reqId, const TankAPIMsgType reqMsg)
{
        // TODO: https://github.com/phaistos-networks/TANK/issues/21
        if (trace)
                SLog("Forgetting ", reqId, " of type ", uint8_t(reqMsg), "\n");
}

void TankClient::abort_inflight_req(connection *const c, const uint32_t reqId, const TankAPIMsgType reqMsg)
{
        // TODO: https://github.com/phaistos-networks/TANK/issues/21
}


// TODO: if too many outgoing messages in bs->outgoing_content, and in Reachability::Blocked reachability,
// we need to flush_broker(bs); return false;  so that we won't use too much memory queuing up too much data
bool TankClient::try_transmit(broker *const bs)
{
        if (trace)
                SLog(ansifmt::bold, "try_transmit() reachability ", uint8_t(bs->reachability), ansifmt::reset, "\n");

        switch (bs->reachability)
        {
                case broker::Reachability::Unreachable:
                        if (trace)
                                SLog("Unreachable\n");

                        if (nowMS < bs->block_ctx.until)
                        {
                                if (trace)
                                        SLog("Still unreachable\n");

                                flush_broker(bs);
                                return false;
                        }
                        else
                        {
                                if (trace)
                                        SLog("Will try again\n");

                                bs->set_reachability(broker::Reachability::MaybeReachable);
                                // fall-through
                        }

                case broker::Reachability::MaybeReachable:
                case broker::Reachability::Reachable:
                {
                        auto c = bs->con;

                        if (trace)
                                SLog("MaybeReachable or Reachable\n");

                        if (!c)
                        {
                                auto fd = init_connection_to(bs->endpoint);

                                if (fd == -1)
                                {
                                        flush_broker(bs);
                                        return false;
                                }

                                c = get_connection();

                                Drequire(bs);
                                c->bs = bs;
                                bs->con = c;

                                if (trace)
                                        SLog("Initalized and bound connection to broker ", bs->endpoint, "\n");

                                bind_fd(c, fd);
                                switch_dlist_insert_after(&connections, &c->list);
                        }

                        if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))))
                        {
                                c->state.flags |= 1u << uint8_t(connection::State::Flags::NeedOutAvail);
                                poller.SetDataAndEvents(c->fd, c, POLLIN | POLLOUT);

                                if (trace)
                                        SLog("UNSETTING NeedOutAvail\n");

                                return true;
                        }
                        else
                                return try_send(c);
                }

                case broker::Reachability::Blocked:
                        // We 'll retry the transmission, just not right now
                        if (trace)
                                SLog("BLOCKED; will retry in a while\n");

                        return true;

                default:
                        IMPLEMENT_ME();
                        break;
        }

        flush_broker(bs);
        return false;
}

uint32_t TankClient::discover_partitions(const strwlen8_t topic)
{
        auto bs = broker_state(defaultLeader);
        auto payload = get_payload();
        auto &b = *payload->b;
        const auto clientReqId = ids_tracker.client.next++;
        const auto reqId = ids_tracker.leader_reqs.next++;

        b.Serialize(uint8_t(TankAPIMsgType::DiscoverPartitions));
	const auto lenOffset = b.length();
	b.MakeSpace(sizeof(uint32_t));

        b.Serialize<uint32_t>(reqId);
        b.Serialize(topic.len);
        b.Serialize(topic.p, topic.len);

        payload->iov[0] = {(void *)b.data(), b.length()};
        payload->iovCnt = 1;

        bs->reqs_tracker.pendingDiscover.insert(reqId);
        payload->flags = (1u << uint8_t(outgoing_payload::Flags::ReqIsIdempotent)) | (1u << uint8_t(outgoing_payload::Flags::ReqMaybeRetried));
        Drequire(payload->tracked_by_reqs_tracker());
        bs->outgoing_content.push_back(payload);

        pendingDiscoverReqs.Add(reqId, {clientReqId, payload, nowMS});
        track_inflight_req(reqId, nowMS, TankAPIMsgType::DiscoverPartitions);

	*(uint32_t *)b.At(lenOffset) = b.length() - lenOffset - sizeof(uint32_t);

        if (!try_transmit(bs))
                return 0;
        else
                return clientReqId;
}

bool TankClient::consume_from_leader(const uint32_t clientReqId, const Switch::endpoint leader, const consume_ctx *const from, const size_t total, const uint64_t maxWait, const uint32_t minSize)
{
        auto bs = broker_state(leader);
        auto payload = get_payload();
        auto &b = *payload->b;
        uint64_t absSeqNums[256];
        uint8_t absSeqNumsCnt{0};
        uint8_t topicsCnt{0};
        const auto reqId = ids_tracker.leader_reqs.next++;

        b.Serialize(uint8_t(TankAPIMsgType::Consume)); // request msg.type
        const auto reqSizeOffset = b.length();
        b.MakeSpace(sizeof(uint32_t)); // request length

        b.Serialize<uint16_t>(1);     // client version
        b.Serialize<uint32_t>(reqId); // request ID
        b.Serialize(clientId.len);
        b.Serialize(clientId.p, clientId.len);
        b.Serialize(uint64_t(maxWait));
        b.Serialize(uint32_t(minSize)); // min bytes

        const auto topicsCntOffset = b.length();
        b.MakeSpace(sizeof(uint8_t));

        for (size_t i{0}; i != total;)
        {
                const auto *it = from + i;
                const auto topic = it->topic;

                ++topicsCnt;
                b.Serialize(topic.len);
                b.Serialize(topic.p, topic.len);

                const auto before = absSeqNumsCnt;
                const auto totalPartitionsOffset = b.length();
                b.MakeSpace(sizeof(uint8_t));

                do
                {
                        b.Serialize<uint16_t>(it->partitionId);
                        b.Serialize<uint64_t>(it->absSeqNum);
                        b.Serialize<uint32_t>(it->fetchSize);

                        if (trace)
                                SLog("Requesting partition = ", it->partitionId, ", seq = ", it->absSeqNum, ", minFetchSize = ", it->fetchSize, "\n");

                        absSeqNums[absSeqNumsCnt++] = it->absSeqNum;
                } while (++i != total && (it = from + i)->topic == topic);

                *(uint8_t *)b.At(totalPartitionsOffset) = absSeqNumsCnt - before;
        }
        *(uint8_t *)b.At(topicsCntOffset) = topicsCnt;

        if (trace)
                SLog("Will consume from ", topicsCnt, " from leder ", leader, "\n");

        // We need to track pending requests
        // will GC them periodically
        auto *const seqsNumsData = (uint64_t *)malloc(sizeof(uint64_t) * absSeqNumsCnt);

        if (unlikely(!seqsNumsData))
                throw Switch::system_error("out of memory");

        memcpy(seqsNumsData, absSeqNums, sizeof(uint64_t) * absSeqNumsCnt);

        // patch request length
        *(uint32_t *)b.At(reqSizeOffset) = b.length() - reqSizeOffset - sizeof(uint32_t);

        payload->iov[payload->iovCnt++] = {(void *)b.data(), b.length()};

        bs->reqs_tracker.pendingConsume.insert(reqId);
        payload->flags = (1u << uint8_t(outgoing_payload::Flags::ReqIsIdempotent)) | (1u << uint8_t(outgoing_payload::Flags::ReqMaybeRetried));
	Drequire(payload->tracked_by_reqs_tracker());
        bs->outgoing_content.push_back(payload);

	// See comments about tracking payload in pendingProduceReqs.
	// If the broker tells us that it is no longer the leader for this (topic, partition) we should be
	// able to reschedule to that new leader.
        pendingConsumeReqs.Add(reqId, {clientReqId, payload, nowMS, seqsNumsData, absSeqNumsCnt});
        track_inflight_req(reqId, nowMS, TankAPIMsgType::Consume);

        if (trace)
                SLog(ansifmt::color_green, "About to transmit", ansifmt::reset, "\n");

        return try_transmit(bs);
}

void TankClient::flush_broker(broker *const bs)
{
        if (trace)
                SLog("Flushing Broker:", bs->reqs_tracker.pendingConsume.size(), ", ", bs->reqs_tracker.pendingProduce.size(), ", ", bs->reqs_tracker.pendingDiscover.size(), "\n");

        for (auto it = bs->outgoing_content.front(); it;)
        {
                auto next = it->next;

		if (!it->tracked_by_reqs_tracker())
                	put_payload(it, __LINE__);

                it = next;
        }
        bs->outgoing_content.front_ = bs->outgoing_content.back_ = nullptr;

        for (auto it = bs->retainedPayloadsList.next; it != &bs->retainedPayloadsList;)
        {
                auto next = it->next;
                auto payload = switch_list_entry(outgoing_payload, pendingRespList, it);

		if (!payload->tracked_by_reqs_tracker())
                	put_payload(payload, __LINE__);

                it = next;
        }

        for (const auto id : bs->reqs_tracker.pendingConsume)
        {
                forget_inflight_req(id, TankAPIMsgType::Consume);

                const auto res = pendingConsumeReqs.detach(id);
                auto info = res.value();

		put_payload(info.reqPayload, __LINE__);
                free(info.seqNums);

                capturedFaults.push_back({info.clientReqId, fault::Type::Network, fault::Req::Consume, {}, 0});
        }
        bs->reqs_tracker.pendingConsume.clear();

	for (const auto id : bs->reqs_tracker.pendingDiscover)
	{
		forget_inflight_req(id, TankAPIMsgType::DiscoverPartitions);

		const auto res = pendingDiscoverReqs.detach(id);
		auto info = res.value();

		put_payload(info.reqPayload, __LINE__);
                capturedFaults.push_back({info.clientReqId, fault::Type::Network, fault::Req::DiscoverPartitions, {}, 0});
	}
	bs->reqs_tracker.pendingDiscover.clear();

        for (const auto id : bs->reqs_tracker.pendingProduce)
        {
                forget_inflight_req(id, TankAPIMsgType::Produce);

                const auto res = pendingProduceReqs.detach(id);
                const auto info = res.value();

		put_payload(info.reqPayload, __LINE__);
                free(info.ctx);

                capturedFaults.push_back({info.clientReqId, fault::Type::Network, fault::Req::Produce, {}, 0});
        }
        bs->reqs_tracker.pendingProduce.clear();

        Drequire(bs->reqs_tracker.pendingConsume.empty());
        Drequire(bs->reqs_tracker.pendingProduce.empty());
        Drequire(bs->reqs_tracker.pendingDiscover.empty());

        if (trace)
                SLog("capturedFaults ", capturedFaults.size(), "\n");
}

void TankClient::track_na_broker(broker *const bs)
{
        // Use of a simple circuit breaker so that we won't be retrying connections immediately
        auto *const ctx = &bs->block_ctx;

        if (trace)
                SLog("N/A broker, current reachability ", uint8_t(bs->reachability), ", retries = ", bs->block_ctx.retries, ", retryStrategy = ", unsigned(retryStrategy), "\n");

        if (retryStrategy == RetryStrategy::RetryNever)
        {
		// circuit breaker tripped immediately
                bs->set_reachability(broker::Reachability::Unreachable);
                bs->block_ctx.until = nowMS + Timings::Seconds::ToMillis(8);
                return;
        }

        if (bs->reachability == broker::Reachability::MaybeReachable)
        {
                if (ctx->retries >= 7)
                {
                        // Curcuit breaker tripped
			// Give up - try again in 1 minute, and reject all new requests until then
                        bs->set_reachability(broker::Reachability::Unreachable);
                        bs->block_ctx.until = nowMS + Timings::Seconds::ToMillis(20);

                        if (trace)
                                SLog(ansifmt::color_blue, "Now set to unreachable", ansifmt::reset, "\n");

                        return;
                }
                else
                {
                        if (trace)
                                SLog("From MaybeReachable to Blocked\n");

                        bs->set_reachability(broker::Reachability::Blocked);
                }
        }

        if (bs->reachability != broker::Reachability::Blocked)
        {
		if (trace)
			SLog("Now set to blocked\n");

                ctx->prevSleep = 600;
                ctx->retries = 0;
                ctx->naSince = nowMS;
                bs->set_reachability(broker::Reachability::Blocked);
        }

        ++(ctx->retries);

        const auto delay = SwitchAlgorithms::ComputeExponentialBackoffWithDeccorelatedJitter(Timings::Seconds::ToMillis(7), 720, ctx->prevSleep);

        ctx->prevSleep = delay;
        ctx->until = nowMS + delay;

        rescheduleQueue.push(bs);

        if (trace)
                SLog(ansifmt::color_blue, "retries = ", ctx->retries, ", until = ", ctx->until, " (nowMS = ", nowMS, "), rescheduleQueue.size() = ", rescheduleQueue.size(), ansifmt::reset, "\n");
}

bool TankClient::shutdown(connection *const c, const uint32_t ref, const bool fault)
{
        Drequire(c->type == connection::Type::Tank);

        if (trace)
                SLog("Shutting Down ", ref, " ", Timings::Microseconds::SysTime(), ", fault ", fault, " ", ptr_repr(c), "\n");

        auto *const bs = c->bs;

        if (fault)
                track_na_broker(bs);

        c->bs = nullptr;
        bs->con = nullptr;

        switch_dlist_del_and_reset(&c->list);
        poller.DelFd(c->fd);
        close(c->fd);

        if (auto b = std::exchange(c->inB, nullptr))
                put_buffer(b);

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::ConnectionAttempt)))
                deregister_connection_attempt(c);

        put_connection(c);

        if (retryStrategy == RetryStrategy::RetryAlways && fault && (bs->outgoing_content.front() || switch_dlist_any(&bs->retainedPayloadsList)))
        {
                // Reset for retransmission
                // We are supposed to retain the outgoing payloads, so in case of failure we can insert them
                // back to the queue, or leave them in the queue and only remove when we got the matching response
                if (trace)
                        SLog("Resetting for retransmission\n");

                prepare_retransmission(bs);
                try_transmit(bs);
                return true;
        }
        else
        {
                flush_broker(bs);
                return false;
        }
}

void TankClient::ack_payload(broker *const bs, outgoing_payload *const p)
{
        if (trace)
                SLog(ansifmt::color_magenta, "ACKnowledgeing payload for broker", ansifmt::reset, "\n");

	Drequire(p);
        Drequire(p->tracked_by_reqs_tracker());

        // In case it's here (almost always only when we retain_for_resp(), for payloads of idempotent requests)
        switch_dlist_del_and_reset(&p->pendingRespList);

        put_payload(p, __LINE__);
}

void TankClient::prepare_retransmission(broker *const bs)
{
        if (trace)
                SLog(ansifmt::color_magenta, ansifmt::bold, "Preparing transmission (retainedPayloadsList.len = ", switch_dlist_size(&bs->retainedPayloadsList), ")", ansifmt::reset, "\n");

        // reset remaining payloads in outgoing queue
        for (auto it = bs->outgoing_content.front(); it && it->iovIdx; it = it->next)
                it->iovIdx = 0;

        // insert all retained payloads for idempotent requests to the front of the outgoing content queue
        for (auto it = bs->retainedPayloadsList.next; it != &bs->retainedPayloadsList; it = it->next)
        {
                auto payload = switch_list_entry(outgoing_payload, pendingRespList, it);

                if (trace)
                        SLog(ansifmt::color_magenta, "Rescheduling retained payload ", ptr_repr(payload), ansifmt::reset, "\n");

                bs->outgoing_content.push_front(payload);
        }
        switch_dlist_init(&bs->retainedPayloadsList);

        bs->outgoing_content.validate();
}

void TankClient::reschedule_any()
{
        while (rescheduleQueue.size())
        {
                const auto bs = rescheduleQueue.top();

                if (trace)
                        SLog("To be rescheduled at ", bs->block_ctx.until, ", now ", nowMS, "\n");

                if (nowMS < bs->block_ctx.until)
                        break;

                // Try again, see if it works now
                bs->set_reachability(broker::Reachability::MaybeReachable);

                rescheduleQueue.pop();

                if (trace)
                        SLog("Will now try reschedule(reachability set to MaybeReachable), rescheduleQueue.size() = ", rescheduleQueue.size(), "\n");

                prepare_retransmission(bs);
                try_transmit(bs);
        }

        if (trace)
                SLog("rescheduleQueue.size = ", rescheduleQueue.size(), "\n");
}

bool TankClient::process_produce(connection *const c, const uint8_t *const content, const size_t len)
{
        auto *const bs = c->bs;
        const auto *p = content, *const e = content + len;
        const auto reqId = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const auto res = pendingProduceReqs.detach(reqId);
        const auto reqInfo = res.value();
        const auto clientReqId = reqInfo.clientReqId;

	// TODO:
	// if broker reports that it is no longer the leader for (topic, broker), we need to retain
	// reqInfo.payload and reqInfo.ctx, and retry with that node instead
        ack_payload(bs, reqInfo.reqPayload);
        Defer({ free(reqInfo.ctx); });

        c->state.flags |= (1u << uint8_t(connection::State::Flags::LockedInputBuffer));

        bs->reqs_tracker.pendingProduce.erase(reqId);
        forget_inflight_req(reqId, TankAPIMsgType::Produce);


        for (const auto *ctx = reqInfo.ctx, *const ctxEnd = ctx + reqInfo.ctxLen; p != e;)
        {
                require(ctx < ctxEnd);

                auto err = *p++;
                const strwlen8_t topicName((char *)ctx + 1, *ctx);
                ctx += topicName.len + sizeof(uint8_t);
                auto partitionsCnt = *ctx++;

                if (trace)
                        SLog("topic ", topicName, " ", partitionsCnt, "\n");

                require(partitionsCnt);

                Drequire(ctx < ctxEnd);

                if (err == 0xff)
                {
                        // unknown topic; skip all partitions we tracked
                        if (trace)
                                SLog("Unknown topic [", topicName, "]\n");

                        capturedFaults.push_back({clientReqId, fault::Type::UnknownTopic, fault::Req::Produce, topicName, 0});
                        ctx += partitionsCnt * sizeof(uint16_t);
                }
                else
                {
                        // topic known, consider all partitions
                        for (;;)
                        {
                                const auto partitionId = *(uint16_t *)ctx;
                                ctx += sizeof(uint16_t);

                                if (trace)
                                        SLog("for partition ", partitionId, ", err = ", err, ", partitionsCnt = ", partitionsCnt, "\n");


                                if (err == 1)
                                {
                                        if (trace)
                                                SLog("Unknown partition ", topicName, ".", partitionId, "\n");

                                        capturedFaults.push_back({clientReqId, fault::Type::UnknownPartition, fault::Req::Produce, topicName, partitionId});
                                }
                                else if (err)
                                {
					if (err == 2)
					{
						// Invald sequence numbers
	                                        capturedFaults.push_back({clientReqId, fault::Type::InvalidReq, fault::Req::Produce, topicName, partitionId});
					}
					else
	                                        capturedFaults.push_back({clientReqId, fault::Type::SystemFail, fault::Req::Produce, topicName, partitionId});
                                }
                                else
                                {
                                        produceAcks.push_back({clientReqId, topicName, partitionId});
                                }



                                if (--partitionsCnt == 0)
                                        break;
                                else
                                {
                                        err = *p++;
                                }
                        }
                }
        }

        return true;
}

bool TankClient::process_discover_partitions(connection *const c, const uint8_t *const content, const size_t len)
{
        const auto *p = content;
        auto *const bs = c->bs;
        const auto reqId = *(uint32_t *)p;
        const auto res = pendingDiscoverReqs.detach(reqId);
        const auto reqInfo = res.value();
        const auto clientReqId = reqInfo.clientReqId;
        strwlen8_t topicName;

        p += sizeof(uint32_t);

        ack_payload(bs, reqInfo.reqPayload);
        bs->reqs_tracker.pendingDiscover.erase(reqId);
        forget_inflight_req(reqId, TankAPIMsgType::DiscoverPartitions);

        topicName.Set(resultsAllocator.CopyOf((char *)p + 1, *p), *p);
        p += topicName.len + sizeof(uint8_t);

        const auto cnt = *(uint16_t *)p;

        if (!cnt)
                capturedFaults.push_back({clientReqId, fault::Type::UnknownTopic, fault::Req::DiscoverPartitions, topicName, 0});
        else
        {
                // because we need to access the (firstAvail, lastAssigned) pairs here
                c->state.flags |= (1u << uint8_t(connection::State::Flags::LockedInputBuffer));

                p += sizeof(uint16_t);

                discoverPartitionsResults.push_back({clientReqId, topicName, {(std::pair<uint64_t, uint64_t> *)p, cnt}});
        }

        return true;
}

// XXX: make sure this reflects the latest encoding scheme
// This is somewhat complex, because of boundary checks - can and will simplify later
bool TankClient::process_consume(connection *const c, const uint8_t *const content, const size_t len)
{
        const auto *p = content;
        auto *const bs = c->bs;
        // Response header length (i.e excluding actual batches content) is encoded in the response
        // so that we can quickly jump to the beginning of the bundles
        // the bundles content is streamed right after the response header
        const auto respHeaderLen = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const auto *bundles = p + respHeaderLen, *const bundlesBase = bundles;
        const auto reqId = *(uint32_t *)p;

        c->state.flags |= (1u << uint8_t(connection::State::Flags::LockedInputBuffer));

        if (trace)
                SLog("Processing consume response for ", reqId, ", of length ", len, "\n");

        p += sizeof(uint32_t);
        const auto topicsCnt = *p++;
        const auto res = pendingConsumeReqs.detach(reqId);
        auto reqInfo = res.value();
        const auto clientReqId = reqInfo.clientReqId;
        const auto *const reqSeqNums = reqInfo.seqNums;
        uint8_t reqOffsetIdx{0};
        strwlen8_t key;
	uint64_t firstMsgSeqNum, lastMsgSeqNum, logBaseSeqNum,  msgSetEnd;

	// TODO:
	// if broker reports that it is no longer the leader for (topic, broker), we need to retain
	// reqInfo.payload and reqInfo.ctx, and retry with that node instead
        ack_payload(bs, reqInfo.reqPayload);
        Defer({ free(reqInfo.seqNums); });

        bs->reqs_tracker.pendingConsume.erase(reqId);
        forget_inflight_req(reqId, TankAPIMsgType::Consume);


        for (uint32_t i{0}; i != topicsCnt; ++i)
        {
                const strwlen8_t topicName((char *)p + 1, *p);
                p += topicName.len + sizeof(uint8_t);
                const auto partitionsCnt = *p++;

                if (trace)
                        SLog("Topic [", topicName, "] ", partitionsCnt, "\n");

                if (*(uint16_t *)p == UINT16_MAX)
                {
                        // Unknown topic
                        if (trace)
                                SLog("Unknown topic [", topicName, "]\n");
                        capturedFaults.push_back({clientReqId, fault::Type::UnknownTopic, fault::Req::Consume, topicName, 0});

                        reqOffsetIdx += partitionsCnt;
                        p += sizeof(uint16_t);
                        continue;
                }

                for (uint8_t k{0}; k != partitionsCnt; ++k)
                {
                        const auto partitionId = *(uint16_t *)p;
                        p += sizeof(uint16_t);
                        const auto errorOrFlags = *p++;
			size_t totalSeenBundles{0};

                        if (errorOrFlags == 0xff)
                        {
                                if (trace)
                                        SLog("Undefined partition ", topicName, ".", partitionId, "\n");

                                capturedFaults.push_back({clientReqId, fault::Type::UnknownPartition, fault::Req::Consume, topicName, partitionId});
                                continue;
                        }
			else if (errorOrFlags == 0xfe)
			{
				// Not provided here, because the first bundle streamed has the SPARSE bit set
				// which means it holds the sequence number itself, and we 'd like to avoid
				// specifying this here and in the bundle header
				//
				// I understand all those variosu encoding semantics are complicating the parsing process, but
				// we this is one of those case where doing that in favor of tighter encoding and better performance is worth it
				// (you only need to build the client/broker once, but you 'll reap the benefits forever)
				if (trace)
					SLog("Base seq.num of first msg in the bundle ", ansifmt::bold, "_not provided here_", ansifmt::reset, "; bundle's sparse bit is set - will get it from there\n");
				logBaseSeqNum = 0;
			}
			else
                        {
                                // base absolute sequence number of the first message in the first bundle in the chunk streamed
                                logBaseSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

				if (trace)
					SLog("Base seq.num of first msg in the bundle provided as ", logBaseSeqNum, "\n");
                        }

                        // last committed sequence number for this (topic, partition).
                        // we should check if the *last* parsed absolute sequence number < highWaterMark, and if so, it means
                        // there's more content and we should request it starting from  *last* parsed absolute sequence number + 1
                        const auto highWaterMark = *(uint64_t *)p;
                        p += sizeof(uint64_t);

                        // bytes streamed from the commit log (may contain a partial bundle)
                        const auto len = *(uint32_t *)p;
                        p += sizeof(uint32_t);

                        // Original fetch request absolute sequence number requested
                        // Special values:
                        // UINT64_MAX 	: get data produced from now on, don't return any old records
                        // 0 		: fetch fromt the first available sequence number
                        const auto requestedSeqNum = reqSeqNums[reqOffsetIdx++];

                        if (trace)
                                SLog("logBaseSeqNum = ", logBaseSeqNum, ", highWaterMark = ", highWaterMark, ", len = ", len, ", requestedSeqNum(", requestedSeqNum, ") for ", reqOffsetIdx - 1, ", for partition ", partitionId, ", errorOrFlags = ", errorOrFlags, "\n");

                        if (errorOrFlags == 0x1)
                        {
                                // A BoundaryCheck fault
                                // server has serialized the first available sequence number here
                                const auto firstAvailSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

                                if (trace)
                                        SLog("firstAvailSeqNum = ", firstAvailSeqNum, "\n");

                                capturedFaults.push_back({clientReqId, fault::Type::BoundaryCheck, fault::Req::Consume, topicName, partitionId, {{firstAvailSeqNum, highWaterMark}}});
                                continue;
                        }
                        else if (errorOrFlags && errorOrFlags < 0xfe)
                        {
				if (trace)
					SLog("Have FAULT: ", errorOrFlags, "\n");

                                capturedFaults.push_back({clientReqId, fault::Type::Access, fault::Req::Consume, topicName, partitionId});
                                continue;
                        }

                        const auto bundlesForThisTopicPartition = bundles;
                        uint32_t lastPartialMsgMinFetchSize{128};

                        bundles += len; // Skip bundles for this (topic, partition), i.e the chunk stream for that partition
                        consumptionList.clear();

                        // Process all bundles in the chunk
                        for (const auto *p = bundlesForThisTopicPartition, *const chunkEnd = p + len; p < chunkEnd;)
                        {

                                // Try to parse the next bundle from the chunk
                                // We may have gotten a partial bundle, so we need to be defensive about it

                                // length of the bundle
                                if (!Compression::UnpackUInt32Check(p, chunkEnd))
                                {
                                        if (trace)
                                                SLog("boundaries\n");

                                        break;
                                }

				if (trace)
					SLog("NEW bundle at ", p - bundlesBase, "(" ,consumptionList.size(), ") ", ++totalSeenBundles, "\n");

                                const auto bundleLen = Compression::UnpackUInt32(p);
                                const auto *const bundleEnd = p + bundleLen;

                                // This is more particularly optimal, for if boundary checks fail, we 'll try to consume the whole thing later
                                // and maybe all we 'd need is one 1K message or so, but for now if boundaries check fail, we 'll advise client to
                                // set minSize to a value high enough that will consume the whole bundle at the very least
                                const auto *const boundaryCheckTarget = bundleEnd + 256;

                                if (p >= chunkEnd)
                                {
                                        lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                        if (trace)
                                                SLog("boundaries\n");

                                        break;
                                }


                                // BEGIN: bundle header
                                const auto bundleFlags = *p++;
                                const uint8_t codec = bundleFlags & 3;
				const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                                range_base<const uint8_t *, size_t> msgSetContent;
                                uint32_t msgsSetSize = (bundleFlags >> 2) & 0xf;

                                if (!msgsSetSize)
                                {
					if (trace)
						SLog("msgSetEnd not packed into flags\n");

                                        if (unlikely(!Compression::UnpackUInt32Check(p, chunkEnd)))
                                        {
                                                if (trace)
                                                        SLog("boundaries\n");

                                                break;
                                        }
                                        else
                                        {
                                                msgsSetSize = Compression::UnpackUInt32(p);
                                        }
                                }

				if (sparseBundleBitSet)
                                {
                                        // See tank_encoding.md
					if (trace)
						SLog("sparseBundleBitSet is set\n");

					if (p + sizeof(uint64_t) >= chunkEnd)
                                        {
                                                if (trace)
                                                        SLog("boundaries\n");

                                                break;
                                        }

                                        firstMsgSeqNum = *(uint64_t *)p;
                                        p += sizeof(uint64_t);

                                        if (msgsSetSize != 1)
                                        {
						if (unlikely(!Compression::UnpackUInt32Check(p, chunkEnd)))
						{
							if (trace)
								SLog("boundaries\n");

							break;
						}

                                                lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                                        }
					else
					{
						lastMsgSeqNum = firstMsgSeqNum;
					}


					logBaseSeqNum = firstMsgSeqNum;
					msgSetEnd = lastMsgSeqNum + 1;

					if (trace)
						SLog("firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum = ", lastMsgSeqNum, "\n");
                                }
				else
				{
					msgSetEnd = logBaseSeqNum + msgsSetSize;
				}
                                // END: bundle header



                                if (trace)
                                        SLog("bundleFlags = ", bundleFlags, ", codec = ", codec, ", msgsSetSize = ", msgsSetSize, ", bundleLen = ", bundleLen, "\n");


                                if (requestedSeqNum < UINT64_MAX && requestedSeqNum >= msgSetEnd)
                                {
                                        // Optimization: can skip this bundle altogether
                                        if (trace)
                                                SLog("Skipping bundle:", requestedSeqNum, ">= ", msgSetEnd, "\n");

                                        p = bundleEnd;

                                        if (p > chunkEnd)
                                        {
                                                // past the chunk?
                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                if (trace)
                                                        SLog("Past the chunk, lastPartialMsgMinFetchSize now = ", lastPartialMsgMinFetchSize, " (requestedSeqNum = ", requestedSeqNum, ")\n");
                                        }

					logBaseSeqNum = msgSetEnd;
                                        continue;
                                }

                                if (codec)
                                {
                                        // we 'd need to decompress [p, bundleEnd)

                                        if (bundleEnd > chunkEnd)
                                        {
                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                if (trace)
                                                        SLog("compresssed, didn't get the whole messages set. Boundaries check\n");
                                                break;
                                        }

                                        if (trace)
                                                SLog("compressed, need to decompress messages set ", bundleEnd - p, " (", size_repr(bundleEnd - p), ")\n");

                                        auto rawData = get_buffer();

                                        rawData->clear();
                                        switch (codec)
                                        {
                                                case 1:
                                                        if (unlikely(!Compression::UnCompress(Compression::Algo::SNAPPY, p, bundleEnd - p, rawData)))
                                                        {
                                                                if (trace)
                                                                        SLog("Failed ", chunkEnd - bundleEnd, "\n");

                                                                throw Switch::data_error("Failed to decompress bundle message set");
                                                        }

                                                        if (trace)
                                                                SLog("Decompressed ", size_repr(bundleEnd - p), " => ", size_repr(rawData->length()), "\n");

                                                        break;

                                                default:
                                                        RFLog("Unexpected codec ", codec, "\n");
                                                        exit(1);
                                        }

                                        msgSetContent.Set(reinterpret_cast<const uint8_t *>(rawData->data()), rawData->length());
                                        usedBufs.push_back(rawData);
                                }
                                else
                                        msgSetContent.Set(p, Min<size_t>(chunkEnd - p, bundleEnd - p));

                                p = bundleEnd;

                                uint64_t ts{0};
				const auto _saved = consumptionList.size();
				uint32_t msgIdx{0};

                                // Parse the bundle's message set
                                // if this a compressed messages set, the boundary checks are not necessary
                                for (const auto *p = msgSetContent.offset, *const endOfMsgSet = p + msgSetContent.len;; ++msgIdx, ++logBaseSeqNum)
                                {
                                        // parse next bundle message
                                        if (p + sizeof(uint8_t) > endOfMsgSet)
                                        {
                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                if (trace)
                                                        SLog("Boundaries, total parsed ", consumptionList.size() - _saved, " ", msgsSetSize, "\n");
                                                break;
                                        }

                                        const auto msgFlags = *p++; // msg.flags

					if (sparseBundleBitSet)
                                        {
						if (msgIdx == 0)
						{
							logBaseSeqNum = firstMsgSeqNum;
						}
                                                else if (msgIdx == msgsSetSize - 1)
                                                {
							logBaseSeqNum = lastMsgSeqNum;
						}
						else
                                                {
							if (unlikely(!Compression::UnpackUInt32Check(p, endOfMsgSet)))
							{
                                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                                if (trace)
                                                                        SLog("boundaries\n");
                                                                break;
                                                        }

                                                        const auto delta = Compression::UnpackUInt32(p);

							if (trace)
								SLog("Delta ", delta, "\n");

                                                        logBaseSeqNum += delta;
                                                }
                                        }

					const auto msgAbsSeqNum = logBaseSeqNum;

                                        if (trace)
                                                SLog("Message ", msgIdx, ", msgFlags = ", msgFlags, " for seq.num = ", msgAbsSeqNum, "\n");


                                        if (msgFlags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))
                                        {
                                                // Using the timestamp of the last message that had a specified timestamp in this bundle
                                                // This is very useful; if all messages in a bundle have the same timestamp, then
                                                // we only need to encode the timestamp once
						if (trace)
							SLog("Using last specified TS\n");
                                        }
                                        else
                                        {
                                                if (p + sizeof(uint64_t) > endOfMsgSet)
                                                {
                                                        lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                        if (trace)
                                                                SLog("Boundaries\n");

                                                        goto nextPartition;
                                                }

                                                ts = *(uint64_t *)p; // msg.timestamp
                                                p += sizeof(uint64_t);

						if (trace)
							SLog("Specifying ts = ", ts, "\n");
                                        }

                                        if (msgFlags & uint8_t(TankFlags::BundleMsgFlags::HaveKey))
                                        {
                                                if (p + sizeof(uint8_t) > endOfMsgSet || (p + (*p) + sizeof(uint8_t) > endOfMsgSet))
                                                {
                                                        lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                        if (trace)
                                                                SLog("Boundaries\n");

							goto nextPartition;
                                                }
                                                else
                                                {
                                                        const auto keyLen = *p++;

                                                        if (p + keyLen > endOfMsgSet)
                                                        {
                                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                                if (trace)
                                                                        SLog("Boundaries\n");

                                                                goto nextPartition;
                                                        }
                                                        else
                                                        {
                                                                key.Set((char *)p, keyLen);
                                                                p += keyLen;
                                                        }
                                                }
                                        }
                                        else
                                                key.Unset();

                                        if (!Compression::UnpackUInt32Check(p, endOfMsgSet))
                                        {
                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                if (trace)
                                                        SLog("Boundaries\n");

                                                goto nextPartition;
                                        }

                                        const auto len = Compression::UnpackUInt32(p); // message.length

                                        if (trace)
                                                SLog("message length = ", len, ", ts = ", ts, "(", Date::ts_repr(Timings::Milliseconds::ToSeconds(ts)), " for (", msgAbsSeqNum, ")\n");

                                        if (p + len > endOfMsgSet)
                                        {
                                                lastPartialMsgMinFetchSize = std::max<size_t>(lastPartialMsgMinFetchSize, boundaryCheckTarget - bundlesBase);

                                                if (trace)
                                                        SLog("Boundaries(msgLength = ", len, " => ", lastPartialMsgMinFetchSize, ")\n");

                                                goto nextPartition;
                                        }


                                        if (msgAbsSeqNum > highWaterMark)
                                        {
                                                if (trace)
                                                        SLog("Reached past high water mark(last assigned sequence number) ", msgAbsSeqNum, " > ", highWaterMark, "\n");

                                                goto nextPartition;
                                        }
                                        else if (requestedSeqNum == UINT64_MAX || msgAbsSeqNum >= requestedSeqNum)
                                        {
                                                const strwlen32_t content((char *)p, len); // message content

                                                if (trace && 0)
                                                        SLog("( flags = ", msgFlags, ", ts = ", ts, ", len = ", len, ", msgAbsSeqNum = ", msgAbsSeqNum, " [", content, ", requestedSeqNum = ", requestedSeqNum, "] )\n");

                                                consumptionList.push_back({msgAbsSeqNum, ts, key, content});
                                        }
                                        else
                                        {
                                                // The Tank broker can and currently is streaming based on index boundaries, so
                                                // it may stream bundles that contain messages with seq.num < requested sequence number, so we
                                                // are just going to ignore those here - wether that is the case or not.
                                                //
                                                // The broker does this for performance, so that it won't need to scan ahead from the index boundary in order
                                                // to locate the bundle that contains the message with requestedSeqNum
                                                //
                                                // Kafka will search forward(scn) for the segment absolute file offset of the last offset that is >= target offset
                                                // all the time via its FileMessageSet#searchFor()
                                                //
                                                // The broker strategy doesn't really matter much all things considered, as long as we are able to properly filter
                                                // bundles and messages here, which we should always do

                                                if (trace)
                                                        SLog("Skipping, msgAbsSeqNum(", msgAbsSeqNum, ") < requestedSeqNum(", requestedSeqNum, ")\n");
                                        }

                                        p += len;
                                }
                        }

                nextPartition:
                        // It's possible that next is what we requested already, if
                        // we couldn't parse a single message from any bundle for this (topic, partition)  - i.e consumptionsList.empty() == true
                        const auto next = consumptionList.size()
                                              ? requestedSeqNum == UINT64_MAX ? consumptionList.back().seqNum + 1 : Max(requestedSeqNum, consumptionList.back().seqNum + 1)
                                              : requestedSeqNum;

                        if (const uint32_t cnt = consumptionList.size())
                        {
                                if (i == topicsCnt - 1 && k == partitionsCnt - 1)
                                {
                                        // optimization
                                        consumedPartitionContent.push_back({clientReqId, topicName, partitionId, {consumptionList.values(), cnt}, {next, lastPartialMsgMinFetchSize}});
                                }
                                else
                                {
                                        auto p = resultsAllocator.CopyOf(consumptionList.values(), cnt);

                                        consumedPartitionContent.push_back({clientReqId, topicName, partitionId, {p, cnt}, {next, lastPartialMsgMinFetchSize}});
                                        consumptionList.clear();
                                }
                        }
                        else
                        {
                                // We may still get an empty list of messages for a (topic, partition)
                                // if the fetch request timed out, etc. Still need to notify the client
                                consumedPartitionContent.push_back({clientReqId, topicName, partitionId, {nullptr, 0}, {next, lastPartialMsgMinFetchSize}});
                        }
                }
        }

        return true;
}

bool TankClient::process(connection *const c, const uint8_t msg, const uint8_t *const content, const size_t len)
{
        if (trace)
                SLog("PROCESS ", msg, " ", len, "\n");

        switch (TankAPIMsgType(msg))
        {
                case TankAPIMsgType::Produce:
                        return process_produce(c, content, len);

                case TankAPIMsgType::Consume:
                        return process_consume(c, content, len);

		case TankAPIMsgType::DiscoverPartitions:
			return process_discover_partitions(c, content, len);

                case TankAPIMsgType::Ping:
                        if (trace)
                                SLog("PING\n");
                        return true;

                default:
                        shutdown(c, __LINE__);
                        return false;
        }
}

bool TankClient::try_recv(connection *const c)
{
        int n, r, fd = c->fd;
        auto b = c->inB;

        if (!b)
                b = c->inB = get_buffer();

        for (;;)
        {
                if (unlikely(ioctl(fd, FIONREAD, &n) == -1))
                        throw Switch::system_error("ioctl() failed:", strerror(errno));

                b->reserve(n);
                r = read(fd, b->End(), b->Capacity());

                if (trace)
                        SLog("Read ", r, "\n");

                if (r == -1)
                {
                        if (errno == EINTR)
                                continue;
                        else if (errno == EAGAIN)
                                return true;
                        else
                        {
                                shutdown(c, __LINE__, true);
                                return false;
                        }
                }
                else if (!r)
                {
                        shutdown(c, __LINE__, true);
                        return false;
                }
                else
                {
                        const auto *p = (uint8_t *)b->AtOffset();

                        if (c->state.flags & (1u << uint8_t(connection::State::Flags::ConnectionAttempt)))
                        {
                                if (trace)
                                        SLog("Connection established\n");

                                require(c->bs);
                                c->bs->set_reachability(broker::Reachability::Reachable);
				c->bs->block_ctx.retries = 0;
                                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::ConnectionAttempt));
                                deregister_connection_attempt(c);
                        }

                        c->state.lastInputTS = nowMS;
                        c->state.flags &= ~(1u << uint8_t(connection::State::Flags::LockedInputBuffer));

                        b->AdvanceLength(r);

                        for (const auto *e = (uint8_t *)b->End(); p != e;)
                        {
                                if (p + sizeof(uint8_t) + sizeof(uint32_t) > e)
                                {
                                        if (trace)
                                                SLog("Need more for msg\n");

                                        return true;
                                }

                                const auto msg = *p++;
                                const auto len = *(uint32_t *)p;
                                p += sizeof(uint32_t);

                                if (0 == (c->state.flags & ((1u << uint8_t(connection::State::Flags::LockedInputBuffer)) | (1u << uint8_t(connection::State::Flags::ConsideredReqHeader)))))
                                {
                                        // So that ingestion of future incoming data will not require buffer reallocations
                                        // We need to make sure we won't force a reallocation of the internal buffer memory
                                        // See LockedInputBuffer comments
                                        const auto o = (char *)p - b->data();

                                        b->reserve(len);

                                        p = (uint8_t *)b->At(o);
                                        e = (uint8_t *)b->End();

                                        c->state.flags |= 1u << uint8_t(connection::State::Flags::ConsideredReqHeader);
                                }

                                // TODO:
                                // if msg == MSG_CONSUME, and len > threshold, then we could
                                // https://github.com/phaistos-networks/TANK/issues/1
                                if (p + len > e)
                                {
                                        // no need to e.g b->reserve(len) here
                                        // because we already reserve()d FIONREAD result earlier and this could also
                                        // cause problems with references in e.g consumedPartitionContent that derefs buffer's data
                                        if (trace)
                                                SLog("Need more content (len = ", len, ") for msg(", msg, "), now have = ", b->length(), "\n");

                                        return true;
                                }

                                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::ConsideredReqHeader));
                                if (!process(c, msg, p, len))
                                        return false;
                                else
                                {
                                        p += len;
                                        b->SetOffset((char *)p);
                                }
                        }



			if (0 == (c->state.flags & (1u << uint8_t(connection::State::Flags::LockedInputBuffer))))
			{
				if (auto b = c->inB)
                                {
                                        if (b->IsAtEnd())
                                        {
                                                put_buffer(b);
                                                c->inB = nullptr;
                                        }
                                        else if (b->Offset() > 1 * 1024 * 1024)
                                        {
                                                b->DeleteChunk(0, b->Offset());
                                                b->SetOffset(uint64_t(0));
                                        }
                                }
                        }
			else
                        {
                                // We need to defer this
                                // for the next Poll() call, because e.g process_consume() is going
                                // to point inside the buffer, and we don't want to free or otherwise modify the buffer until
                                // the caller had a chance to e.g go through all consumed messages.
                                connsBufs.push_back({c, b});
                        }


                        return true;
                }
        }
}

void TankClient::retain_for_resp(broker *const bs, outgoing_payload *const p)
{
        switch_dlist_init(&p->pendingRespList);
        switch_dlist_insert_after(&bs->retainedPayloadsList, &p->pendingRespList);
}

bool TankClient::try_send(connection *const c)
{
        auto fd = c->fd;
        auto bs = c->bs;

        Drequire(bs);
        Drequire(c->fd != -1);

        bs->outgoing_content.validate();

        for (;;)
        {
                struct iovec iov[128], *out = iov, *const outEnd = out + sizeof_array(iov);

                for (auto it = bs->outgoing_content.front(); it && out != outEnd; it = it->next)
                {
                        const auto n = Min<uint32_t>(outEnd - out, (it->iovCnt - it->iovIdx));

                        if (trace)
                                SLog(ansifmt::bold, ansifmt::color_red, "SENDING ", ptr_repr(it), " ", it->iovIdx, ansifmt::reset, "\n");

                        memcpy(out, it->iov + it->iovIdx, n * sizeof(struct iovec));
                        out += n;
                }

                if (out == iov)
                {
                        if (trace)
                                SLog("Nothing to send now\n");

                        break;
                }

                if (trace)
                        SLog("Will attempt to send ", out - iov, " iovecs\n");

                for (;;)
                {
                        auto r = writev(fd, iov, out - iov);

                        if (trace)
                                SLog("writev()  = ", r, "\n");

                        if (r == -1)
                        {
                                if (errno == EINTR)
                                        continue;
                                else if (errno == EAGAIN)
                                {
                                unsetNeedAvail:
                                        if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))))
                                        {
                                                c->state.flags |= 1u << uint8_t(connection::State::Flags::NeedOutAvail);
                                                poller.SetDataAndEvents(fd, c, POLLIN | POLLOUT);

                                                if (trace)
                                                        SLog("UNSETTING NeedOutAvail\n");
                                        }
                                        return true;
                                }
                                else
                                        return shutdown(c, __LINE__, true);
                        }

                        c->state.lastOutputTS = nowMS;
                        while (auto it = bs->outgoing_content.front())
                        {
                                size_t len;
                                iovec *ptr;

                                while (r >= (len = (ptr = it->iov + it->iovIdx)->iov_len))
                                {
                                        r -= len;
                                        if (++(it->iovIdx) == it->iovCnt)
                                        {
                                		auto *const next = it->next;
						const auto flags = it->flags;

                                                bs->outgoing_content.pop_front();

						if (flags & (1u << uint8_t(outgoing_payload::Flags::ReqIsIdempotent)))
                                                {
							// Payload for an idempotent request.
							// We are going to retain the payload and we will reschedule it to the same broker if need be
                                                        retain_for_resp(bs, it);
                                                }
						else if (flags & (1u << uint8_t(outgoing_payload::Flags::ReqMaybeRetried)))
						{
							// Not an idempotent request, but broker may request that we
							// retry the request with another broker, a new leader for a (topic, partition)
							// so we need to keep the payload around, and not reuse it via put_payload()
							// we won't reschedule this to the same broker though
						}
                                                else
                                                        put_payload(it, __LINE__);

                                                bs->outgoing_content.front_ = next;
                                                goto next;
                                        }
                                }
                                ptr->iov_len -= r;
                                ptr->iov_base = (char *)ptr->iov_base + r;
                                break;

                        next:;
                        }

                        break;
                }
        }

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))
        {
                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::NeedOutAvail));
                poller.SetDataAndEvents(c->fd, c, POLLIN);

                if (trace)
                        SLog("UNSETTING NeedOutAvail\n");
        }

        return true;
}

int TankClient::init_connection_to(const Switch::endpoint e)
{
        struct sockaddr_in sa;
        int fd;

        memset(&sa, 0, sizeof(sa));
        sa.sin_addr.s_addr = e.addr4;
        sa.sin_port = htons(e.port);
        sa.sin_family = AF_INET;

        fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

        if (fd == -1)
        {
                RFLog("socket() failed:", strerror(errno), "\n");
                return -1;
        }

        if (trace)
                SLog("Connecting to ", e, "\n");

        Switch::SetNoDelay(fd, 1);

	// On Linux, the default send buffer size is 16kb, and the default recv buffer size is 86k
	// use getsockopt() to determine it
	if (sndBufSize)
	{
		if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&sndBufSize, sizeof(sndBufSize)) == -1)
			Print("WARNING: unable to set socket send buffer size:", strerror(errno), "\n");
	}

	if (rcvBufSize)
	{
		if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&rcvBufSize, sizeof(rcvBufSize)) == -1)
			Print("WARNING: unable to set socket rcv buffer size:", strerror(errno), "\n");
	}

        if (connect(fd, (sockaddr *)&sa, sizeof(sa)) == -1 && errno != EINPROGRESS)
        {
                close(fd);
                RFLog("connect() failed:", strerror(errno), "\n");
                return -1;
        }
        else
                return fd;
}

void TankClient::poll(uint32_t timeoutMS)
{
        // if this client has joined a consumer group, it is important that the application poll()s giving chances to
        // the tank client to send H/Bs to the broker so that it won't remove the client from the consumer group. See Kafka Consumer Groups


        // Reset state / buffers used for tracking collected content and responses in last poll() call
        for (auto &it : connsBufs)
        {
                auto c = it.first;
                auto b = it.second;

                if (c->inB == b) 	// make sure they are still paired together
                {
                        if (b->IsAtEnd())
                        {
                                put_buffer(b);
                                c->inB = nullptr;
                        }
                        else if (b->Offset() > 1 * 1024 * 1024)
                        {
                                b->DeleteChunk(0, b->Offset());
                                b->SetOffset(uint64_t(0));
                        }
                }
        }
        connsBufs.clear();



        // TODO: https://github.com/phaistos-networks/TANK/issues/6
	if (const auto n = usedBufs.size())
	{
		put_buffers(usedBufs.values(), n);
		usedBufs.clear();
	}



        // Reset prior to reschedule_any() not after we have called it
        // because it may push to capturedFaults[] so we don't want to clear it after we called it
        resultsAllocator.Reuse();
        consumedPartitionContent.clear();
        capturedFaults.clear();
        produceAcks.clear();
	discoverPartitionsResults.clear();


	// it is important that we update_time_cache() here before we invoke reschedule_any() and right after Poll()
	update_time_cache();

        reschedule_any();

        // Adjust timeout if we have any ongoing connection attempts
        if (connectionAttempts.size())
        {
                const auto first = connectionAttempts.front()->state.lastInputTS + 256;

                timeoutMS = first >= nowMS ? first - nowMS : 0;
        }

        polling.store(true, std::memory_order_relaxed);

        const auto r = poller.Poll(timeoutMS);

        polling.store(false, std::memory_order_relaxed);

        if (r == -1)
        {
                if (errno == EINTR || errno == EAGAIN)
                        return;
                else
                        throw Switch::system_error("epoll_wait(): ", strerror(errno));
        }

	update_time_cache();

        for (const auto *it = poller.Events(), *const e = it + r; it != e; ++it)
        {
                const auto events = it->events;
                auto *const c = (connection *)it->data.ptr;

                if (events & (POLLERR | POLLHUP))
                {
                        shutdown(c, __LINE__, true);
                        continue;
                }

                if (c->fd == pipeFd[0])
                {
                        if (events & POLLIN)
                        {
                                // drain it
                                int n;
                                char buf[512];
                                int fd = c->fd;

                                if (unlikely(ioctl(fd, FIONREAD, &n) == -1))
                                        throw Switch::system_error("ioctl() failed:", strerror(errno));
				else if (unlikely(!n))
				{
					// Just in case we get 0; if we do, read() will fail with EAGAIN anyway
					// We shouldn't get n == 0 but let's be on the safe side
					n = sizeof(buf);
				}

                                do
                                {
                                        const auto r = read(fd, buf, sizeof(buf));

                                        if (r == -1)
                                        {
                                                if (errno != EAGAIN && errno != EINTR)
                                                        throw Switch::system_error("Unable to drain pipe:", strerror(errno));
                                                else
                                                        break;
                                        }
                                        else if (!r)
                                        {
                                                break;
                                        }
                                        else
                                                n -= r;

                                } while (n);
                        }

                        continue;
                }

                if ((events & POLLIN) && !try_recv(c))
                        continue;

                if (events & POLLOUT)
                        try_send(c);
        }

        if (connectionAttempts.size())
        {
                connsList.clear();

                for (auto c : connectionAttempts)
                {
                        if (c->state.lastInputTS + 256 <= nowMS) // it is important to test for <= nowMS, not < nowMS
                                connsList.push_back(c);
                        else
                                break;
                }

                while (connsList.size())
                        shutdown(connsList.Pop(), __LINE__, true);
        }

        if (nowMS > nextInflightReqsTimeoutCheckTs)
        {
                consider_inflight_reqs(nowMS);
                nextInflightReqsTimeoutCheckTs = nowMS + 800;
        }
}

uint32_t TankClient::produce_to(const topic_partition &to, const std::vector<msg> &msgs)
{
        const auto clientReqId = ids_tracker.client.next++;
	const auto leader = leader_for(to.first, to.second);
	produce_ctx ctx;

	ctx.leader = leader;
	ctx.topic = to.first;
	ctx.partitionId = to.second;
	ctx.msgs = msgs.data();
	ctx.msgsCnt = msgs.size();

	update_time_cache();
        if (!produce_to_leader(clientReqId, leader, &ctx, 1))
		return 0;
	else
		return clientReqId;
}

uint32_t TankClient::produce_with_base(const std::vector<
	std::pair<topic_partition, std::pair<uint64_t, std::vector<msg>>>> &req)
{
        auto &out = produceOut;

        if (trace)
                SLog("Producing into ", req.size(), " partitions\n");

        out.clear();
        for (const auto &it : req)
        {
                const auto ref = it.first;
                const auto topic = ref.first;

                if (trace)
                        SLog("For (", topic, ", ", ref.second, "): ", it.second.second.size(), "\n");

                out.push_back(produce_ctx{{leader_for(topic, ref.second)}, topic, ref.second, it.second.first, it.second.second.data(), it.second.second.size()});
        }

        std::sort(out.begin(), out.end(), [](const auto &a, const auto &b) {
                return a.leader < b.leader;
        });

        auto *const all = out.values();
        const auto cnt = out.size();
        const auto clientReqId = ids_tracker.client.next++;

	update_time_cache();
        for (uint32_t i{0}; i != cnt;)
        {
                auto it = all + i;
                const auto leader = it->leader;
                const auto base{i};

                for (++i; i != cnt && (it = all + i)->leader == leader; ++i)
                        continue;

                std::sort(all + base, all + i, [](const auto &a, const auto &b) {
                        return a.topic < b.topic;
                });

                if (!produce_to_leader_with_base(clientReqId, leader, all + base, i - base))
                        return 0;
        }

        return clientReqId;
}

uint32_t TankClient::produce(const std::vector<
                             std::pair<topic_partition, std::vector<msg>>> &req)
{
        auto &out = produceOut;

        if (trace)
                SLog("Producing into ", req.size(), " partitions\n");

        out.clear();
        for (const auto &it : req)
        {
                const auto ref = it.first;
                const auto topic = ref.first;

                if (trace)
                        SLog("For (", topic, ", ", ref.second, "): ", it.second.size(), "\n");

                out.push_back(produce_ctx{{leader_for(topic, ref.second)}, topic, ref.second, 0, it.second.data(), it.second.size()});
        }

        std::sort(out.begin(), out.end(), [](const auto &a, const auto &b) {
                return a.leader < b.leader;
        });

        auto *const all = out.values();
        const auto cnt = out.size();
        const auto clientReqId = ids_tracker.client.next++;

	update_time_cache();
        for (uint32_t i{0}; i != cnt;)
        {
                auto it = all + i;
                const auto leader = it->leader;
                const auto base{i};

                for (++i; i != cnt && (it = all + i)->leader == leader; ++i)
                        continue;

                std::sort(all + base, all + i, [](const auto &a, const auto &b) {
                        return a.topic < b.topic;
                });

                if (!produce_to_leader(clientReqId, leader, all + base, i - base))
                        return 0;
        }

        return clientReqId;
}

void TankClient::set_topic_leader(const strwlen8_t topic, const strwlen32_t endpoint)
{
        const auto e = Switch::ParseSrvEndpoint(endpoint, {_S("tank")}, 11011);

        Drequire(topic);
        if (!e)
                throw Switch::data_error("Unable to parse leader endpoint");

        leadersMap.Add(topic, e);
}

Switch::endpoint TankClient::leader_for(const strwlen8_t topic, const uint16_t partition)
{
#ifndef LEAN_SWITCH
        if (const auto *const p = leadersMap.FindPointer(topic))
                return *p;
#else
        const auto it = leadersMap.find(topic);

        if (it != leadersMap.end())
                return it->second;
#endif

        if (!defaultLeader)
                throw Switch::data_error("Default leader not specified: use set_default_leader() to specify it");

        return defaultLeader;
}

uint32_t TankClient::consume_from(const topic_partition &from, const uint64_t seqNum, const uint32_t minFetchSize, const uint64_t maxWait, const uint32_t minSize)
{
	const auto leader = leader_for(from.first, from.second);
        const auto clientReqId = ids_tracker.client.next++;
	consume_ctx ctx;

	ctx.leader = leader;
	ctx.topic = from.first;
	ctx.partitionId = from.second;
	ctx.absSeqNum = seqNum;
	ctx.fetchSize = minFetchSize;

	update_time_cache();
        if (!consume_from_leader(clientReqId, leader, &ctx, 1, maxWait, minSize))
		return 0;
	else
		return clientReqId;
}

uint32_t TankClient::consume(const std::vector<
                                 std::pair<topic_partition,
                                           std::pair<uint64_t, uint32_t>>> &req,
                             const uint64_t maxWait, const uint32_t minSize)
{
        auto &out = consumeOut;

        if (trace)
                SLog("About to request consume from ", req.size(), " topics\n");

        out.clear();
        for (const auto &p : req)
        {
                const auto ref = p.first;
                const auto topic = ref.first;

                out.push_back({leader_for(topic, ref.second), topic, ref.second, p.second.first, p.second.second});
        }

        std::sort(out.begin(), out.end(), [](const auto &a, const auto &b) {
                return a.leader < b.leader;
        });

        const auto n = out.size();
        auto *const all = out.values();
        const auto clientReqId = ids_tracker.client.next++;

	update_time_cache();
        for (uint32_t i{0}; i != n;)
        {
                const auto leader = all[i].leader;
                const auto base = i;

                for (++i; i != n && all[i].leader == leader; ++i)
                        continue;

                std::sort(all + base, all + i, [](const auto &a, const auto &b) {
                        return a.topic.Cmp(b.topic) < 0;
                });

                if (!consume_from_leader(clientReqId, leader, all + base, i - base, maxWait, minSize))
                {
                        if (trace)
                                SLog("consume_from_leader() returned false\n");

                        return 0;
                }
        }

        return clientReqId;
}

void TankClient::interrupt_poll()
{
        bool to{true};

        if (polling.compare_exchange_weak(to, false, std::memory_order_release, std::memory_order_relaxed))
                write(pipeFd[1], " ", 1);
}

void TankClient::broker::set_reachability(const Reachability r)
{
        if (trace)
                SLog(ansifmt::bold, ansifmt::color_green, "Reachability from ", uint8_t(reachability), " to ", uint8_t(r), ansifmt::reset, "\n");

        reachability = r;
}

#define _BUFFERS_PRESSURE_THRESHOLD (4 * 1024 * 1024)
#define _BUFFERS_POOL_THRESHOLD 512

void TankClient::put_buffer(IOBuffer *const b)
{
        require(b);
        const size_t newValue = buffersPoolPressure + b->Reserved();

        if (newValue > _BUFFERS_PRESSURE_THRESHOLD || buffersPool.size() > _BUFFERS_POOL_THRESHOLD)
                delete b;
        else
        {
                buffersPoolPressure = newValue;
                b->clear();
                buffersPool.push_back(b);
        }
}

void TankClient::put_buffers(IOBuffer **const list, const size_t n)
{
        uint32_t i{0};
        IOBuffer *b;
        size_t newValue;

        for (; i != n && (newValue = buffersPoolPressure + (b = list[i])->Reserved()) < _BUFFERS_PRESSURE_THRESHOLD 
		&& buffersPool.size() < _BUFFERS_POOL_THRESHOLD; ++i)
        {
                buffersPoolPressure = newValue;
                b->clear();
                buffersPool.push_back(b);
        }

        while (i != n)
                delete list[i++];
}

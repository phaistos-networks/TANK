#pragma once
#include "common.h"
#include <atomic>
#include <compress.h>
#include <network.h>
#include <queue>
#include <set>
#include <switch.h>
#include <switch_dictionary.h>
#include <switch_ll.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include <vector>

// Tank, because its a large container of liquid or gas
// and data flow (as in, liquid), and also, this is a Trinity character name
class TankClient
{
      private:
        struct broker;

      public:
        enum class ProduceFlags : uint8_t
        {

        };

        enum class RetryStrategy : uint8_t
        {
                RetryNever = 0,
                RetryAlways
        };

        enum class CompressionStrategy : uint8_t
        {
                CompressNever = 0,
                CompressAlways,
                CompressIntelligently
        };

        struct msg
        {
                strwlen32_t content;
                uint64_t ts;
                strwlen8_t key;
        };

        using topic_partition = std::pair<strwlen8_t, uint16_t>;

        // there is one such payload per request
        // we are going to keep them around even after we have dispatched them
        // in case we want to retry them; that is the purprose of the pendingRespList and RetainForAck flag
        struct outgoing_payload
        {
                outgoing_payload *next;
                switch_dlist pendingRespList;
                IOBuffer *b, *b2;
                struct iovec iov[64];
                uint8_t iovCnt;
                uint8_t iovIdx;

                enum class Flags : uint8_t
                {
                        // if set, it means the payload must be retained if it has been sent via writev(), until the response for the request of this payload is received
                        // That is, after we write() to the socket buffer, we can't know if the broker has gotten the chance to process the request or not, so retrying it
                        // may lead to problems. If this is set, it means rescheduling, even if the original request was processes, won't affect state
                        ReqIsIdempotent = 0,

                        // If the broker report it is no longer the leader for a (topic, partition), we may need to retry the same request
                        // XXX: However, a request may include multiple distinct (topic, partition)s, so we may not
                        // be able to reschedule it as-is, and so we 'd need to account for that - that is, we should either set ReqMaybeRetried and
			// track it in pendingConsumeReqs and pendingProduceReqs iff number of partitions involved in the request == 1, or we should
			// set another flag, set another flag, and when we are told to try another node, either unpack the tracked payload to send
			// the data to where we need to send them, or do something else.
                        ReqMaybeRetried = 1
                };
                uint8_t flags;
                uint32_t __id;

		// If the payload is tracked by reqs_tracker.pendingConsume or reqs_tracker.pendingProduce, then
		// we shouldn't try to put_payload() if it's registered with outgoing_content (either in pendingRespList or in outgoing payloads list)
		inline bool tracked_by_reqs_tracker() const
		{
                        return flags & ((1u << uint8_t(Flags::ReqIsIdempotent)) | (1u << uint8_t(Flags::ReqMaybeRetried)));
		}
        };

        struct consume_ctx
        {
                Switch::endpoint leader;
                strwlen8_t topic;
                uint16_t partitionId;
                uint64_t absSeqNum;
                uint32_t fetchSize;
        };

        struct produce_ctx
        {
                Switch::endpoint leader;
                strwlen8_t topic;
                uint16_t partitionId;
                const msg *msgs;
                size_t msgsCnt;
        };

        struct connection
        {
                int fd{-1};
                switch_dlist list;
                IOBuffer *inB{nullptr};
                broker *bs{nullptr};

                enum class Type : uint8_t
                {
                        Tank
                } type{Type::Tank};

                struct State
                {
                        enum class Flags : uint8_t
                        {
                                ConnectionAttempt = 0,
                                NeedOutAvail,
                                RetryingConnection,
                                ConsideredReqHeader,

                                // Consume responses, produce acks, and errors almost all derefence the connection input buffer data.
                                // Once we dereference it, we can't reallocate the buffer internal memory, so this is set.
				//
                                // This flag is unset when we read data, and may be set by various process_() methods, and considered
                                // when we process the ingested input
                                LockedInputBuffer
                        };

                        uint8_t flags;
                        uint64_t lastInputTS, lastOutputTS;
                } state;

                connection()
                {
                        switch_dlist_init(&list);
                }
        };

        void deregister_connection_attempt(connection *const c)
        {
                connectionAttempts.RemoveByValue(c);
        }

        struct active_consume_req
        {
                uint32_t clientReqId;
                outgoing_payload *reqPayload;
                uint64_t ts;
                uint64_t *seqNums;
                uint8_t seqNumsCnt;
        };

        struct active_produce_req
        {
                uint32_t clientReqId;
                outgoing_payload *reqPayload;
                uint64_t ts;
                uint8_t *ctx;
                uint32_t ctxLen;
        };

        struct produce_ack
        {
                uint32_t clientReqId;
                strwlen8_t topic;
                uint16_t partition;
        };

        struct consumed_msg
        {
                uint64_t seqNum;
                uint64_t ts;
                strwlen8_t key;
                strwlen32_t content;
        };

        struct partition_content
        {
                uint32_t clientReqId;
                strwlen8_t topic;
                uint16_t partition;
                range_base<consumed_msg *, uint32_t> msgs;

                // We can't rely on msgs.back().seqNum + 1 to compute the next sequence number
                // to consume from, because if no message at all can be parsed because
                // of the request fetch size, msgs will be empty()
                struct
                {
                        // to get more messages
                        // consume from (topic, partition) starting from seqNum,
                        // and set fetchSize to be at least minFetchSize
                        uint64_t seqNum;
                        uint32_t minFetchSize;
                } next;
        };

        struct fault
        {
                uint32_t clientReqId;

                enum class Type : uint8_t
                {
                        UnknownTopic = 1,
                        UnknownPartition,
                        Access,
                        Network,
                        BoundaryCheck
                } type;

                enum class Req : uint8_t
                {
                        Consume = 1,
                        Produce
                } req;

                strwlen8_t topic;
                uint16_t partition;

                union {
                        // Set when fault is BoundaryCheck
                        struct
                        {
                                uint64_t firstAvailSeqNum;
                                uint64_t highWaterMark;
                        };
                } ctx;
        };

      private:
        struct
        {
                struct
                {
                        struct
                        {
                                uint32_t next{1};
                        } consume;

                        struct
                        {
                                uint32_t next{1};
                        } produce;
                } client;

                struct
                {
                        uint32_t next{1};
                } leader_reqs;
        } ids_tracker;

        struct broker
        {
                struct connection *con{nullptr};
                const Switch::endpoint endpoint;

                switch_dlist retainedPayloadsList;

                enum class Reachability : uint8_t
                {
                        Reachable = 0,  // Definitely reachable, or we just don't know yet for we haven't tried to connect
                        Unreachable,    // Definitely unreachable; can't retry connection until block_ctx.until
                        MaybeReachable, // Was blocked, and will now retry the connection in case it is now possible
                        Blocked         // Blocked; will retry the connection at block_ctx.until
                } reachability{Reachability::Reachable};

                struct
                {
                        uint32_t prevSleep;
                        uint64_t until;
                        uint64_t naSince;
                        uint16_t retries;
                } block_ctx;

                void set_reachability(const Reachability r);

                // outgoing content will be associated with a leader state, not its connection
                // and the connection will drain this outgoing_content
                struct
                {
                        outgoing_payload *front_{nullptr}, *back_{nullptr};

                        void push_front(outgoing_payload *const p)
                        {
                                p->iovIdx = 0;
                                p->next = front_;
                                front_ = p;
                                if (!back_)
                                        back_ = front_;
                        }

                        // this will be a no-op a few revisions later
                        // just need to make sure ordering is preserved
                        void validate()
                        {
                                if (auto it = front_)
                                {
                                        require(it->__id);

                                        for (auto prev = it; (it = it->next);)
                                        {
                                                require(it->__id);
                                                require(it->__id > prev->__id);
                                        }
                                }
                        }

                        void push_back(outgoing_payload *const p)
                        {
                                p->iovIdx = 0;
                                p->next = nullptr;

                                if (back_)
                                        back_->next = p;
                                else
                                        front_ = p;

                                back_ = p;
                        }

                        auto front()
                        {
                                return front_;
                        }

                        void pop_front()
                        {
                                front_ = front_->next;
                                if (!front_)
                                        back_ = nullptr;
                        }

                } outgoing_content;

                struct
                {
                        std::set<uint32_t> pendingConsume;
                        std::set<uint32_t> pendingProduce;
                } reqs_tracker;

                broker(const Switch::endpoint e)
                    : endpoint{e}
                {
                        switch_dlist_init(&retainedPayloadsList);
                }
        };

        uint64_t nextInflightReqsTimeoutCheckTs{0};
        RetryStrategy retryStrategy{RetryStrategy::RetryAlways};
        CompressionStrategy compressionStrategy{CompressionStrategy::CompressIntelligently};
        Switch::unordered_map<Switch::endpoint, broker *> bsMap;
        Switch::unordered_map<strwlen8_t, Switch::endpoint> leadersMap;
        Switch::endpoint defaultLeader{};
	int sndBufSize{0}, rcvBufSize{0};
        strwlen8_t clientId{"c++"};
        switch_dlist connections;
        Switch::vector<std::pair<connection *, IOBuffer *>> connsBufs;
        // TODO: https://github.com/phaistos-networks/TANK/issues/6
        Switch::vector<IOBuffer *> usedBufs;
        simple_allocator resultsAllocator{4 * 1024 * 1024};
        Switch::vector<partition_content> consumedPartitionContent;
        Switch::vector<fault> capturedFaults;
        Switch::vector<produce_ack> produceAcks;
        Switch::vector<consumed_msg> consumptionList;
        Switch::vector<consume_ctx> consumeOut;
        Switch::vector<produce_ctx> produceOut;
        uint64_t nowMS;
        uint32_t nextConsumeReqId{1}, nextProduceReqId{1};
        Switch::vector<connection *> connectionAttempts, connsList;
        EPoller poller;
        int pipeFd[2];
        std::atomic<bool> polling{false};
        Switch::vector<connection *> connectionsPool;
        Switch::vector<outgoing_payload *> payloadsPool;
        Switch::vector<IOBuffer *> buffersPool;
        size_t buffersPoolPressure{0};
        Switch::vector<range32_t> ranges;
        IOBuffer produceCtx;
        Switch::unordered_map<uint32_t, active_consume_req> pendingConsumeReqs;
        Switch::unordered_map<uint32_t, active_produce_req> pendingProduceReqs;
        struct reschedule_queue_entry_cmp
        {
                bool operator()(const broker *const b1, const broker *const b2)
                {
                        return b1->block_ctx.until > b2->block_ctx.until;
                }
        };
        std::priority_queue<broker *, std::vector<broker *>, reschedule_queue_entry_cmp> rescheduleQueue;

      private:
        inline void update_time_cache()
        {
                nowMS = Timings::Milliseconds::Tick();
        }

        broker *broker_state(const Switch::endpoint e);

        static uint8_t choose_compression_codec(const msg *const, const size_t);

        // this is somewhat complicated, because we want to use varint for the bundle length and we want to
        // encode this efficiently (no copying or moving data across buffers)
        // so we construct payload->iov[] properly
        bool produce_to_leader(const uint32_t clientReqId, const Switch::endpoint leader, const produce_ctx *const produce, const size_t cnt);

        // minSize: The minimum amount of data the server should return for a fetch request.
        // If insufficient data is available, the request will wait for >= that much data to accumlate before answering the request.
        // 0 (default) means that the fetch requests are answered as soon as a single byte is available or the fetch request times out waiting for data to arrive.
        //
        // maxWait: The maximum amount of time the server will block before answering a fetch request, if ther isn't sufficeint data to immediately satisfy the requirement set by minSize
        bool consume_from_leader(const uint32_t clientReqId, const Switch::endpoint leader, const consume_ctx *const from, const size_t total, const uint64_t maxWait, const uint32_t minSize);

        void track_na_broker(broker *);

        bool is_unreachable(const Switch::endpoint) const;

        bool consider_retransmission(broker *);

        void track_inflight_req(const uint32_t, const uint64_t, const TankAPIMsgType);

	void abort_inflight_req(connection *, const uint32_t, const TankAPIMsgType);

        void consider_inflight_reqs(const uint64_t);

        void forget_inflight_req(const uint32_t, const TankAPIMsgType);

        bool shutdown(connection *const c, const uint32_t ref, const bool fault = false);

        void reschedule_any();

        bool process_produce(connection *const c, const uint8_t *const content, const size_t len);

        bool process_consume(connection *const c, const uint8_t *const content, const size_t len);

        bool process(connection *const c, const uint8_t msg, const uint8_t *const content, const size_t len);

        auto get_buffer()
        {
                if (buffersPool.size())
                {
                        auto b = buffersPool.Pop();

                        buffersPoolPressure -= b->Reserved();
                        return b;
                }
                else
                        return new IOBuffer();
        }

        void ack_payload(broker *, outgoing_payload *);

        void prepare_retransmission(broker *);

        bool try_recv(connection *const c);

        bool try_send(connection *const c);

        void retain_for_resp(broker *, outgoing_payload *);

        void put_buffer(IOBuffer *const b)
        {
                require(b);

                if (buffersPoolPressure + b->Reserved() > 4 * 1024 * 1024 || buffersPool.size() > 128)
                        delete b;
                else
                {
                        buffersPoolPressure -= b->Reserved();
                        b->clear();
                        buffersPool.push_back(b);
                }
        }

        uint32_t __nextPayloadId{0};

        auto get_payload()
        {
                auto res = payloadsPool.size() ? payloadsPool.Pop() : new outgoing_payload();

                res->iovCnt = res->iovIdx = 0;
                res->next = nullptr;
                res->flags = 0;
                res->b = get_buffer();
                res->b2 = get_buffer();

                {
                        // See broker::outgoing_content::validate()
                        res->__id = ++__nextPayloadId;
                        require(res->__id);
                }

                switch_dlist_init(&res->pendingRespList);
                return res;
        }

        outgoing_payload *get_payload_for(connection *, const size_t);

        void put_payload(outgoing_payload *const p, const uint32_t ref)
        {
                require(p->__id);

                if (p->b)
                {
                        put_buffer(p->b);
                        p->b = nullptr;
                }
                if (p->b2)
                {
                        put_buffer(p->b2);
                        p->b2 = nullptr;
                }

                p->__id = 0; // so that validate() will catch it if e.g we re-use a payload without put_payload() first

                payloadsPool.push_back(p);
        }

        auto get_connection()
        {
                return connectionsPool.size() ? connectionsPool.Pop() : new connection();
        }

        void put_connection(connection *const c)
        {
                c->state.flags = 0;
                connectionsPool.push_back(c);
        }

        int init_connection_to(const Switch::endpoint e);

        void bind_fd(connection *const c, int fd);

        bool try_transmit(broker *);

        void flush_broker(broker *bs);

      public:
        TankClient(const strwlen32_t defaultLeader = {});

        ~TankClient();

        const auto &consumed() const noexcept
        {
                return consumedPartitionContent;
        }

        const auto &faults() const noexcept
        {
                return capturedFaults;
        }

        const auto &produce_acks() const noexcept
        {
                return produceAcks;
        }

        void poll(uint32_t timeoutMS);

        uint32_t produce(const std::vector<std::pair<topic_partition, std::vector<msg>>> &req);

        uint32_t produce_to(const topic_partition &to, const std::vector<msg> &msgs);

        Switch::endpoint leader_for(const strwlen8_t topic, const uint16_t partition);

        uint32_t consume(const std::vector<std::pair<topic_partition, std::pair<uint64_t, uint32_t>>> &req, const uint64_t maxWait, const uint32_t minSize);

        uint32_t consume_from(const topic_partition &from, const uint64_t seqNum, const uint32_t minFetchSize, const uint64_t maxWait, const uint32_t minSize);

        void set_client_id(const char *const p, const uint32_t len)
        {
                clientId.Set(p, len);
        }

        void set_retry_strategy(const RetryStrategy r)
        {
                retryStrategy = r;
        }

        void set_compression_strategy(const CompressionStrategy c)
        {
                compressionStrategy = c;
        }

	void set_sock_sndbuf_size(const int v)
	{
		sndBufSize = v;
	}

	void set_sock_rcvbuf_size(const int v)
	{
		rcvBufSize = v;
	}

        void set_default_leader(const Switch::endpoint e)
        {
                if (!e)
                        throw Switch::data_error("Unable to parse default leader endpoint");

                defaultLeader = e;
        }

        void set_default_leader(const strwlen32_t e)
        {
                set_default_leader(Switch::ParseSrvEndpoint(e, {_S("tank")}, 11011));
        }

        void set_topic_leader(const strwlen8_t topic, const strwlen32_t e);

        void interrupt_poll();

        bool should_poll() const
        {
                return connectionAttempts.size() || pendingConsumeReqs.size() || pendingProduceReqs.size() || rescheduleQueue.size();
        }
};

#ifdef LEAN_SWITCH
namespace std
{
        template <>
        struct hash<TankClient::topic_partition>
        {
                using argument_type = TankClient::topic_partition;
                using result_type = std::size_t;

                inline result_type operator()(const argument_type &v) const
                {
                        size_t hash{2166136261U};

                        for (uint32_t i{0}; i != v.first.len; ++i)
                                hash = (hash * 16777619) ^ v.first.p[i];

                        hash ^= std::hash<uint16_t>{}(v.second) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                        return hash;
                }
        };
}
#else
namespace Switch
{
        template <>
        struct hash<TankClient::topic_partition>
        {
                using argument_type = TankClient::topic_partition;
                using result_type = uint32_t;

                inline result_type operator()(const argument_type &v) const
                {
                        uint32_t seed = Switch::hash<strwlen8_t>{}(v.first);

                        Switch::hash_combine(seed, Switch::hash<uint16_t>{}(v.second));
                        return seed;
                }
        };
}
#endif

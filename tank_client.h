#pragma once
#include "common.h"
#include <atomic>
#include <compress.h>
#include <ext/flat_hash_map.h>
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
class TankClient final {
      private:
        struct broker;

      public:
        enum class ProduceFlags : uint8_t {

        };

        enum class RetryStrategy : uint8_t {
                RetryNever = 0,
                RetryAlways
        };

        enum class CompressionStrategy : uint8_t {
                CompressNever = 0,
                CompressAlways,
                CompressIntelligently
        };

        struct msg final {
                strwlen32_t content;
                uint64_t    ts;
                strwlen8_t  key;
        };

        using topic_partition = std::pair<strwlen8_t, uint16_t>;

        // there is one such payload per request
        // we are going to keep them around even after we have dispatched them
        // in case we want to retry them; that is the purprose of the pendingRespList and RetainForAck flag
        struct outgoing_payload final {
                outgoing_payload *next;
                switch_dlist      pendingRespList;
                IOBuffer *        b, *b2;
                struct iovec      iov[250];
                uint8_t           iovCnt;
                uint8_t           iovIdx;

                enum class Flags : uint8_t {
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
                uint8_t  flags;
                uint32_t __id;

                // If the payload is tracked by reqs_tracker.pendingConsume or reqs_tracker.pendingProduce or another reqs_tracker tracker, then
                // we shouldn't try to put_payload() if it's registered with outgoing_content (either in pendingRespList or in outgoing payloads list)
                constexpr bool tracked_by_reqs_tracker() const noexcept {
                        return flags & ((1u << uint8_t(Flags::ReqIsIdempotent)) | (1u << uint8_t(Flags::ReqMaybeRetried)));
                }
        };

        struct consume_ctx final {
                Switch::endpoint leader;
                strwlen8_t       topic;
                uint16_t         partitionId;
                uint64_t         absSeqNum;
                uint32_t         fetchSize;
        };

        struct produce_ctx final {
                Switch::endpoint leader;
                strwlen8_t       topic;
                uint16_t         partitionId;
                uint64_t         baseSeqNum;
                const msg *      msgs;
                size_t           msgsCnt;
        };

        struct connection final {
                int          fd{-1};
                switch_dlist list;
                IOBuffer *   inB{nullptr};
                broker *     bs{nullptr};

                enum class Type : uint8_t {
                        Tank
                } type{Type::Tank};

                struct State final {
                        enum class Flags : uint8_t {
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

                        uint8_t  flags;
                        uint64_t lastInputTS, lastOutputTS;
                } state;

                connection() {
                        switch_dlist_init(&list);
                }
        };

        void deregister_connection_attempt(connection *const c) {
                connectionAttempts.RemoveByValue(c);
        }

        struct active_consume_req final {
                uint32_t          clientReqId;
                outgoing_payload *reqPayload;
                uint64_t          ts;
                uint64_t *        seqNums;
                uint8_t           seqNumsCnt;
        };

        struct active_ctrl_req final {
                uint32_t          clientReqId;
                outgoing_payload *reqPayload;
                uint64_t          ts;
        };

        struct active_produce_req final {
                uint32_t          clientReqId;
                outgoing_payload *reqPayload;
                uint64_t          ts;
                uint8_t *         ctx;
                uint32_t          ctxLen;
        };

        struct discovered_topic_partitions final {
                uint32_t                                              clientReqId;
                strwlen8_t                                            topic;
                range_base<std::pair<uint64_t, uint64_t> *, uint16_t> watermarks;
        };

        struct created_topic final {
                uint32_t   clientReqId;
                strwlen8_t topic;
        };

        struct produce_ack final {
                uint32_t   clientReqId;
                strwlen8_t topic;
                uint16_t   partition;
        };

        struct consumed_msg final {
                uint64_t    seqNum;
                uint64_t    ts;
                strwlen8_t  key;
                strwlen32_t content;
        };

        struct partition_content final {
                uint32_t                             clientReqId;
                strwlen8_t                           topic;
                uint16_t                             partition;
                range_base<consumed_msg *, uint32_t> msgs;

                // https://github.com/phaistos-networks/TANK/issues/1
                // For now, this is always true, but when we implement support for pseudo-streaming (see GH issue)
                // this may be false, in which case, the response is not complete -- more messages are expected for
                // the request with id `clientReqId`
                // See TankClient::set_allow_streaming_consume_responses();
                bool respComplete;

                // We can't rely on msgs.back().seqNum + 1 to compute the next sequence number
                // to consume from, because if no message at all can be parsed because
                // of the request fetch size, msgs will be empty().
                // Furthermore, this separation allows us to support (especially in future revisions)
                // different semantics
                struct
                {
                        // to get more messages
                        // consume from (topic, partition) starting from seqNum,
                        // and set fetchSize to be at least minFetchSize
                        uint64_t seqNum;
                        uint32_t minFetchSize;
                } next;
        };

        struct fault final {
                uint32_t clientReqId;

                enum class Type : uint8_t {
                        UnknownTopic = 1,
                        UnknownPartition,
                        Access,
                        Network,
                        BoundaryCheck,
                        InvalidReq,
                        SystemFail,
                        AlreadyExists
                } type;

                enum class Req : uint8_t {
                        Consume = 1,
                        Produce,
                        Ctrl
                } req;

                strwlen8_t topic;
                uint16_t   partition;

                union {
                        // Set when fault is BoundaryCheck
                        struct
                        {
                                uint64_t firstAvailSeqNum;
                                uint64_t highWaterMark;
                        };
                } ctx;

                // handy utility function if type == Type::BoundaryCheck
                // adjusts sequence number to be within [firstAvailSeqNum, highWaterMark + 1]
                uint64_t adjust_seqnum_by_boundaries(uint64_t seqNum) const {
                        require(type == Type::BoundaryCheck);

                        if (seqNum < ctx.firstAvailSeqNum)
                                seqNum = ctx.firstAvailSeqNum;
                        else if (seqNum > ctx.highWaterMark)
                                seqNum = ctx.highWaterMark + 1;

                        return seqNum;
                }
        };

      private:
        struct
        {
                struct
                {
                        uint32_t next{1};
                } client;

                struct
                {
                        uint32_t next{1};
                } leader_reqs;
        } ids_tracker;

        struct broker final {
                struct connection *    con{nullptr};
                const Switch::endpoint endpoint;

                switch_dlist retainedPayloadsList;

                enum class Reachability : uint8_t {
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
                        uint16_t retries{0};
                } block_ctx;

                void set_reachability(const Reachability r, const uint32_t);

                // outgoing content will be associated with a leader state, not its connection
                // and the connection will drain this outgoing_content
                struct
                {
                        outgoing_payload *front_{nullptr}, *back_{nullptr};

                        void push_front(outgoing_payload *const p) noexcept {
                                p->iovIdx = 0;
                                p->next   = front_;
                                front_    = p;
                                if (!back_)
                                        back_ = front_;
                        }

                        // this will be a no-op a few revisions later
                        // just need to make sure ordering is preserved
                        void validate() {
                                if (auto it = front_) {
                                        require(it->__id);

                                        for (auto prev = it; (it = it->next);) {
                                                require(it->__id);
                                                require(it->__id > prev->__id);
                                        }
                                }
                        }

                        void push_back(outgoing_payload *const p) {
                                p->iovIdx = 0;
                                p->next   = nullptr;

                                if (back_)
                                        back_->next = p;
                                else
                                        front_ = p;

                                back_ = p;
                        }

                        constexpr auto front() const noexcept {
                                return front_;
                        }

                        constexpr void pop_front() noexcept {
                                front_ = front_->next;
                                if (!front_)
                                        back_ = nullptr;
                        }

                } outgoing_content;

                struct
                {
                        std::set<uint32_t> pendingConsume;
                        std::set<uint32_t> pendingProduce;
                        std::set<uint32_t> pendingCtrl;
                } reqs_tracker;

                broker(const Switch::endpoint e)
                    : endpoint{e} {
                        switch_dlist_init(&retainedPayloadsList);
                }
        };

        uint64_t                                            nextInflightReqsTimeoutCheckTs{0};
        RetryStrategy                                       retryStrategy{RetryStrategy::RetryAlways};
        CompressionStrategy                                 compressionStrategy{CompressionStrategy::CompressIntelligently};
        ska::flat_hash_map<Switch::endpoint, broker *>      bsMap;
        ska::flat_hash_map<strwlen8_t, Switch::endpoint>    leadersMap;
        Switch::endpoint                                    defaultLeader{};
        bool                                                allowStreamingConsumeResponses{false};
        int                                                 sndBufSize{128 * 1024}, rcvBufSize{1 * 1024 * 1024};
        strwlen8_t                                          clientId{"c++"};
        switch_dlist                                        connections;
        Switch::vector<std::pair<connection *, IOBuffer *>> connsBufs;
        // TODO: https://github.com/phaistos-networks/TANK/issues/6
        Switch::vector<IOBuffer *>                          usedBufs;
        simple_allocator                                    resultsAllocator{2 * 1024 * 1024};
        Switch::vector<void *>                              resultsAllocations;
        Switch::vector<partition_content>                   consumedPartitionContent;
        Switch::vector<fault>                               capturedFaults;
        Switch::vector<produce_ack>                         produceAcks;
        Switch::vector<discovered_topic_partitions>         discoverPartitionsResults;
        Switch::vector<created_topic>                       createdTopicsResults;
        Switch::vector<consumed_msg>                        consumptionList;
        Switch::vector<consume_ctx>                         consumeOut;
        Switch::vector<produce_ctx>                         produceOut;
        uint64_t                                            nowMS;
        uint32_t                                            nextConsumeReqId{1}, nextProduceReqId{1};
        Switch::vector<connection *>                        connectionAttempts, connsList;
        EPoller                                             poller;
        int                                                 pipeFd[2];
        std::atomic<bool>                                   polling{false};
        Switch::vector<connection *>                        connectionsPool;
        Switch::vector<outgoing_payload *>                  payloadsPool;
        Switch::vector<IOBuffer *>                          buffersPool;
        size_t                                              buffersPoolPressure{0};
        Switch::vector<range32_t>                           ranges;
        IOBuffer                                            produceCtx;
        Switch::unordered_map<uint32_t, active_consume_req> pendingConsumeReqs;
        Switch::unordered_map<uint32_t, active_produce_req> pendingProduceReqs;
        Switch::unordered_map<uint32_t, active_ctrl_req>    pendingCtrlReqs;
        struct reschedule_queue_entry_cmp {
                bool operator()(const broker *const b1, const broker *const b2) noexcept {
                        return b1->block_ctx.until > b2->block_ctx.until;
                }
        };
        std::priority_queue<broker *, std::vector<broker *>, reschedule_queue_entry_cmp> rescheduleQueue;

      private:
        inline void update_time_cache() {
                nowMS = Timings::Milliseconds::Tick();
        }

        broker *broker_state(const Switch::endpoint e);

        static uint8_t choose_compression_codec(const msg *const, const size_t);

        // this is somewhat complicated, because we want to use varint for the bundle length and we want to
        // encode this efficiently (no copying or moving data across buffers)
        // so we construct payload->iov[] properly
        bool produce_to_leader(const uint32_t clientReqId, const Switch::endpoint leader, const produce_ctx *const produce, const size_t cnt);

        bool produce_to_leader_with_base(const uint32_t clientReqId, const Switch::endpoint leader, const produce_ctx *const produce, const size_t cnt);

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

        bool process_discover_partitions(connection *const c, const uint8_t *const content, const size_t len);

        bool process_create_topic(connection *const c, const uint8_t *const content, const size_t len);

        bool process(connection *const c, const uint8_t msg, const uint8_t *const content, const size_t len);

        auto get_buffer() {
                if (buffersPool.size()) {
                        auto b = buffersPool.Pop();

                        buffersPoolPressure -= b->Reserved();
                        return b;
                } else
                        return new IOBuffer();
        }

        void ack_payload(broker *, outgoing_payload *);

        void prepare_retransmission(broker *);

        bool try_recv(connection *const c);

        bool try_send(connection *const c);

        void retain_for_resp(broker *, outgoing_payload *);

        void put_buffer(IOBuffer *const b);

        void put_buffers(IOBuffer **const list, const size_t n);

        uint32_t __nextPayloadId{0};

        auto get_payload() {
                auto res = payloadsPool.size() ? payloadsPool.Pop() : new outgoing_payload();

                res->iovCnt = res->iovIdx = 0;
                res->next                 = nullptr;
                res->flags                = 0;
                res->b                    = get_buffer();
                res->b2                   = get_buffer();

                {
                        // See broker::outgoing_content::validate()
                        res->__id = ++__nextPayloadId;
                        require(res->__id);
                }

                switch_dlist_init(&res->pendingRespList);
                return res;
        }

        outgoing_payload *get_payload_for(connection *, const size_t);

        void put_payload(outgoing_payload *const p, const uint32_t ref) {
                require(p->__id);

                if (p->b) {
                        put_buffer(p->b);
                        p->b = nullptr;
                }
                if (p->b2) {
                        put_buffer(p->b2);
                        p->b2 = nullptr;
                }

                p->__id = 0; // so that validate() will catch it if e.g we re-use a payload without put_payload() first

                payloadsPool.push_back(p);
        }

        auto get_connection() {
                return connectionsPool.size() ? connectionsPool.Pop() : new connection();
        }

        void put_connection(connection *const c) {
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

        const auto &consumed() const noexcept {
                return consumedPartitionContent;
        }

        const auto &faults() const noexcept {
                return capturedFaults;
        }

        const auto &produce_acks() const noexcept {
                return produceAcks;
        }

        const auto &discovered_partitions() const noexcept {
                return discoverPartitionsResults;
        }

        const auto &created_topics() const noexcept {
                return createdTopicsResults;
        }

        void poll(uint32_t timeoutMS);

        // Maybe you want to use it after poll() has returned
        // TODO: check if (!vector.empty()) instead; should be faster for std::vector<>
        bool any_responses() const noexcept {
                return consumed().size() || faults().size() || produce_acks().size() || discovered_partitions().size() || created_topics().size();
        }

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t produce(const std::vector<std::pair<topic_partition, std::vector<msg>>> &req);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t produce(const std::pair<topic_partition, std::vector<msg>> *, const size_t);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t produce_to(const topic_partition &to, const std::vector<msg> &msgs);

        // This is needed for Tank system tools. Applications should never need to use this method
        // e.g tank-ctl mirroring functionality
        //
        // Right now, it's only required for implementing the mirroring functionality
        [[ gnu::warn_unused_result, nodiscard ]] uint32_t produce_with_base(const std::vector<std::pair<topic_partition, std::pair<uint64_t, std::vector<msg>>>> &req);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t produce_with_base(const std::pair<topic_partition, std::pair<uint64_t, std::vector<msg>>> *, const size_t);

        Switch::endpoint leader_for(const strwlen8_t topic, const uint16_t partition);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t consume(const std::vector<std::pair<topic_partition, std::pair<uint64_t, uint32_t>>> &req, const uint64_t maxWait, const uint32_t minSize);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t consume_from(const topic_partition &from, const uint64_t seqNum, const uint32_t minFetchSize, const uint64_t maxWait, const uint32_t minSize);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t discover_partitions(const strwlen8_t topic);

        [[ gnu::warn_unused_result, nodiscard ]] uint32_t create_topic(const strwlen8_t topic, const uint16_t numPartitions, const strwlen32_t configuration);

        void reset();

        void set_client_id(const char *const p, const uint32_t len) {
                clientId.Set(p, len);
        }

        void set_retry_strategy(const RetryStrategy r) noexcept {
                retryStrategy = r;
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

        void set_topic_leader(const strwlen8_t topic, const strwlen32_t e);

        void interrupt_poll();

        bool should_poll() const noexcept;

        // A handy utility method
        // Will check that reqID is valid, and then will wait until it gets an ack. for this request
        // or any fault - and if it fails, it will throw an exception..
        // This is mostly useful for produce responses
        void wait_scheduled(const uint32_t reqID);
};

namespace std {
        template <>
        struct hash<TankClient::topic_partition> {
                using argument_type = TankClient::topic_partition;
                using result_type   = std::size_t;

                inline result_type operator()(const argument_type &v) const {
                        size_t hash{2166136261U};

                        for (uint32_t i{0}; i != v.first.len; ++i)
                                hash = (hash * 16777619) ^ v.first.p[i];

                        hash ^= std::hash<uint16_t>{}(v.second) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                        return hash;
                }
        };
} // namespace std

#ifndef LEAN_SWITCH
namespace Switch {
        template <>
        struct hash<TankClient::topic_partition> {
                using argument_type = TankClient::topic_partition;
                using result_type   = uint32_t;

                inline result_type operator()(const argument_type &v) const {
                        uint32_t seed = Switch::hash<strwlen8_t>{}(v.first);

                        Switch::hash_combine(seed, Switch::hash<uint16_t>{}(v.second));
                        return seed;
                }
        };
} // namespace Switch
#endif

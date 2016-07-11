#pragma once
#include "common.h"
#include <compress.h>
#include <network.h>
#include <set>
#include <switch.h>
#include <switch_dictionary.h>
#include <switch_ll.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include <vector>
#include <atomic>

// Tank, because its a large container of liquid or gas
// and data flow (as in, liquid), and also, this is a Trinity character name
class TankClient
{
      public:
        enum class ProduceFlags : uint8_t
        {

        };

        struct msg
        {
                strwlen32_t content;
                uint64_t ts;
                strwlen8_t key;
        };

        using topic_partition = std::pair<strwlen8_t, uint16_t>;

        struct outgoing_payload
        {
                outgoing_payload *next;
                IOBuffer *b{nullptr}, *b2{nullptr};

                struct iovec iov[256];
                uint8_t iovCnt{0};
                uint8_t iovIdx{0};
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
                outgoing_payload *outgoingFront{nullptr}, *outgoingBack{nullptr};
                switch_dlist list;
                IOBuffer *inB{nullptr};
                uint32_t pendingResponses;
                std::set<uint32_t> pendingConsume, pendingProduce;
                Switch::endpoint peer;

                struct State
                {
                        enum class Flags : uint8_t
                        {
                                ConnectionAttempt,
                                NeedOutAvail,
                                RetryingConnection
                        };

                        uint8_t flags;
                        uint64_t lastInputTS, lastOutputTS;
                } state;

                void push_back(outgoing_payload *const p)
                {
                        p->iovIdx = 0;
                        p->next = nullptr;

                        if (outgoingBack)
                                outgoingBack->next = p;
                        else
                                outgoingFront = p;

                        outgoingBack = p;
                }

                auto front()
                {
                        return outgoingFront;
                }

                void pop_front()
                {
                        outgoingFront = outgoingFront->next;
                        if (!outgoingFront)
                                outgoingBack = nullptr;
                }

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
                uint64_t ts;
                uint64_t *seqNums;
                uint8_t seqNumsCnt;
        };

        struct active_produce_req
        {
                uint32_t clientReqId;
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

        struct unreachable_broker_ctx
        {
                uint32_t prevSleep;
                uint64_t until;
        };

        // topic to standalone broker
        Switch::unordered_map<strwlen8_t, Switch::endpoint> leadersMap;
        Switch::endpoint defaultLeader{};
        Switch::unordered_map<Switch::endpoint, unreachable_broker_ctx> naBrokers;
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
        Switch::vector<range32_t> ranges;
        IOBuffer produceCtx;
        Switch::unordered_map<uint32_t, active_consume_req> pendingConsumeReqs;
        Switch::unordered_map<uint32_t, active_produce_req> pendingProduceReqs;
        Switch::unordered_map<Switch::endpoint, connection *> connectionsMap;

      private:
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

        void track_na_broker(const Switch::endpoint);

        bool shutdown(connection *const c, const uint32_t ref, const bool fault = false);

        bool process_produce(connection *const c, const uint8_t *const content, const size_t len);

        bool process_consume(connection *const c, const uint8_t *const content, const size_t len);

        bool process(connection *const c, const uint8_t msg, const uint8_t *const content, const size_t len);

        auto get_buffer()
        {
                return buffersPool.size() ? buffersPool.Pop() : new IOBuffer();
        }

        bool try_recv(connection *const c);

        bool try_send(connection *const c);

        void put_buffer(IOBuffer *const b)
        {
                require(b);
                if (b->Reserved() > 1024 * 1024 || buffersPool.size() > 128)
                        delete b;
                else
                {
                        b->clear();
                        buffersPool.push_back(b);
                }
        }

        auto get_payload()
        {
                auto res = payloadsPool.size() ? payloadsPool.Pop() : new outgoing_payload();

                res->b = get_buffer();
                res->b2 = get_buffer();

                return res;
        }

        outgoing_payload *get_payload_for(connection *, const size_t);

        void put_payload(outgoing_payload *const p)
        {
                p->iovCnt = p->iovIdx = 0;
                p->next = nullptr;

                if (p->b)
                        put_buffer(p->b);
                if (p->b2)
                        put_buffer(p->b2);

                p->b = nullptr;
                p->b2 = nullptr;

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

        connection *init_connection(const Switch::endpoint e);

        connection *leader_connection(Switch::endpoint leader);

        template <typename L>
        bool submit(const Switch::endpoint leader, outgoing_payload *const p, L &&l)
        {
                if (auto *const c = leader_connection(leader))
                {
                        l(c);

                        ++(c->pendingResponses);
                        c->push_back(p);

                        if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))))
                                return try_send(c);
                        else
                                return true;
                }
                else
                {
                        put_payload(p);
                        return false;
                }
        }

      public:
        TankClient();

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

        Switch::endpoint leader_for(const strwlen8_t topic, const uint16_t partition);

        uint32_t consume(const std::vector<std::pair<topic_partition, std::pair<uint64_t, uint32_t>>> &req, const uint64_t maxWait, const uint32_t minSize);

        void set_client_id(const char *const p, const uint32_t len)
        {
                clientId.Set(p, len);
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

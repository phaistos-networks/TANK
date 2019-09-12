// Tank, because its a large container of liquid or gas
// and data flow (as in, liquid), and also, this is a Trinity character name
#pragma once
#include "common.h"
#include <atomic>
#include <compress.h>
#include <network.h>
#include <queue>
#include <vector>
#include <unordered_map>
#include <switch.h>
#include <switch_ll.h>
#include <switch_mallocators.h>
#include <switch_vector.h>
#include "client_common.h"
#include <ext/ebtree/eb64tree.h>
#include <unordered_set>

// XXX: experimental, see TODO
// this does seem to work, although the performance improvement is rather small
// it's definitely not ready yet, but once it is, we should be able to better understand the performance delta
//
// the idea is that by interleaving parsing of partitions/messages with network packets collection (i.e
// packets arriving from peer and stored in the socket's queue) we can produce responses faster because
// while the CPU is busy parsing the messages, packets are accumulated, and when its done parsing, we  'll have
// more-fresh packets to process
//#define TANK_CLIENT_FAST_CONSUME 1


// The problem we want to solve is how to *reliably* abort broker requests
//
// We need to abort a broker request if:
// 	- the broker request times-out
// 	- the api request is aborted(times-out? etc), and we subsequently wish to abort all associated broker requests
//
// Once a broker request is associated with a broker:
//	- its payload may not be materialized yet
// 	- its payload may be scheduled for transfer, in full
//	- its payload may be scheduled for transfer, in part
//
// Problems include:
// 	- If we generate a timeout for a (request, topic, partition) but the payload of that request is transmitted anyway, then
// what we report to the client(failure) is misleading.
//
// Important to consider:
// 	- Once we have scheduled for transmission via writev() 1+ bytes of a materialized request payload, we can't abort it
//	without closing the connection and migrating (broker, etc) to another connection, because of TCP semantics
// 	- We need to be able to quickly figure out if there is a payload for a specific broker request so that we can
//	remove it from the broker's outgoing_content list (iff we haven't transferred any bytes yet, see above).
//	We can just scan the outgoing_content list for that (we associate the broker_requests with the payload so
//	that's easy), but because the search is linear, this could potentially take too long.
//	- if the broker's connection is closed, we can trivially abort everything

//#define HAVE_NETIO_THROTTLE 1

// specialize here otherwise compiler will fail (leaders)
namespace std {
        template <>
        struct hash<std::pair<str_view8, uint16_t>> {
                using argument_type = std::pair<str_view8, uint16_t>;
                using result_type   = std::size_t;

                inline result_type operator()(const argument_type &v) const {
                        size_t hash{2166136261U};

                        for (const auto c : v.first) {
                                hash = (hash * 16777619) ^ c;
                        }

                        hash ^= std::hash<uint16_t>{}(v.second) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                        return hash;
                }
        };
} // namespace std

// TankClient is not marked as final, because we need to use TEST_CASE_METHOD() (see catch2)
class TankClient {
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

#if 0
        void deregister_connection_attempt(connection *const c) {
                connectionAttempts.RemoveByValue(c);
        }

#endif
        struct discovered_topic_partitions final {
                uint32_t clientReqId;

                strwlen8_t                                            topic;
                range_base<std::pair<uint64_t, uint64_t> *, uint16_t> watermarks;
        };

        struct reload_conf_result final {
                uint32_t  clientReqId;
                str_view8 topic;
                uint16_t  partition;
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
                union {
                        uint64_t seqNum;
                        uint64_t seq_num;
                };
                uint64_t    ts;
                strwlen8_t  key;
                strwlen32_t content;
        };

        struct srv_status final {
                struct {
                        uint32_t topics;
                        uint32_t partitions;
                        uint32_t nodes;
                } counts;

                struct {
                        char    data[64];
                        uint8_t len;
                } cluster_name;
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

                // NEW:
                // set to true if there was no content returned
                // regardless of the minFetchSize
                // this is the reliable way to check if the partition has been drained
                bool drained;

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
                        union {
                                uint64_t seqNum;
                                uint64_t seq_num;
                        };

                        union {
                                uint32_t minFetchSize;
                                uint32_t min_fetch_size;
                        };
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
                        AlreadyExists,
                        NotAllowed,
                        Timeout,
                        UnsupportedReq,
                        InsufficientReplicas,
                } type;

                enum class Req : uint8_t {
                        Consume = 1,
                        Produce,
                        Ctrl,
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

                        // for
                        uint64_t seq_num;
                } ctx;

                // handy utility function if type == Type::BoundaryCheck
                // adjusts sequence number to be within [firstAvailSeqNum, highWaterMark + 1]
                uint64_t adjust_seqnum_by_boundaries(uint64_t seqNum) const {
                        TANK_EXPECT(type == Type::BoundaryCheck);

                        if (seqNum < ctx.firstAvailSeqNum) {
                                seqNum = ctx.firstAvailSeqNum;
                        } else if (seqNum > ctx.highWaterMark) {
                                seqNum = ctx.highWaterMark + 1;
                        }

                        return seqNum;
                }
        };

      protected:
        struct timer final {
                enum class Type : uint8_t {
                        TrackedResponseExpiration = 0,
                } type;

                eb64_node node;

                void reset() {
                        node.node.leaf_p = nullptr;
                }

                bool is_linked() const noexcept {
                        return node.node.leaf_p;
                }
        };

        struct connection;
        struct broker;
        struct connection_handle final {
                connection *c_{nullptr};
                uint64_t    c_gen;

                void reset() {
                        c_gen = std::numeric_limits<uint64_t>::max();
                        c_    = nullptr;
                }

                inline void set(connection *const c) noexcept;

                inline connection *get() noexcept;

                inline const connection *get() const noexcept;
        };

        struct request_partition_ctx final {
                str_view8 topic;
                // for some types of requests, partition is not used
                uint16_t     partition;
                switch_dlist partitions_list_ll;

		// for no_leader, retry chaining where it makes sense
		request_partition_ctx *_next;

                union Op final {
                        struct {
                                uint64_t seq_num;
                                uint32_t min_fetch_size;

                                struct {
                                        struct {
                                                uint64_t seq_num;
                                                size_t   min_size;
                                        } next;

                                        struct {
                                                IOBuffer **data;
                                                uint32_t   size;
                                        } used_buffers;

                                        struct {
                                                uint32_t cnt;

                                                union {
                                                        consumed_msg  small[8];
                                                        consumed_msg *large; // if (cnt >= sizeof_array(small)) we are going to be allocating
                                                } list;
                                        } msgs;

                                        bool drained;
                                } response;
                        } consume;

                        struct {
                                struct {
                                        // we will sort them when we construct the response
                                        std::vector<std::pair<uint16_t, std::pair<uint64_t, uint64_t>>> *all;
                                } response;
                        } discover_partitions;

                        struct {
                                struct {
                                        uint8_t *data;
                                        uint32_t size;
                                } payload;

                                // for ProduceWithBase requests
                                uint64_t first_msg_seqnum;
                        } produce;

			struct {
				struct {
					uint32_t nodes;
					uint32_t topics;
					uint32_t partitions;
				} counts;

				struct {
					char data[64];
					uint8_t len; // 0 if not cluster aware
				} cluster_name;
			} srv_status;
                } as_op;

                void reset() {
                        partitions_list_ll.reset();
			_next = nullptr;
                        as_op.discover_partitions.response.all = nullptr;
                }
        };

        struct api_request;
        struct retry_bundle final {
                switch_dlist           retry_bundles_ll;
                uint64_t               expiration;
                api_request *          api_req;
                eb64_node              node;
                uint16_t               size;
                request_partition_ctx *data[0];
        };

        // a request to a specific broker, for 1+ partitions
        // this is managed in a fan-out/coordinator fashion by an api_request
        //
        // each partition involved with this broker request has a request specific configuration
        struct broker_api_request final {
                broker *     br;
                api_request *api_req;

                // we need a distinct id for every such request, and that's encoded
                // in the broker_outgoing_payload we generate and stream to the broker
                // because we need to to encode it in the payload so that
                // when the broker responds, its response will include
                // this request ID and we can look up the payload by that id (see payloads_map)
                uint32_t     id;
                switch_dlist partitions_list;         // tracks request_partition_ctx's
                switch_dlist broker_requests_list_ll; // tracked by the api_request via this ll

                // when we deliver (in full) a payload to a broker via writev()
                // we need to track them, so that if the broker fails, we 'll know which requests those were
                switch_dlist pending_responses_list_ll;

                bool have_payload;

                // Some requests, e.g discover_partitions
                // are not associated with any partitions, so we could just include request-state here
                // in a union - but for simplicity reasons, we 'll just use a request_partition_ctx as well, where the partititon doesn't really matter

                void reset() {
                        partitions_list.reset();
                        broker_requests_list_ll.reset();
                        pending_responses_list_ll.reset();
                        api_req      = nullptr;
                        br           = nullptr;
                        id           = 0;
                        have_payload = false;
                }
        };

        struct msgs_bucket final {
                consumed_msg data[512];
                msgs_bucket *next;
        };

        // coordinates(fan-out) 1+ broker_api_request's
        struct managed_buf;
        struct api_request final {
                enum class Type : uint8_t {
                        Consume = 0,
                        Produce,
                        ProduceWithSeqnum,
                        DiscoverPartitions,
                        CreateTopic,
                        ReloadConfig,
			SrvStatus,
                } type;
                uint32_t request_id; // client request ID

#ifdef TANK_RUNTIME_CHECKS 
		uint64_t init_ms;
#endif
                eb64_node api_reqs_expirations_tree_node;

                switch_dlist broker_requests_list;
                // those are migrated from broker_api_request to api_request
                // when we get a response for them
                switch_dlist               ready_partitions_list;
                switch_dlist               retry_bundles_list;
                std::vector<managed_buf *> managed_bufs;
                bool                       _failed;

                bool ready() const noexcept {
                        return retry_bundles_list.empty() && broker_requests_list.empty();
                }

                void set_failed() noexcept {
                        _failed = true;
                }

                bool failed() const noexcept {
                        return _failed;
                }

                auto expiration() const noexcept {
                        return api_reqs_expirations_tree_node.key;
                }

                // for some types of requests, we may
                // want to materialize something in-memory
                // so that when we gc_api_request() it will be destroyed
                union {
                        struct {
                                std::pair<uint64_t, uint64_t> *v;
                        } discover_partitions;
                } materialized_resp;

                union As final {
                        struct Consume final {
                                uint64_t max_wait;
                                size_t   min_size;
                        } consume;

                        struct CreateTopic final {
                                uint16_t   partitions;
                                str_view32 config;
                        } create_topic;

                        As()
                            : create_topic{} {
                        }

                } as;

                void reset() {
                        broker_requests_list.reset();
                        ready_partitions_list.reset();
                        managed_bufs.clear();
                        retry_bundles_list.reset();
                        request_id                                 = 0;
                        api_reqs_expirations_tree_node.key         = 0;
                        api_reqs_expirations_tree_node.node.leaf_p = nullptr;
                        _failed                                    = false;

                        memset(&materialized_resp, 0, sizeof(materialized_resp));
                }
        };

        // a *materialized* broker_api_request
        // it's really just holds a buffer and the iovecs
        struct broker_outgoing_payload final {
                broker_outgoing_payload *next;
                IOBuffer *               b;
                broker_api_request *     broker_req;

                struct IOVECS final {
                        struct iovec data[256];
                        uint8_t      size;
                } iovecs;
        };

        struct buf_llhdr final {
                IOBuffer * b;
                buf_llhdr *next;
        };

        struct managed_buf final {
                enum class Flags : uint8_t {
                        Locked = 1u << 0,
                };

                managed_buf *next{nullptr};
                uint32_t     rc{1}, locks_{0};
                uint32_t     capacity_{0};
                uint32_t     length{0};
                uint32_t     offset_{0};
                char *       data_{nullptr};
                // an object may depend on 0+ buffers
                // a buffer may be a dependncy of 0+ objects
                switch_dlist users_list{&users_list, &users_list};

                ~managed_buf() {
                        if (data_) {
                                std::free(data_);
                        }
                }

                void serialize(int8_t *ptr, const size_t l) {
			TANK_EXPECT(locks_ == 0);

                        reserve(l);
                        memcpy(data_ + length, ptr, l);
                        length += l;
                }

                auto size() const noexcept {
                        return length;
                }

                const char *data() const noexcept {
                        return data_;
                }

                char *data() noexcept {
                        return data_;
                }

                void lock() noexcept {
                        ++locks_;
                }

                bool is_locked() const noexcept {
                        return locks_;
                }

                bool unlock() {
                        TANK_EXPECT(locks_);
                        return --locks_ == 0;
                }

                void reserve(const size_t n) {
                        if (n > capacity_ && likely(n)) {
                                capacity_ = n;
                                data_     = static_cast<char *>(realloc(data_, capacity_ * sizeof(char)));
                        }
                }

                auto capacity() const noexcept {
                        return capacity_;
                }

                void clear() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
			TANK_EXPECT(locks_ == 0);

                        length  = 0;
                        locks_  = 0;
                        offset_ = 0;
                        next    = nullptr;
                }

                void set_offset(const char *p) {
                        TANK_EXPECT(p >= data() && p <= data() + size());
                        offset_ = p - data();
                }

                void set_offset(const uint32_t o) {
                        TANK_EXPECT(o <= size());
                        offset_ = o;
                }

                void erase_chunk(const uint32_t o, const uint32_t s) {
                        auto p = data_ + o;

			TANK_EXPECT(locks_ == 0);
                        TANK_EXPECT(o + s <= size());
                        memmove(p, p + s, size() - (o + s));
                        length -= s;
                }

                auto offset() const noexcept {
                        return offset_;
                }

                void retain() noexcept {
                        ++rc;
                }

                bool release() noexcept {
                        return --rc == 0;
                }

                auto use_count() const noexcept {
                        return rc;
                }
        };

        struct connection final {
                int fd{-1};

                enum class Type : uint8_t {
                        Tank
                } type;

#ifdef HAVE_NETIO_THROTTLE
                struct {
                        struct {
                                switch_dlist ll;
                                uint64_t     until;

                                void reset() {
                                        ll.reset();
                                        until = std::numeric_limits<uint64_t>::max();
                                }
                        } read, write;
                } throttler;
#endif

                uint64_t     gen;
                switch_dlist all_conns_list_ll;

                union {
                        struct {
                                switch_dlist list;
                                uint64_t     expiration;
                        };
                };

                struct {
                        managed_buf *b;
                } in;

                struct State final {
                        enum class Flags : uint8_t {
                                ConnectionAttempt = 0,
                                NeedOutAvail,
                                RetryingConnection,
                        };

                        uint8_t flags;
                } state;

                struct As final {
                        struct Tank final {
                                enum class Flags : uint8_t {
                                        ConsideredReqHeader     = 0,
                                        InterleavedRespAssembly = 1,
                                };

#ifdef TANK_CLIENT_FAST_CONSUME
				// this is used exclusively for consume responses
				struct Response final {
					enum class State : uint8_t {
						ParseHeader = 0,
						ParseTopic,
						ParseFirstTopicPartition,
						ParsePartition,
						ParsePartitionBundle,
						ParsePartitionBundleMsgSet,
						Drain,
						Ready,
					} state;

					request_partition_ctx *no_leader_l, *retry_l;
					bool any_faults;
					bool retain_buf;
					uint32_t resp_end_offset;
                                        broker_api_request *breq;
                                        uint32_t            req_id;
                                        uint32_t            hdr_size;
                                        uint8_t             topics_cnt;
                                        uint8_t             topic_partitions_cnt;
                                        switch_dlist *      br_req_partctx_it;
                                        buf_llhdr *         used_bufs;
                                        struct {
                                                char data_[256];
						uint8_t len;

						auto size() const noexcept {
							return len;
						}

						const char *data() const noexcept {
							return data_;
						}
                                        } topic_name;

                                        struct {
                                                uint64_t     highwater_mark;
                                                uint32_t     bundles_chunk_len;
                                                uint64_t     log_base_seqnum;
                                                uint32_t     need_upto, need_from;

                                                struct {
                                                        msgs_bucket *first_bucket, *last_bucket;
                                                        uint32_t     last_bucket_size;
                                                        uint32_t     consumed;

                                                        void reset() {
                                                                first_bucket = last_bucket = nullptr;
                                                                last_bucket_size           = sizeof_array(msgs_bucket::data);
                                                                consumed                   = 0;
                                                        }
                                                } capture_ctx;

                                                struct {
                                                        uint32_t offset;
                                                        uint32_t end;
                                                } bundles_chunk;

                                                struct {
                                                        uint8_t  codec;
							bool sparse;
							bool any_captured;
							uint64_t first_msg_seqnum, last_msg_seqnum;
							uint32_t size;
							uint32_t conumed;

                                                        union MsgSetContent final {
                                                                struct {
                                                                        const uint8_t *p;
                                                                        const uint8_t *e;
                                                                } tmpbuf_range;
                                                                struct {
                                                                        uint32_t o;
                                                                        uint32_t e;
                                                                } inb_range;
                                                        } msgset_content;

                                                        struct {
                                                                uint64_t ts;
                                                                uint32_t msg_idx;
                                                                uint64_t min_accepted_seqnum;
								uint32_t size;
                                                        } cur_msg_set;
                                                } cur_bundle;
                                        } cur_partition;

                                        void reset() {
                                                state       = State::ParseHeader;
                                                retain_buf  = false;
                                                used_bufs   = nullptr;
                                                any_faults  = false;
                                                no_leader_l = nullptr;
                                                retry_l     = nullptr;
                                        }
                                } cur_resp;
#endif


                                uint8_t flags;
                                broker *br;


                                void reset() {
                                        flags = 0;
                                        br    = nullptr;
#ifdef TANK_CLIENT_FAST_CONSUME
					cur_resp.reset();
#endif
                                }
                        } tank;
                } as;

                void reset() noexcept {
                        in.b = nullptr;
                        fd   = -1;
                        list.reset();
                        all_conns_list_ll.reset();


#ifdef HAVE_NETIO_THROTTLE
                        throttler.read.reset();
                        throttler.write.reset();
#endif
                }
        };

        uint32_t next_broker_request_id{1};
        uint32_t next_api_request_id{1};

        struct broker final {
                static constexpr size_t max_consequtive_connection_failures{5};
                uint64_t                blocked_until{0};
                eb64_node               unreachable_brokers_tree_node{.node.leaf_p = nullptr};
                connection_handle       ch;
                const Switch::endpoint  ep;
                uint8_t                 consequtive_connection_failures{0};
                switch_dlist            all_brokers_ll{&all_brokers_ll, &all_brokers_ll};
		uint8_t flags{0};

                enum class Flags : uint8_t {
                        Important = 0,
                };

                enum class Reachability : uint8_t {
                        LikelyUnreachable,
                        LikelyReachable,
                        MaybeReachable,
                        Blocked,
                } reachability{Reachability::MaybeReachable};

                switch_dlist pending_responses_list{&pending_responses_list, &pending_responses_list};

                void set_reachability(const Reachability, const size_t);

                // Tank node connections will drain a broker's outgoing_content
                // i.e data will be tracked by the broker, not the connection
                struct {
                        using payload = broker_outgoing_payload;

                        payload *front_{nullptr}, *back_{nullptr};
                        uint8_t  front_payload_iovecs_index{0};
                        bool     first_payload_partially_transferred{false};

                        size_t size() const noexcept {
                                size_t n{0};

                                for (auto it = front_; it; it = it->next) {
                                        ++n;
                                }

                                return n;
                        }

                        void push_front(payload *p) noexcept {
                                p->next = nullptr;

                                if (front_) {
                                        front_->next = p;
                                } else {
                                        back_ = p;
                                }
                                front_ = p;
                        }

                        void push_back(payload *p) {
                                p->next = nullptr;

                                if (back_) {
                                        back_->next = p;
                                        TANK_EXPECT(front_);
                                } else {
                                        front_ = p;
                                }

                                back_ = p;
                        }

                        auto front() noexcept {
                                return front_;
                        }

                        auto back() noexcept {
                                return back_;
                        }

                        auto empty() noexcept {
                                return !front_;
                        }

                        void pop_back() noexcept {
                                back_ = back_->next;

                                if (!back_) {
                                        front_ = nullptr;
                                }
                        }

                        void pop_front() noexcept {
                                front_ = front_->next;

                                if (!front_) {
                                        back_ = nullptr;
                                }
                        }

                        void clear() noexcept {
                                front_ = back_                      = nullptr;
                                front_payload_iovecs_index          = 0;
                                first_payload_partially_transferred = false;
                        }
                } outgoing_content;

                broker(const Switch::endpoint e)
                    : ep{e} {
                        outgoing_content.clear();
                }

                // see module headers
                //
                // if we can safely abort the first payload, we can safely abort them all
                //
                // if we wish to abort a payload associated with a broker request, we can safely drop it from
                // the outgoing_content payloads list if (can_safely_abort_broker_request_payload() == true)
                bool can_safely_abort_first_payload() const noexcept {
                        return outgoing_content.first_payload_partially_transferred == false;
                }

                bool can_safely_abort_broker_request_payload(broker_api_request *breq) noexcept {
                        if (auto p = outgoing_content.front(); p && p->broker_req == breq) {
                                return can_safely_abort_first_payload();
                        }

                        return true;
                }

                broker_outgoing_payload *abort_broker_request_payload(const broker_api_request *);
        };

      protected:
        // we need to track resources in-use
        // this helps with figuring out situtations where a resource may be released more than once, and
        // also help with unit tests
        struct ResourceTracker final {
                uint32_t api_request;
                uint32_t broker_api_request;
                uint32_t request_partition_ctx;
                uint32_t buffer;
                uint32_t managed_buf;
                uint32_t payload;
                uint32_t connection;

                void clear() {
                        memset(this, 0, sizeof(*this));
                }

                bool any() const noexcept {
                        return api_request | broker_api_request | request_partition_ctx | buffer | managed_buf | payload | connection;
                }

                ResourceTracker() {
                        clear();
                }
        } rsrc_tracker;
#ifdef HAVE_NETIO_THROTTLE
        switch_dlist throttled_connections_read_list{&throttled_connections_read_list, &throttled_connections_read_list};
        switch_dlist throttled_connections_write_list{&throttled_connections_write_list, &throttled_connections_write_list};
        uint64_t     throttled_connections_read_list_next{std::numeric_limits<uint64_t>::max()};
        uint64_t     throttled_connections_write_list_next{std::numeric_limits<uint64_t>::max()};
#endif

        eb_root                                   api_reqs_expirations_tree{EB_ROOT};
        uint64_t                                  api_reqs_expirations_tree_next{std::numeric_limits<uint64_t>::max()};
        eb_root                                   timers_ebtree_root{EB_ROOT};
        uint64_t                                  timers_ebtree_next{std::numeric_limits<uint64_t>::max()};
        eb_root                                   retry_bundles_ebt_root{EB_ROOT};
        uint64_t                                  retry_bundles_next{std::numeric_limits<uint64_t>::max()};
        eb_root                                   unreachable_brokers_tree{EB_ROOT};
        uint64_t                                  unreachable_brokers_tree_next{std::numeric_limits<uint64_t>::max()};
        simple_allocator                          reqs_allocator;
        std::vector<std::unique_ptr<api_request>> reusable_api_requests;
        std::vector<broker_api_request *>         reusable_broker_api_requests;
        std::vector<request_partition_ctx *>      reusable_request_partition_contexts;

        CompressionStrategy                                           compressionStrategy{CompressionStrategy::CompressIntelligently};
        robin_hood::unordered_map<Switch::endpoint, std::unique_ptr<broker>> brokers;
        switch_dlist                                                  all_brokers{&all_brokers, &all_brokers};
        robin_hood::unordered_map<topic_partition, Switch::endpoint>         leaders;
        robin_hood::unordered_map<str_view8, bool>                           topics_intern_map;
        bool                                                          allowStreamingConsumeResponses{false};
        int                                                           sndBufSize{128 * 1024}, rcvBufSize{1 * 1024 * 1024};
        strwlen8_t                                                    clientId{"c++"};
        switch_dlist                                                  conns_pend_est_list{&conns_pend_est_list, &conns_pend_est_list};
        uint64_t                                                      conns_pend_est_next_expiration{std::numeric_limits<uint64_t>::max()};
        switch_dlist                                                  all_conns_list{&all_conns_list, &all_conns_list};
        uint64_t                                                      next_conns_gen{1};
        simple_allocator                                              resultsAllocator{2 * 1024 * 1024};

        std::vector<partition_content>           consumed_content;
        std::vector<fault>                       all_captured_faults;
        std::vector<produce_ack>                 produce_acks_v;
        std::vector<discovered_topic_partitions> all_discovered_partitions;
        std::vector<reload_conf_result>          reload_conf_results_v;
        std::vector<created_topic>               created_topics_v;
        std::vector<srv_status>                  collected_cluster_status_v;

        robin_hood::unordered_map<uint32_t, broker_api_request *>         pending_brokers_requests;
        robin_hood::unordered_map<uint32_t, std::unique_ptr<api_request>> pending_responses;
        std::vector<std::unique_ptr<api_request>>                  ready_responses;

        EPoller                                   poller;
	int interrupt_fd{-1};
        std::atomic<bool>                         sleeping{false};
        uint64_t                                  now_ms{0};
        uint64_t                                  next_curtime_update{0};
        time_t                                    cur_time{0};
        simple_allocator                          core_allocator;
        std::vector<broker_outgoing_payload *>    reusable_payloads;
        std::vector<std::unique_ptr<connection>>  reusable_connections;
        std::vector<std::unique_ptr<IOBuffer>>    reusable_buffers;
        std::vector<std::unique_ptr<managed_buf>> reusable_managed_buffers;
        std::vector<range32_t>                    ranges;

#include "client_pragmas.h"
};

TankClient::connection *TankClient::connection_handle::get() noexcept {
        return c_ && c_->gen == c_gen ? c_ : nullptr;
}

const TankClient::connection *TankClient::connection_handle::get() const noexcept {
        return c_ && c_->gen == c_gen ? c_ : nullptr;
}

void TankClient::connection_handle::set(connection *c) noexcept {
        c_    = c;
        c_gen = c->gen;
}

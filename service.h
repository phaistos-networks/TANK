#pragma once
#include "common.h"
#include <fs.h>
#include <network.h>
#include <switch.h>
#include <switch_ll.h>
#include <switch_refcnt.h>
#include <switch_vector.h>
#include <switch_dictionary.h>
#include <thread.h>
#include <sys/mman.h>
#include <sys/sendfile.h>

// https://github.com/phaistos-networks/TANK/issues/7
// We are not going to use multiple threads anywyay for Service
//#define TANK_SERIALIZE_OPS 

#if !defined(LEAN_SWITCH) && defined(TANK_SERIALIZE_OPS)
#include <switch_sharedmutex.h>
#elif defined(TANK_SERIALIZE_OPS)
#undef TANK_SERIALIZE_OPS
#endif



// The index is sparse, so we need to be able to locate the _last_ index entry for relative sequence number <= targetRelativeSeqNumber
// so that we can either
// 1. read from the log file starting from there onwards, until we find the first message
// /w sequence number >= target sequence number, or
// 2. we just stream rom that previous message and expect the clients to skip messages themselves
namespace std
{
        // Returns an iterator to the _last_ element in [first, last) that is <= `value`
        // You should probably return TrivialCmp(comp, value);
        template <class ForwardIt, class T, class Compare>
        static ForwardIt upper_bound_or_match(ForwardIt first, ForwardIt last, const T &value, const Compare comp)
        {
                ForwardIt res = last;

                for (int32_t top = (last - first) - 1, btm{0}; btm <= top;)
                {
                        const auto mid = (btm + top) / 2;
                        const auto p = first + mid;
                        const auto r = comp(*p, value);

                        if (!r)
                                return p;
                        else if (r > 0)
                        {
                                res = p;
                                btm = mid + 1;
                        }
                        else
                                top = mid - 1;
                }

                return res;
        }
}

struct index_record
{
        uint32_t relSeqNum;
        uint32_t absPhysical;
};

struct ro_segment_lookup_res
{
        index_record record;
        uint32_t span;
};

struct fd_handle
    : public RefCounted<fd_handle>
{
        int fd;

        fd_handle(int f)
            : fd{f}
        {
        }

        ~fd_handle()
        {
                if (fd != -1)
                        close(fd);
        }
};

// A read-only (immutable, frozen-sealed) partition commit log(Segment) (and the index file for quick lookups)
// we don't need to acquire a lock to access this
struct ro_segment
{
        // the absolute sequence number of the first message in this segment
        const uint64_t baseSeqNum;
	// the absolute sequence number of the last message in this segment
	// i.e this segment contains [baseSeqNum, lastAssignedSeqNum]
	// See: https://github.com/phaistos-networks/TANK/issues/2 for rationale
	const uint64_t lastAvailSeqNum;

	const uint32_t createdTS;
	uint32_t lastModTS;


        Switch::shared_refptr<fd_handle> fdh;
        uint32_t fileSize;

        // Every log file is associated with this skip-list index
        struct
        {
                const uint8_t *data;
                uint32_t fileSize;

                // last record in the index
                index_record lastRecorded;
        } index;

        ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const uint32_t creationTS)
            : baseSeqNum{absSeqNum}, lastAvailSeqNum{lastAbsSeqNum}, createdTS{creationTS}, lastModTS{0}
        {
        }

        ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const strwlen32_t base, const uint32_t);

        ~ro_segment()
        {
                if (index.data && index.data != MAP_FAILED)
                        munmap((void *)index.data, index.fileSize);
        }

        // Locate the (relative seq.num, abs.file offset) for the LAST index record where record.relSqNum <= targetRelSeqNum
        std::pair<uint32_t, uint32_t> snapDown(const uint64_t absSeqNum) const;

        // Locate the (relative seq.num, abs.file offset) for the LAST index record where record.relSqNum >= targetRelSeqNum
        std::pair<uint32_t, uint32_t> snapUp(const uint64_t absSeqNum) const;

        ro_segment_lookup_res translateDown(const uint64_t absSeqNum, const uint32_t max) const
        {
                const auto res = snapDown(absSeqNum);

                return {{res.first, res.second}, fileSize - res.second};
        }
};

struct append_res
{
        Switch::shared_refptr<fd_handle> fdh;
        range32_t dataRange;
        range_base<uint64_t, uint16_t> msgSeqNumRange;
};

struct lookup_res
{
        enum class Fault : uint8_t
        {
                NoFault = 0,
                Empty,
                BoundaryCheck,
                AtEOF
        } fault;

        Switch::shared_refptr<fd_handle> fdh;

        // Absolute base sequence number of the first message in the first bundle
        // of all bundles in the log chunk in range.
        // Incremenent this by each bundle.header.msgSetMsgsCnt to compute the absolute sequence number
        // of each bundle message (use post-increment!)
        uint64_t absBaseSeqNum;

        // chunk file range; we can start streaming from there
        range32_t range;

        // The last committed absolute sequence number
        uint64_t highWatermark;

        lookup_res(lookup_res &&o)
            : fault{o.fault}, fdh(std::move(o.fdh)), absBaseSeqNum{o.absBaseSeqNum}, range{o.range}, highWatermark{o.highWatermark}
        {
        }

        lookup_res(const lookup_res &) = delete;

        lookup_res()
            : fault{Fault::NoFault}
        {
        }

        lookup_res(fd_handle *const f, const uint64_t seqNum, const range32_t r, const uint64_t h)
            : fault{Fault::NoFault}, fdh{f}, absBaseSeqNum{seqNum}, range{r}, highWatermark{h}
        {
        }

        lookup_res(const Fault f, const uint64_t h)
            : fault{f}, highWatermark{h}
        {
        }
};

struct partition_config
{
	// Kafka defaults
        size_t roSegmentsCnt{0};
        uint64_t roSegmentsSize{0};
        uint64_t maxSegmentSize{1 * 1024 * 1024 * 1024};
        size_t indexInterval{4096};
	size_t maxIndexSize{10 * 1024 * 1024};
	size_t maxRollJitterSecs{0};
	size_t lastSegmentMaxAge{24 * 86400 * 7}; // 1 week (soft limit)
	size_t curSegmentMaxAge{24 * 86400 * 7}; // 1 week (soft limit)
	size_t flushIntervalMsgs{0}; 		// never
	size_t flushIntervalSecs{0}; 		// never
} config;

static void PrintImpl(Buffer &out, const lookup_res &res)
{
        if (res.fault != lookup_res::Fault::NoFault)
                out.append("{fd = ", res.fdh ? res.fdh->fd : -1, ", absBaseSeqNum = ", res.absBaseSeqNum, ", range = ", res.range, "}");
        else
                out.append("{fault = ", unsigned(res.fault), "}");
}

// An append-only log for storing bundles, divided into segments
struct topic_partition_log
{
#ifdef TANK_SERIALIZE_OPS
        // in practice, we 'll only need to acquire the lock when accessing the last/current commit log
        // all other immutable frozen commit log files do not require serialization
        Switch::SharedMutexReadPriority lock;
#endif

        // The first available absolute sequence number across all segments
        uint64_t firstAvailableSeqNum;

        // This will be initialized from the latest segment in initPartition()
        uint64_t lastAssignedSeqNum{0};

        Buffer basePath;

        struct
        {
                Switch::shared_refptr<fd_handle> fdh;

                // The absolute sequence number of the first message in the current segment
                uint64_t baseSeqNum;
                uint32_t fileSize;

                // We are going to be updating the index and skiplist frequently
                // This is always initialized to UINT32_MAX, so that we always index the first bundle in the segment, for impl.simplicity
                uint32_t sinceLastUpdate;

		// Computed whenever we roll/create a new mutable segment
		uint32_t rollJitterSecs;

		// When this was created, in seconds
		uint32_t createdTS;
		bool nameEncodesTS;

                struct
                {
                        int fd;

                        // relative sequence number => file physical offset
                        // relative sequence number = absSeqNum - baseSeqNum
                        Switch::vector<std::pair<uint32_t, uint32_t>> skipList;

                        // We may access the index both directly, if ondisk is valid, and via the skipList
                        // This is so that we won't have to restore the skiplist on init
                        struct
                        {
                                const uint8_t *data;
                                uint32_t span;

                                // last recorded tuple in the index; we need this here
                                struct
                                {
                                        uint32_t relSeqNum;
                                        uint32_t absPhysical;
                                } lastRecorded;

                        } ondisk;

                } index;


		struct
		{
			uint64_t pendingFlushMsgs{0};
			uint32_t nextFlushTS;
		} flush_state;

        } cur; // the _current_ (latest) segment

	partition_config config;

        // a topic partition is comprised of a set of segments(log file, index file) which
        // are immutable, and we don't need to serialize access to them, and a cur(rent) segment, which is not immutable.
        //
        // roSegments can also be atomically exchanged with a new vector, so we don't need to protect that either
        // make sure roSegments is sorted
	std::shared_ptr<Switch::vector<ro_segment *>> roSegments;

        ~topic_partition_log()
        {
                if (roSegments)
                {
                        for (ro_segment *it : *roSegments)
                                delete it;
                }
        }

        lookup_res read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum);

        lookup_res range_for(uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum);

        append_res append_bundle(const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt);

	bool should_roll(const uint32_t) const;

	void schedule_flush(const uint32_t);

        void consider_ro_segments();
};

struct connection;
struct topic_partition;

// In order to support minBytes semantics, we will
// need to track produced data for each tracked topic partition, so that
// we will be able to flush once we can satisfy the minBytes semantics
struct wait_ctx_partition
{
        fd_handle *fdh;
        uint64_t seqNum;
        range32_t range;
        topic_partition *partition;
};

struct wait_ctx
{
        connection *c;
        uint32_t requestId;
        switch_dlist list, expList;
        uint64_t expiration; // in MS
        uint32_t minBytes;

        // A request may involve multiple partitions
        // minBytes applies to the sum of all captured content for all specified partitions
        uint32_t capturedSize;

        uint8_t partitionsCnt;
        wait_ctx_partition partitions[0];
};

struct topic;
struct topic_partition
    : public RefCounted<topic_partition>
{
        uint16_t idx; // (0, ...)
        uint32_t distinctId;
        topic *owner{nullptr};
        uint16_t localBrokerId; // for convenience

        struct replica
            : public RefCounted<replica>
        {
                const uint16_t brokerId; // owner
                uint64_t time;
                uint64_t initialHighWaterMark;

                // The high watermark sequence number; maintained for followers replicas
                uint64_t highWatermMark;

                // Tracked across all replicas
                // for the local replica, it's the partition's lastAssignedSeqNum, for remote replicas, this is updated by followers fetch reqs
                uint64_t lastAssignedSeqNum;

                replica(const uint16_t id)
                    : brokerId{id}
                {
                }
        };

        struct
        {
                uint16_t brokerId;
                uint64_t epoch;
                Switch::shared_refptr<struct replica> replica;
        } leader;

        // this node may or may not be a replica for this partition
        std::unique_ptr<topic_partition_log> log_;

#ifndef LEAN_SWITCH
#ifdef TANK_SERIALIZE_OPS
        Switch::SharedMutexReadPriority replicasLock;
#endif
        Switch::unordered_map<uint16_t, replica *, ReleaseRefDestructor> replicasMap;
#else
	Switch::unordered_map<uint16_t, replica *> replicasMap;
#endif

        Switch::vector<replica *> insyncReplicas;

        auto highwater_mark() const
        {
                return log_->lastAssignedSeqNum;
        }

#ifdef TANK_SERIALIZE_OPS
        Switch::mutex waitingListLock;
#endif
        Switch::vector<wait_ctx *> waitingList;

        Switch::shared_refptr<replica> replicaByBrokerId(const uint16_t brokerId)
        {
                return replicasMap[brokerId];
        }

        void consider_append_res(append_res &res, Switch::vector<wait_ctx *> &waitCtxWorkL);

        append_res append_bundle_to_leader(const uint8_t *const bundle, const size_t bundleLen, const uint8_t bundleMsgsCnt, Switch::vector<wait_ctx *> &waitCtxWorkL);

        lookup_res read_from_local(const bool fetchOnlyFromLeader, const bool fetchOnlyComittted, const uint64_t absSeqNum, const uint32_t fetchSize);
};

struct topic
    : public RefCounted<topic>
{
        const strwlen8_t name_;
        Switch::vector<topic_partition *> *partitions_;

        topic(const strwlen8_t name)
            : name_{name.Copy(), name.len}
        {
                partitions_ = new Switch::vector<topic_partition *>();
        }

        auto name() const
        {
                return name_;
        }

        ~topic()
        {
                if (auto *const p = const_cast<char *>(name_.p))
                        free(p);

		if (partitions_)
                {
                        while (partitions_->size())
                                 partitions_->Pop()->Release();

                        delete partitions_;
                }
        }

        void register_partition(topic_partition *const p)
        {
                p->owner = this;
                partitions_->push_back(p);
                std::sort(partitions_->begin(), partitions_->end(), [](const auto a, const auto b) {
                        return a->idx < b->idx;
                });

                require(partitions_->back()->idx == partitions_->size() - 1);
        }

        const topic_partition *partition(const uint16_t idx) const
        {
                return idx < partitions_->size() ? partitions_->at(idx) : nullptr;
        }

        topic_partition *partition(const uint16_t idx)
        {
                return idx < partitions_->size() ? partitions_->at(idx) : nullptr;
        }
};

// We could have used Switch::deque<> but let's just use something simpler
// TODO: Maybe we should just use a linked list instead
struct outgoing_queue
{
        struct payload
        {
                bool payloadBuf;
                IOBuffer *buf;

                struct content_file_range
                {
                        fd_handle *fdh;
                        range32_t range;

                        content_file_range &operator=(const content_file_range &o)
                        {
                                fdh = o.fdh;
                                range = o.range;
                                return *this;
                        }

                } file_range;

                payload()
                    : payloadBuf{false}
                {
                }

                payload(IOBuffer *const b)
                {
                        payloadBuf = true;
                        buf = b;
                }

                payload(fd_handle *const fdh, const range32_t r)
                {
                        payloadBuf = false;
                        file_range.fdh = fdh;
                        file_range.range = r;
			file_range.fdh->Retain();
                }

                payload &operator=(const payload &o)
                {
                        payloadBuf = o.payloadBuf;
                        if (payloadBuf)
                                buf = o.buf;
                        else
                                file_range = o.file_range;

                        return *this;
                }
        };

        using reference = payload &;
        using reference_const = const payload &;

        payload A[128]; // fixed size
        uint8_t backIdx{0}, frontIdx{0}, size_{0};
        static constexpr size_t capacity{sizeof_array(A)};

        inline uint32_t prev(const uint32_t idx) const
        {
                return (idx + (capacity - 1)) & (capacity - 1);
        }

        inline uint32_t next(const uint32_t idx) const
        {
                return (idx + 1) & (capacity - 1);
        }

        inline reference front()
        {
                return A[frontIdx];
        }

        inline reference_const front() const
        {
                return A[frontIdx];
        }

        inline reference back()
        {
                return A[prev(backIdx)];
        }

        inline reference_const back() const
        {
                return A[prev(backIdx)];
        }

        reference at(const size_t idx)
        {
                return A[(frontIdx + idx) & (capacity - 1)];
        }

        reference_const at(const size_t idx) const
        {
                return A[(frontIdx + idx) & (capacity - 1)];
        }

        void push_back(const payload &v)
        {
                if (unlikely(size_ == capacity))
                        throw Switch::system_error("Queue full");

                A[backIdx] = v;
                backIdx = next(backIdx);
                ++size_;
        }

        void push_back(payload &&v)
        {
                if (unlikely(size_ == capacity))
                        throw Switch::system_error("Queue full");

                A[backIdx] = std::move(v);
                backIdx = next(backIdx);
                ++size_;
        }

        void push_front(const payload &v)
        {
                if (unlikely(size_ == capacity))
                        throw Switch::system_error("Queue full");

                frontIdx = prev(frontIdx);
                A[frontIdx] = v;
                ++size_;
        }

        void push_front(payload &&v)
        {
                if (unlikely(size_ == capacity))
                        throw Switch::system_error("Queue full");

                frontIdx = prev(frontIdx);
                A[frontIdx] = std::move(v);
                ++size_;
        }

        void pop_back()
        {
                backIdx = prev(backIdx);
                --size_;
        }

        void pop_front()
        {
                frontIdx = next(frontIdx);
                --size_;
        }

        bool full() const
        {
                return size_ == capacity;
        }

        bool empty() const
        {
                return !size_;
        }

        auto size() const
        {
                return size_;
        }

        template <typename L>
        void clear(L &&l)
        {
                while (frontIdx != backIdx)
                {
                        auto &p = A[frontIdx];

                        if (p.payloadBuf)
                                l(p.buf);
                        else
                                p.file_range.fdh->Release();

                        frontIdx = next(frontIdx);
                }

                size_ = 0;
                frontIdx = backIdx = 0;
        }

        outgoing_queue()
        {
        }

        ~outgoing_queue()
        {
                require(empty());
        }
};

struct connection
{
        int fd;
        IOBuffer *inB{nullptr};

        // May have an attached outgoing queue
        outgoing_queue *outQ{nullptr};

        switch_dlist connectionsList, waitCtxList;

        // This is initialized to(0) for clients, but followers are expected to
        // issue an REPLICA_ID request as soon as they connect
        uint16_t replicaId;

        struct State
        {
                enum class Flags : uint8_t
                {
                        PendingIntro = 0,
                        NeedOutAvail,
                        Busy

                };

                uint8_t flags;
                uint64_t lastInputTS;
        } state;
};

class Service
{
      private:
        Switch::vector<wait_ctx *> waitCtxPool[255];
#ifndef LEAN_SWITCH
        Switch::unordered_map<strwlen8_t, topic *, ReleaseRefDestructor> topics;
#else
        Switch::unordered_map<strwlen8_t, topic *> topics;
#endif
        Buffer basePath_;
        uint16_t selfBrokerId{1};
        Switch::vector<IOBuffer *> bufs;
        Switch::vector<connection *> connsPool;
        Switch::vector<outgoing_queue *> outgoingQueuesPool;
        switch_dlist allConnections, waitExpList;
        Switch::vector<wait_ctx *> expiredCtxList;
        uint32_t nextDistinctPartitionId{0};
        int listenFd;
        EPoller poller;
        Switch::vector<topic_partition *> deferList;

      private:
        static bool parse_partition_config(const char *, partition_config *);

        auto get_outgoing_queue()
        {
                return outgoingQueuesPool.size() ? outgoingQueuesPool.Pop() : new outgoing_queue();
        }

        void put_outgoing_queue(outgoing_queue *const q)
        {
                q->clear([this](auto buf) {
                        this->put_buffer(buf);
                });

                outgoingQueuesPool.push_back(q);
        }

        auto get_buffer()
        {
                return bufs.size() ? bufs.Pop() : new IOBuffer();
        }

        void put_buffer(IOBuffer *b)
        {
                b->clear();
                bufs.push_back(b);
        }

        auto get_connection()
        {
                return connsPool.size() ? connsPool.Pop() : new connection();
        }

        void put_connection(connection *const c)
        {
                if (c->inB)
                {
                        put_buffer(c->inB);
                        c->inB = nullptr;
                }
                connsPool.push_back(c);
        }

        void register_topic(topic *const t)
        {
                if (!topics.Add(t->name(), t))
                        throw Switch::exception("Topic ", t->name(), " already registered");
        }

        Switch::shared_refptr<topic_partition> init_local_partition(const uint16_t idx, const char *const bp, const partition_config&);

        bool isValidBrokerId(const uint16_t replicaId)
        {
                return replicaId != 0;
        }

        uint32_t partitionLeader(const topic_partition *const p)
        {
                return 1;
        }

        const topic_partition *getPartition(const strwlen8_t topic, const uint16_t partitionIdx) const
        {
#ifdef LEAN_SWITCH
		const auto it = topics.find(topic);

		if (it != topics.end())
			return it->second->partition(partitionIdx);
#else
                if (const auto t = topics[topic])
                        return t->partition(partitionIdx);
#endif

                return nullptr;
        }

        topic_partition *getPartition(const strwlen8_t topic, const uint16_t partitionIdx)
        {
                if (auto t = topics[topic])
                        return t->partition(partitionIdx);

                return nullptr;
        }

        bool process_consume(connection *const c, const uint8_t *p, const size_t len);

        bool process_replica_reg(connection *const c, const uint8_t *p, const size_t len);

        wait_ctx *get_waitctx(const uint8_t totalPartitions)
        {
                if (waitCtxPool[totalPartitions].size())
                        return waitCtxPool[totalPartitions].Pop();
                else
                        return (wait_ctx *)malloc(sizeof(wait_ctx) + totalPartitions * sizeof(wait_ctx_partition));
        }

        void put_waitctx(wait_ctx *const ctx)
        {
                waitCtxPool[ctx->partitionsCnt].push_back(ctx);
        }

        bool register_consumer_wait(connection *const c, const uint32_t requestId, const uint64_t maxWait, const uint32_t minBytes, topic_partition **const partitions, const uint32_t totalPartitions);

        bool process_produce(connection *const c, const uint8_t *p, const size_t len);

        bool process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len);

        void wakeup_wait_ctx(wait_ctx *const wctx, const append_res &appendRes);

        void abort_wait_ctx(wait_ctx *const wctx);

        void destroy_wait_ctx(wait_ctx *const wctx);

	void cleanup_connection(connection *);

        bool shutdown(connection *const c, const uint32_t ref);

        bool try_send_ifnot_blocked(connection *const c)
        {
                if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))
                {
                        // Blocked
                        return true;
                }
                else
                        return try_send(c);
        }

        bool try_send(connection *const c);

      public:
        Service()
        {
                switch_dlist_init(&allConnections);
                switch_dlist_init(&waitExpList);
        }

	~Service();

        int start(int argc, char **argv);
};

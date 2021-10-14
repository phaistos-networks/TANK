#include "common.h"
#include <fs.h>
#include <network.h>
#include <switch.h>
#include <switch_ll.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <thread.h>
#include <ext/ebtree/eb64tree.h>
#include <thread>
#include <text.h>
#include <unordered_set>
#include <deque>
#include <switch_bitops.h>
#include <queue>
#include <zlib.h>
#include <crypto.h>
#include <switch_mallocators.h>
#include <condition_variable>
#include <atomic>
#include <unordered_map>

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wc99-designator"
#endif


// if HWM_UPDATE_BASED_ON_ACKS is defined, the current semantics apply:
// Assuming two producers PR1 and PR2, both publishing to partition P0.
// Assuming PR1 publishes a single message with LSN 100 with required acks = 2 (+1 for implicit local ack)
// and PR2 publishing a single message that gets LSN 101, with required acks = 1 (+1 for implicit local ack)
// If HWM_UPDATE_BASED_ON_ACKS is defined, then the HWM will advance as soon as both messages 100 and 101 are acknowledged.
//
// Assuming one of the RS nodes ask to cosume from 102. That will ackn. the message from PR2 (because it required
// just 1 ack. from remote RS peer). However, PR1's message can't be ackowledged yet (requires one more consumer to fetch
// from 101+). We would only acknowlege PR2's message if all other messages before it(i.e PR1's) were acknowledged, and we would only
// advance the HWMark to X as long as no other messages < X are not acknowledged yet.
// This is very powerful, but apparently it doesn't comply with Kafka semantics. Kafka semantics are much simpler to implement
//
// See: https://gist.github.com/markpapadakis/5c0e0ee74fe5fcc4d06fd87563c29236
//
// Kafka semantics in a nutshell, based on Gwen's input(thanks, Gwen!):
// - HWM advances based on the "lowest offset replicated by the entire ISR" (so really, just the lowest LSN
//	in a ConsumePeer request among all nodes in the ISR)
// - Producers get an ack **regardless** of HWM advancement (i.e if producer P produces a message with LSN 100 and ack = 2(requires
//	the implicit ack from the leader and 2 acks from the remote nodes in the ISR), then as soon as 2+ different remote peers
// 	ConsumePeer for LSN > 100, that producer P gets the ack).
//
//#define HWM_UPDATE_BASED_ON_ACKS 1

namespace TANKUtil {
        // A handy class for reading ahead
        // as an alternative to entering the kernel too often.
        //
        // This works great in practice, but it's not optimal.
        //
        // We should cache blocks of e.g 8k each. A simple eviction policy wouldn't really work
        // because whenever we 'd get rid of an ROLog or the current log we 'd need to purge whatever blocks we had from those.
        //
        // However, we could also maintaina a RA cache per ROLog/iCur
        // where the key is offset aligned to e.g 4096
        // TODO: figure out a better alternative.
        //
        // We need a simple API where we request data by (offset, range)
        // and we 'll get back either a str_view32 to that data, or a str_view32 to data copied somewhere if
        // the data cross blocks boundaries
        // see 2Q eviction algorithm
        static constexpr const std::size_t read_ahead_default_stride = 4096;

        struct range_start final {
                bool     first_bundle_is_sparse;
                uint64_t abs_seqnum;
                uint32_t file_offset;
        };

        template <size_t BUF_SIZE = read_ahead_default_stride>
        class read_ahead final {
                enum {
                        trace = false
                };

              private:
                int      fd{-1};
                uint8_t  buf[BUF_SIZE];
                uint64_t start{0};
                uint64_t end{0};

              private:
                auto at(const uint64_t offset) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                        if (trace) {
                                SLog("Accessing offset = ", offset, ", start = ", start, ", end = ", end, "\n");
                        }

                        TANK_EXPECT(offset >= start);
                        TANK_EXPECT(offset < end);

                        return range_base<const uint8_t *, size_t>(buf + (offset - start),
                                                                   end - offset);
                }

                auto buffer() const noexcept {
                        return str_view32(reinterpret_cast<const char *>(buf), end - start);
                }

                int read_impl([[maybe_unused]] const size_t bytes) {
                        enum {
                                trace = false,
                        };
                        const auto at       = size();
                        const auto capacity = sizeof(buf) - at;
                        int        r;

                        assert(fd > 2);

                        if (trace) {
                                struct stat st;

                                fstat(fd, &st);

                                SLog("Attempting to read ", capacity, " at ", end, " in buffer + ", at, " for fd = ", fd, "\n");
                                SLog("filesize = ", st.st_size, "\n");
                        }

                        for (;;) {
                                r = pread64(fd, buf + at, capacity, end);

                                if (-1 == r) {
                                        if (EINTR == errno) {
                                                continue;
                                        } else {
                                                if (trace) {
                                                        SLog("pread64() into ", at, " for ", capacity, " at ", end, " failed with:", strerror(errno), "\n");
                                                }

                                                return -1;
                                        }
                                } else {
                                        break;
                                }
                        }

                        if (trace) {
                                SLog("end += ", r, " => ", end + r, "\n");
                        }

                        end += r;
                        return 0;
                }

                bool reset_to(const uint64_t offset, const size_t n) {
                        start = offset;
                        end   = offset;
                        return read_impl(sizeof(buf)) != -1;
                }

                bool expand(const uint64_t offset, const size_t n) {
                        const auto upto = offset + n;

                        if (upto <= end) {
                                // hit
                                return true;
                        }

                        const auto required   = upto - end;
                        const auto buffer_len = size();

                        // can we expand the buffer?
                        if (required + buffer_len <= sizeof(buf)) {
                                return read_impl(required) != -1;
                        }

                        const auto useful_span = end - offset;

                        memmove(buf, buf + (offset - start), useful_span);
                        start = offset;
                        end   = start + useful_span;

                        return read_impl(required) != -1;
                }

                bool read1(const uint64_t offset, const size_t n) {
                        if (offset < start) {
                                // optimized for read-ahead
                                // so we 'll just reset (i.e not optimised for this)
                                return reset_to(offset, n);
                        }

                        if (offset >= end) {
                                return reset_to(offset, n);
                        } else if (const auto upto = offset + n; upto < start) {
                                return reset_to(offset, n);
                        } else if (upto <= end) {
                                // hit
                                return true;
                        } else {
                                return expand(offset, n);
                        }
                }

              public:
                range_base<const uint8_t *, std::size_t> read(const uint64_t offset, const size_t n) {
                        if (not read1(offset, n)) {
                                return {nullptr, 0};
                        }

                        return at(offset);
                }

                void clear() noexcept {
                        start = 0;
                        end   = 0;
                }

                void reset_to(int _fd) {
                        assert(fd == -1 or fd > 2);

                        fd = _fd;
                        clear();
                }

                read_ahead(const int _fd = -1) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS
                    : fd{_fd} {
                        //
                        assert(fd == -1 or fd > 2);
                }

                std::size_t size() const noexcept {
                        return end - start;
                }

                auto get_fd() const noexcept {
                        return fd;
                }
        };
} // namespace TANKUtil

struct index_record final {
        uint32_t relSeqNum;
        uint32_t absPhysical;
};

struct ro_segment_lookup_res final {
        index_record record;
        uint32_t     span;
};

struct fd_handle final {
        int fd{-1};

        fd_handle(int f)
            : fd{f} {
        }

        ~fd_handle() {
                if (fd != -1) {
                        fdatasync(fd);
                        TANKUtil::safe_close(fd);
                }
        }
};

struct adjust_range_start_cache_value final {
        bool     first_bundle_is_sparse;
        uint32_t file_offset;
        uint64_t seq_num;
};

struct topic_partition;

// A read-only (immutable, frozen-sealed) partition commit log(Segment) (and the index file for quick lookups)
// we don't need to acquire a lock to access this
//
// we need to ref-count ro_segments so that we can hand off the list of current ro segments to another thread for compaction, so that
// while the compaction is in progess, we won't delete any of those segments passed to the thread (i.e in consider_ro_segments() )
// For now, we can return immediately from consider_ro_segments() if compaction is scheduled for the partition, and later we can
// do this properly.
struct ro_segment final {
        // the absolute sequence number of the first message in this segment
        const uint64_t baseSeqNum;

        // the absolute sequence number of the last message in this segment
        // i.e this segment contains [baseSeqNum, lastAssignedSeqNum]
        // See: https://github.com/phaistos-networks/TANK/issues/2 for rationale
        uint64_t lastAvailSeqNum;

        // For RO segments, this used to be set to the creation time of the
        // mutable segment that was then turned into a R/O segment.
        //
        // This however turned out to be problematic, because retention logic would
        // consider that timestamp for retentions, instead of what makes more sense, the time when
        // the last message was appended to the mutable segment, before it was frozen as a RO segment.
        //
        // We need to encode this in the file path, because a process may update the mtime of the RO segment for whatever reason
        // and so it's important that we do not depend on the file's mtime, and instead encode it in the path.
        // see: https://github.com/phaistos-networks/TANK/issues/37
        const uint32_t createdTS;

        std::shared_ptr<fd_handle> fdh;
        uint32_t                   fileSize;

        // In order to support compactions (in the future), in the very improbable and unlikely case compaction leads
        // to situations where because of deduplication we will end up having to store messages in a segment where any of those
        // message.absSeqNum - segment.baseSeqNum > UINT32_MAX, and we don't want to just create a new immutable segment to deal with it(maybe because
        // that'd lead to creating very small segments), then we encode {absSeqNum:u64, fileOffet:u32} instead in the immutable segment's index and
        // to set that that, we 'll use a different name for those .index files.
        //
        // For now, the implementation is missing, but we 'll implement what's required if and when we need to do so.
        const bool haveWideEntries;

        // Every log file is associated with this skip-list index
        struct
        {
                const uint8_t *data{nullptr};
                uint32_t       fileSize{0};

                // last record in the index
                index_record lastRecorded;
        } index;

        ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const uint32_t creationTS)
            : baseSeqNum{absSeqNum}
            , lastAvailSeqNum{lastAbsSeqNum}
            , createdTS{creationTS}
            , haveWideEntries{false} {
	    	assert(not fdh);
        }

        ro_segment(const uint64_t absSeqNum, uint64_t lastAbsSeqNum, const str_view32 base, const uint32_t, const bool haveWideEntries);

        ~ro_segment() {
                if (index.data && index.data != MAP_FAILED) {
                        munmap((void *)index.data, index.fileSize);
                }
        }

        bool prepare_access(const topic_partition *);
};

struct timer_node final {
        enum class ContainerType : uint8_t {
                ConnEstTimeout = 0,
                PeerConnEstTimeout,
                Connection,
                WaitCtx,
                CleanupTracker,
                ScheduleRenewConsulSess,
                TryConsulClusterReg,
                SchedConsulReq,
                ShutdownConsumerConn,
                ForceSetReactorStateIdle,
                TryBecomeClusterLeader,
        } type;

        eb64_node node;

        void reset() {
                node.node.leaf_p = nullptr;
        }

        bool is_linked() const noexcept {
                return node.node.leaf_p;
        }
};

struct append_res final {
        std::shared_ptr<fd_handle>     fdh;
        range32_t                      dataRange;
        range_base<uint64_t, uint16_t> msgSeqNumRange;
};

struct lookup_res final {
        enum class Fault : uint8_t {
                NoFault = 0,
                Empty,
                BoundaryCheck,
                PastMax,
                AtEOF,
                SystemFault,
        } fault;

        // This is set to either fileSize of the segment log file or lower if
        // we are are setting a boundary based on last assigned committed sequence number phys.offset
        // adjust_range() cannot exceed that offset
        uint32_t                   fileOffsetCeiling;
        std::shared_ptr<fd_handle> fdh;

        // Absolute base sequence number of the first message in the first bundle
        // of all bundles in the log chunk in range.
        // Incremenent this by each bundle.header.msgSetMsgsCnt to compute the absolute sequence number
        // of each bundle message (use post-increment!)
        uint64_t absBaseSeqNum;

        // file offset for the bundle with the first message == absBaseSeqNum
        uint32_t fileOffset;

        bool first_bundle_is_sparse;

        lookup_res(lookup_res &&o)
            : fault{o.fault}
            , fileOffsetCeiling{o.fileOffsetCeiling}
            , fdh(std::move(o.fdh))
            , absBaseSeqNum{o.absBaseSeqNum}
            , fileOffset{o.fileOffset}
            , first_bundle_is_sparse{o.first_bundle_is_sparse} {
        }

        lookup_res(const lookup_res &o)
            : fileOffsetCeiling{o.fileOffsetCeiling}
            , fdh{o.fdh}
            , absBaseSeqNum{o.absBaseSeqNum}
            , fileOffset{o.fileOffset}
            , first_bundle_is_sparse{o.first_bundle_is_sparse} {
        }

        lookup_res()
            : fault{Fault::NoFault} {
        }

        lookup_res(std::shared_ptr<fd_handle> f, const uint32_t c, const uint64_t seqNum, const uint32_t o, const bool s)
            : fault{Fault::NoFault}
            , fileOffsetCeiling{c}
            , fdh{f}
            , absBaseSeqNum{seqNum}
            , fileOffset{o}
            , first_bundle_is_sparse{s} {
        }

        lookup_res(const Fault f)
            : fault{f} {
        }

        auto &operator=(const lookup_res &o) {
                fault                  = o.fault;
                fileOffsetCeiling      = o.fileOffsetCeiling;
                fdh                    = o.fdh;
                absBaseSeqNum          = o.absBaseSeqNum;
                fileOffset             = o.fileOffset;
                first_bundle_is_sparse = o.first_bundle_is_sparse;

                return *this;
        }
};

enum class CleanupPolicy : uint8_t {
        DELETE = 0,
        CLEANUP
};

extern struct partition_config final {
        // Kafka defaults
        size_t        roSegmentsCnt{0}; // maximum segments to retain (0 all)
        uint64_t      roSegmentsSize{0};
        uint64_t      maxSegmentSize{1 * 1024 * 1024 * 1024};
        size_t        indexInterval{4096};
        size_t        maxIndexSize{10 * 1024 * 1024};
        size_t        maxRollJitterSecs{0};
        size_t        lastSegmentMaxAge{0};        // Kafka's default is 1 week, we don't want to explicitly specify a retention limit
        size_t        curSegmentMaxAge{86400 * 7}; // 1 week (soft limit)
        size_t        flushIntervalMsgs{0};        // never
        size_t        flushIntervalSecs{0};        // never
        CleanupPolicy logCleanupPolicy{CleanupPolicy::DELETE};
        float         logCleanRatioMin{0.5}; //
} config;

static void PrintImpl(Buffer &out, const lookup_res &res) {
        if (res.fault != lookup_res::Fault::NoFault) {
                out.append("{fd = ", res.fdh ? res.fdh->fd : -1, ", absBaseSeqNum = ", res.absBaseSeqNum, ", fileOffset = ", res.fileOffset, "}");
        } else {
                out.append("{fault = ", unsigned(res.fault), "}");
        }
}

// An append-only log for storing bundles, divided into segments
struct topic_partition;
struct topic_partition_log final {
        // the absolute sequence number of the first available message across all log segments
        uint64_t firstAvailableSeqNum;

        // the absolute sequence number of the last available message across all log segments
        uint64_t lastAssignedSeqNum{0};

        topic_partition  *partition;
        std::atomic<bool> compacting{false};

        // Whenever we cleanup, we update lastCleanupMaxSeqNum with the lastAvailSeqNum of the latest ro segment compacted
        uint64_t lastCleanupMaxSeqNum{0};

        auto first_dirty_offset() const {
                return lastCleanupMaxSeqNum + 1;
        }

        struct {
                std::shared_ptr<fd_handle> fdh;

                // The absolute sequence number of the first message in the current segment
                uint64_t baseSeqNum;
                uint32_t fileSize;

                // We are going to be updating the index and skiplist frequently
                // This is always initialized to UINT32_MAX, so that we always index the first bundle in the segment, for impl.simplicity
                uint32_t sinceLastUpdate;

                // Computed whenever we roll/create a new mutable segment
                uint32_t rollJitterSecs{0};

                // When this was created, in seconds
                uint32_t createdTS{0};
                bool     nameEncodesTS;

                // make sure we flush when we rotate
                robin_hood::unordered_map<uint64_t, adjust_range_start_cache_value> triangulation_cache;

                struct {
                        TANKUtil::read_ahead<TANKUtil::read_ahead_default_stride> ra;

                        void clear() {
                                ra.clear();
                        }
                } ra_proxy;

                struct
                {
                        int fd{-1};

                        // relative sequence number to (absolute base sequence number, file offset)
                        // See https://github.com/phaistos-networks/TANK/issues/63
                        robin_hood::unordered_map<uint64_t, std::pair<uint64_t, uint32_t>> cache;

                        // this is populated by append_bundle().
                        // When it reaches 64k or so entries, it's flushed.
                        //
                        //  We consult it whenever we can, but because we only populate it with
                        // monotonically increasing sequence numbers, we also use a tiny cache(ondisk.cache)
                        // that protects us from what would otherwise need to access the on-disk memory mapped index
                        // and should provide us with some real befits

                        // relative sequence number => file physical offset
                        // relative sequence number = absSeqNum - baseSeqNum
                        std::vector<std::pair<uint32_t, uint32_t>> skipList;

                        // see above
                        bool haveWideEntries;

                        // We may access the index both directly, if ondisk is valid, and via the skipList
                        // This is so that we won't have to restore the skiplist on init
                        struct
                        {
                                const uint8_t *data;
                                uint32_t       span;

                                // small cache that help with tailing semantics
                                // make sure we flush whenever we rotate
                                robin_hood::unordered_map<uint32_t, index_record> cache;

                                // last recorded tuple in the index; we need this here
                                struct
                                {
                                        uint32_t relSeqNum;
                                        uint32_t absPhysical;
                                } lastRecorded;
                        } ondisk;
                } index;

                // TODO:
                // This is definitely not optimal
                // We should perhaps have a fixed size cache of N lanes, each lane M slots wide
                // For now, we 'll use a hash map though
                std::unordered_map<uint64_t, TANKUtil::range_start> file_range_start_cache;

                void reset_cache() {
                        file_range_start_cache.clear();
                        index.ondisk.cache.clear();
                        ra_proxy.clear();
                }

                struct
                {
                        uint64_t pendingFlushMsgs{0};
                        uint32_t nextFlushTS;
                } flush_state;

                void sanity_checks() const {
                        // no-op
                }
        } cur; // the _current_ (latest) segment

        partition_config config;

        // a topic partition is comprised of a set of segments(log file, index file) which
        // are immutable, and we don't need to serialize access to them, and a cur(rent) segment, which is not immutable.
        //
        // roSegments can also be atomically exchanged with a new vector, so we don't need to protect that either
        // make sure roSegments is sorted
        std::shared_ptr<std::vector<ro_segment *>> roSegments;

        ~topic_partition_log() {
                if (auto ptr = reinterpret_cast<void *>(const_cast<uint8_t *>(cur.index.ondisk.data)); ptr && ptr != MAP_FAILED) {
                        munmap(ptr, cur.index.ondisk.span);
                }

                if (roSegments) {
                        for (ro_segment *it : *roSegments) {
                                delete it;
                        }
                }

                if (cur.index.fd != -1) {
                        fdatasync(cur.index.fd);
                        TANKUtil::safe_close(cur.index.fd);
                }
        }

        lookup_res read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum);

        lookup_res range_for(uint64_t, const uint32_t, uint64_t);

        lookup_res range_for_immutable_segments(uint64_t, const uint32_t, uint64_t);

        lookup_res no_immutable_segment(const bool);

        lookup_res from_immutable_segment(const topic_partition_log *,
                                          ro_segment *,
                                          const uint64_t,
                                          const uint32_t,
                                          const uint64_t);

        append_res append_bundle(const time_t, const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt, const uint64_t, const uint64_t);

        // utility method: appends a non-sparse bundle
        // This is handy for appending bundles to the internal topics/partitions
        append_res append_bundle(const time_t ts, const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt) {
                return append_bundle(ts, bundle, bundleSize, bundleMsgsCnt, 0, 0);
        }

        append_res append_msg(const time_t ts, const strwlen8_t key, const str_view32 msg);

        bool should_roll(const uint32_t) const;

        void roll(const uint64_t, const uint64_t);

        void flush_index_skiplist();

        bool may_switch_index_wide(const uint64_t);

        void schedule_flush(const uint32_t);

        void consider_ro_segments();
};

struct pending_compaction final {
        pending_compaction       *next;
        char                      basePartitionPath[PATH_MAX];
        std::vector<ro_segment *> prevSegments;
        topic_partition_log      *log;
};

using nodeid_t = uint16_t;
struct cluster_node;

namespace NodesPartitionsUpdates {
        // topic number of replicas were reduded to new_size
        struct reduced_rs final {
                topic_partition *p;
                uint16_t         new_size;
        };

        // topic number of replicas increased; new replicas appended to
        // p->cluster.replicas.nodes
        // and size of p->rcluster.replicas.nodes is tracked on original_size
        // (for convenience we will append and then later we resize back to original_size)
        struct expanded_rs final {
                topic_partition *p;
                uint16_t         original_size;
        };

        // partition leader has changed to n(can be nullptr)
        struct leadership_promotion final {
                topic_partition *p;
                cluster_node    *n;
        };

} // namespace NodesPartitionsUpdates

struct connection;

// In order to support minBytes semantics, we will
// need to track produced data for each tracked topic partition, so that
// we will be able to flush once we can satisfy the minBytes semantics
struct wait_ctx_partition final {
        std::shared_ptr<fd_handle> fdh;
        uint64_t                   seqNum;
        uint64_t                   hwmark_threshold;
        range32_t                  range;
        topic_partition           *partition;
};

struct wait_ctx final {
        connection    *c;             // connection of consumer or peer replication partition content from this node
        TankAPIMsgType _msg;          // ConsumePeer if this is for a peer replication content from this node
        uint32_t       requestId;     // this is meaningfuil for Consume requests;
        switch_dlist   list;          // ll for c->as.tank.waitCtxList
        timer_node     exp_tree_node; // node for timer
        uint32_t       minBytes;

        // A request may involve multiple partitions
        // minBytes applies to the sum of all captured content for all specified partitions
        uint32_t capturedSize;

        // we don't really need this anymore
        wait_ctx_partition *find(const topic_partition *tp) const noexcept {
                for (size_t i{0}; i < total_partitions; ++i) {
                        auto &it = partitions[i];

                        if (it.partition == tp) {
                                return const_cast<wait_ctx_partition *>(&it);
                        }
                }

                return nullptr;
        }

        uint16_t           total_partitions;
        wait_ctx_partition partitions[0];
};

// tracks an ISR(In-Sync-Replica) (node, partition)
struct isr_entry final {
        // XXX: if this becomes expensive because
        // we are tracking pointers, we can use a handle:u32 instead
        // which just points to a global vector that holds all distinct partitions and nodes
        cluster_node    *node_;
        topic_partition *partition_;

#ifdef HWM_UPDATE_BASED_ON_ACKS
        // see partition::cluster::pending_client_produce_acks_tracker::pending_ack
        // this is a very important optimization
        //
        // see topic_partition::cluster::pending_client_produce_acks_tracker
        uint8_t partition_isr_node_id;
#endif

        auto node() {
                return node_;
        }

        auto partition() {
                return partition_;
        }

        switch_dlist node_ll;      // cluster_node::isr::list
        switch_dlist partition_ll; // partition::Cluster::isr::list

        // for tracking ack.expiration
        switch_dlist pending_next_ack_ll;
        uint64_t     pending_next_ack_timeout;

#ifndef HWM_UPDATE_BASED_ON_ACKS
        uint64_t last_msg_lsn;
#endif

        void reset() {
                node_ll.reset();
                partition_ll.reset();
                pending_next_ack_ll.reset();

#ifdef HWM_UPDATE_BASED_ON_ACKS
                partition_isr_node_id = 0;
#endif
                node_      = nullptr;
                partition_ = nullptr;

#ifndef HWM_UPDATE_BASED_ON_ACKS
                last_msg_lsn = 0;
#endif
        }
};

struct connection_handle final {
        connection *c_{nullptr};
        uint64_t    c_gen;

        void reset() {
                c_gen = std::numeric_limits<uint64_t>::max();
                c_    = nullptr;
        }

        inline void set(connection *const c) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

        inline connection *get() noexcept;

        inline const connection *get() const noexcept;
};

// For each client PRODUCE request, we collect
// all participants; the partitions involved
// in the request.
struct topic;
struct produce_response final {
        struct {
                connection_handle ch;
                uint32_t          req_id;
                switch_dlist      connection_ll; // c->as.tank.deferred_produce_responses_list.push_back(&dpr->connection_ll);

                void reset() {
                        ch.reset();
                        req_id = 0;
                        connection_ll.reset();
                }
        } client_ctx;

        // a partition participating in the produce op.
        struct participant final {
                str_view8        topic_name; // needed because topic can be == nullptr
                topic           *topic;
                topic_partition *p;
                uint16_t         partition;

                struct {
                        range_base<const uint8_t *, size_t> bundle;
                        uint64_t                            first_msg_seq_num;
                } update;

                enum class OpRes : uint8_t {
                        OK = 0,
                        ReadOnly,
                        InvalidSeqNums,
                        UnknownTopic,
                        UnknownPartition,
                        NoLeader,
                        OtherLeader,
                        Pending,
                        IO_Fault,
                        InsufficientReplicas,
                } res;
        };

#if 1
        uint64_t gen;
#else
        // instead of a gen, we can just use an rc
        // so 1 for when  associated with a node, and 1 for when in the std::deque of pending produce reqs to ack
        uint8_t rc;
#endif

        std::vector<participant> participants;
        std::vector<char *>      unknown_topic_names;

        // a produce resposne may be DEFERRED
        struct {
                uint8_t pending_partitions;

                struct {
                        // see deferred_produce_responses_expiration_list
                        switch_dlist ll;
                        uint64_t     ts;
                } expiration;

                void reset() {
                        expiration.ll.reset();
                        expiration.ts      = 0;
                        pending_partitions = 0;
                }
        } deferred;

        void reset() {
                participants.clear();
                deferred.reset();

                for (auto ptr : unknown_topic_names) {
                        std::free(ptr);
                }

                unknown_topic_names.clear();
                client_ctx.reset();
        }
};

struct repl_stream final {
        topic_partition  *partition;       // fetching content for this partion
        cluster_node     *src;             // from this peer
        size_t            min_fetch_size;  // next request must be for at least as much data
        switch_dlist      repl_streams_ll; // links to cluster_state.replication_streams
        connection_handle ch;

        void reset() {
                partition      = nullptr;
                src            = nullptr;
                min_fetch_size = 512;
                repl_streams_ll.reset();
                ch.reset();
        }
};

struct cluster_node final {
        const nodeid_t   id;
        bool             available_{true}; // lock (session) set for the node ns key
        Switch::endpoint ep{};             // if we have no endpoint, we consider it N/A
        // TODO: if we fail to connect, block it
        // for a while and enable it again later(when you do, try_replicate_from(node))
        bool blocked{false};

        cluster_node(const nodeid_t _id)
            : id{_id} {
        }

        bool likely_reachable() const noexcept {
                return available_ and not blocked;
        }

        bool available() const noexcept {
                return available_ and ep;
        }

        struct Leadership final {
                // track partitions this node is a leader for
                size_t       partitions_cnt{0};
                switch_dlist list{&list, &list};
                bool         dirty{false};

                // whenever needed, we will rebuild this list
                // it will include all partitions <local node> node is a leader of, that we are interested in (i.e we
                // are replica of)
                // i.e all partitions this node is a leader of that are also replicated to the <local node>
                std::unique_ptr<std::vector<topic_partition *>> local_replication_list;
        } leadership;

        // this node may also be a replica of 0+ partitions
        // this rarely changes so we should use an array
        std::vector<topic_partition *> replica_for;

        bool is_replica_for(const topic_partition *) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

        struct {
                // A node may also be in the ISR of a partition
                // this should change fairly often, so we need to
                // use a list instead.
                // see isr_entry
                ///
                // A problem with this idea is that we may have 50k partitions
                // and maybe just 2 nodes. We can't reallistically expect to scan a linked list, so we
                // need a fast hash map instead.
                // Thankfully we never have to use linear search to find anything in this lit
                switch_dlist list{&list, &list};
                uint16_t     cnt{0}; // how many ISR lists is this node in?

                auto size() const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                        TANK_EXPECT(cnt == list.size());

                        return cnt;
                }
        } isr;

        struct {
                // a connection from this node to that peer
                // for CONSUMING from it
                connection_handle ch;

                // total replication streams  in cluster_state.local_node.replication_streams
                // that depend on this connection
                size_t repl_streams_cnt{0};
        } consume_conn;

        struct {
                switch_dlist ll{&ll, &ll};
                uint64_t     when{0};
        } consume_retry_ctx;
};

struct node_partition_rel_update final {
        cluster_node    *n;
        topic_partition *p;
        bool             assign; // if false, it's no longer assigned
};

struct cluster_nodeid_update final {
        nodeid_t         id;
        Switch::endpoint ep;
        bool             have_owner;
};

struct topic;
struct topic_partition final {
        enum class Flags : uint8_t {
                // this is set when reset_partition_log()
                // runs, so that subsequent calls to reset_partition_log() won't need to iterate the dir. contents
                // it is cleared when we open() a file in the partition dir.
                NoDataFiles = 1u << 0,

                // Failed to persist messages
                // likely ran out of disk space or disk is busted
                IOFailed = 1u << 1,
        };
        uint16_t         idx; // (0, ...)
        uint32_t         distinctId;
        topic           *owner{nullptr};
        partition_config config;
        uint8_t          flags{0};

        struct {
                time_t       last_access{0};
                switch_dlist ll{&ll, &ll};
        } access;

        uint64_t hwmark() const noexcept;

        // this is only meaningful if cluster_aware()
        struct HW_mark final {
                // XXX:
                // this is very tricky
                // if (cluster_aware()), then we need to track BOTH the last 'committed' sequence number
                // and we also need to offset of the next message right after that last committed sequence number
                //
                // Example:
                // Assuming cluster of two nodes, with 2 replicas in the ISR of partition, and cluster hasn't
                // gotten any events published to the partition for a while, so we
                // assume the are no current publishers. Last published message seq.num is X, and
                // offet of the *next* message past X is at O.
                //
                // 1. Client A produces 1 new message M1 to partition
                // 2. Node N1 accepts the produce req with first message.seq num to be X + 1 AT O and advances O to O1, and waits
                // 	for N2 to consume from >= (X+1)  to ack it
                // 3. Client A produces 1 new message M2 to partition
                // 4. Node N1 still haven't received a consume req. from N2. It accepts the
                // 	produce reque with first message seq.num to be X + 1 + 1(see above), and advances O to O2,  and
                //	waits for N2 to consume from (X + 1 + 1) to ack it
                // 5. Client B wants to consume from EOF
                // 6. Node N1 should be able to do the right thing here.
                //	What's the EOF and where does it start?
                // 	- if M1 is acknowledged, HW mark should be M1, and N1 should stream from O1 <--
                //	- if M2 is acknowledged, HW mark should be M2, and N2 should stream from O1 <--
                //
                // Why should that happen? maybe we need to track for each consume request pending wake-up the
                // (file handle, file offset) of the last highwater mark at the time when the consume request was queued to be woken up?
                // 	-- It is important that the client can ignore messages with seq.num < requested sequest number and that it won't go past the highwater mark --
                // So we need to always keep track of (file, offset)

                // sequence number of the *last* confirmed message
                uint64_t seq_num;

                // handle of the file that should include the *next* message after
                // the last confirmed(i.e highwater mark) message, and file size
                // at the time it was assigned to file.handle
                //
                // this is very important in cluster_aware() configurations.
                // When we get a CONSUME request from EOF, we shouldn't wait for (lastAssignedSeqNum + 1)
                // we need to instead wait for (highwater_mark + 1)
                // and we need to read starting from (highwater_mark.handle, highwater_mark.size), as opposed to
                // start reading from the current file
                struct {
                        std::shared_ptr<fd_handle> handle;
                        uint32_t                   size;
                } file;
        } highwater_mark;

        struct Cluster final {
                enum class Flags : uint8_t {
                        ISRDirty        = 1u << 0,
                        GC_ISR          = 1u << 1,
                        GeneratedUpdate = 1u << 7,
                };

                uint8_t flags{0};

                bool isr_dirty() const noexcept {
                        return flags & unsigned(Flags::ISRDirty);
                }

                struct ISR final {
                        // potentially frequently updated
                        // see isr_entry.
                        // Unlike with node's ISR tracker, this list here will be very short
                        switch_dlist list{&list, &list};
                        uint64_t     gen{0};
                        uint8_t      cnt{0}; // size of list
                        bool         dirty{false};

#ifndef HWM_UPDATE_BASED_ON_ACKS
                        struct Tracker final {
                                struct {
                                        nodeid_t nid;
                                        // we will track the LSN of the last message reported as persisted by the peer
                                        uint64_t lsn;
                                } data[16]; // XXX:
                                uint8_t size{0};

                                // returns true if at least K many nodes have reported last message lsn being >= seqnum
                                //
                                // so if a client published a message set where last message id is X
                                // and we want to ack the produce request as soon as 2+ different *peers* issue ConsumePeer for LSN > X
                                // then we will use confirmed(2, X)
                                bool confirmed(const uint8_t k, const uint64_t seqnum) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;
                        } tracker;
#endif

                        struct {
                                uint64_t seq_num{0};
                                uint64_t nodes_mask{0};
                        } max_ack;

                        auto size() const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(cnt == list.size());

                                return cnt;
                        }

                        auto empty() const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(cnt == list.size());

                                return !cnt;
                        }

                        bool count(const cluster_node *n) const noexcept {
                                for (auto it : list) {
                                        if (switch_list_entry(isr_entry, partition_ll, it)->node() == n) {
                                                return true;
                                        }
                                }
                                return false;
                        }

#ifdef HWM_UPDATE_BASED_ON_ACKS
                        uint8_t next_partition_isr_node_id() const noexcept {
                                if (list.empty()) {
                                        return 0;
                                }

                                uint8_t max{0};

                                for (auto it : list) {
                                        max = std::max(max, switch_list_entry(isr_entry, partition_ll, it)->partition_isr_node_id);
                                }

                                return max + 1;
                        }
#endif

                        // a partition shouldn't have more than 3-4 nodes in its ISR
                        // so this should be fast
                        auto find(const cluster_node *node) const noexcept -> isr_entry * {
                                for (auto it : list) {
                                        const auto isr_e = switch_list_entry(isr_entry, partition_ll, it);

                                        if (isr_e->node() == node) {
                                                return isr_e;
                                        }
                                }
                                return nullptr;
                        }
                } isr;

                struct {
                        cluster_node *node{nullptr};
                        // see cluster_node::leadership
                        switch_dlist leadership_ll{&leadership_ll, &leadership_ll};
                } leader;

                struct {
                        // TODO: we should probably
                        // use a SBO design instead
                        std::vector<cluster_node *> nodes;

                        // this is going to be fast because replicas are always ordered
                        cluster_node *by_id(const nodeid_t id, const size_t span) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(std::is_sorted(nodes.begin(), nodes.begin() + span, [](const auto a, const auto b) noexcept { return a->id < b->id; }));

                                for (int32_t top = static_cast<int32_t>(span) - 1, btm = 0; btm <= top;) {
                                        const auto mid    = (btm + top) / 2;
                                        const auto mn     = nodes[mid];
                                        const auto at_mid = mn->id;

                                        if (id < at_mid) {
                                                top = mid - 1;
                                        } else if (id > at_mid) {
                                                btm = mid + 1;
                                        } else {
                                                return mn;
                                        }
                                }

                                return nullptr;
                        }

                        cluster_node *by_id(const nodeid_t id) const {
                                return by_id(id, nodes.size());
                        }

                        bool count(const nodeid_t id) const {
                                // for now we will rely on by_id()
                                // which is not necessarily very efficient
                                return by_id(id);
                        }

                        bool count(const nodeid_t id, const size_t span) const {
                                return by_id(id, span);
                        }

                        void sort_nodes() noexcept {
                                std::sort(nodes.begin(), nodes.end(), [](const auto a, const auto b) noexcept {
                                        return a->id < b->id;
                                });
                        }

                        uint64_t last_update_gen{0};
                } replicas;

                struct pending_ack_bundle_desc final {
                        // seqnuence number of the last message in the bundle
                        uint64_t last_msg_seqnum;

                        // when we initialise this, we also
                        // track partition_log()->cur.fdh.get() and partition_log()->cur.fileSize
                        // because otherwise we don't know where to start reading when we get requests for EOF
                        // we may not need that, but it's cheap to track it anyway
                        struct {
                                fd_handle *handle;
                                uint32_t   size;
                        } next;
                };

#ifdef HWM_UPDATE_BASED_ON_ACKS
                struct pending_ack_bundle_desc_cmp final {
                        inline bool operator()(const pending_ack_bundle_desc &a, const pending_ack_bundle_desc &b) const noexcept {
                                return b.last_msg_seqnum < a.last_msg_seqnum;
                        }
                };
#endif

                struct PendingClientProduceAcks final {
                        // this represents a produce request pending ack.
                        struct pending_ack final {
                                pending_ack_bundle_desc bundle_desc;

#ifdef HWM_UPDATE_BASED_ON_ACKS
                                // we can't just use a acknowledged counter
                                // because the same peer (in the ISR) may issue
                                // a CONSUMEPEER for the same sequence number more than once, so we can't rely on
                                // acks. to be distinct. (in practice, we don't expect this to happen
                                // but we need to guard against that)
                                //
                                // Instead, for each node in a partition's ISR, we assign it
                                // a partition-local ISR index [0, sizeof(isr_nodes_acknowledged_bm) - 1)
                                // and we use isr_nodes_acknowledged_bm here for tracking
                                // distinct nodes that acknowleded this, but just checking
                                // for (isr_nodes_acknowledged_bm & (1 << isr_e->partition_isr_node_id)
                                // the size of an ISR is bound by (sizeof(isr_nodes_acknowledged_bm) * 8)
                                //
                                // XXX: can we really have 64 different peers in an ISR?
                                // maybe u8 for upto 8 replicas should do
                                uint64_t isr_nodes_acknowledged_bm;

                                // how many _peers_ (i.e _excluding_ local) ISR nodes have acknowledged this?
                                inline auto ack_count() const noexcept {
                                        return isr_nodes_acknowledged_bm ? SwitchBitOps::PopCnt(isr_nodes_acknowledged_bm) : 0;
                                }
#endif

                                struct {
                                        produce_response *deferred_resp;
                                        uint64_t          deferred_resp_gen; // cookie
                                };

                                // index in deferred_resp->participants[]
                                uint8_t pr_participant_idx;

                                // how many acks we need in total from *PEERS* (i.e
                                // this does not include the ack we will implicitly get from the coordinator)
                                //
                                // this is specific to each produce request
                                // Again, this is how many acks we need in total from _PEERS_
                                uint8_t required_acks;
                        };

                        std::deque<pending_ack> pending;

#ifdef HWM_UPDATE_BASED_ON_ACKS
                        std::priority_queue<pending_ack_bundle_desc,
                                            std::vector<pending_ack_bundle_desc>,
                                            pending_ack_bundle_desc_cmp>
                            acknowledged;
#endif
                } pending_client_produce_acks_tracker;

                // set if we are replicating from this partition
                repl_stream *rs{nullptr};

                uint64_t consume_next_lsn{std::numeric_limits<uint64_t>::max()};
        } cluster;

        // for foreach_msg()
        struct msg final {
                uint64_t   seqNum;
                uint64_t   ts;
                strwlen8_t key;
                str_view32 data;
        };

        inline bool defined() const noexcept;

        inline bool enabled() const noexcept;

        topic_partition(topic *t)
            : owner{t} {
                TANK_EXPECT(owner);
        }

        // log_ should be initialized iff this node is a replica of this partition
        // it encapsulates the state of this node's partition dataset
        std::unique_ptr<topic_partition_log> _log;
        bool                                 open_ok{false};

        // TODO: make waitingList a singly-linked list
        // so that we won't need to reallocate any memory here
        // just include a next field in wait_ctx
        //
        //
        // pair of (wait_ctx, index), where the `index` is the index for this partition in wait_ctx::partitions[],
        // so that we don't need to look it up using linear search later
        std::vector<std::pair<wait_ctx *, uint8_t>> waiting_list;

        void erase_from_waiting_list(const size_t index) {
                // faster than waiting_list.erase(waiting_list.begin() + index)
                // because we don't need to preserve order
                waiting_list[index] = std::move(waiting_list.back());
                waiting_list.pop_back();
        }

        append_res append_bundle_to_leader(const time_t, const uint8_t *const bundle, const size_t bundleLen, const uint32_t bundleMsgsCnt, std::vector<wait_ctx *> &waitCtxWorkL, const uint64_t, const uint64_t);

        lookup_res read_from_local(const bool, const uint64_t absSeqNum, const uint32_t fetchSize);

        // This is mostly useful for scanning internal topics, e.g on startup
        // where you want to, for example, use it to checkpoint offsets and whatnot
        // https://github.com/phaistos-networks/TANK/issues/40
        bool foreach_msg(std::function<bool(msg &)> &) const;

        uint16_t required_replicas() const noexcept;

        bool require_leader() const noexcept;

        bool safe_to_reset() const noexcept;

        bool log_open() const noexcept {
                return _log.get();
        }
};

struct topic final {
        const strwlen8_t                               name_;
        std::vector<std::shared_ptr<topic_partition>> *partitions_{nullptr};
        // We may have multiple registered partitions, but only some of the first ones may be enabled
        uint16_t         total_enabled_partitions{0};
        partition_config partitionConf;
        // a topic may have been created and registered with this service
        // but an update in topology excluded this topic
        // We do not erase topics, but we set enabled to false
        bool enabled{true};

        enum class Flags : uint8_t {
                under_construction = 1u << 0,
        };

        uint8_t flags{0};

        // for Prometheus metrics
        struct metrics_struct final {
                struct latency_struct final {
                        // inline is handy here
                        static inline constexpr uint32_t histogram_scale[]{1, 2, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 65, 75, 85, 95, 100, 110, 120, 130, 140, 150, 200, 250, 300, 350, 400, 450, 500};
                        uint64_t                         hist_buckets[sizeof_array(histogram_scale)] = {0};
                        uint64_t                         sum{0}, cnt{0};

                        static inline int32_t bucket_index(const uint32_t delta) noexcept {
                                int32_t top = sizeof_array(histogram_scale) - 1, btm{0};

                                while (btm <= top) {
                                        const auto mid = (btm + top) / 2;
                                        const auto v   = histogram_scale[mid];

                                        if (v == delta) {
                                                btm = mid;
                                                break;
                                        } else if (delta < v)
                                                top = mid - 1;
                                        else
                                                btm = mid + 1;
                                }

                                // -1 if out of bounds
                                return btm;
                        }

                        void reg_sample(const uint32_t delta) {
                                ++cnt;
                                sum += delta;

                                // currently, histogram_scale[], contains, with the exception of the first 2 values, values that increment by 5 so
                                // we can compute the index with a simple idiv, but later we may want to use arbitrary scales, and this binary search
                                // is fast anyway, so we are sticking to it for now
                                if (const auto idx = bucket_index(delta); idx != -1)
                                        ++hist_buckets[idx];
                        }
                } latency;

                uint64_t bytes_in{0};
                uint64_t msgs_in{0};
                uint64_t bytes_out{0};
                // TODO: count current distinct consumers and producers
                // i.e distinct connections that have consumed or produced at least one from/to this topic
        } metrics;

        struct {
                uint64_t last_update_gen{0};
                // by default, we are not replicating anything nor are we accepting any requests
                // we require that RF is explicitly specified for the topic first
                //
                // rf_ cannot be 0. This is because we don't want anyone to accidently setting rf to 0 and thus
                // forcing all nodes to reset_partition_log()
                //
                // XXX: it is important to understand that this the number of times a message should be replicated in the cluster
                // 1 means only the leader should store it, 2 means a leader and another node, etc.
                //
                // that is to say, this is not the number of _additional_ peers that need to replicate from the leader
                uint8_t rf_{1};
        } cluster;

        topic(const strwlen8_t name, const partition_config c)
            : name_{name.Copy(), name.len}
            , partitionConf{c} {
                partitions_ = new std::vector<std::shared_ptr<topic_partition>>();
        }

        auto name() const {
                return name_;
        }

        ~topic() {
                if (auto *const p = const_cast<char *>(name_.p)) {
                        free(p);
                }

                delete partitions_;
        }

        uint8_t compute_required_peers_acks(const topic_partition *, const uint8_t) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS;

        void register_partition(std::shared_ptr<topic_partition> p) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                TANK_EXPECT(p->idx < std::numeric_limits<uint16_t>::max()); // UINT16_MAX is reserved

                p->owner = this;
                partitions_->emplace_back(p);

                std::sort(partitions_->begin(), partitions_->end(), [](const auto a, const auto b) noexcept {
                        return a->idx < b->idx;
                });

                TANK_EXPECT(partitions_->back()->idx == partitions_->size() - 1);
                total_enabled_partitions = partitions_->size();
        }

        void register_partitions(std::shared_ptr<topic_partition> *const all, const size_t n) {
                for (uint32_t i{0}; i != n; ++i) {
                        TANK_EXPECT(all[i]);
                        TANK_EXPECT(all[i]->idx < std::numeric_limits<uint16_t>::max()); // UINT16_MAX is reserved

                        all[i]->owner = this;
                        partitions_->emplace_back(all[i]);
                }

                std::sort(partitions_->begin(), partitions_->end(), [](const auto a, const auto b) noexcept {
                        return a->idx < b->idx;
                });

                TANK_EXPECT(partitions_->back()->idx == partitions_->size() - 1);
                total_enabled_partitions = partitions_->size();
        }

#if 0
        void register_partitions(std::shared_ptr<topic_partition> *const all, const size_t n) {
                for (uint32_t i{0}; i != n; ++i) {
                        TANK_EXPECT(all[i]);
                        TANK_EXPECT(all[i]->idx < std::numeric_limits<uint16_t>::max()); // UINT16_MAX is reserved

                        all[i]->owner = this;
                        partitions_->push_back(all[i].get());
                }

                std::sort(partitions_->begin(), partitions_->end(), [](const auto a, const auto b) noexcept {
                        return a->idx < b->idx;
                });

                TANK_EXPECT(partitions_->back()->idx == partitions_->size() - 1);
                total_enabled_partitions = partitions_->size();
        }
#endif

        const topic_partition *partition(const uint16_t idx) const {
                return idx < partitions_->size() ? partitions_->at(idx).get() : nullptr;
        }

        topic_partition *partition(const uint16_t idx) {
                return idx < partitions_->size() ? partitions_->at(idx).get() : nullptr;
        }

        const topic_partition *enabled_partition(const uint16_t idx) const {
                if (auto p = partition(idx); p and p->enabled()) {
                        return p;
                }

                return nullptr;
        }

        topic_partition *enabled_partition(const uint16_t idx) {
                if (auto p = partition(idx); p and p->enabled()) {
                        return p;
                }

                return nullptr;
        }

        void foreach_msg(std::function<bool(topic_partition::msg &)> &l) const {
                for (auto p : *partitions_) {
                        if (not p->foreach_msg(l)) {
                                return;
                        }
                }
        }

        uint8_t replication_factor() const noexcept;
};

bool topic_partition::defined() const noexcept {
        return idx < owner->total_enabled_partitions;
}

bool topic_partition::enabled() const noexcept {
        // a topic's RF cannot be 0, but we are supporting this just in case
        // in case that was ever true, the partition would have been considered disabled anwyay because
        // there is no replica supporting it
        return defined() && likely(owner->cluster.rf_);
}

struct content_file_range final {
        std::shared_ptr<fd_handle> fdhandle;
        range32_t                  range;

        content_file_range &operator=(const content_file_range &o) {
                fdhandle = o.fdhandle;
                range    = o.range;
                return *this;
        }

        content_file_range &operator=(content_file_range &&o) {
                fdhandle = std::move(o.fdhandle);
                range    = o.range;

                o.range.reset();
                return *this;
        }
};

struct payload {
        payload *next;

        enum class Source : uint8_t {
                DataVector = 0,
                FileContents,
        } src;

        void reset() {
                next = nullptr;
        }
};

struct file_contents_payload final
    : public payload {
        content_file_range file_range;
        struct
        {
                uint64_t since;
                topic   *src_topic;
        } tracker;

        void reset() {
                payload::reset();
                file_range.fdhandle.reset();
                tracker.src_topic = nullptr;
                tracker.since     = 0;
        }

        void init(std::shared_ptr<fd_handle> fdh, const range32_t r, const uint64_t start, topic *t) {
                TANK_EXPECT(fdh);

                tracker.since       = start;
                tracker.src_topic   = t;
                file_range.fdhandle = fdh;
                file_range.range    = r;
        }
};

struct data_vector_payload final
    : public payload {
        IOBuffer    *buf;
        char         small_buf[256];
        struct iovec iov[64];
        uint8_t      iov_cnt;

        static_assert(std::numeric_limits<decltype(iov_cnt)>::max() >= sizeof_array(iov));

        void reset() {
                payload::reset();
                iov_cnt = 0;
                buf     = nullptr;
        }

        inline bool can_fit(const uint32_t cnt) const noexcept {
                return cnt <= sizeof_array(iov);
        }

        inline auto size() const noexcept {
                return iov_cnt;
        }

        inline auto empty() const noexcept {
                return 0 == iov_cnt;
        }

        static size_t capacity() noexcept {
                return sizeof_array(iov);
        }

        void set_iov(const range32_t *const l, const uint32_t cnt) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                TANK_EXPECT(buf);
                TANK_EXPECT(cnt <= sizeof_array(iov));

                iov_cnt = cnt;
                for (uint32_t i{0}; i < cnt; ++i) {
                        auto ptr = iov + i;

                        ptr->iov_base = static_cast<void *>(buf->data() + l[i].offset);
                        ptr->iov_len  = static_cast<int>(l[i].len);
                }
        }

        void append(const str_view32 s) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                TANK_EXPECT(iov_cnt < sizeof_array(iov));

                iov[iov_cnt].iov_base  = reinterpret_cast<void *>(const_cast<char *>(s.data()));
                iov[iov_cnt++].iov_len = s.size();
        }
};

struct outgoing_queue final {
        payload *front_, *back_;
        // so that we wont' need to include an index in data_vector_payload
        decltype(data_vector_payload::iov_cnt) dv_iov_index;

        size_t size() const noexcept {
                size_t res{0};

                for (auto it = front_; it; it = it->next, ++res) {
                        //
                }
                return res;
        }

        void reset() noexcept {
                front_ = back_ = nullptr;
                dv_iov_index   = 0;
        }

        void verify() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
#ifdef TANK_RUNTIME_CHECKS
                EXPECT(!front_ || back_);
#endif
        }

        void pop_front() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                TANK_EXPECT(front_);

                verify();

                front_ = front_->next;
                if (!front_) {
                        back_ = nullptr;
                }

                verify();
        }

        void pop_front_expected(const payload *expected) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                TANK_EXPECT(front_);
                TANK_EXPECT(front_ == expected);

                verify();

                front_ = front_->next;
                if (!front_) {
                        back_ = nullptr;
                }

                verify();
        }

        void push_back(payload *p) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                verify();

                if (!back_) {
                        front_ = back_ = p;
                } else {
                        back_->next = p;
                }
                back_ = p;

                verify();
        }

        void insert_after(payload *const p, payload *const after) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                TANK_EXPECT(p);
                TANK_EXPECT(after);

                p->next     = after->next;
                after->next = p;
                if (p->next == nullptr) {
                        back_ = p;
                }

                verify();
        }

        bool empty() const noexcept {
                return nullptr == front_;
        }

        auto front() noexcept {
                return front_;
        }

        auto back() noexcept {
                return back_;
        }
};

struct consul_request final {
        // for chaining in consul_state.pending_reqs
        consul_request *next;

        // A request may depend on another request
        // that is, it cannot be scheduled before a request has been processed
        // to support that, we are going to just append requests using next
        // so that once a request is processed, we check its next to see if it has a dependancy, and if so, we schedule that a swell
        // we track all pending consul requests
        consul_request *then;

        // we may want to reschedule a request sometime later
        // see process_drained_consul_resp_impl()
        timer_node tn;

        enum class Flags : uint8_t {
                // Request has been assigned to a connection
                Assigned    = 1u << 0,
                Urgent      = 1u << 1,
                Deferred    = 1u << 2,
                Released    = 1u << 3,
                OverHandled = 1u << 4,
        };

        enum class Type : uint8_t {
                InitNS_Topology = 1,
                InitNS_Nodes,
                InitNS_Leaders,
                InitNS_ISR,
                // Whenever a topic's configuration has been updated (Replication factor, max.seg size etc)
                // we are not going to update anything under TANK/<cluster>/topology/<topic>
                // because we monitor topology, so we 'll wind up getting the confgiuration for all topics if it is updated for just one of them.
                // We will instead "touch" a key (no value, bumped version) in TANK/<cluster/conf-updates/<topic> so that
                // we 'll determine that the version of that key now differs from what we track locally, and then
                // pull  TANK/<cluster>/configs/<topic> and apply it.
                //
                // We can use multi-key transactions to atomically update configs/<topic> and conf_updates</topic>
                InitNS_ConfUpdates,
                RetrieveTopicConfig,
                // There is a single configuration file (see also, RetrieveTopicConfig)
                // that specifies the cluster's configuration
                // Initially, we 'll fetch the configuration for all topics _AND_ the cluster configuration
                RetrieveClusterConfig,
                RetrieveConfigs,
                TryBecomeClusterLeader = 9,
                MonitorTopology,
                MonitorLeaders,
                MonitorISRs,
                MonitorConfUpdates,
                MonitorConfUpdatesNoApply,
                MonitorNodes,
                AcquireNodeID,
                CreateSession,
                // We deliberately choose a very short TTL for the created sessions, so that we will renew them regularly
                // failing to renew them in due time, will release the acquired lock on whichever keys(i.e partitions or CLUSTER)
                // so that other cluster nodes react in time.
                // Also, renewals are a HB of sort -- by communicating with the consul endpoint very often, we can detect
                // when it becomes NA and thus resign leadership
                RenewSession,
                DestroySession,
                ReleaseClusterLeadership, // 20
                ReleaseNodeLock,
                // Namespaces Updates
                PartitionsTX,
                CreatePartititons,
        } type;

        union {
                topic           *tref;
                bool             no_apply;
                topic_partition *part;
                IOBuffer        *tx_repr;

                struct {
                        topic_partition *data[64];
                        uint8_t          size;
                } partitions;

                struct {
                        struct {
                                char    data[64];
                                uint8_t size;
                        } topic_name;

                        uint8_t           first_partition_index;
                        uint8_t           partitions_cnt;
                        connection_handle client_ch;
                        uint32_t          client_req_id;
                } new_partitions;
        };

        uint8_t flags;

        bool is_set() const noexcept {
                return flags & unsigned(Flags::Assigned);
        }

        bool is_released() const noexcept {
                return flags & unsigned(Flags::Released);
        }

        bool will_long_poll() const noexcept {
                switch (type) {
                        case Type::MonitorTopology:
                        case Type::MonitorNodes:
                        case Type::MonitorLeaders:
                        case Type::MonitorISRs:
                        case Type::MonitorConfUpdates:
                        case Type::MonitorConfUpdatesNoApply:
                                return true;

                        default:
                                return false;
                }
        }

        void reset() {
                next                = nullptr;
                then                = nullptr;
                flags               = 0;
                tn.node.node.leaf_p = nullptr;
                tref                = nullptr;
        }
};

struct CompressionContext final {
        enum class State : uint8_t {
                Headers = 0,
                Content,
                Footer,
                Fin,
        } state;

        z_stream       stream;
        CRC32Generator crc32_gen;
        uint32_t       processed_bytes;
        IOBuffer      *b;

        void reset() {
                b     = nullptr;
                state = State::Headers;
        }
};

struct connection final {
        int fd;
#ifdef TANK_RUNTIME_CHECKS
        uintptr_t sentinel;
#endif
        IOBuffer *inB{nullptr};
        uint32_t  addr4;
        uint64_t  gen;

        // May have an attached outgoing queue
        outgoing_queue *outQ{nullptr};

        // all active wait_ctx's
        switch_dlist connectionsList;

        // A connection is classified as Unclassified, Idle, or Active
        // and depending on the classifiication, it may be attached to a list
        struct ClassificationTracker final {
                uint64_t     since; // when it was classified
                switch_dlist ll;    // optionally link to a classification speicific list

                enum class Type : uint8_t {
                        NotClassified = 0,
                        Idle,
                        Active,
                        LongPolling
                };
        } classification;

        enum class Type : uint8_t {
                TankClient = 0,
                Prometheus,
                ConsulClient,
                Consumer,
                UNKNOWN,
        } type;

        struct State final {
                enum class Flags : uint8_t {
                        NeedOutAvail = 0,
                        ShutdownReasonTimeout,
                        DrainingForShutdown,
                        // 3 MSBs are reserved for the activity tracker list
                };

                auto classification() const noexcept {
                        return static_cast<ClassificationTracker::Type>(flags >> 5);
                }

                void set_classsification(const ClassificationTracker::Type l) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                        TANK_EXPECT(unsigned(l) < 7);

                        flags = (flags & 0b11111) | (unsigned(l) << 5);
                }

                auto flags_sans_class() const noexcept {
                        return flags & 0b11111;
                }

                uint8_t flags;
        } state;

        union As final {
                struct Consul final {
                        // this is useful for tracking active and idle consul connections
                        // with consul_state
                        switch_dlist conns_ll;

                        enum class State : uint8_t {
                                Idle,
                                Connecting,
                                TxReq,
                                WaitRespFirstBytes,
                                ReadHeaders,
                                ReadContent,
                                ReadCompressedContent,
                        } state;

                        enum class Flags : uint8_t {
                                WaitShutdown = 1 << 0,
                        };

                        uint8_t    flags;
                        timer_node attached_timer;

                        struct Cur final {
                                struct Response final {
                                        uint64_t           consul_index;
                                        size_t             content_length;
                                        uint8_t            flags;
                                        CompressionContext comp_ctx;

                                        enum class Flags : uint8_t {
                                                KeptAlive      = 1u << 0,
                                                GZIP_Encoding  = 1u << 1,
                                                ChunksTransfer = 1u << 2,
                                                Draining       = 1u << 3,
                                        };

                                        union {
                                                size_t remaining_content_bytes;
                                                // if we we are processing a chunk-encoded response
                                                size_t decoded_content_bytes;
                                        };

                                        uint16_t rc;

                                        void reset() {
                                                content_length          = 0;
                                                remaining_content_bytes = 0;
                                                rc                      = 0;
                                                flags                   = 0;
                                                consul_index            = 0;
                                                comp_ctx.reset();
                                        }

                                        inline bool chunked_transfer_enc() const noexcept {
                                                return flags & unsigned(Flags::ChunksTransfer);
                                        }
                                } resp;

                                consul_request *req;
                        } cur;

                        void reset() {
                                conns_ll.reset();
                                cur.resp.content_length = 0;
                                state                   = State::Idle;
                                cur.req                 = nullptr;
                                flags                   = 0;
                        }

                } consul;

                struct Prometheus final {
                        void reset() {
                        }

                } prometheus;

                struct Tank final {
                        enum class Flags : uint8_t {
                                PendingIntro        = 1u << 0,
                                ConsideredReqHeader = 1u << 1,
                        };

                        uint8_t      flags;
                        switch_dlist waitCtxList;
                        switch_dlist produce_responses_list;

                        // see consider_pending_client_produce_responses()
                        // for each product request from this client
                        struct {
                                // so that once the connection is closed, we can get rid of
                                // whatever we use to track those pending produce acks
                        } pending_produce_reqs_acks;

                        void reset() {
                                flags = 0;
                                waitCtxList.reset();
                                produce_responses_list.reset();
                        }
                } tank;

                // consumes from a peer
                struct Consumer final {
                        enum class State : uint8_t {
                                Idle = 0,
                                Busy,
                                Connecting,
                                ScheduledShutdown,
                        } state;

                        cluster_node *node;
                        timer_node    attached_timer;

                        void reset() {
                                state = State::Idle;
                                node  = nullptr;
                                attached_timer.reset();
                        }
                } consumer;

                As() {
                }
        } as;

        inline bool is_consul() const noexcept {
                return type == Type::ConsulClient;
        }

        inline bool is_prometheus() const noexcept {
                return type == Type::Prometheus;
        }

        inline bool is_tank() const noexcept {
                return type == Type::TankClient;
        }

        void set_close_on_flush() {
                state.flags |= (1 << unsigned(State::Flags::DrainingForShutdown));
        }

        bool close_on_flush() const noexcept {
                return state.flags & (1 << unsigned(State::Flags::DrainingForShutdown));
        }

        void verify() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
#ifdef TANK_RUNTIME_CHECKS
                TANK_EXPECT(sentinel == reinterpret_cast<uintptr_t>(this));
                TANK_EXPECT(gen);
                TANK_EXPECT(gen != std::numeric_limits<uint64_t>::max());
                TANK_EXPECT(type != Type::UNKNOWN);

                if (connectionsList.prev || connectionsList.next) {
                        TANK_EXPECT(connectionsList.prev);
                        TANK_EXPECT(connectionsList.next);
                } else {
                        TANK_EXPECT(!connectionsList.next);
                        TANK_EXPECT(!connectionsList.prev);
                }
#endif
        }
};

void connection_handle::set(connection *const c) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        TANK_EXPECT(c);

        c_    = c;
        c_gen = c->gen;
}

connection *connection_handle::get() noexcept {
        return c_ && c_->gen == c_gen ? c_ : nullptr;
}

const connection *connection_handle::get() const noexcept {
        return c_ && c_->gen == c_gen ? c_ : nullptr;
}

template <typename T>
struct PubSubQueue final {
        alignas(64 /* cache line size */) std::atomic<T *> list{nullptr};

        void push_back(T *const v) {
                T *old;

                do {
                        old     = list.load(std::memory_order_relaxed);
                        v->next = old;
                } while (!list.compare_exchange_weak(old, v, std::memory_order_release, std::memory_order_relaxed));
        }

        bool any() const {
                return list.load(std::memory_order_relaxed);
        }

        inline T *drain() {
                if (!list.load(std::memory_order_relaxed)) {
                        return nullptr;
                } else {
                        return list.exchange(nullptr, std::memory_order_acquire);
                }
        }
};

#define TANK_SRV_LAZY_PARTITION_INIT 1

class Service {
        friend struct ro_segment;
        friend struct topic_partition_log;

      protected:
        struct partition_leader_update final {
                str_view8 topic;
                uint16_t  partition;
                nodeid_t  leader_id;
                bool      locked;
        };

        typedef struct topology_partition final {
                str_view8                       topic;
                uint16_t                        partition;
                uint64_t                        gen;
                range_base<uint16_t *, uint8_t> replicas;
        } partition_isr_info;

        struct ConsulState final {
                char    _token[64], _session_id[64];
                uint8_t _token_len{0}, _session_id_len{0};

                // linked via connection::as::consul::conns_ll
                switch_dlist idle_conns{&idle_conns, &idle_conns};
                switch_dlist active_conns{&active_conns, &active_conns};

                uint64_t active_conns_next_process_ts{std::numeric_limits<uint64_t>::max()};

                struct {
                        consul_request *front_{nullptr};
                        consul_request *back_{nullptr};

                        void push_back(consul_request *r) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(r);
                                TANK_EXPECT(!r->is_released());

                                r->next = nullptr;
                                if (back_) {
                                        back_->next = r;
                                } else {
                                        front_ = r;
                                }

                                back_ = r;
                        }

                        bool empty() const noexcept {
                                return front_ == nullptr;
                        }

                        void pop_front() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(front_);
                                TANK_EXPECT(back_);

                                front_ = front_->next;
                                if (nullptr == front_) {
                                        back_ = nullptr;
                                }
                        }

                        auto front() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(!front_->is_released());
                                return front_;
                        }

                        auto back() TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                                TANK_EXPECT(!back_->is_released());
                                return back_;
                        }

                } pending_reqs;

                // the (current) consul service
                struct Srv final {
                        str_view32       hn{_S("localhost")};
                        Switch::endpoint endpoint{};
                        uint64_t         unreachable_since{0};
                        uint8_t          consequtive_faults{0};
                        timer_node       retry_reg_timer{
                                  .type             = timer_node::ContainerType::TryConsulClusterReg,
                                  .node.node.leaf_p = nullptr,
                        };

                        enum class State : uint8_t {
                                Unknown = 0,
                                Available,
                                Unreachable,
                                Unreliable,
                        } state{State::Unknown};
                } srv;

                enum class Flags : uint8_t {
                        // Set when our CreateSession request was successful
                        // and we acquired as ession ID
                        ConfirmedSession = 1 << 0,

                        // Acquired ID but have _not_ yet assigned our endpoint to the value of the /nodes/key
                        RegistrationComplete = 1 << 1,

                        // Set in consul_ns_retrieval_complete()
                        // that function is invoked when we have been retrieved (topology, leaders, configs, ISRs)
                        StateRetrieved = 1 << 2,

                        // Set in process_consul_configs() when all configurations have been retrieved
                        ConfigsRetrieved = 1 << 3,

                        // Set once we apply_cluster_state_updates() for all updates collected during bootstrap
                        BootstrapStateUpdatesProcessed = 1 << 4,

                        // Set if there is an active TryBecomeClusterLeader request
                        // XXX: Need to make sure we are doing this properly; if we are not unsetting this
                        // when we retrieve(prior to processing) the request to TryBecomeClusterLeader
                        // and when we consul_state.put_req() then
                        // we may get stuck where we will never be able to try_become_cluster_leader_timer() again
                        //
                        // TODO: we should probably do this for all other namespace retrieval requests
                        // because it makes little sense to e.g have more than 1 active /nodes fetch requests
                        AttemptBecomeClusterLeader = 1 << 5,

                        // Last time we attempted to register with the consul agent resulted in a fault
                        LastRegFailed = 1 << 6,
                };

                bool bootstrap_state_updates_processed() const noexcept {
                        return flags & unsigned(Flags::BootstrapStateUpdatesProcessed);
                }

                bool reg_completed() const noexcept {
                        return flags & unsigned(Flags::RegistrationComplete);
                }

                uint32_t                      flags{0};
                std::vector<consul_request *> reusable_reqs;
                simple_allocator              reqs_allocator{sizeof(consul_request) * 512};
                timer_node                    renew_timer{
                                       .type             = timer_node::ContainerType::ScheduleRenewConsulSess,
                                       .node.node.leaf_p = nullptr,
                };
                uint64_t                      topology_monitor_modify_index{0}, leaders_monitor_modify_index{0}, isrs_monitor_modify_index{0}, conf_updates_monitor_modify_index{0}, nodes_monitor_modify_index{0};
                consul_request               *deferred_reqs_head{nullptr};
                // used for namespace init. and for monitor registration
                uint8_t  ack_cnt{0};
                uint64_t reg_init_ts;
                int      _interrupt_efd{-1};

                str_view8 token() const noexcept {
                        return {_token, _token_len};
                }

                str_view8 session_id() const noexcept {
                        return {_session_id, _session_id_len};
                }

                auto get_req() {
                        consul_request *req;

                        if (!reusable_reqs.empty()) {
                                constexpr auto bm = unsigned(consul_request::Flags::Released);

                                req = reusable_reqs.back();
                                reusable_reqs.pop_back();

                                TANK_EXPECT(req->flags & bm);
                                req->flags &= ~bm;
                        } else {
                                req = static_cast<consul_request *>(reqs_allocator.Alloc(sizeof(consul_request)));
                        }

                        req->reset();
                        return req;
                }

                auto get_req(const consul_request::Type t) {
                        auto r = get_req();

                        TANK_EXPECT(r);

                        r->type = t;
                        return r;
                }

                auto get_req(const consul_request &o) {
                        auto r = get_req();

                        TANK_EXPECT(r);
                        r->type = o.type;

                        switch (o.type) {
                                case consul_request::Type::RetrieveTopicConfig:
                                        r->tref = o.tref;
                                        break;

                                case consul_request::Type::PartitionsTX:
                                        r->tx_repr = o.tx_repr;
                                        break;

                                default:
                                        break;
                        }

                        return r;
                }

                void put_req(consul_request *r, const size_t ref) {
                        constexpr auto bm = unsigned(consul_request::Flags::Released);

                        TANK_EXPECT(r->flags & unsigned(consul_request::Flags::OverHandled));

                        if (r->type == consul_request::Type::PartitionsTX) {
                                delete r->tx_repr;
                        }

                        TANK_EXPECT(r);
                        TANK_EXPECT(0 == (r->flags & bm));

                        r->flags |= bm;
                        r->flags &= ~unsigned(consul_request::Flags::OverHandled);
                        reusable_reqs.emplace_back(r);
                }
        } consul_state;

        struct ClusterState final {
                static constexpr size_t                                            K_max_replicas{8};
                nodeid_t                                                           leader_id{0};
                switch_dlist                                                       replication_streams{&replication_streams, &replication_streams};
                robin_hood::unordered_map<nodeid_t, std::unique_ptr<cluster_node>> nodes;
                std::vector<cluster_node *>                                        all_nodes, all_available_nodes;
                bool                                                               all_nodes_dirty{false};
                char                                                               _name[64];
                uint8_t                                                            _name_len{0};
                const str_view32                                                   tank_ns{_S("TANK")};

                inline str_view32 name() const noexcept {
                        return {_name, _name_len};
                }

                bool registered{false};

                bool leader_self() const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
                        TANK_EXPECT(local_node.id);

                        return local_node.id == leader_id;
                }

                struct {
                        nodeid_t      id{0};
                        cluster_node *ref{nullptr};
                } local_node;

                cluster_node *find_node(const nodeid_t id) {
                        if (const auto it = nodes.find(id); it != nodes.end()) {
                                return it->second.get();
                        } else {
                                return nullptr;
                        }
                }

                cluster_node *find_or_create_node(const nodeid_t id) {
                        const auto it = nodes.emplace(id, nullptr);

                        if (it.second) {
                                auto n = new cluster_node(id);

                                it.first->second.reset(n);
                                all_nodes.emplace_back(n);
                                all_nodes_dirty = true;
                        }
                        return it.first->second.get();
                }

                void begin_nodes_updates() {
                        //
                }

                void end_nodes_update() {
                        if (all_nodes_dirty) {
                                std::sort(all_nodes.begin(), all_nodes.end(), [](const auto a, const auto b) noexcept {
                                        return a->id < b->id;
                                });

                                all_nodes_dirty = false;
                        }
                }

                struct {
                        uint64_t last_update_gen{0};
                } local_conf;

                // enqueue updates here and update them once
                struct Updates final {
                        struct {
                                nodeid_t nid;
                                bool     defined{false};
                        } cluster_leader;

                        void set_cluster_leader(const nodeid_t v) {
                                cluster_leader.nid     = v;
                                cluster_leader.defined = true;
                        }

                        struct partition final {
                                struct {
                                        nodeid_t id;
                                        bool     defined{false};
                                } leader;

                                struct {
                                        std::vector<cluster_node *> nodes;
                                        bool                        updated{false};
                                } replicas;

                                std::unique_ptr<std::vector<nodeid_t>> isr_update;

                                void set_leader(const nodeid_t id) {
                                        leader.id      = id;
                                        leader.defined = true;
                                }

                                void set_replicas(cluster_node **nodes, const size_t n) {
                                        replicas.nodes.clear();
                                        replicas.nodes.insert(replicas.nodes.end(), nodes, nodes + n);

                                        TANK_EXPECT(std::is_sorted(replicas.nodes.begin(), replicas.nodes.end(), [](const auto a, const auto b) noexcept {
                                                return a->id < b->id;
                                        }));

                                        replicas.updated = true;
                                }
                        };

                        struct topic final {
                                struct {
                                        bool value;
                                        bool defined{false};
                                } state;

                                struct {
                                        uint16_t value;
                                        bool     defined{false};
                                } total_enabled;

                                struct {
                                        uint16_t value;
                                        bool     defined{false};
                                } rf;

                                void set_disabled() noexcept {
                                        state.value   = false;
                                        state.defined = true;
                                }

                                void set_enabled() noexcept {
                                        state.value   = true;
                                        state.defined = true;
                                }

                                void set_total_enabled_partitions(const size_t n) noexcept {
                                        total_enabled.value   = n;
                                        total_enabled.defined = true;
                                }

                                void set_rf(const uint16_t v) noexcept {
                                        rf.value   = v;
                                        rf.defined = true;
                                }
                        };

                        simple_allocator                                                             a;
                        robin_hood::unordered_map<cluster_node *, std::pair<Switch::endpoint, bool>> nodes;
                        robin_hood::unordered_map<topic_partition *, std::unique_ptr<partition>>     pm;
                        robin_hood::unordered_map<::topic *, std::unique_ptr<topic>>                 tm;

                        auto get_partition(topic_partition *tp) {
                                TANK_EXPECT(tp);
                                const auto res = pm.emplace(tp, nullptr);

                                if (res.second) {
                                        res.first->second.reset(new partition());
                                }

                                return res.first->second.get();
                        }

                        auto get_topic(::topic *t) {
                                TANK_EXPECT(t);
                                const auto res = tm.emplace(t, nullptr);

                                if (res.second) {
                                        res.first->second.reset(new topic());
                                }

                                return res.first->second.get();
                        }
                } updates;
        } cluster_state;

        struct {
                std::vector<wait_ctx *>                                         woken_up;
                std::vector<topic_partition::msg>                               partition_msgs;
                std::vector<IOBuffer *>                                         acquired_buffers;
                std::unordered_set<cluster_node *>                              nodes_set;
                std::vector<std::pair<str_view8, uint16_t>>                     v;
                std::vector<node_partition_rel_update>                          updates;
                std::vector<std::pair<topic *, range_base<uint16_t, uint16_t>>> pending_partitions;
                std::vector<std::shared_ptr<topic>>                             pending_reg_topics;
                std::unordered_set<topic *>                                     retained_topics;
                std::vector<cluster_node *>                                     nodes;
                std::vector<std::pair<str_view8, uint64_t>>                     conf_updates;
                simple_allocator                                                json_a;
                std::vector<partition_leader_update>                            new_leadership;
                std::vector<cluster_nodeid_update>                              nodes_updates;
                std::vector<uint16_t>                                           all_replicas;
                robin_hood::unordered_map<str_view8, bool>                      intern_map;
                std::vector<partition_isr_info>                                 isr_updates;
                std::vector<topology_partition>                                 new_topology;

                std::unordered_set<cluster_node *>                                         dirty_nodes;
                std::unordered_set<topic *>                                                dirty_topics;
                std::unordered_set<topic_partition *>                                      dirty_partitions;
                std::vector<std::pair<topic_partition *, bool>>                            part_bool_hashmap;
                std::vector<topic_partition *>                                             pv;
                std::vector<std::pair<cluster_node *, std::pair<topic_partition *, bool>>> nodes_replicas_updates;
                std::vector<std::pair<topic_partition *, cluster_node *>>                  stream_start, stream_stop;

                std::vector<NodesPartitionsUpdates::reduced_rs>                    reduced;
                std::vector<NodesPartitionsUpdates::expanded_rs>                   expanded;
                std::vector<NodesPartitionsUpdates::leadership_promotion>          promotions;
                std::unordered_set<cluster_node *>                                 peers_set;
                std::vector<std::pair<str_view8, std::pair<str_view32, uint64_t>>> json_conf_updates;
        } reusable;

      protected:
        static constexpr size_t idle_connection_ttl{16 * 1000};
        static constexpr size_t active_consul_connection_ttl{4 * 1000};
        enum class ReactorState : uint8_t {
                Idle = 0, // it's important this is (== 0)
                Active,
                ReleaseSess,
                WaitAllConnsIdle,
                TearDown,
        } reactor_state;
        Switch::endpoint tank_listen_ep{0}, prom_endpoint{};
        struct {
                PubSubQueue<pending_compaction> pendingCompactions;
                std::unique_ptr<std::thread>    compaction_thread;
                std::condition_variable         workCond;
                std::mutex                      workLock;
        } compactions;
        timer_node set_reactor_state_idle_timer{
            .type             = timer_node::ContainerType::ForceSetReactorStateIdle,
            .node.node.leaf_p = nullptr,
        };
        timer_node try_become_cluster_leader_timer{
            .type             = timer_node::ContainerType::TryBecomeClusterLeader,
            .node.node.leaf_p = nullptr,
        };
        std::vector<wait_ctx *>                                       now_awake;
        std::vector<topic_partition_log *>                            cleanup_tracker;
        Buffer                                                        base64_dec_buffer;
        int                                                           _interrupt_efd{-1};
        uint64_t                                                      next_deferred_produce_response_gen{0};
        switch_dlist                                                  deferred_produce_responses_expiration_list{&deferred_produce_responses_expiration_list, &deferred_produce_responses_expiration_list};
        uint64_t                                                      deferred_produce_responses_next_expiration{std::numeric_limits<uint64_t>::max()};
        std::vector<topic_partition *>                                isr_dirty_list;
        std::vector<topic_partition *>                                cluster_partitions_dirty;
        std::vector<isr_entry *>                                      reusable_isr_entries;
        simple_allocator                                              isr_entries_allocator{sizeof(isr_entry) * 128};
        std::vector<wait_ctx *>                                       waitCtxPool[TANK_Limits::max_topic_partitions];
        robin_hood::unordered_map<strwlen8_t, std::shared_ptr<topic>> topics;
        uint32_t                                                      total_open_partitions{0}, open_partitions_time{0};
        time32_t                                                      no_roll_until{0};
        size_t                                                        partitions_io_failed_cnt{0};
        time32_t                                                      startup_ts;
        std::vector<topic_partition *>                                partitions_v;
        std::mutex                                                    partitions_v_lock;
        std::vector<repl_stream *>                                    reusable_replication_streams;
        simple_allocator                                              repl_streams_allocator;
        std::vector<IOBuffer *>                                       bufs;
        std::vector<std::unique_ptr<produce_response>>                reusable_produce_responses;
        uint64_t                                                      next_produce_response_gen{0};
        std::vector<data_vector_payload *>                            reusable_data_vector_payloads;
        std::vector<file_contents_payload *>                          reusable_file_contents_payloads;
        std::vector<connection *>                                     reusable_conns, pending_reusable_conns;
        simple_allocator                                              payloads_allocator;
        std::vector<outgoing_queue *>                                 outgoingQueuesPool;
        simple_allocator                                              connections_allocators;
        switch_dlist                                                  active_partitions{&active_partitions, &active_partitions};
        switch_dlist                                                  allConnections, idle_connections{&idle_connections, &idle_connections};
        switch_dlist                                                  long_polling_conns{&long_polling_conns, &long_polling_conns};
        switch_dlist                                                  isr_pending_ack_list{&isr_pending_ack_list, &isr_pending_ack_list};
        uint64_t                                                      isr_pending_ack_list_next{std::numeric_limits<uint64_t>::max()};
        size_t                                                        idle_connections_count{0};
        uint32_t                                                      next_connection_generation{0};
        switch_dlist                                                  scheduled_consume_retries_list{&scheduled_consume_retries_list, &scheduled_consume_retries_list};
        uint64_t                                                      scheduled_consume_retries_list_next{std::numeric_limits<uint64_t>::max()};
        uint64_t                                                      next_cluster_state_apply{std::numeric_limits<uint64_t>::max()};
        uint64_t                                                      next_active_partitions_check{std::numeric_limits<uint64_t>::max()};

        uint64_t   next_idle_check_ts{std::numeric_limits<uint64_t>::max()};
        eb_root    timers_ebtree_root;
        uint64_t   timers_ebtree_next{std::numeric_limits<uint64_t>::max()};
        timer_node cleanup_tracker_timer{
            .type             = timer_node::ContainerType::CleanupTracker,
            .node.node.leaf_p = nullptr,
        };
        // In the past, it was possible, however improbably, that
        // we 'd invoke destroy_wait_ctx() twice on the same context, or would otherwise
        // use or re-use the same context while it was either invalid, or was reused
        // and that would obviously cause problems.
        // In ordet to eliminate that possibility, destroy_wait_ctx() now
        // defers put_waitctx() until the next I/O loop iteration
        // where it's safe to release and reuse that context
        //
        // We used to use a single expiredCtxList, but now using multiple just so that we
        // won't accidently clear/use one for multiple occasions. It was used in order to track
        // down a hasenbug, though it turned out to be a silly application bug, not a Tank bug.
        // It's nonethless great that we figured out this edge case(no evidence that this
        // has ever happened) and we are dealing with it here.
        std::vector<wait_ctx *>        expiredCtxList, expiredCtxList2, expiredCtxList3, waitctx_deferred_gc;
        uint32_t                       nextDistinctPartitionId{0};
        int                            listenFd{-1}, prom_listen_fd{-1};
        std::atomic<bool>              sleeping alignas(64){false};
        EPoller                        poller{2048};
        pthread_t                      main_thread_id;
        std::unique_ptr<std::thread>   sync_thread;
        std::vector<topic_partition *> partitions_requested_eof;
        range32_t                     *patch_list{nullptr};
        uint32_t                      *partitions_requested_eof_patch_list_indices{nullptr};
        time_t                         curTime;
        uint64_t                       now_ms{Timings::Milliseconds::Tick()};

      protected:
        // so that changing that file won't trigger rebuild all service*.o
#include "service_pragmas.h"
      protected:
        static uint64_t segment_lastmsg_seqnum(int, const uint64_t);

        static void rebuild_index(int, int, const uint64_t);

        static uint32_t verify_log(int);

        static void verify_index(int, const bool);

      public:
        void maybe_wakeup_reactor();

        void schedule_cleanup();

        int safe_open(const char *path, int flags, mode_t mode = 0);

        void track_log_cleanup(topic_partition_log *log) {
                cleanup_tracker.emplace_back(log);
        }

      public:
        static inline std::atomic<uint64_t> pending_signals{0};

      public:
        Service();

        ~Service();

        int start(int argc, char **argv);
};

// basic type-erasure for the callable of std::bind
struct mainthread_closure final {
        struct callable {
                virtual void invoke() = 0;
                virtual ~callable()   = default;
        };

        template <typename T>
        struct internal final
            : public callable {
                T v;

                internal(T &&call)
                    : v(std::move(call)) {
                }

                void invoke() override {
                        v();
                }
        };

        template <typename T>
        mainthread_closure(T &&foo)
            : L{new internal<T>(std::move(foo))} {
        }

        void operator()() {
                L->invoke();
        }

        mainthread_closure       *next;
        std::unique_ptr<callable> L;
};

extern PubSubQueue<mainthread_closure> mainThreadClosures;
extern Service                        *this_service;
extern Buffer                          basePath_;
extern bool                            read_only;

template <typename F, typename... Arg>
static inline void run_on_main_thread(F &&l, Arg &&...args) {
        mainThreadClosures.push_back(new mainthread_closure(std::bind(l, std::forward<Arg>(args)...)));
        this_service->maybe_wakeup_reactor();
}

template <typename... T>
inline void track_shutdown(connection *const c, const size_t line, T &&...args) {
        // no-op
}

#ifdef __clang__
#pragma GCC diagnostic pop
#endif

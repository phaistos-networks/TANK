#include "service.h"
#include <ansifmt.h>
#include <compress.h>
#include <csignal>
#include <date.h>
#include <fcntl.h>
#include <fs.h>
#include <future>
#include <random>
#include <set>
#include <switch_mallocators.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <text.h>
#include <thread>
#include <timings.h>
#include <unistd.h>
#ifndef LEAN_SWITCH
#include <switch_debug.h>
#endif

// From SENDFILE(2): The original Linux sendfile() system call was not designed to handle large file offsets.
// Consequently, Linux 2.4 added sendfile64(), with a wider type for the offset argument.
// The glibc sendfile() wrapper function transparently deals with the kernel differences.
#define HAVE_SENDFILE64 1

static constexpr bool trace{false};

static Service *                          this_service;
static std::mutex                         mboxLock;
static std::vector<std::pair<int, int>>   mbox;
static std::condition_variable            mbox_cv;
static Buffer                             basePath_;
static bool                               read_only{false};
static bool                               cleanupTrackerIsDirty{false};
static std::vector<topic_partition_log *> cleanupTracker;

static int Rename(const char *oldpath, const char *newpath) {
        if (trace)
                SLog("rename(", oldpath, ", ", newpath, ")\n");

        return rename(oldpath, newpath);
}

static int Unlink(const char *pathname) {
        if (trace)
                SLog("unlink(", pathname, ")\n");

        return unlink(pathname);
}

ro_segment::ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const strwlen32_t base, const uint32_t creationTS, const bool wideEntries)
    : baseSeqNum{absSeqNum}, lastAvailSeqNum{lastAbsSeqNum}, createdTS{creationTS}, haveWideEntries{wideEntries} {
        int           fd, indexFd;
        struct stat64 st;

        if (trace) {
                SLog("New ro_segment(this = ", ptr_repr(this), ", baseSeqNum = ", baseSeqNum, ", lastAbsSeqNum = ", lastAbsSeqNum, ", createdTS = ", createdTS, ", haveWideEntries = ", haveWideEntries, " ", base, "/", absSeqNum, "\n");
        }

        EXPECT(lastAbsSeqNum >= baseSeqNum);

        index.data = nullptr;
        if (createdTS) {
                fd = open(Buffer::build(base, "/", absSeqNum, "-", lastAbsSeqNum, "_", createdTS, ".ilog").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        } else {
                fd = open(Buffer::build(base, "/", absSeqNum, "-", lastAbsSeqNum, ".ilog").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        }

        if (fd == -1) {
                throw Switch::system_error("Failed to access log file:", strerror(errno));
        }

        fdh.reset(new fd_handle(fd));
        Drequire(fdh.use_count() == 2);
        fdh->Release();

        if (fstat64(fd, &st) == -1) {
                throw Switch::system_error("Failed to fstat():", strerror(errno));
        }

        auto size = st.st_size;

        if (unlikely(size == (off64_t)-1)) {
                throw Switch::system_error("lseek64() failed: ", strerror(errno));
        }

        EXPECT(size < std::numeric_limits<std::remove_reference<decltype(fileSize)>::type>::max());

        fileSize = size;

        if (trace) {
                SLog(ansifmt::bold, "fileSize = ", fileSize, ", createdTS = ", Date::ts_repr(creationTS), ansifmt::reset, "\n");
        }

        if (haveWideEntries) {
                indexFd = open(Buffer::build(base, "/", absSeqNum, "_64.index").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        } else {
                indexFd = open(Buffer::build(base, "/", absSeqNum, ".index").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        }

        DEFER({ if (indexFd != -1) close(indexFd); });

        if (indexFd == -1) {
                if (haveWideEntries) {
                        indexFd = open(Buffer::build(base, "/", absSeqNum, "_64.index").data(), read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME), 0775);
                } else {
                        indexFd = open(Buffer::build(base, "/", absSeqNum, ".index").data(), read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME), 0775);
                }

                if (indexFd == -1) {
                        throw Switch::system_error("Failed to rebuild index file:", strerror(errno));
                }

                if (haveWideEntries) {
                        IMPLEMENT_ME();
                }

                Service::rebuild_index(fdh->fd, indexFd);
        }

        size = lseek64(indexFd, 0, SEEK_END);

        if (unlikely(size == (off64_t)-1)) {
                throw Switch::system_error("lseek64() failed: ", strerror(errno));
        }

        EXPECT(size < std::numeric_limits<std::remove_reference<decltype(index.fileSize)>::type>::max());

        // TODO(markp): if (haveWideEntries), index.lastRecorded.relSeqNum should be a union, and we should
        // properly set index.lastRecorded here
        require(haveWideEntries == false); // not implemented yet

        index.fileSize               = size;
        index.lastRecorded.relSeqNum = index.lastRecorded.absPhysical = 0;

        if (size) {
                auto data = mmap(nullptr, index.fileSize, PROT_READ, MAP_SHARED, indexFd, 0);

                if (unlikely(data == MAP_FAILED)) {
                        throw Switch::system_error("Failed to access the index file. mmap() failed:", strerror(errno), " for ", size_repr(index.fileSize), " ", base, "/");
                }

                madvise(data, index.fileSize, MADV_DONTDUMP);

                index.data = static_cast<const uint8_t *>(data);

                if (likely(index.fileSize >= sizeof(uint32_t) + sizeof(uint32_t))) {
                        // last entry in the index; very handy
                        const auto *const p = (uint32_t *)(index.data + index.fileSize - sizeof(uint32_t) - sizeof(uint32_t));

                        index.lastRecorded.relSeqNum   = p[0];
                        index.lastRecorded.absPhysical = p[1];

                        if (trace) {
                                SLog("lastRecorded = ", index.lastRecorded.relSeqNum, "(", index.lastRecorded.relSeqNum + baseSeqNum, "), ", index.lastRecorded.absPhysical, "\n");
                        }
                }
        } else {
                index.data = nullptr;
        }
}

std::pair<uint32_t, uint32_t> ro_segment::snapDown(const uint64_t absSeqNum) const {
        if (haveWideEntries) {
                if (trace)
                        SLog("haveWideEntries for ", ptr_repr(this), "\n");

                IMPLEMENT_ME();
        }

        const auto relSeqNum = uint32_t(absSeqNum - baseSeqNum);

        if (relSeqNum == baseSeqNum) {
                // optimization
                return {relSeqNum, 0};
        }

        const auto *const all = reinterpret_cast<const index_record *>(index.data);
        const auto *const end = all + index.fileSize / sizeof(index_record);
        const auto        it  = std::upper_bound_or_match(all, end, relSeqNum, [](const auto &a, const auto num) {
                return TrivialCmp(num, a.relSeqNum);
        });

        if (it == end) {
                // no index record where record.relSeqNum <= relSeqNum
                // that is, the first index record.relSeqNum > relSeqNum
                return {0, 0};
        } else {
                return {it->relSeqNum, it->absPhysical};
        }
}

std::pair<uint32_t, uint32_t> ro_segment::snapUp(const uint64_t absSeqNum) const {
        if (haveWideEntries) {
                IMPLEMENT_ME();
        }

        const auto        relSeqNum = uint32_t(absSeqNum - baseSeqNum);
        const auto *const all       = reinterpret_cast<const index_record *>(index.data);
        const auto *const end       = all + index.fileSize / sizeof(index_record);
        const auto        it        = std::lower_bound(all, end, relSeqNum, [](const auto &a, const auto num) {
                return a.relSeqNum < num;
        });

        if (it == end) {
                // no index record where record.relSeqNum >= relSeqNum
                // that is, the last index record.relSeqNum < relSeqNum
                return {UINT32_MAX, UINT32_MAX};
        } else
                return {it->relSeqNum, it->absPhysical};
}

// Searches forward starting from `fileOffset` until it finds a message(set) with absSeqNum > `maxAbsSeqNum`, and
// returns the file offset of that message(which is the end of file before
// that message); also respects maxSize
// TODO(markp): read in 4k chunks/time or something more appropriate
static uint32_t search_before_offset(uint64_t baseSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum, int fd, const uint32_t fileSize, uint32_t fileOffset) {
        uint8_t    buf[128];
        auto       o     = fileOffset;
        const auto limit = maxSize != UINT32_MAX ? Min<uint32_t>(fileSize, fileOffset + maxSize) : fileSize;
        uint64_t   lastMsgSeqNum;

        if (trace)
                SLog("Searching for offset ", maxAbsSeqNum, ", limit = ", limit, "(maxSize = ", maxSize, ", baseSeqNum = ", baseSeqNum, ", fileOffset = ", fileOffset, ")\n");

        while (fileOffset < limit) {
                const auto r = pread64(fd, buf, sizeof(buf), fileOffset);

                if (unlikely(r == -1))
                        throw Switch::system_error("pread() failed:", strerror(errno));

                const auto *p                   = buf;
                const auto  bundleLen           = Compression::UnpackUInt32(p);
                const auto  encodedBundleLenLen = p - buf;
                const auto  bundleFlags         = *p++;
                const bool  sparseBundleBitSet  = bundleFlags & (1u << 6);
                uint32_t    msgSetSize          = (bundleFlags >> 2) & 0xf;

                if (!msgSetSize)
                        msgSetSize = Compression::UnpackUInt32(p);

                if (sparseBundleBitSet) {
                        const auto firstMsgSeqNum = *(uint64_t *)p;
                        p += sizeof(uint64_t);

                        if (msgSetSize != 1) {
                                lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                        } else {
                                lastMsgSeqNum = firstMsgSeqNum;
                        }

                        if (trace)
                                SLog("sparseBundleBitSet is set, firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum = ", lastMsgSeqNum, "\n");
                }

                if (trace)
                        SLog("abs = ", baseSeqNum, "(msgSetSize = ", msgSetSize, ") VS ", maxAbsSeqNum, " at ", o, "\n");

                if (baseSeqNum > maxAbsSeqNum)
                        break;
                else {
                        if (sparseBundleBitSet) {
                                baseSeqNum = lastMsgSeqNum + 1;
                        } else {
                                baseSeqNum += msgSetSize;
                        }

                        fileOffset += bundleLen + encodedBundleLenLen;
                }
        }

        if (trace)
                SLog("Returning fileOffset = ", fileOffset, "\n");

        return fileOffset;
}

// We are operating on index boundaries, so our res.fileOffset is aligned on an index boundary, which means
// we may stream (0, partition_config::indexInterval] excess bytes.
// This is probably fine, but we may as well
// scan ahead from that fileOffset until we find an more appropriate file offset to begin streaming for, and adjust
// res.fileOffset and res.baseSeqNum accordidly if we can.
//
// Kafka does something similar(see its FileMessageSet.scala#searchFor() impl.)
//
// If we don't adjust_range_start(), we'll avoid the scanning I/O cost, which should be minimal anyway, but we can potentially send
// more data at the expense of network I/O and transfer costs
//
// it returns true if it parsed the first bundle to stream, and that bundle is a sparse bundle (which means
// it encodes the sequence number of its first message in its header)
static bool adjust_range_start(lookup_res &res, const uint64_t absSeqNum) {
        uint64_t baseSeqNum = res.absBaseSeqNum;

        if (trace == false) // explicitly allow so that we can verify it does the right thing when tracing
        {
                if (baseSeqNum == absSeqNum || absSeqNum <= 1) {
                        // No need for any adjustments
                        if (trace)
                                SLog("No need for any adjustments\n");

                        return false;
                }
        }

        int        fd = res.fdh->fd;
        uint8_t    tinyBuf[sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint64_t)];
        const auto baseOffset        = res.fileOffset;
        auto       o                 = baseOffset;
        const auto fileOffsetCeiling = res.fileOffsetCeiling;
        uint64_t   before            = trace ? Timings::Microseconds::Tick() : 0, lastMsgSeqNum;
        bool       firstBundleIsSparse{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "About to adjust range fileOffset ", baseOffset, ", absBaseSeqNum(", baseSeqNum, "), absSeqNum(", absSeqNum, "), fileOffsetCeiling = ", fileOffsetCeiling, ansifmt::reset, "\n");
        }

        while (o < fileOffsetCeiling) {
                const auto r = pread64(fd, tinyBuf, sizeof(tinyBuf), o);

                if (unlikely(r == -1))
                        throw Switch::system_error("pread64() failed:", strerror(errno));

                const auto *const baseBuf   = tinyBuf;
                const uint8_t *   p         = baseBuf;
                const auto        bundleLen = Compression::UnpackUInt32(p);

                require(bundleLen);

                const auto encodedBundleLenLen = p - baseBuf;
                const auto bundleFlags         = *p++;
                const bool sparseBundleBitSet  = bundleFlags & (1u << 6);
                uint32_t   msgSetSize          = (bundleFlags >> 2) & 0xf;

                if (!msgSetSize)
                        msgSetSize = Compression::UnpackUInt32(p);

                if (sparseBundleBitSet) {
                        const auto firstMsgSeqNum = *(uint64_t *)p;
                        p += sizeof(uint64_t);

                        if (msgSetSize != 1) {
                                lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                        } else {
                                lastMsgSeqNum = firstMsgSeqNum;
                        }

                        if (trace)
                                SLog("sparseBundleBitSet, firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum, ", lastMsgSeqNum, "\n");

                        firstBundleIsSparse = true;
                } else
                        firstBundleIsSparse = false;

                const auto nextBundleBaseSeqNum = sparseBundleBitSet ? lastMsgSeqNum + 1 : baseSeqNum + msgSetSize;

                if (trace)
                        SLog("Now at bundle(", baseSeqNum, "), msgSetSize(", msgSetSize, "), bundleFlags(", bundleFlags, "), nextBundleBaseSeqNum = ", nextBundleBaseSeqNum, "\n");

                if (absSeqNum >= nextBundleBaseSeqNum) {
                        // Our target is in later bundle
                        o += bundleLen + encodedBundleLenLen;
                        baseSeqNum = nextBundleBaseSeqNum;

                        if (trace)
                                SLog("Target in later bundle, advanced to = ", o, "\n");
                } else {
                        // Our target is in this bundle
                        if (trace)
                                SLog("Target in this bundle(", o, ")\n");

                        res.fileOffset    = o;
                        res.absBaseSeqNum = baseSeqNum;
                        break;
                }
        }

        if (trace)
                SLog(ansifmt::color_blue, "After adjustement, fileOffset ", res.fileOffset, ", absBaseSeqNum(", res.absBaseSeqNum, "), took  ", duration_repr(Timings::Microseconds::Since(before)), ansifmt::reset, "\n");

        return firstBundleIsSparse;
}

lookup_res topic_partition_log::read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum) {
        // lock is expected to be locked
        require(absSeqNum >= cur.baseSeqNum);

        if (cur.index.haveWideEntries) {
                // need to use the appropriate skipList64 and a different index encoding format
                IMPLEMENT_ME();
        }

        lookup_res  res;
        const auto  highWatermark = lastAssignedSeqNum;
        bool        inSkiplist;
        const auto  relSeqNum = uint32_t(absSeqNum - cur.baseSeqNum);
        const auto &skipList  = cur.index.skipList;
        const auto  end       = skipList.end();
        const auto  it        = std::upper_bound_or_match(skipList.begin(), end, relSeqNum, [](const auto &a, const auto seqNum) {
                return TrivialCmp(seqNum, a.first);
        });

        res.fdh = cur.fdh;

        if (it != end) {
                if (trace)
                        SLog("Found in skiplist\n");

                res.absBaseSeqNum = cur.baseSeqNum + it->first;
                res.fileOffset    = it->second;

                inSkiplist = true;
        } else {
                require(cur.index.ondisk.span); // we checked if it's in this current segment

                if (trace)
                        SLog("Considering ondisk index (cur.fileSize = ", cur.fileSize, ")\n");

                const auto        size = cur.index.ondisk.span;
                const auto *const all  = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                const auto *const e    = all + size / sizeof(index_record);
                const auto        i    = std::upper_bound_or_match(all, e, relSeqNum, [](const auto &a, const auto seqNum) {
                        return TrivialCmp(seqNum, a.relSeqNum);
                });

                if (i != e) {
                        if (trace)
                                SLog("In ondisk index (relSeqNum:", i->relSeqNum, ", absPhysical:", i->absPhysical, ")\n");

                        res.absBaseSeqNum = cur.baseSeqNum + i->relSeqNum;
                        res.fileOffset    = i->absPhysical;
                } else {
                        res.absBaseSeqNum = cur.baseSeqNum;
                        res.fileOffset    = 0;
                }

                inSkiplist = false;
        }

        if (maxAbsSeqNum != UINT64_MAX) {
                index_record ref;

                if (inSkiplist) {
                        const auto it = std::upper_bound_or_match(skipList.begin(), skipList.end(), uint32_t(maxAbsSeqNum - cur.baseSeqNum), [](const auto &a, const auto seqNum) {
                                return TrivialCmp(seqNum, a.first);
                        });

                        ref.relSeqNum   = it->first;
                        ref.absPhysical = it->second;
                } else {
                        const auto        size = cur.index.ondisk.span;
                        const auto *const all  = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                        const auto *const e    = all + size / sizeof(index_record);
                        const auto        it   = std::upper_bound_or_match(all, e, uint32_t(maxAbsSeqNum - cur.baseSeqNum), [](const auto &a, const uint32_t seqNum) {
                                return TrivialCmp(seqNum, a.relSeqNum);
                        });

                        ref = *it;
                }

#if 0
		res.fileOffsetCeiling = ref.absPhysical; 	// XXX: we actually need to set this to (it + 1).absPhysical so that we may not skip a bundle that includes
								// both the highwater mark but also messages with seqnum < highwater mark
#else
                // Yes, incur some tiny I/O overhead so that we 'll properly cut-off the content
                res.fileOffsetCeiling = search_before_offset(cur.baseSeqNum, maxSize, maxAbsSeqNum, cur.fdh->fd, cur.fileSize, ref.absPhysical);
#endif

                if (trace)
                        SLog("maxAbsSeqNum = ", maxAbsSeqNum, " ", res.fileOffsetCeiling, "\n");
        } else {
                res.fileOffsetCeiling = cur.fileSize;

                if (trace)
                        SLog("res.fileOffsetCeiling = ", res.fileOffsetCeiling, "\n");
        }

        res.highWatermark = highWatermark;
        return res;
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
                if (!list.load(std::memory_order_relaxed))
                        return nullptr;
                else
                        return list.exchange(nullptr, std::memory_order_acquire);
        }
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

        mainthread_closure *      next;
        std::unique_ptr<callable> L;
};

static PubSubQueue<mainthread_closure> mainThreadClosures;

template <typename F, typename... Arg>
static void run_on_main_thread(F &&l, Arg &&... args) {
        mainThreadClosures.push_back(new mainthread_closure(std::bind(l, std::forward<Arg>(args)...)));
        this_service->maybe_wakeup_reactor();
}

static void compact_partition(topic_partition_log *const log, const char *const basePartitionPath, std::vector<ro_segment *> prevSegments) {
        struct msg final {
                strwlen8_t  key;
                uint64_t    seqNum;
                uint64_t    ts;
                strwlen32_t content;
        };

        static constexpr bool                  trace{false};
        uint64_t                               firstMsgSeqNum, lastMsgSeqNum, msgSeqNum;
        range_base<const uint8_t *, size_t>    msgSetContent;
        strwlen8_t                             key;
        [[maybe_unused]] strwlen32_t           msgContent, msgValue;
        bool                                   anyDropped{false};
        std::vector<std::unique_ptr<IOBuffer>> pool;
        IOBuffer *                             cur{nullptr};
        size_t                                 base{0};
        static constexpr uint64_t              ptrBit{uint64_t(1) << (sizeof(uintptr_t) * 8 - 1)};

        const auto compact = [&anyDropped](Switch::vector<msg> &msgs) {
                std::sort(msgs.begin(), msgs.end(), [](const auto &a, const auto &b) noexcept {
                        return a.key.Cmp(b.key) < 0;
                });

                auto *out = msgs.data();

                for (const auto *it = out, *const e = it + msgs.size(); it != e;) {
                        const auto k = it->key;

                        if (!k) {
                                do {
                                        *out++ = *it;
                                } while (++it != e && !it->key);
                        } else {
                                auto last = it->seqNum;
                                auto sel  = it;

                                for (++it; it != e && it->key == k; ++it) {
                                        if (it->seqNum > last) {
                                                last = it->seqNum;
                                                sel  = it;
                                        }
                                }

                                *out++ = *sel;
                        }
                }

                const auto n = out - msgs.data();

                if (n == msgs.size()) {
                        if (!anyDropped) {
                                // Nothing to do here, and no tombstones found, no compaction necessary
                                return false;
                        }
                } else {
                        // need to resize anyway
                        msgs.resize(n);

                        std::sort(msgs.begin(), msgs.end(), [](const auto &a, const auto &b) noexcept {
                                return a.seqNum < b.seqNum;
                        });
                }

                return true;
        };

        Switch::vector<msg>                    msgs;
        std::vector<std::pair<void *, size_t>> vmas;

        DEFER(
            {
                    while (!vmas.empty()) {
                            auto it = vmas.back();

                            madvise(it.first, it.second, MADV_DONTNEED);
                            munmap(it.first, it.second);
                            vmas.pop_back();
                    }
            });

        if (trace)
                SLog(prevSegments.size(), " segments\n");

        for (auto it : prevSegments) {
                int         fd         = it->fdh->fd;
                const auto  fileSize   = it->fileSize;
                const auto  baseSeqNum = it->baseSeqNum;
                auto *const fileData   = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                if (fileData == MAP_FAILED)
                        throw Switch::system_error("mmap() failed:", strerror(errno));

                madvise(fileData, fileSize, MADV_DONTDUMP);

                vmas.emplace_back(fileData, fileSize);

                if (trace)
                        SLog("baseSeqNum for segment ", baseSeqNum, "\n");

                msgSeqNum = baseSeqNum;
                for (const auto *p = static_cast<const uint8_t *>(fileData), *const e = p + fileSize; p != e;) {
                        const auto bundleLen          = Compression::UnpackUInt32(p);
                        const auto nextBundle         = p + bundleLen;
                        const auto bundleFlags        = *p++;
                        const auto codec              = bundleFlags & 3;
                        const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                        uint32_t   msgsSetSize        = (bundleFlags >> 2) & 0xf;

                        if (!msgsSetSize)
                                msgsSetSize = Compression::UnpackUInt32(p);

                        if (trace)
                                SLog("New bundle msgSetSize = ", msgsSetSize, ", bundleFlags = ", bundleFlags, ", codec = ", codec, "\n");

                        if (sparseBundleBitSet) {
                                firstMsgSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

                                if (msgsSetSize != 1) {
                                        lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                                } else {
                                        lastMsgSeqNum = firstMsgSeqNum;
                                }

                                if (trace)
                                        SLog("sparse bundle (first ", firstMsgSeqNum, ", last ", lastMsgSeqNum, ")\n");
                        }

                        if (codec) {
                                if (!cur || cur->Reserved() > 64 * 1024 * 1024) // XXX: arbitrary
                                {
                                        const auto n = msgs.size();

                                        while (base != n) {
                                                auto &     it  = msgs[base++];
                                                const auto ptr = uintptr_t(it.content.p);
                                                const auto to  = ptr & (~ptrBit);

                                                if (ptr != to)
                                                        it.content.p = cur->At(to);

                                                if (it.key) {
                                                        const auto ptr = uintptr_t(it.key.p);
                                                        const auto to  = ptr & (~ptrBit);

                                                        if (ptr != to)
                                                                it.key.p = cur->At(to);
                                                }
                                        }

                                        auto owner = std::make_unique<IOBuffer>();

                                        cur = owner.get();
                                        pool.push_back(std::move(owner));
                                }

                                const auto len = cur->size();

                                if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, nextBundle - p, cur))
                                        throw Switch::system_error("failed to decompress message set");

                                msgSetContent.Set(reinterpret_cast<const uint8_t *>(cur->at(len)), cur->size() - len);
                        } else {
                                msgSetContent.Set(p, nextBundle - p);
                        }

                        p = nextBundle;

                        uint64_t          msgTs{0};
                        uint32_t          msgIdx{0};
                        const auto *const ptrBase = cur ? cur->data() : nullptr;

                        if (trace)
                                SLog("Parsing Message Set\n");

                        for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e; ++msgIdx, ++msgSeqNum) {
                                const auto flags = *p++;

                                if (trace)
                                        SLog("Message ", msgIdx, ", flags ", flags, "\n");

                                if (sparseBundleBitSet) {
                                        if (msgIdx == 0)
                                                msgSeqNum = firstMsgSeqNum;
                                        else if (msgIdx == msgsSetSize - 1)
                                                msgSeqNum = lastMsgSeqNum;
                                        else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                // incremented in for() (in previous loop iteration)
                                                if (trace)
                                                        SLog("SeqNumPrevPlusOne set\n");
                                        } else {
                                                // we encode delta from last - 1, but we already ++msgSeqNum in for() (in previous iteration)
                                                msgSeqNum += Compression::UnpackUInt32(p);

                                                if (trace)
                                                        SLog("Adjusting delta\n");
                                        }
                                }

                                if (trace)
                                        SLog("SeqNum = ", msgSeqNum, "\n");

                                if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                        msgTs = *(uint64_t *)p;
                                        p += sizeof(uint64_t);
                                }

                                if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                        key.Set((char *)p + 1, *p);
                                        p += key.len + sizeof(uint8_t);

                                        if (trace)
                                                SLog("MSG ", msgSeqNum, ", key [", key, "] ", Date::ts_repr(Timings::Milliseconds::ToSeconds(msgTs)), "\n");

                                        if (codec)
                                                key.p = (char *)uintptr_t(key.p - ptrBase);
                                } else {
                                        key.reset();
                                }

                                const auto msgLen = Compression::UnpackUInt32(p);

                                if (msgLen || !key) {
                                        msgValue.Set((char *)p, msgLen);
                                        p += msgLen;

                                        if (trace)
                                                SLog("value [", msgValue, "]\n");

                                        if (codec)
                                                msgValue.p = (char *)uintptr_t(msgValue.p - ptrBase);

                                        msgs.push_back({key, msgSeqNum, msgTs, msgValue});
                                } else {
                                        // Drop deleted messages(messages with a key and no content)
                                        anyDropped = true;
                                }
                        }
                }
        }

        if (cur) {
                const auto n = msgs.size();

                while (base != n) {
                        auto &     it  = msgs[base++];
                        const auto ptr = uintptr_t(it.content.p);
                        const auto to  = ptr & (~ptrBit);

                        if (ptr != to)
                                it.content.p = cur->At(to);

                        if (it.key) {
                                const auto ptr = uintptr_t(it.key.p);
                                const auto to  = ptr & (~ptrBit);

                                if (ptr != to)
                                        it.key.p = cur->At(to);
                        }
                }
        }

        if (!compact(msgs)) {
#if 1
                if (trace)
                        SLog("No need for compaction\n");

                run_on_main_thread([log]() {
                        log->compacting = false;
                        Print("Did not need to compact log\n");
                });

                return;
#else
                Print("ENABLE AGAIN\n");

                std::sort(msgs.begin(), msgs.end(), [](const auto &a, const auto &b) {
                        return a.seqNum < b.seqNum;
                });
#endif
        }

        if (trace) {
                for (const auto &it : msgs)
                        Print(it.seqNum, " [", it.key, "] [", it.content, "]\n");
        }

        // go through all segments, rebuild each of them by keeping only the messages that existed in that segment (we can just use a range check for first available, last assigned)
        // but if after compaction a segment's too small (in terms of file size), then include into it messages from successive segments, and in that case
        // use the last segment's timestamp that is to be encoded in the filename
        static constexpr size_t   sinceLastUpdateBytesThreshold{10000}, sinceLastUpdateMsgsCntThreshold{128}, maxBundleMsgsSetSize{5}, maxBundleMsgsSetSizeBytes{65536}; // XXX: arbitrary
        static constexpr size_t   minSegmentLogFileSize{64 * 1024};                                                                                                      // XXX: arbitrary
        std::vector<ro_segment *> newSegments;
        int                       fd;
        char                      logPath[PATH_MAX];
        const auto                n   = msgs.size();
        const auto *const         all = msgs.data();
        IOBuffer                  out, cbuf, index;
        struct iovec              iov[1024];
        uint32_t                  iovLen{0};
        const auto                flush = [&iovLen, &iov, &out, &cbuf, &fd]() {
                for (uint32_t i{0}; i != iovLen; ++i) {
                        auto &it  = iov[i];
                        auto  ptr = uintptr_t(it.iov_base);

                        if (ptr & (1u << 31)) {
                                ptr &= ~(1u << 31);
                                it.iov_base = out.At(ptr);
                        } else {
                                ptr &= ~(1u << 30);
                                it.iov_base = cbuf.At(ptr);
                        }
                }

                if (trace)
                        SLog("Flushing ", iovLen, "\n");

                const auto r = writev(fd, iov, iovLen);

                if (unlikely(r == -1))
                        throw Switch::system_error("writev() failed:", strerror(errno));

                out.clear();
                cbuf.clear();
                iovLen = 0;
        };

        try {
                const char *destPartitionPath;
                uint32_t    curSegmentIdx{0};

#if 0
                if (getenv("FOOOOO"))
                        destPartitionPath = "/tmp/foo/0/";
                else
		{
                        destPartitionPath = "/tmp/tankREPO/msgs/0/";
		}
#else
                destPartitionPath = basePartitionPath;
#endif

                // Process all collected messages, pack into segments
                for (uint32_t i{0}; i != n;) {
                        uint8_t      bundleFlags;
                        size_t       outFileSize{0};
                        size_t       sinceLastUpdateBytes{UINT32_MAX}, sinceLastUpdateMsgsCnt{UINT32_MAX};
                        auto         curSegment = prevSegments[curSegmentIdx];
                        const auto   baseSeqNum{all[i].seqNum};
                        auto         curSegmentLastAvailSeqNum = curSegment->lastAvailSeqNum;
                        index_record indexLastRecorded;
                        auto         expected = all[i].seqNum;

                        if (trace)
                                SLog(ansifmt::bold, ansifmt::color_blue, "Now processing segment ", curSegmentIdx, "/", prevSegments.size(), " (", baseSeqNum, ", ", curSegment->lastAvailSeqNum, ")", ansifmt::reset, "\n");

                        // new segment
                        index.clear();
                        out.clear();
                        cbuf.clear();
                        iovLen = 0;

                        Snprint(logPath, sizeof(logPath), destPartitionPath, baseSeqNum, "-", 0, "_", 0, ".ilog.cleaned");
                        fd = open(logPath, O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC, 0775);
                        require(fd != -1);

                        DEFER({
                                if (fd != -1)
                                        close(fd);
                        });

                        for (;;) {
                                // new bundle
                                const auto base{i};
                                const auto upto = Min<size_t>(n, i + maxBundleMsgsSetSize);
                                bool       asSparse{false};
                                size_t     sum{0};

                                do {
                                        const auto n = all[i].seqNum;

                                        if (n != expected)
                                                asSparse = true;

                                        if (trace)
                                                SLog("consider ", i, " ", all[i].seqNum, "\n");

                                        expected = n + 1;
                                        sum += all[i].key.len + all[i].content.len + 8;
                                } while (++i != upto && sum < maxBundleMsgsSetSizeBytes && all[i].seqNum <= curSegmentLastAvailSeqNum);

                                if (trace)
                                        SLog("expected = ", expected, ", asSparse = ", asSparse, "\n");

                                if (sinceLastUpdateBytes > sinceLastUpdateBytesThreshold || sinceLastUpdateMsgsCnt > sinceLastUpdateMsgsCntThreshold) {
                                        // TODO(markp): if (all[base].seqNum - baseSeqNum > threshold, need to
                                        // switch to wide-entries index
                                        indexLastRecorded.relSeqNum   = all[base].seqNum - baseSeqNum;
                                        indexLastRecorded.absPhysical = outFileSize;

                                        index.Serialize<uint32_t>(indexLastRecorded.relSeqNum);
                                        index.Serialize<uint32_t>(indexLastRecorded.absPhysical);
                                        sinceLastUpdateBytes   = 0;
                                        sinceLastUpdateMsgsCnt = 0;
                                }

                                const uint32_t msgSetSize              = i - base;
                                const auto     bundleHeaderFlagsOffset = out.size();
                                const auto     bundleLengthIOVIdx      = iovLen++;

                                out.reserve(sum + 1024);
                                if (asSparse) {
                                        bundleFlags = (1u << 6);

                                        if (msgSetSize < 16) {
                                                bundleFlags |= (msgSetSize << 2);
                                                out.Serialize(bundleFlags);
                                        } else {
                                                out.Serialize(bundleFlags);
                                                out.SerializeVarUInt32(msgSetSize);
                                        }

                                        const auto first = all[base].seqNum, last = all[i - 1].seqNum;

                                        if (trace)
                                                SLog("Sparse bundle first = ", first, ", last = ", last, ", set size = ", msgSetSize, "\n");

                                        out.Serialize<uint64_t>(first);
                                        if (msgSetSize != 1)
                                                out.SerializeVarUInt32(last - first - 1);
                                } else {
                                        bundleFlags = 0;

                                        if (msgSetSize < 16) {
                                                bundleFlags |= (msgSetSize << 2);
                                                out.Serialize(bundleFlags);
                                        } else {
                                                out.Serialize(bundleFlags);
                                                out.SerializeVarUInt32(msgSetSize);
                                        }
                                }

                                const auto savedOutFileSize   = outFileSize;
                                const auto bundleHeaderLength = out.size() - bundleHeaderFlagsOffset;
                                uint64_t   lastTS{0};
                                const auto msgSetOffset = out.size();

                                sinceLastUpdateMsgsCnt += msgSetSize;
                                outFileSize += bundleHeaderLength;

                                iov[iovLen++] = {(void *)uintptr_t(bundleHeaderFlagsOffset | (1u << 31)), bundleHeaderLength};

                                if (trace)
                                        SLog("Encoding Messages Set asSparse = ", asSparse, "\n");

                                for (uint32_t k{base}; k != i; ++k) {
                                        const auto &m        = all[k];
                                        uint8_t     msgFlags = m.key ? uint8_t(TankFlags::BundleMsgFlags::HaveKey) : uint8_t(0);
                                        bool        encodeTS, encodeSparseDelta;

                                        if (asSparse && k != base && k != i - 1) {
                                                if (m.seqNum == all[k - 1].seqNum + 1) {
                                                        msgFlags |= uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                                        encodeSparseDelta = false;
                                                } else
                                                        encodeSparseDelta = true;
                                        } else {
                                                encodeSparseDelta = false;
                                        }

                                        if (trace)
                                                SLog("message ", k - base, " ", m.seqNum, ", asSparse = ", asSparse, ", encodeSparseDelta = ", encodeSparseDelta, "\n");

                                        if (m.ts == lastTS && k != base) {
                                                msgFlags |= uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                                encodeTS = false;
                                        } else {
                                                lastTS   = m.ts;
                                                encodeTS = true;
                                        }

                                        out.Serialize(msgFlags);

                                        if (encodeSparseDelta) {
                                                out.SerializeVarUInt32(m.seqNum - all[k - 1].seqNum - 1);
                                                if (trace)
                                                        SLog("Serializing delta ", m.seqNum - all[k - 1].seqNum - 1, "\n");
                                        }

                                        if (encodeTS) {
                                                out.Serialize<uint64_t>(m.ts);
                                        }

                                        if (m.key) {
                                                out.Serialize(m.key.len);
                                                out.Serialize(m.key.p, m.key.len);
                                        }

                                        out.SerializeVarUInt32(m.content.len);
                                        out.Serialize(m.content.p, m.content.len);
                                }

                                const auto msgSetLen = out.size() - msgSetOffset;

                                if (trace)
                                        SLog("msgSetLen = ", msgSetLen, ", bundleFlags = ", bundleFlags, "\n");

                                if (msgSetLen > 1024) // XXX: arbitrary
                                {
                                        const auto offset = cbuf.size();

                                        if (!Compression::Compress(Compression::Algo::SNAPPY, out.At(msgSetOffset), msgSetLen, &cbuf))
                                                throw Switch::system_error("Compression failed");

                                        const auto span = cbuf.size() - offset;

                                        if (span >= msgSetLen) {
                                                // not worth it
                                                if (trace)
                                                        SLog("Not worth compressing bundle msgs set\n");

                                                cbuf.resize(offset);
                                                goto l10;
                                        } else {
                                                iov[iovLen++] = {(void *)uintptr_t(offset | (1u << 30)), span};
                                                out.resize(msgSetOffset);

                                                *reinterpret_cast<uint8_t *>(out.At(bundleHeaderFlagsOffset)) |= 1; // set codec
                                                outFileSize += span;

                                                if (trace)
                                                        SLog(">> Compressed ", msgSetLen, " ", span, "\n");
                                        }
                                } else {
                                l10:
                                        iov[iovLen++] = {(void *)uintptr_t(msgSetOffset | (1u << 31)), msgSetLen};
                                        outFileSize += msgSetLen;
                                }

                                const auto bundleLength = outFileSize - savedOutFileSize;
                                const auto _l           = out.size();

                                out.SerializeVarUInt32(bundleLength);
                                const auto bundleLengthReprLen = out.size() - _l;
                                iov[bundleLengthIOVIdx]        = {(void *)uintptr_t(_l | (1u << 31)), bundleLengthReprLen};

                                outFileSize += bundleLengthReprLen;

                                sinceLastUpdateBytes += bundleLength;
                                if (iovLen > sizeof_array(iov) - 16)
                                        flush();

                                if (i == n) {
                                        // done and done
                                        break;
                                } else if (all[i].seqNum > curSegmentLastAvailSeqNum) {
                                        if (trace)
                                                SLog("Consumed current segment ", curSegmentIdx, "\n");

                                        ++curSegmentIdx;
                                        if (outFileSize > minSegmentLogFileSize) {
                                                // we got enough messages for this segment
                                                break;
                                        } else {
                                                // we still haven't had enough messages stored in the currently produced segment, so keep
                                                // consuming from successive segments
                                                curSegment                = prevSegments[curSegmentIdx];
                                                curSegmentLastAvailSeqNum = curSegment->lastAvailSeqNum;
                                        }
                                }
                        }

                        if (iovLen)
                                flush();

                        auto       logFd           = fd;
                        const auto lastAvailSeqNum = all[i - 1].seqNum;

                        fd = open(Buffer::build(destPartitionPath, "/", baseSeqNum, ".index.cleaned").data(), O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC, 0775);

                        if (fd == -1) {
                                close(logFd);
                                throw Switch::system_error("Failed to access new segment's index:", strerror(errno));
                        }

                        if (write(fd, index.data(), index.size()) != index.size()) {
                                close(logFd);
                                close(fd);
                                throw Switch::system_error("Failed to create new segment's index:", strerror(errno));
                        }

                        fsync(logFd);

                        // We could have instead used (firstSegmentConsumedForThisNewSegment->baseSeqNum, curSegmentLastAvailSeqNum)
                        // instead of (baseSeqNum, all[i - 1].seqNum), which would have retained the filename for some segments cleaned up onto themselves
                        // and would reduce need to scan forward for a ro_segment if the query seqNum > segment.lastSeqNum and < nextSegment.baseSeqNum
                        // but we'd rather not do this
                        if (Rename(logPath, Buffer::build(destPartitionPath, baseSeqNum, "-", lastAvailSeqNum, "_", curSegment->createdTS, ".ilog.cleaned")) == -1)
                                throw Switch::system_error("Failed to rename segment:", strerror(errno));

                        auto newSegment = std::make_unique<ro_segment>(baseSeqNum, lastAvailSeqNum, curSegment->createdTS);

                        newSegment->fdh.reset(new fd_handle(logFd));
                        require(newSegment->fdh.use_count() == 2);
                        newSegment->fdh->Release();
                        newSegment->fileSize           = outFileSize;
                        newSegment->index.data         = reinterpret_cast<const uint8_t *>(mmap(nullptr, index.size(), PROT_READ, MAP_SHARED, fd, 0));
                        newSegment->index.fileSize     = index.size();
                        newSegment->index.lastRecorded = indexLastRecorded;

                        if (trace)
                                SLog(newSegment->fileSize, " ", lseek64(newSegment->fdh->fd, 0, SEEK_END), "\n");

                        require(newSegment->index.fileSize == lseek64(fd, 0, SEEK_END));
                        require(newSegment->fileSize == lseek64(newSegment->fdh->fd, 0, SEEK_END));

                        close(fd);
                        fd = -1;

                        if (newSegment->index.data == MAP_FAILED)
                                throw Switch::system_error("mmap() failed:", strerror(errno));

                        madvise((void *)newSegment->index.data, index.size(), MADV_DONTDUMP);

                        require(newSegment->fdh.use_count() == 1);
                        newSegments.push_back(newSegment.release());

                        if (trace)
                                SLog("Out segment, output ", outFileSize, "(", size_repr(outFileSize), ") ", dotnotation_repr(index.size()), " index entries\n");
                }

                if (trace)
                        SLog("Done scanning RO segments\n");

                // We have created a new set of segments, so we need to replace their .cleaned extension with a .swap extension
                // During startup, if we find any *.cleaned files, then we'll delete them and will also remove any .swap files left around
                for (auto it : newSegments) {
                        if (Rename(Buffer::build(destPartitionPath, it->baseSeqNum, "-", it->lastAvailSeqNum, "_", it->createdTS, ".ilog.cleaned").data(),
                                   Buffer::build(destPartitionPath, it->baseSeqNum, "-", it->lastAvailSeqNum, "_", it->createdTS, ".ilog.swap").data()) == -1) {
                                throw Switch::system_error("Failed to rename files:", strerror(errno));
                        }

                        if (Rename(Buffer::build(destPartitionPath, "/", it->baseSeqNum, ".index.cleaned").data(),
                                   Buffer::build(destPartitionPath, "/", it->baseSeqNum, ".index.swap").data()) == -1) {
                                throw Switch::system_error("Failed to rename files:", strerror(errno));
                        }
                }

                // Rename input segments by appending the .log extension to both log files and index files
                for (auto it : prevSegments) {
                        if (const auto createdTS = it->createdTS) {
                                if (Rename(Buffer::build(basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, "_", createdTS, ".ilog").data(),
                                           Buffer::build(basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, "_", createdTS, ".ilog.old").data()) == -1) {
                                        throw Switch::system_error("Failed to rename files:", strerror(errno));
                                }
                        } else {
                                if (Rename(Buffer::build(basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, ".ilog").data(),
                                           Buffer::build(basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, ".ilog.old").data()) == -1) {
                                        throw Switch::system_error("Failed to rename files:", strerror(errno));
                                }
                        }

                        if (Rename(Buffer::build(basePartitionPath, "/", it->baseSeqNum, ".index").data(),
                                   Buffer::build(basePartitionPath, "/", it->baseSeqNum, ".index.old").data()) == -1) {
                                throw Switch::system_error("Failed to rename files:", strerror(errno));
                        }
                }

                // Strip .swap extension from the set of new segments files
                for (auto it : newSegments) {
                        if (Rename(Buffer::build(destPartitionPath, it->baseSeqNum, "-", it->lastAvailSeqNum, "_", it->createdTS, ".ilog.swap").data(),
                                   Buffer::build(destPartitionPath, it->baseSeqNum, "-", it->lastAvailSeqNum, "_", it->createdTS, ".ilog").data()) == -1) {
                                throw Switch::system_error("Failed to rename files:", strerror(errno));
                        }

                        if (Rename(Buffer::build(destPartitionPath, "/", it->baseSeqNum, ".index.swap").data(),
                                   Buffer::build(destPartitionPath, "/", it->baseSeqNum, ".index").data()) == -1) {
                                throw Switch::system_error("Failed to rename files:", strerror(errno));
                        }
                }

                // Unlink all input segment files
                for (auto it : prevSegments) {
                        char   path[PATH_MAX];
                        size_t pathLen;

                        if (const auto createdTS = it->createdTS)
                                pathLen = Snprint(path, sizeof(path), basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, "_", createdTS, ".ilog.old");
                        else
                                pathLen = Snprint(path, sizeof(path), basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, ".ilog.old");

                        if (Unlink(path) == -1)
                                throw Switch::system_error("Failed to remove ", strwlen32_t(path, pathLen), ": ", strerror(errno));

                        Snprint(path, sizeof(path), basePartitionPath, "/", it->baseSeqNum, ".index.old");
                        if (Unlink(path) == -1)
                                throw Switch::system_error("Failed to unlink file:", strerror(errno));
                }

                // Replace segments
                run_on_main_thread([log, segments = std::move(prevSegments), newSegments = std::move(newSegments)]() {
                        auto       roSegments = log->roSegments.get();
                        auto       it         = std::find(roSegments->begin(), roSegments->end(), segments.front());
                        const auto upto       = segments.back()->lastAvailSeqNum;

                        EXPECT(it != roSegments->end());

                        // remove segments from current roSegments[]
                        std::for_each(it, it + segments.size(), [](auto ptr) { delete ptr; });
                        roSegments->erase(it, it + segments.size());

                        // replace removed segments with new segments
                        roSegments->insert(it, newSegments.begin(), newSegments.end());

                        log->compacting = false;
                        if (!log->lastCleanupMaxSeqNum) {
                                cleanupTracker.push_back(log);
                        }
                        log->lastCleanupMaxSeqNum = upto;

                        this_service->schedule_cleanup();

                        if (trace) {
                                for (auto it : *roSegments)
                                        SLog("(", it->baseSeqNum, ", ", it->lastAvailSeqNum, ") ", it->fdh.use_count(), " ", it->fdh->fd, "\n");
                        }

                        Print("Compacted partition segments\n");
                });
        } catch (...) {
                while (!newSegments.empty()) {
                        delete newSegments.back();
                        newSegments.pop_back();
                }

                run_on_main_thread([log]() {
                        Print("Failed to compact partition segments\n");
                        log->compacting = false;
                });
        }
}

void topic_partition_log::compact(const char *const basePartitionPath) {
        std::vector<ro_segment *>      prevSegments;
        static std::once_flag          onceFlag;
        static std::condition_variable workCond;
        static std::mutex              workLock;

        struct pending_compaction final {
                pending_compaction *      next;
                char                      basePartitionPath[PATH_MAX];
                std::vector<ro_segment *> prevSegments;
                topic_partition_log *     log;
        };

        Drequire(compacting == false);

        static PubSubQueue<pending_compaction> pendingCompactions;
        auto                                   compaction = new pending_compaction();
        const auto                             l          = strlen(basePartitionPath);

        require(l < sizeof(compaction->basePartitionPath));
        compaction->log = this;
        strwlen32_t(basePartitionPath, l).ToCString(compaction->basePartitionPath);
        compaction->prevSegments.reserve(roSegments->size());
        for (auto it : *roSegments)
                compaction->prevSegments.push_back(it);

        if (trace)
                SLog("Compaction for [", basePartitionPath, "]\n");

        Drequire(compaction->prevSegments.size());

        std::call_once(onceFlag, [] {
                std::thread([]() {
                        std::vector<pending_compaction *> localWork;
                        sigset_t                          mask;

                        sigfillset(&mask);
                        pthread_sigmask(SIG_SETMASK, &mask, nullptr);
                        for (;;) {
                                std::unique_lock<std::mutex> lock(workLock);

                                workCond.wait(lock, [] { return pendingCompactions.any(); });
                                for (auto it = pendingCompactions.drain(); it; it = it->next)
                                        localWork.push_back(it);

                                lock.unlock();

                                while (!localWork.empty()) {
                                        auto c = localWork.back();

                                        try {
                                                compact_partition(c->log, c->basePartitionPath, std::move(c->prevSegments));
                                        } catch (...) {
                                        }

                                        localWork.pop_back();
                                        delete c;
                                }
                        }
                })
                    .detach();
        });

        compacting = true;
        pendingCompactions.push_back(compaction);
        workCond.notify_one();
}

// Aligns to indices boundaries
lookup_res topic_partition_log::range_for(uint64_t absSeqNum, const uint32_t maxSize, uint64_t maxAbsSeqNum) {
        if (trace)
                SLog("Read for absSeqNum = ", absSeqNum, ", lastAssignedSeqNum = ", lastAssignedSeqNum, ", firstAvailableSeqNum = ", firstAvailableSeqNum, "\n");

        // (UINT64_MAX) is reserved: fetch only newly produced bundles
        // see process_consume()
        // (0) is also reserved; start from the first available sequence
        if (absSeqNum == 0) {
                if (trace)
                        SLog("Asked to fetch starting from first available sequence number ", firstAvailableSeqNum, "\n");

                if (firstAvailableSeqNum)
                        absSeqNum = firstAvailableSeqNum;
                else {
                        // No content at all
                        absSeqNum = 1;

                        if (trace)
                                SLog("No content, will start from ", absSeqNum, " (lastAssignedSeqNum = ", lastAssignedSeqNum, ")\n");
                }
        }

        const auto highWatermark = lastAssignedSeqNum;

        if (maxSize == 0) {
                // Should be handled by the client
                if (trace)
                        SLog("maxSize == 0\n");

                return {lookup_res::Fault::Empty, highWatermark};
        } else if (maxAbsSeqNum < absSeqNum) {
                // Should be handled by the client
                if (trace)
                        SLog("Past maxAbsSeqNum = ", maxAbsSeqNum, "\n");

                return {lookup_res::Fault::Empty, highWatermark};
        } else if (absSeqNum == lastAssignedSeqNum + 1) {
                // return empty
                // UPDATE: let's wait instead
                if (trace)
                        SLog("Empty\n");

                return {lookup_res::Fault::AtEOF, highWatermark};
        } else if (absSeqNum > lastAssignedSeqNum) {
                // past last assigned sequence number? nope
                // throw an error
                if (trace)
                        SLog("PAST absSeqNum(", absSeqNum, ") > lastAssignedSeqNum(", lastAssignedSeqNum, ")\n");

                return {lookup_res::Fault::BoundaryCheck, highWatermark};
        } else if (absSeqNum < firstAvailableSeqNum) {
                if (trace)
                        SLog("< firstAvailableSeqNum\n");

                return {lookup_res::Fault::BoundaryCheck, highWatermark};
        } else if (absSeqNum >= cur.baseSeqNum) {
                // Great, definitely in the current segment
                if (trace)
                        SLog("absSeqNum >= cur.baseSeqNum(", cur.baseSeqNum, "). Will get from current\n");

                return read_cur(absSeqNum, maxSize, maxAbsSeqNum);
        }

        auto prevSegments = roSegments;

        const auto end = prevSegments->end();
        auto       it  = std::upper_bound_or_match(prevSegments->begin(), end, absSeqNum, [](const auto s, const auto absSeqNum) {
                return TrivialCmp(absSeqNum, s->baseSeqNum);
        });

        if (it != end) {
                // Solves:  https://github.com/phaistos-networks/TANK/issues/2
                // for e.g (1,26), (28, 50)
                // when you request 27 which may no longer exist because of compaction/cleanup, we need
                // to properly advance to the _next_ segment, and adjust absSeqNum
                // accordingly
                // (TODO: we should probably come up with a binary search alternative to this linear search scan)
                auto f = *it;

                while (absSeqNum > f->lastAvailSeqNum && ++it != end) {
                        if (trace)
                                SLog("Adjusting ", absSeqNum, " to ", (*it)->baseSeqNum, "\n");

                        f         = *it;
                        absSeqNum = f->baseSeqNum;
                }

                const auto res = f->translateDown(absSeqNum, UINT32_MAX);
                uint32_t   offsetCeil;

                Drequire(f->fdh.use_count());
                Drequire(f->fdh->fd != -1);

                if (trace) {
                        SLog("Found in RO segment (", f->baseSeqNum, ", ", f->lastAvailSeqNum, ")\n");
                        for (const auto &it : *prevSegments)
                                SLog(">> (", it->baseSeqNum, ", ", it->lastAvailSeqNum, ")\n");
                }

                if (maxAbsSeqNum != UINT64_MAX) {
                        // Scan forward for the last message with absSeqNum < maxAbsSeqNum
                        const auto r = f->snapDown(maxAbsSeqNum);

                        if (trace)
                                SLog("maxAbsSeqNum = ", maxAbsSeqNum, " => ", r, "\n");

#if 0
			offsetCeil = r.second;  // XXX: we actually need to set this to (it + 1).absPhysical
#else
                        // Yes, incur some tiny I/O overhead so that we 'll properly cut-off the content
                        offsetCeil = search_before_offset(f->baseSeqNum, maxSize, maxAbsSeqNum, f->fdh->fd, f->fileSize, r.second);
#endif
                } else
                        offsetCeil = f->fileSize;

                if (trace)
                        SLog("offsetCeil = ", offsetCeil, ", fileSize = ", f->fileSize, "\n");

                return {f->fdh, offsetCeil, f->baseSeqNum + res.record.relSeqNum, res.record.absPhysical, highWatermark};
        }

        if (trace)
                SLog("No segment suitable for absSeqNum\n");

        // No segment, either one of the immutalbe segments or the current segment, that holds
        // a message with seqNum >= absSeqNum
        //
        // so just point to the first(oldest) segment
        if (!prevSegments->empty()) {
                const auto f = prevSegments->front();

                if (trace)
                        SLog("Will use first R/O segment\n");

                return {f->fdh, f->fileSize, f->baseSeqNum, 0, highWatermark};
        } else {
                if (trace)
                        SLog("Will use current segment\n");

                return {cur.fdh, cur.fileSize, cur.baseSeqNum, 0, highWatermark};
        }
}

void topic_partition_log::consider_ro_segments() {
        if (compacting) {
                // Busy compacting
                if (trace)
                        SLog("Compacting\n");

                return;
        }

        uint64_t       sum{0};
        const uint32_t nowTS = time(nullptr);

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_blue, "Considering segments sum=", sum, ", total = ", roSegments->size(), " limits { roSegmentsCnt ", config.roSegmentsCnt, ", roSegmentsSize ", config.roSegmentsSize, "}", ansifmt::reset, "\n");

        if (config.logCleanupPolicy == CleanupPolicy::DELETE) {
                if (trace)
                        SLog("DELETE policy\n");

                for (auto it : *roSegments)
                        sum += it->fileSize;

                while (!roSegments->empty() && ((config.roSegmentsCnt && roSegments->size() > config.roSegmentsCnt) || (config.roSegmentsSize && sum > config.roSegmentsSize) || (roSegments->front()->createdTS && config.lastSegmentMaxAge && roSegments->front()->createdTS + config.lastSegmentMaxAge < nowTS))) {
                        Buffer basePath;

                        basePath.append(basePath_, "/", partition->owner->name(), "/", partition->idx, "/");

                        auto       segment     = roSegments->front();
                        const auto basePathLen = basePath.size();

                        if (trace)
                                SLog(ansifmt::bold, ansifmt::color_red, "Removing ", segment->baseSeqNum, ansifmt::reset, "\n");

                        basePath.append("/", segment->baseSeqNum, "-", segment->lastAvailSeqNum, "_", segment->createdTS, ".ilog");
                        if (Unlink(basePath.data()) == -1)
                                Print("Failed to unlink ", basePath, ": ", strerror(errno), "\n");
                        else if (trace)
                                SLog("Removed ", basePath, "\n");

                        basePath.resize(basePathLen);
                        basePath.append("/", segment->baseSeqNum, ".index");
                        if (Unlink(basePath.data()) == -1)
                                Print("Failed to unlink ", basePath, ": ", strerror(errno), "\n");
                        else if (trace)
                                SLog("Removed ", basePath, "\n");

                        basePath.resize(basePathLen);

                        segment->fdh.reset(nullptr);

                        sum -= segment->fileSize;
                        delete segment;

                        roSegments->erase(roSegments->begin()); // pop_front()
                }

                if (!roSegments->empty())
                        firstAvailableSeqNum = roSegments->front()->baseSeqNum;
                else
                        firstAvailableSeqNum = cur.baseSeqNum;

                if (trace)
                        SLog("firstAvailableSeqNum now = ", firstAvailableSeqNum, "\n");
        } else if (config.logCleanupPolicy == CleanupPolicy::CLEANUP) {
                const auto firstDirtyOffset = first_dirty_offset();
                uint64_t   dirtyBytes{0};

                if (trace)
                        SLog("CLEANUP policy ", firstDirtyOffset, "\n");

                for (auto it : *roSegments) {
                        if (it->baseSeqNum >= firstDirtyOffset)
                                dirtyBytes += it->fileSize;

                        sum += it->fileSize;
                }

                const double cleanable_ratio = double(dirtyBytes) / double(sum);

                if (trace)
                        SLog(ansifmt::color_blue, "dirtyBytes = ", dirtyBytes, ", sum = ", sum, ", cleanable_ratio = ", cleanable_ratio, ansifmt::reset, "\n");

                if (cleanable_ratio >= config.logCleanRatioMin) {
                        compact(Buffer::build(basePath_, "/", partition->owner->name(), "/", partition->idx, "/").data());
                }
        }
}

bool topic_partition_log::should_roll(const uint32_t now) const {
        if (cur.fileSize == UINT32_MAX)
                return true;
        else if (cur.fileSize) {
                if (trace)
                        SLog(ansifmt::color_green, " Consider roll:cur.fileSize(", cur.fileSize, "), config.maxSegmentSize(", config.maxSegmentSize, "), skipList.size(", cur.index.skipList.size(), "), config.curSegmentMaxAge (", config.curSegmentMaxAge, "), ", Timings::Seconds::SysTime() - cur.createdTS, " old,  cur.rollJitterSecs = ", cur.rollJitterSecs, ansifmt::reset, "\n");

                if (cur.fileSize > config.maxSegmentSize) {
                        if (trace)
                                SLog(ansifmt::bold, "Should roll: cur.fileSize(", cur.fileSize, ") > config.maxSegmentSize(", config.maxSegmentSize, ")", ansifmt::reset, "\n");

                        return true;
                }

                const size_t curIndexSizeBytes = cur.index.ondisk.span + (cur.index.skipList.size() * (sizeof(uint32_t) + sizeof(uint32_t)));

                if (curIndexSizeBytes > config.maxIndexSize) {
                        // index is full
                        if (trace)
                                SLog(ansifmt::bold, "curIndexSizeBytes(", curIndexSizeBytes, ") > config.maxIndexSize(", config.maxIndexSize, ")", ansifmt::reset, "\n");

                        return true;
                }

                if (const auto v = config.curSegmentMaxAge) {
                        if (now - cur.createdTS > v - cur.rollJitterSecs) {
                                // Soft limit
                                if (trace)
                                        SLog(ansifmt::bold, "now - cur.createdTS(", now - cur.createdTS, ") > (", v - cur.rollJitterSecs, ")", ansifmt::reset, "\n");

                                return true;
                        }
                }
        }

        return false;
}

bool topic_partition_log::may_switch_index_wide(const uint64_t lastMsgSeqNum) {
        // This is required for proper support for sparse segments
        // maybe instead of transforming the index we should instead roll?
        if (trace)
                SLog("considering switch to wide index, lastMsgSeqNum = ", lastMsgSeqNum, ", cur.baseSeqNum = ", cur.baseSeqNum, "\n");

        if (unlikely(lastMsgSeqNum - cur.baseSeqNum > INT32_MAX)) // arbitrary
        {
                IMPLEMENT_ME();
                cur.index.haveWideEntries = true;
        }

        return false;
}

// if (firstMsgSeqNum != 0 && lastMsgSeqNum != 0), we have expicitly specified message sequence numbers for the bundle first/last message
append_res topic_partition_log::append_bundle(const time_t now, const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt, const uint64_t firstMsgSeqNum, const uint64_t lastMsgSeqNum) {
        const auto savedLastAssignedSeqNum = lastAssignedSeqNum;
        const auto absSeqNum               = firstMsgSeqNum ?: lastAssignedSeqNum + 1;

        if (unlikely(absSeqNum < cur.baseSeqNum && cur.baseSeqNum != UINT64_MAX)) {
                // This is odd
                throw Switch::data_error("Unexpected absSeqNum(", absSeqNum, ") < cur.baseSeqNum(", cur.baseSeqNum, ") for ", partition->owner->name(), "/", partition->idx);
        }

        EXPECT(bundleMsgsCnt);

        if (lastMsgSeqNum) {
                // Sparse bundle; last message seqNum encoded in the bundle header
                lastAssignedSeqNum = lastMsgSeqNum;
        } else if (firstMsgSeqNum) {
                // either sparse bundle(first message encoded in the bundle header), or bundle in
                // a TankAPIMsgType::ProduceWithBaseSeqNum request, where the bundle is encoded in the partition header
                lastAssignedSeqNum = firstMsgSeqNum + bundleMsgsCnt - 1;
        } else {
                lastAssignedSeqNum += bundleMsgsCnt;
        }

        if (trace) {
                SLog("firstMsgSeqNum(", firstMsgSeqNum, "), lastMsgSeqNum(", lastMsgSeqNum, "), bundleMsgsCnt(", bundleMsgsCnt, ") => ", lastAssignedSeqNum, "\n");
        }

        if (should_roll(now)) {
                Buffer basePath;

                basePath.append(basePath_, "/", partition->owner->name(), "/", partition->idx, "/");

                const auto basePathLen = basePath.size();

                if (trace)
                        SLog("Need to switch to another commit log (", cur.fileSize, "> ", config.maxSegmentSize, ") ", cur.index.skipList.size(), " ", basePath, "\n");

                if (cur.fileSize != UINT32_MAX) {
// See ro_segment::createdTS declaration comments
#if 0
			const auto freezeTs = cur.createdTS;
#else
                        struct stat st;

                        if (fstat(cur.fdh->fd, &st) == -1)
                                throw Switch::system_error("fstat() failed:", strerror(errno));

                        const uint64_t freezeTs = st.st_mtime;
#endif
                        auto       newROFiles = std::make_unique<std::vector<ro_segment *>>();
                        auto       newROFile  = std::make_unique<ro_segment>(cur.baseSeqNum, savedLastAssignedSeqNum, freezeTs);
                        const auto n          = cur.fdh.use_count();

                        require(n >= 1);
                        newROFile->fdh = cur.fdh;
                        require(cur.fdh.use_count() == n + 1);
                        newROFile->fileSize = cur.fileSize;

                        newROFile->index.fileSize               = lseek64(cur.index.fd, 0, SEEK_END);
                        newROFile->index.lastRecorded.relSeqNum = newROFile->index.lastRecorded.absPhysical = 0;

                        if (cur.index.haveWideEntries) {
                                IMPLEMENT_ME();
                        }

                        // We now encode the [first,last] range into the filename for simplicity and future-proofing; we 'd like to
                        // support sparse sequence numbers space
                        if (cur.nameEncodesTS) {
                                if (Rename(Buffer::build(basePath, "/", cur.baseSeqNum, "_", cur.createdTS, ".log").data(),
                                           Buffer::build(basePath, "/", cur.baseSeqNum, "-", savedLastAssignedSeqNum, "_", freezeTs, ".ilog").data()) == -1) {
                                        throw Switch::system_error("Failed to Rename():", strerror(errno));
                                }
                        } else if (Rename(Buffer::build(basePath, "/", cur.baseSeqNum, ".log").data(),
                                          Buffer::build(basePath, "/", cur.baseSeqNum, "-", savedLastAssignedSeqNum, "_", freezeTs, ".ilog").data()) == -1) {
                                throw Switch::system_error("Failed to Rename():", strerror(errno));
                        }

                        if (newROFile->index.fileSize) {
                                newROFile->index.data = static_cast<uint8_t *>(mmap(nullptr, newROFile->index.fileSize, PROT_READ, MAP_SHARED, cur.index.fd, 0));

                                if (unlikely(newROFile->index.data == MAP_FAILED))
                                        throw Switch::system_error("mmap() failed:", strerror(errno));

                                madvise((void *)newROFile->index.data, newROFile->index.fileSize, MADV_DONTDUMP);

                                if (newROFile->index.fileSize >= sizeof(uint32_t) + sizeof(uint32_t)) {
                                        const auto *const p = (uint32_t *)(newROFile->index.data + newROFile->index.fileSize - sizeof(uint32_t) - sizeof(uint32_t));

                                        newROFile->index.lastRecorded.relSeqNum   = p[0];
                                        newROFile->index.lastRecorded.absPhysical = p[1];

                                        if (trace)
                                                SLog("set lastRecorded to ", newROFile->index.lastRecorded.relSeqNum, ", ", newROFile->index.lastRecorded.absPhysical, "\n");
                                }
                        } else
                                newROFile->index.data = nullptr;

                        const auto prevSize = newROFiles->size();

                        newROFiles->insert(newROFiles->end(), roSegments->begin(), roSegments->end());
                        require(newROFiles->size() == prevSize + roSegments->size());
                        newROFiles->push_back(newROFile.release());

                        roSegments.reset(newROFiles.release());
                        consider_ro_segments();
                } else {
                        // first segment for this partition
                        firstAvailableSeqNum = absSeqNum;
                }

                cur.baseSeqNum = absSeqNum;
                cur.fileSize   = 0;
                // It is very important than the first bundle's recorded immediately in the index
                // so we set cur.sinceLastUpdate = UINT32_MAX to force that
                cur.sinceLastUpdate       = UINT32_MAX;
                cur.createdTS             = time(nullptr);
                cur.nameEncodesTS         = true;
                cur.index.haveWideEntries = false;

                cur.index.skipList.clear();
                fsync(cur.index.fd);
                close(cur.index.fd);

                if (cur.index.ondisk.data != nullptr && cur.index.ondisk.data != MAP_FAILED) {
                        madvise((void *)cur.index.ondisk.data, cur.index.ondisk.span, MADV_DONTNEED);
                        munmap((void *)cur.index.ondisk.data, cur.index.ondisk.span);
                        cur.index.ondisk.data = nullptr;
                }

                basePath.append(cur.baseSeqNum, "_", cur.createdTS, ".log");

                cur.fdh.reset(new fd_handle(open(basePath.data(), read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND), 0775)));
                require(cur.fdh->use_count() == 2);
                cur.fdh->Release();

                if (cur.fdh->fd == -1)
                        throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot load segment log");

                basePath.resize(basePathLen);
                basePath.append(cur.baseSeqNum, ".index");
                cur.index.fd = open(basePath.data(), read_only ? O_RDWR : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND), 0775);
                basePath.resize(basePathLen);

                if (cur.index.fd == -1)
                        throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot load segment index");

                if (const uint32_t max = config.maxRollJitterSecs) {
                        std::random_device                      dev;
                        std::mt19937                            rng(dev());
                        std::uniform_int_distribution<uint32_t> distr(0, max);

                        cur.rollJitterSecs = distr(rng);
                } else
                        cur.rollJitterSecs = 0;

                cur.flush_state.pendingFlushMsgs = 0;
                cur.flush_state.nextFlushTS      = config.flushIntervalSecs ? now + config.flushIntervalSecs : UINT32_MAX;

                if (trace)
                        SLog("Switched\n");
        } else {
                if (lastMsgSeqNum && !cur.index.haveWideEntries) {
                        // This may be necessary
                        may_switch_index_wide(lastMsgSeqNum);
                }

                if (unlikely(cur.index.skipList.size() > 65536)) {
                        // Implements https://github.com/phaistos-networks/TANK/issues/27
                        if (trace)
                                SLog(ansifmt::bold, ansifmt::color_red, "Emptying skiplist", ansifmt::reset, "\n");

                        if (cur.index.ondisk.data != nullptr && cur.index.ondisk.data != MAP_FAILED) {
                                madvise((void *)cur.index.ondisk.data, cur.index.ondisk.span, MADV_DONTNEED);
                                munmap((void *)cur.index.ondisk.data, cur.index.ondisk.span);
                        }

                        cur.index.ondisk.span     = lseek64(cur.index.fd, 0, SEEK_END);
                        cur.index.ondisk.data     = static_cast<const uint8_t *>(mmap(nullptr, cur.index.ondisk.span, PROT_READ, MAP_SHARED, cur.index.fd, 0));
                        cur.index.haveWideEntries = false;

                        if (cur.index.ondisk.data == MAP_FAILED)
                                throw Switch::system_error("mmap() failed:", strerror(errno));

                        madvise((void *)cur.index.ondisk.data, cur.index.ondisk.span, MADV_DONTDUMP);

                        cur.index.skipList.clear();
                }
        }

        require(cur.fdh.use_count() >= 1);

        uint8_t            varint[8];
        const uint8_t      varintLen = Compression::PackUInt32(bundleSize, varint) - varint;
        auto               fd        = cur.fdh->fd;
        const struct iovec iov[] =
            {
                {(void *)varint, varintLen},
                {const_cast<void *>(bundle), bundleSize}};
        const auto                       entryLen = iov[0].iov_len + iov[1].iov_len;
        const range32_t                  fileRange(cur.fileSize, entryLen);
        const auto                       before = cur.fdh.use_count();
        Switch::shared_refptr<fd_handle> fdh(cur.fdh);
        const auto                       b = trace ? Timings::Microseconds::Tick() : uint64_t(0);

        require(cur.fdh.use_count() == before + 1);

        // https://github.com/phaistos-networks/TANK/issues/14
        if (unlikely(writev(fd, iov, sizeof_array(iov)) != entryLen)) {
                RFLog("Failed to writev():", strerror(errno), "\n");
                lastAssignedSeqNum = savedLastAssignedSeqNum;
                return {nullptr, {}, {}};
        } else {
                if (trace)
                        SLog("writev() took ", duration_repr(Timings::Microseconds::Since(b)), "\n");

                // Even if we fail to update the index, that's not a big deal because
                // 1. we can always rebuild the index 2. we use the index to locate the closest bundle to the target sequence number
                if (cur.sinceLastUpdate > config.indexInterval) {
                        require(absSeqNum >= cur.baseSeqNum); // sanity check

                        const uint32_t out[] = {uint32_t(absSeqNum - cur.baseSeqNum), cur.fileSize};

                        cur.index.skipList.push_back({out[0], out[1]});

                        if (trace)
                                SLog(">> ", out[0], ", ", out[1], " ", cur.index.skipList.size(), "\n");

                        if (unlikely(write(cur.index.fd, out, sizeof(out)) != sizeof(out))) {
                                RFLog("Failed to write():", strerror(errno), "\n");
                                // don't restore neither lastAssignedSeqNum from savedLastAssignedSeqNum,  nor fileSize
                                // because this has been accepted
                                return {nullptr, {}, {}};
                        }

                        if (0 == cur.fileSize) {
                                // Make sure we get that first record synced
                                fsync(cur.index.fd);
                        }
                        cur.sinceLastUpdate = 0;
                }

                cur.fileSize += entryLen;
                cur.sinceLastUpdate += entryLen;

                cur.flush_state.pendingFlushMsgs += bundleMsgsCnt;

                if (trace)
                        SLog("cur.flush_state.pendingFlushMsgs = ", cur.flush_state.pendingFlushMsgs, ", config.flushIntervalMsgs = ", config.flushIntervalMsgs, ", config.flushIntervalMsgs = ", config.flushIntervalMsgs, "\n");

                if (config.flushIntervalMsgs && cur.flush_state.pendingFlushMsgs >= config.flushIntervalMsgs) {
                        if (trace)
                                SLog("Scheduling flush\n");

                        schedule_flush(now);
                } else if (now >= cur.flush_state.nextFlushTS) {
                        if (trace)
                                SLog("Scheduling flush\n");

                        schedule_flush(now);
                }

                return {fdh, fileRange, {absSeqNum, uint16_t(bundleMsgsCnt)}};
        }

        // return here and not in (writev() ! entryLen) check because some older compilers warn about
        // a path with no return from a non-void function. Sigh
        return {nullptr, {}, {}};
}

void topic_partition_log::schedule_flush(const uint32_t now) {
        cur.flush_state.pendingFlushMsgs = 0;
        cur.flush_state.nextFlushTS      = now + config.flushIntervalSecs;

        // This is obviously not optimal; we should have used a bounded queue, or some lock/wait-free construct
        // but given this is a rare event, it's not worth it yet
        mboxLock.lock();
        mbox.push_back({cur.fdh->fd, cur.index.fd});
        mboxLock.unlock();

        mbox_cv.notify_one();
}

// XXX: this works great for standalone mode, but when the highwater mark is based on an ISR, which means
// it doesn't get incremented immediately as soon as the leader appends the bundle to its local segment, waking up
// any waiting consumers is not going to work. We likely need to do this only iff running in standalone mode, otherwise
// only when the highwater mark is updated.
// i.e opMode == OperationMode::Standalone
void topic_partition::consider_append_res(append_res &res, Switch::vector<wait_ctx *> &waitCtxWorkL) {
        bool newLogFile{false};

        if (trace)
                SLog(ansifmt::color_blue, " waitingList.size() = ", waitingList.size(), ansifmt::reset, "\n");

        for (uint32_t i{0}; i < waitingList.size();) // for all wait contexts this partition is registered with
        {
                auto it = waitingList[i];

                for (uint8_t i{0}; i != it->partitionsCnt; ++i) {
                        auto &ctxP = it->partitions[i];

                        if (ctxP.partition != this)
                                continue;

                        if (!ctxP.fdh) {
                                const auto __n = res.fdh->use_count();

                                ctxP.fdh = res.fdh.get();
                                ctxP.fdh->Retain();

                                require(ctxP.fdh->use_count() == __n + 1);

                                ctxP.range       = res.dataRange;
                                ctxP.seqNum      = res.msgSeqNumRange.offset;
                                it->capturedSize = res.dataRange.len;

                                if (trace)
                                        SLog("Just registered fdh for wait ctx, capturedSize(", it->capturedSize, "), range = ", ctxP.range, " ", ptr_repr(ctxP.fdh), " ", ctxP.fdh->use_count(), "\n");
                        } else if (res.fdh.get() != ctxP.fdh) {
                                // Switched to another log file
                                newLogFile = true;

                                if (trace)
                                        SLog("Switched to a new fdh for wait ctx\n");
                        } else {
                                // extend the range
                                ctxP.range.len += res.dataRange.len;
                                it->capturedSize += res.dataRange.len;

                                if (trace)
                                        SLog("Extending range, capturedSize(", it->capturedSize, "), range ", ctxP.range, "\n");
                        }

                        break;
                }

                if (newLogFile || it->capturedSize >= it->minBytes) {
                        if (trace)
                                SLog("Go either newLogFile(", newLogFile, "), or capturedSize(", it->capturedSize, ") >= minBytes(", it->minBytes, ")\n");

                        waitCtxWorkL.push_back(it);
                        waitingList.PopByIndex(i);
                } else
                        ++i;
        }
}

bool topic_partition::foreach_msg(std::function<bool(topic_partition::msg &)> &l) const {
        const auto scan_vma = [&](const auto fileData, const auto fileSize, auto seqNum) {
                topic_partition::msg                     msg;
                IOBuffer                                 db;
                uint64_t                                 firstMsgSeqNum, lastMsgSeqNum;
                range_base<const uint8_t *, std::size_t> msgSetContent;

                for (const auto *p = static_cast<const uint8_t *>(fileData), *const e = p + fileSize; p != e;) {
                        const auto bundleLen          = Compression::UnpackUInt32(p);
                        const auto nextBundle         = p + bundleLen;
                        const auto bundleFlags        = *p++;
                        const auto codec              = bundleFlags & 3;
                        const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                        uint32_t   msgsSetSize        = (bundleFlags >> 2) & 0xf;

                        if (!msgsSetSize)
                                msgsSetSize = Compression::UnpackUInt32(p);

                        if (sparseBundleBitSet) {
                                firstMsgSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

                                if (msgsSetSize != 1)
                                        lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                                else
                                        lastMsgSeqNum = firstMsgSeqNum;
                        }

                        if (codec) {
                                db.clear();
                                if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, nextBundle - p, &db))
                                        throw Switch::system_error("failed to decompress message set");

                                msgSetContent.Set(reinterpret_cast<const uint8_t *>(db.data()), db.size());
                        } else
                                msgSetContent.Set(p, nextBundle - p);

                        // advance ptr to the next bundle
                        p = nextBundle;

                        // parse current bundle
                        uint32_t msgIdx{0};

                        msg.ts = 0;
                        for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.size(); p != e; ++msgIdx, ++seqNum) {
                                const auto flags{*p++};

                                if (sparseBundleBitSet) {
                                        if (msgIdx == 0)
                                                seqNum = firstMsgSeqNum;
                                        else if (msgIdx == msgsSetSize - 1)
                                                seqNum = lastMsgSeqNum;
                                        else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                // incremented in for() (in previous loop iteration)
                                        } else {
                                                // we encode delta from last - 1, but we already ++seqNum in for() (in previous iteration)
                                                seqNum += Compression::UnpackUInt32(p);
                                        }
                                }

                                if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                        msg.ts = *(uint64_t *)p;
                                        p += sizeof(uint64_t);
                                }

                                if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                        msg.key.Set((char *)p + 1, *p);
                                        p += msg.key.size() + sizeof(uint8_t);
                                } else
                                        msg.key.reset();

                                if (const auto msgLen = Compression::UnpackUInt32(p)) {
                                        msg.data.Set(reinterpret_cast<const char *>(p), msgLen);
                                        p += msgLen;
                                } else
                                        msg.data.reset();

                                msg.seqNum = seqNum;
                                if (false == l(msg)) {
                                        // no need to consume any more messages
                                        return false;
                                }
                        }
                }

                return true;
        };

        auto log = log_.get();

        for (const auto seg : *log->roSegments) {
                const auto fileSize{seg->fileSize};
                auto       fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, seg->fdh->fd, -1);

                if (fileData == MAP_FAILED)
                        throw Switch::data_error("Failed to mmap() partition log segment");

                DEFER({
                        munmap(fileData, fileSize);
                });

                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

                if (!scan_vma(fileData, fileSize, seg->baseSeqNum))
                        return false;
        }

        if (const auto fileSize = log->cur.fileSize) {
                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, log->cur.fdh->fd, -1);

                if (fileData == MAP_FAILED)
                        throw Switch::data_error("Failed to mmap() partition log segment");

                DEFER({
                        munmap(fileData, fileSize);
                });

                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

                if (!scan_vma(fileData, fileSize, log->cur.baseSeqNum)) {
                        return false;
                }
        }

        return true;
}

append_res topic_partition::append_bundle_to_leader(const time_t curTime, const uint8_t *const bundle, const size_t bundleLen, const uint32_t bundleMsgsCnt, Switch::vector<wait_ctx *> &waitCtxWorkL, const uint64_t firstMsgSeqNum, const uint64_t lastMsgSeqNum) {
        // TODO(markp): route to leader
        try {
                if (lastMsgSeqNum) {
                        // Sparse bundle; last message seqNum encoded in the bundle header
                        if (unlikely(lastMsgSeqNum < firstMsgSeqNum)) {
                                if (trace)
                                        SLog("Unexpected, lastMsgSeqNum(", lastMsgSeqNum, ") < firstMsgSeqNum(", firstMsgSeqNum, ")\n");

                                return {nullptr, {0, UINT32_MAX}, {}};
                        } else if (unlikely(firstMsgSeqNum <= log_->lastAssignedSeqNum)) {
                                if (trace)
                                        SLog("Unexpected, firstMsgSeqNum(", firstMsgSeqNum, ") <= lastAssignedSeqNum(", log_->lastAssignedSeqNum, ")\n");

                                return {nullptr, {0, UINT32_MAX}, {}};
                        }
                } else if (unlikely(firstMsgSeqNum && firstMsgSeqNum <= log_->lastAssignedSeqNum)) {
                        // either sparse bundle(first message encoded in the bundle header), or bundle in
                        // a TankAPIMsgType::ProduceWithBaseSeqNum request, where the bundle is encoded in the partition header
                        if (trace)
                                SLog("Unexpected, firstMsgSeqNum(", firstMsgSeqNum, ") <= lastAssignedSeqNum(", log_->lastAssignedSeqNum, ")\n");

                        return {nullptr, {0, UINT32_MAX}, {}};
                }

                auto res = log_->append_bundle(curTime, bundle, bundleLen, bundleMsgsCnt, firstMsgSeqNum, lastMsgSeqNum);

                if (trace)
                        SLog("Appended to ", ptr_repr(this), " ", res.fdh ? res.fdh->use_count() : 0, "\n");

                consider_append_res(res, waitCtxWorkL);

                if (trace)
                        SLog("after consider_append_res() ", res.fdh ? res.fdh->use_count() : 0, "\n");

                return res;
        } catch (const std::exception &e) {
                RFLog("Failed, cought exception:", e.what(), "\n");
                return {nullptr, {}, {}};
        }
}

lookup_res topic_partition::read_from_local(const bool /*fetchOnlyFromLeader*/, const bool /*fetchOnlyComittted*/, const uint64_t absSeqNum, const uint32_t fetchSize) {
        // TODO(markp):
        // For a multi-node configurations, maxAbsSeqNum should be set to the last committed absolute sequence number (i.e highwater mark)
        // range_for() will identify the ceiling file offset based on that
        //
        // search_before_offset() will need to access the segment log file though, and that may not be optimal; instead
        // we may just want to rely on the client stopping processing chunk bundles if it reaches message id > maxAbsSeqNum(provided
        // in the fetch response), which it already takes into account. This makes sense because we use the index to quickly identify
        // a ceiling close to the last confirmed sequence anyway, and this is aligned on index interval, which is usually 4k
        const uint64_t maxAbsSeqNum{UINT64_MAX};

        if (trace)
                SLog("Fetching log segment for partition ", idx, ", abs.sequence number ", absSeqNum, ", fetchSize ", fetchSize, ", maxAbsSeqNum = ", maxAbsSeqNum, "\n");

        return log_->range_for(absSeqNum, fetchSize, maxAbsSeqNum);
}

static uint32_t parse_duration(strwlen32_t in) {
        uint32_t sum{0};

        do {
                uint32_t i{0}, n{0}, scale{1};

                for (i = 0; i != in.len && isdigit(in.p[i]); ++i)
                        n = n * 10 + (in.p[i] - '0');

                if (!i)
                        throw Switch::data_error("Unable to parse duration format");

                in.StripPrefix(i);

                if (in.StripPrefix(_S("weeks")) || in.StripPrefix(_S("week")) || in.StripPrefix(_S("w")))
                        scale = 86400 * 7;
                else if (in.StripPrefix(_S("years")) || in.StripPrefix(_S("year")) || in.StripPrefix(_S("y")))
                        scale = 86400 * 365;
                else if (in.StripPrefix(_S("months")) || in.StripPrefix(_S("month")) || in.StripPrefix(_S("mon")))
                        scale = 86400 * 365;
                else if (in.StripPrefix(_S("days")) || in.StripPrefix(_S("day")) || in.StripPrefix(_S("d")))
                        scale = 86400;
                else if (in.StripPrefix(_S("hours")) || in.StripPrefix(_S("hour")) || in.StripPrefix(_S("h")))
                        scale = 3600;
                else if (in.StripPrefix(_S("minutes")) || in.StripPrefix(_S("minute")) || in.StripPrefix(_S("mins")) || in.StripPrefix(_S("min")))
                        scale = 60;
                else if (in.StripPrefix(_S("seconds")) || in.StripPrefix(_S("second")) || in.StripPrefix(_S("secs")) || in.StripPrefix(_S("sec")) || in.StripPrefix(_S("s")))
                        scale = 1;

                // Optinally, separated by ',' or '+'
                in.StripPrefix(_S(","));
                in.StripPrefix(_S("+"));
                sum += n * scale;
        } while (in);

        return sum;
}

static uint64_t parse_size(strwlen32_t in) {
        uint64_t sum{0};

        do {
                uint32_t i{0};
                uint64_t n{0}, scale{1};

                for (i = 0; i != in.len && isdigit(in.p[i]); ++i)
                        n = n * 10 + (in.p[i] - '0');

                if (!i)
                        throw Switch::data_error("Unable to parse size format");

                in.StripPrefix(i);

                if (in.StripPrefix(_S("terabytes")) || in.StripPrefix(_S("terabyte")) || in.StripPrefix(_S("tbs")) || in.StripPrefix(_S("tb")) || in.StripPrefix(_S("t")))
                        scale = 1024ul * 1024ul * 1024ul * 1024ul;
                else if (in.StripPrefix(_S("gigabytes")) || in.StripPrefix(_S("gibabyte")) || in.StripPrefix(_S("gbs")) || in.StripPrefix(_S("gb")) || in.StripPrefix(_S("g")))
                        scale = 1024u * 1024u * 1024ul;
                else if (in.StripPrefix(_S("megabytes")) || in.StripPrefix(_S("megabyte")) || in.StripPrefix(_S("mbs")) || in.StripPrefix(_S("mb")) || in.StripPrefix(_S("m")))
                        scale = 1024 * 1024;
                else if (in.StripPrefix(_S("kilobytes")) || in.StripPrefix(_S("kilobyte")) || in.StripPrefix(_S("kbs")) || in.StripPrefix(_S("kb")) || in.StripPrefix(_S("k")))
                        scale = 1024;
                else if (in.StripPrefix(_S("bytes")) || in.StripPrefix(_S("byte")) || in.StripPrefix(_S("kbs")) || in.StripPrefix(_S("b")))
                        scale = 1;

                // Optinally, separated by ',' or '+'
                in.StripPrefix(_S(","));
                in.StripPrefix(_S("+"));
                sum += n * scale;
        } while (in);

        return sum;
}

void Service::parse_partition_config(const strwlen32_t contents, partition_config *const l) {
        for (auto &&line : contents.split('\n')) {
                strwlen32_t k, v;

                if (auto p = line.Search('#')) {
                        line.SetEnd(p);
                }

                if (!line) {
                        continue;
                }

                std::tie(k, v) = line.divided('=');
                k.TrimWS();
                v.TrimWS();

                if (!IsBetweenRange<size_t>(v.len, 1, 128)) {
                        throw Switch::data_error("Unexpected value for ", k);
                }

                // We are now using Kafka's configuration keys and semantics - for the most part - for simplicity
                // Keep it Simple.
                if (k && v) {
                        if (k.EqNoCase(_S("retention.segments.count"))) {
                                l->roSegmentsCnt = v.AsUint32();
                                if (l->roSegmentsCnt < 2 && l->roSegmentsCnt)
                                        throw Switch::range_error("Invalid value for ", k);
                        } else if (k.EqNoCase(_S("log.cleanup.policy"))) {
                                if (v.EqNoCase(_S("cleanup")))
                                        l->logCleanupPolicy = CleanupPolicy::CLEANUP;
                                else if (v.EqNoCase(_S("delete")))
                                        l->logCleanupPolicy = CleanupPolicy::DELETE;
                                else
                                        throw Switch::range_error("Unexpected value for ", k, ": available options are cleanup and delete");
                        } else if (k.EqNoCase(_S("log.cleaner.min.cleanable.ratio"))) {
                                l->logCleanRatioMin = v.AsDouble();

                                if (l->logCleanRatioMin < 0 || l->logCleanRatioMin > 1)
                                        throw Switch::range_error("Invalid value for ", k);
                        } else if (k.EqNoCase(_S("log.retention.secs"))) {
                                l->lastSegmentMaxAge = parse_duration(v);
                        } else if (k.EqNoCase(_S("log.retention.bytes"))) {
                                l->roSegmentsSize = parse_size(v);
                                if (l->roSegmentsSize < 128 && l->roSegmentsSize)
                                        throw Switch::range_error("Invalid value for ", k);
                        } else if (k.EqNoCase(_S("log.segment.bytes"))) {
                                l->maxSegmentSize = parse_size(v);
                                if (l->maxSegmentSize < 64)
                                        throw Switch::range_error("Invalid value for ", k);
                        } else if (k.EqNoCase(_S("log.index.interval.bytes"))) {
                                l->indexInterval = parse_size(v);
                                if (l->indexInterval < 128)
                                        throw Switch::range_error("Invalid value for ", k);
                        } else if (k.EqNoCase(_S("log.index.size.max.bytes"))) {
                                l->maxIndexSize = parse_size(v);
                                if (l->maxIndexSize < 128)
                                        throw Switch::range_error("Invalid value for ", k);
                        } else if (k.EqNoCase(_S("log.roll.jitter.secs"))) {
                                l->maxRollJitterSecs = parse_duration(v);
                        } else if (k.EqNoCase(_S("log.roll.secs"))) {
                                l->curSegmentMaxAge = parse_duration(v);
                        } else if (k.EqNoCase(_S("flush.messages"))) {
                                // The number of messages accumulated on a log partition before messages are flushed on disk
                                l->flushIntervalMsgs = v.AsUint32();
                        } else if (k.EqNoCase(_S("flush.secs"))) {
                                // The amount of time the log can have dirty data before a flush is forced
                                l->flushIntervalSecs = parse_duration(v);
                        } else
                                Print("Unknown topic/partition configuration key '", k, "'\n");
                }
        }
}

void Service::parse_partition_config(const char *const path, partition_config *const l) {
        int fd = open(path, O_RDONLY | O_LARGEFILE | O_NOATIME);

        if (fd == -1)
                throw Switch::system_error("Failed to access topic/partition config file(", path, "):", strerror(errno));
        else if (const auto fileSize = lseek64(fd, 0, SEEK_END)) {
                require(fileSize != off64_t(-1));

                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                close(fd);
                if (fileData == MAP_FAILED)
                        throw Switch::system_error("Failed to access topic/partition config file(", path, ") of size ", fileSize, ":", strerror(errno));

                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);
                DEFER({ munmap(fileData, fileSize); });

                parse_partition_config(strwlen32_t(static_cast<char *>(fileData), fileSize), l);
        }
}

// TODO(markp): respect configuration
void Service::rebuild_index(int logFd, int indexFd) {
        static constexpr bool   trace{false};
        const auto              fileSize = lseek64(logFd, 0, SEEK_END);
        auto *const             fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, logFd, 0);
        IOBuffer                b;
        uint32_t                relSeqNum{0};
        static constexpr size_t step{4096};
        uint64_t                firstMsgSeqNum;

        if (fileData == MAP_FAILED)
                throw Switch::system_error("Unable to mmap():", strerror(errno));

        DEFER({ munmap(fileData, fileSize); });
        madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

        Print("Rebuilding index of log of size ", size_repr(fileSize), " ..\n");
        for (const auto *p = reinterpret_cast<const uint8_t *>(fileData), *const e = p + fileSize, *const base = p, *next = p; p != e;) {
                const auto bundleBase = p;
                const auto bundleLen  = Compression::UnpackUInt32(p);
                const auto nextBundle = p + bundleLen;

                if (unlikely(nextBundle > e)) {
                        if (getenv("TANK_FORCE_SALVAGE_CURSEGMENT")) {
                                if (ftruncate(indexFd, 0) == -1) {
                                        Print("Failed to truncate the index:", strerror(errno), "\n");
                                        exit(1);
                                } else {
                                        Print("Please restart tank\n");
                                        exit(0);
                                }
                        } else {
                                Print("Likely corrupt segment(ran out of disk space?).\n");
                                Print("Set ", ansifmt::bold, "TANK_FORCE_SALVAGE_CURSEGMENT=1", ansifmt::reset, " and restart Tank so that\n");
                                Print("it will _delete_ the current segment index, and then you will need to restart again for Tank to get a chance to salvage the segment data\n");
                                Print("Will not index any further\n");
                                exit(1);
                        }
                }

                EXPECT(p < e);

                const auto bundleFlags        = *p++;
                const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                uint32_t   msgsSetSize        = (bundleFlags >> 2) & 0xf;

                if (!msgsSetSize)
                        msgsSetSize = Compression::UnpackUInt32(p);

                EXPECT(p <= e);

                if (trace)
                        SLog("New bundle bundleFlags = ", bundleFlags, ", msgSetSize = ", msgsSetSize, "\n");

                if (sparseBundleBitSet) {
                        uint64_t lastMsgSeqNum;

                        firstMsgSeqNum = *(uint64_t *)p;
                        p += sizeof(uint64_t);

                        if (msgsSetSize != 1) {
                                lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                        } else {
                                lastMsgSeqNum = firstMsgSeqNum;
                        }

                        if (trace)
                                SLog("bundle's sparse first ", firstMsgSeqNum, ", last ", lastMsgSeqNum, "\n");
                } else {
                        firstMsgSeqNum = relSeqNum;
                        relSeqNum += msgsSetSize;
                }

                if (p >= next) {
                        if (trace)
                                SLog("Indexing ", firstMsgSeqNum, " ", bundleBase - base, "\n");

                        b.Serialize<uint32_t>(firstMsgSeqNum);
                        b.Serialize<uint32_t>(bundleBase - base);
                        next = bundleBase + step;
                }

                p = nextBundle;
                EXPECT(p <= e);
        }

        if (trace)
                SLog("Rebuilt index ", size_repr(b.size()), ", last ", relSeqNum, ", ", b.size(), "\n");

        if (pwrite64(indexFd, b.data(), b.size(), 0) != b.size())
                throw Switch::system_error("Failed to store index:", strerror(errno));
        if (ftruncate(indexFd, b.size()))
                throw Switch::system_error("Failed to truncate index file:", strerror(errno));

        fsync(indexFd);

        if (trace)
                SLog("REBUILT INDEX\n");
}

void Service::verify_index(int fd, const bool wideEntries) {
        static constexpr bool trace{false};
        const auto            fileSize = lseek64(fd, 0, SEEK_END);

        if (!fileSize)
                return;
        else if (fileSize & 7)
                throw Switch::system_error("Unexpected index filesize");

        auto *const fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

        if (fileData == MAP_FAILED)
                throw Switch::system_error("Unable to mmap():", strerror(errno));

        DEFER(
            {
                    madvise(fileData, fileSize, MADV_DONTNEED);
                    munmap(fileData, fileSize);
            });

        madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

        if (wideEntries) {
                IMPLEMENT_ME();
        }

        for (const auto *      p = reinterpret_cast<const uint32_t *>(fileData),
                        *const e = p + (fileSize / sizeof(uint32_t)), *const b = p;
             p != e; p += 2) {
                if (trace)
                        SLog("Entry(", *p, ", ", p[1], ")\n");

                if (p != b) {
                        if (unlikely(p[0] <= p[-2])) {
                                Print("Unexpected rel.seq.num ", p[0], ", should have been > ", p[-2], ", at entry ", dotnotation_repr((p - static_cast<uint32_t *>(fileData)) / 2), " of index of ", dotnotation_repr(fileSize / (sizeof(uint32_t) + sizeof(uint32_t))), " entries\n");
                                throw Switch::system_error("Corrupt Index");
                        }

                        if (unlikely(p[1] <= p[-1])) {
                                Print("Unexpected file offset ", p[0], ", should have been > ", p[-2], ", at entry ", dotnotation_repr((p - static_cast<uint32_t *>(fileData)) / 2), " of index of ", dotnotation_repr(fileSize / (sizeof(uint32_t) + sizeof(uint32_t))), " entries\n");
                                throw Switch::system_error("Corrupt Index");
                        }
                } else {
                        if (p[0] != 0 || p[1] != 0) {
                                Print("Expected first entry to be (0, 0)\n");
                                throw Switch::system_error("Corrupt Index");
                        }
                }
        }
}

uint32_t Service::verify_log(int fd) {
        const auto fileSize = lseek64(fd, 0, SEEK_END);

        if (!fileSize)
                return 0;

        auto *const                         fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
        strwlen8_t                          key;
        strwlen32_t                         msgContent;
        IOBuffer                            cb;
        range_base<const uint8_t *, size_t> msgSetContent;
        uint64_t                            msgSeqNum{1}, firstMsgSeqNum, lastMsgSeqNum;
        static constexpr bool               trace{false};

        if (fileData == MAP_FAILED)
                throw Switch::system_error("Unable to mmap():", strerror(errno));

        DEFER(
            {
                    madvise(fileData, fileSize, MADV_DONTNEED);
                    munmap(fileData, fileSize);
            });
        madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

        for (const auto *p = static_cast<uint8_t *>(fileData), *const e = p + fileSize, *const base = p; p != e;) {
                const auto *const bundleBase = p;
                const auto        bundleLen  = Compression::UnpackUInt32(p);
                const auto        nextBundle = p + bundleLen;

                EXPECT(p < e);
                EXPECT(bundleLen);

                const auto bundleFlags        = *p++;
                const auto codec              = bundleFlags & 3;
                const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                uint32_t   msgsSetSize        = (bundleFlags >> 2) & 0xf;

                if (!msgsSetSize)
                        msgsSetSize = Compression::UnpackUInt32(p);

                if (trace)
                        SLog("Bundle, flags = ", bundleFlags, ", codec = ", codec, ", sparseBundleBitSet = ", sparseBundleBitSet, ", msgsSetSize = ", msgsSetSize, "\n");

                if (sparseBundleBitSet) {
                        firstMsgSeqNum = decode_pod<uint64_t>(p);

                        if (msgsSetSize != 1) {
                                lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                        } else {
                                lastMsgSeqNum = firstMsgSeqNum;
                        }

                        if (trace)
                                SLog("sparse bundle - firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum = ", lastMsgSeqNum, "\n");

                        EXPECT(firstMsgSeqNum && lastMsgSeqNum >= firstMsgSeqNum);
                }

                EXPECT(p <= e);
                EXPECT(msgsSetSize);
                EXPECT(codec == 0 || codec == 1);

                if (false)
                        Print(msgSeqNum, " => OFFSET ", bundleBase - base, "\n");

                if (codec) {
                        cb.clear();
                        if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, nextBundle - p, &cb))
                                throw Switch::system_error("Failed to decompress content");
                        msgSetContent.Set(reinterpret_cast<uint8_t *>(cb.data()), cb.size());
                } else
                        msgSetContent.Set(p, nextBundle - p);

                [[maybe_unused]] uint64_t msgTs { 0 };
                uint32_t                  msgIdx{0};

                for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e; ++msgIdx, ++msgSeqNum) {
                        // Next message set message
                        const auto flags = *p++;

                        if (sparseBundleBitSet) {
                                if (msgIdx == 0 || msgIdx == msgsSetSize - 1)
                                        msgSeqNum = firstMsgSeqNum;
                                else if (msgIdx == msgsSetSize - 1)
                                        msgSeqNum = lastMsgSeqNum;
                                else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                        // incremented in for()
                                } else {
                                        // delta from prev - 1
                                        msgSeqNum += Compression::UnpackUInt32(p);
                                }
                        }

                        if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                msgTs = *(uint64_t *)p;
                                p += sizeof(uint64_t);
                        }

                        if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                key.Set((char *)p + 1, *p);
                                p += key.len + sizeof(uint8_t);
                        } else
                                key.reset();

                        const auto msgLen = Compression::UnpackUInt32(p);

                        msgContent.Set((char *)p, msgLen);
                        p += msgLen;

                        if (trace)
                                Print("MSG:", msgSeqNum, ", size = ", msgLen, "\n");
                }

                p = nextBundle;
                EXPECT(p <= e);
        }

        return msgSeqNum;
}

Switch::shared_refptr<topic_partition> Service::init_local_partition(const uint16_t idx, const char *const bp, const partition_config &partitionConf) {
        char   basePath[PATH_MAX];
        size_t basePathLen = strlen(bp);

        require(basePathLen < PATH_MAX - 2);
        memcpy(basePath, bp, basePathLen);
        basePath[basePathLen]   = '/';
        basePath[++basePathLen] = '\0';

        if (trace)
                SLog("Initializing partition ", bp, "\n");

#ifndef LEAN_SWITCH
        if (!SwitchFS::BuildPath(basePath))
                throw Switch::system_error("Failed to create or access directory ", basePath);
#endif

        try {
                struct rosegment_ctx final {
                        uint64_t firstAvailableSeqNum;
                        uint64_t lastAvailSeqNum;
                        uint32_t creationTS;
                };

                // TODO(markp): reuse roLogs, wideEntyRoLogIndices, swapped and the allocator
                Switch::vector<rosegment_ctx> roLogs;
                std::set<uint64_t>            wideEntyRoLogIndices;
                simple_allocator              allocator{1024};
                std::vector<strwlen32_t>      swapped;
                auto                          partition = Switch::make_sharedref<topic_partition>();
                uint64_t                      curLogSeqNum{0};
                uint32_t                      curLogCreateTS{0};
                const strwlen32_t             b(basePath, basePathLen);
                int                           fd;
                auto                          l = new topic_partition_log();
                bool                          processSwapped{true};
                struct stat                   st;
                char                          _base_path[PATH_MAX];

                l->partition          = partition;
                l->config             = partitionConf;
                partition->distinctId = ++nextDistinctPartitionId;

                partition->log_.reset(l);
                partition->idx = idx;

                l->roSegments                                = nullptr;
                l->cur.index.ondisk.data                     = nullptr;
                l->cur.index.ondisk.span                     = 0;
                l->cur.index.haveWideEntries                 = false;
                l->cur.index.ondisk.lastRecorded.relSeqNum   = 0;
                l->cur.index.ondisk.lastRecorded.absPhysical = 0;

                b.CopyTo(_base_path);
                _base_path[b.size()] = '/';

                for (auto &&name : DirectoryEntries(basePath)) {
                        if (*name.p == '.')
                                continue;

                        if (name.Eq(_S("config"))) {
                                // override
                                parse_partition_config(Buffer::build(basePath, "/", name).data(), &l->config);
                                continue;
                        }

                        auto r = name.Divided('.');

                        if (r.second.StripSuffix(_S(".cleaned"))) {
                                // Compaction failed mid-way
                                // remove this file and make sure any .swap file files are also deleted
                                auto fullPath = Buffer::build(basePath, "/", name);

                                if (Unlink(fullPath.data()) == -1)
                                        throw Switch::system_error("Failed to Unlink(", fullPath, "):", strerror(errno));

                                processSwapped = false;
                                continue;
                        } else if (r.second.EndsWith(_S(".swap"))) {
                                // Compaction failed mid-way
                                // Before we had a chance to append .old to all previous segment files, the broker crashed
                                //
                                // If any *.cleaned files are found, we didn't get to append .swap to all new segments files so we need to
                                //	undo the effects by removing all .swap and .cleaned files
                                // If no *.cleaned files are found, we got to append .swap to all new segments files, so we should
                                // 	remove the *.swap extension and remove all *.old files
                                if (processSwapped == false) {
                                        auto fullPath = Buffer::build(basePath, "/", name);

                                        if (Unlink(fullPath.data()) == -1)
                                                throw Switch::system_error("Failed to Unlink(", fullPath, "):", strerror(errno));
                                } else {
                                        // we 'll process them in the end, iff processSwapped is still true by then
                                        swapped.emplace_back(allocator.CopyOf(name.p, name.len), name.len);
                                }

                                continue;
                        } else if (r.second.StripSuffix(_S(".old"))) {
                                // Compaction failed mid-way
                                //
                                // If any *.swap files are found, we didn't manage to strip the .swap extension from all new segments files
                                // but that's OK. We should still remove the .swap extension and remove all *.old files
                                //
                                // We are just going to remove the extension and consider the file
                                if (Rename(Buffer::build(basePath, "/", name).data(), Buffer::build(basePath, "/", strwlen8_t(name.p, name.len - STRLEN(".old"))).data()) == -1)
                                        throw Switch::system_error("Unable to rename .old file:", strerror(errno));
                        }

                        if (r.second.Eq(_S("index"))) {
                                // accept
                                const auto v = r.first.Divided('_');

                                if (v.second.Eq(_S("64"))) {
                                        // Just so ro_segment::ro_segment() won't have to try different names until it gets it right
                                        wideEntyRoLogIndices.insert(v.first.AsUint64());
                                }
                        } else if (r.second.Eq(_S("ilog"))) {
                                // Immutable log
                                auto     s = r.first;
                                uint32_t creationTS{0};

                                if (const auto *const p = s.Search('_')) {
                                        // Creation TS is encoded in the name
                                        creationTS = s.SuffixFrom(p + 1).AsUint32();
                                        s          = s.PrefixUpto(p);
                                }

                                const auto repr       = s.Divided('-');
                                const auto baseSeqNum = repr.first.AsUint64(), lastAvailSeqNum = repr.second.AsUint64();

                                if (trace)
                                        SLog("(	", baseSeqNum, ", ", lastAvailSeqNum, ", ", creationTS, ") ", r.first, "\n");

                                roLogs.push_back({baseSeqNum, lastAvailSeqNum, creationTS});
                        } else if (r.second.Eq(_S("log"))) {
                                // current, mutable log(segment)
                                // Either num.log or
                                // num_ts.log
                                // the later encodes the creation timestamp in the path, which is useful because
                                // we 'd like to know when this was created, when we restart the service and get to continue using the selected log
                                if (const auto *const p = r.first.Search('_')) {
                                        name.CopyTo(_base_path + b.size() + 1);

                                        if (-1 == stat(_base_path, &st)) {
                                                Print("Failed to stat ", _base_path, ": ", strerror(errno), "\n");
                                                std::abort();
                                        }

                                        if (0 == st.st_size) {
                                                // This is a stray - whatever the reason this is here, it needs to go
                                                Print("Found a stray ", _base_path, ", deleting it\n");
                                                unlink(_base_path);
                                                continue;
                                        }

                                        const auto seqRepr = r.first.PrefixUpto(p);
                                        const auto tsRepr  = r.first.SuffixFrom(p + 1);

                                        if (!seqRepr.IsDigits() || !tsRepr.IsDigits()) {
                                                throw Switch::system_error("Unexpected name ", name);
                                        } else {
                                                const auto seq = seqRepr.AsUint64();

                                                EXPECT(seq);
                                                EXPECT(!curLogSeqNum);

                                                curLogSeqNum   = seq;
                                                curLogCreateTS = tsRepr.AsUint32();

                                                if (trace) {
                                                        SLog("curLogSeqNum = ", curLogSeqNum, ", curLogCreateTS = ", curLogCreateTS, " from ", r.first, "\n");
                                                }
                                        }
                                } else {
                                        if (unlikely(!r.first.IsDigits())) {
                                                throw Switch::system_error("Unexpected name ", name);
                                        } else {
                                                const auto seq = r.first.AsUint64();

                                                EXPECT(seq);
                                                EXPECT(!curLogSeqNum);

                                                curLogSeqNum = seq;
                                        }
                                }
                        } else {
                                Print("Unexpected name ", name, " in ", basePath, "\n");
                        }
                }

                if (processSwapped == false) {
                        if (trace) {
                                SLog("Cannot process any swapped files\n");
                        }

                        while (!swapped.empty()) {
                                auto it = swapped.back();

                                if (Unlink(Buffer::build(basePath, "/", it).data()) == -1)
                                        throw Switch::system_error("Failed to unlink swapped file:", strerror(errno));

                                swapped.pop_back();
                        }
                } else {
                        if (trace) {
                                SLog("Can process swapped files\n");
                        }

                        while (!swapped.empty()) {
                                auto name = swapped.back();

                                name.StripSuffix(STRLEN(".swap"));

                                if (trace) {
                                        SLog("Processing swapped ", name, "\n");
                                }

                                if (Rename(Buffer::build(basePath, "/", name, ".swap").data(), Buffer::build(basePath, "/", name).data()) == -1) {
                                        throw Switch::system_error("Failed to rename swapped file:", strerror(errno));
                                }

                                const auto r = name.Divided('.');

                                if (r.second.Eq(_S("index"))) {
                                        const auto v = r.first.Divided('_');

                                        if (v.second.Eq(_S("64"))) {
                                                // Just so ro_segment::ro_segment() won't have to try different names until it gets it right
                                                wideEntyRoLogIndices.insert(v.first.AsUint64());
                                        }
                                } else if (r.second.Eq(_S("ilog"))) {
                                        auto     s = r.first;
                                        uint32_t creationTS{0};

                                        if (const auto *const p = s.Search('_')) {
                                                // Creation TS is encoded in the name
                                                creationTS = s.SuffixFrom(p + 1).AsUint32();
                                                s          = s.PrefixUpto(p);
                                        }

                                        const auto repr       = s.Divided('-');
                                        const auto baseSeqNum = repr.first.AsUint64(), lastAvailSeqNum = repr.second.AsUint64();

                                        if (trace) {
                                                SLog("(	", baseSeqNum, ", ", lastAvailSeqNum, ", ", creationTS, ") ", r.first, "\n");
                                        }

                                        roLogs.push_back({baseSeqNum, lastAvailSeqNum, creationTS});
                                }

                                swapped.pop_back();
                        }
                }

                if (trace) {
                        SLog("roLogs.size() = ", roLogs.size(), ", curLogSeqNum = ", curLogSeqNum, ", curLogCreateTS = ", curLogCreateTS, "\n");
                }

                if (!roLogs.empty()) {
                        std::sort(roLogs.begin(), roLogs.end(), [](const auto &a, const auto &b) noexcept {
                                return a.firstAvailableSeqNum < b.firstAvailableSeqNum;
                        });

                        l->firstAvailableSeqNum = roLogs.front().firstAvailableSeqNum;
                        l->roSegments.reset(new std::vector<ro_segment *>());

                        for (const auto &it : roLogs) {
                                if (trace)
                                        SLog("Initializing [firstAvailableSeqNum=", it.firstAvailableSeqNum, ",lastAvailSeqNum=", it.lastAvailSeqNum, ", base = ", b, "]\n");

                                l->roSegments->push_back(new ro_segment(it.firstAvailableSeqNum, it.lastAvailSeqNum, b, it.creationTS, wideEntyRoLogIndices.count(it.firstAvailableSeqNum)));
                        }
                } else {
                        l->firstAvailableSeqNum = curLogSeqNum;
                        l->roSegments.reset(new std::vector<ro_segment *>());
                }

                if (curLogSeqNum) {
                        // Have a current segment
                        if (curLogCreateTS) {
                                Snprint(basePath, sizeof(basePath), b, curLogSeqNum, "_", curLogCreateTS, ".log");
                        } else {
                                Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".log");
                        }

                        fd = open(basePath, read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME), 0775);
                        if (fd == -1) {
                                throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot open current segment log");
                        }

                        l->cur.fdh.reset(new fd_handle(fd));
                        require(l->cur.fdh->use_count() == 2);
                        l->cur.fdh->Release();
                        l->cur.baseSeqNum = curLogSeqNum;
                        l->cur.fileSize   = lseek64(fd, 0, SEEK_END);

                        // TODO(markp): check if index has wideEntries
                        // and set l->cur.index.haveWideEntries accordingly
                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".index");
                        fd = open(basePath, read_only ? O_RDWR : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND), 0775);

                        if (trace) {
                                SLog("Considering ", basePath, "\n");
                        }

                        if (fd == -1) {
                                throw Switch::system_error("open(", basePath_, ") failed:", strerror(errno), ". Cannot open current segment index");
                        } else if (lseek64(fd, 0, SEEK_END) == 0 && l->cur.fileSize) {
                                Service::rebuild_index(l->cur.fdh->fd, fd);
                        }

                        l->cur.index.fd = fd;
                        // if this an empty commit log, need to update the index immediately
                        l->cur.sinceLastUpdate = l->cur.fileSize == 0 ? UINT32_MAX : 0;

                        if (const auto size = lseek64(fd, 0, SEEK_END)) {
                                // we don't want to deserialize the skiplist for faster startup
                                // we 'll mmap the region though
                                l->cur.index.ondisk.span = size;
                                l->cur.index.ondisk.data = static_cast<const uint8_t *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0));

                                if (unlikely(l->cur.index.ondisk.data == MAP_FAILED)) {
                                        throw Switch::system_error("mmap() failed:", strerror(errno));
                                }

                                madvise((void *)l->cur.index.ondisk.data, size, MADV_DONTDUMP);

                                if (l->cur.index.haveWideEntries) {
                                        IMPLEMENT_ME();
                                } else {
                                        const auto *const p = (uint32_t *)(l->cur.index.ondisk.data + l->cur.index.ondisk.span - sizeof(uint32_t) - sizeof(uint32_t));

                                        l->cur.index.ondisk.lastRecorded.relSeqNum   = p[0];
                                        l->cur.index.ondisk.lastRecorded.absPhysical = p[1];
                                }

                                if (trace) {
                                        SLog("Have cur.index.ondisk.span = ",
                                             l->cur.index.ondisk.span, " lastRecorded =  ( relSeqNum = ",
                                             l->cur.index.ondisk.lastRecorded.relSeqNum, ", absPhysical = ", l->cur.index.ondisk.lastRecorded.absPhysical, ")\n");

#if 1
                                        if (l->cur.index.haveWideEntries) {
                                                IMPLEMENT_ME();
                                        } else {
                                                for (const auto *it = (uint32_t *)l->cur.index.ondisk.data, *const base = it, *const e = it + l->cur.index.ondisk.span / sizeof(uint32_t); it != e; it += 2) {
                                                        if (it != base) {
                                                                EXPECT(it[0] != it[-2]);
                                                        }
                                                }
                                        }

#endif
                                }
                        }

                        l->lastAssignedSeqNum = 0;

                        if (const auto s = l->cur.fileSize) {
                                const auto o = l->cur.index.ondisk.lastRecorded.absPhysical;
                                // This is somewhat expensive; but we only need to do this once
                                // We just start from the last tracked-recorded (relSeqNum, absPhysical) and skip bundles until EOF
                                // keeping track of offsets as we go.
                                const auto span = s - o;

                                EXPECT(span);

                                auto *const data = static_cast<uint8_t *>(malloc(span));
                                // first message in the first bundle we 'll parse
                                uint64_t   next = l->cur.index.ondisk.lastRecorded.relSeqNum + l->cur.baseSeqNum;
                                const auto savedNext{next};
                                int        fd = l->cur.fdh->fd;

                                if (trace) {
                                        SLog("From lastRecorded.relSeqNum = ", l->cur.index.ondisk.lastRecorded.relSeqNum, ", lastRecorded.absPhysical = ", o, ", cur.baseSeqNum = ", l->cur.baseSeqNum, "\n");
                                        SLog("span = ", span, ", start from ", next, "\n");
                                }

                                if (unlikely(!data)) {
                                        throw Switch::system_error("malloc(", span, ") failed");
                                }

                                DEFER({ free(data); });

                                EXPECT(fd != -1);

                                const auto     res = pread64(fd, data, span, o);
                                const uint8_t *lastCheckpoint{data};

                                if (trace) {
                                        SLog("Attempting to read ", span, " bytes at ", o, ": ", res, "\n");
                                }

                                if (res != span) {
                                        throw Switch::system_error("pread64() failed:", strerror(errno));
                                }

                                for (const auto *p = data, *const e = p + span; p != e;) {
                                        const auto *      saved{p};
                                        const auto        bundleLen = Compression::UnpackUInt32(p);
                                        const auto *const bundleEnd = p + bundleLen;

                                        if (unlikely(bundleLen == 0 || bundleEnd > e)) {
                                                const auto ckpt = (lastCheckpoint - data) + o;

                                                Print("Likely corrupt segment(ran out of disk space?).\n");
                                                if (getenv("TANK_FORCE_SALVAGE_CURSEGMENT")) {
                                                        if (ftruncate(l->cur.fdh->fd, ckpt) == -1) {
                                                                Print("Failed to truncate:", strerror(errno), "\n");
                                                                exit(1);
                                                        } else if (l->cur.index.fd != -1 && ftruncate(l->cur.index.fd, 0) == -1) {
                                                                Print("Failed to truncate:", strerror(errno), "\n");
                                                                exit(1);
                                                        } else
                                                                Print("Salvaged segment. Please restart Tank\n");
                                                } else {
                                                        Print("Set TANK_FORCE_SALVAGE_CURSEGMENT=1 and restart Tank so that it will _delete_ the current segment index and truncate the current segment file so that it will salvage whatever's possible\n");
                                                        Print("Can save up to ", size_repr(ckpt), ", will lose ", size_repr(s - ckpt), "\n");
                                                        Print("Aborting\n");
                                                }
                                                exit(1);
                                        }

                                        Drequire(bundleLen);
                                        lastCheckpoint = saved;

                                        const auto bundleFlags        = *p++;
                                        const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                                        uint32_t   msgSetSize         = (bundleFlags >> 2) & 0xf;

                                        if (!msgSetSize) {
                                                msgSetSize = Compression::UnpackUInt32(p);

                                                Drequire(msgSetSize);
                                        }

                                        if (trace)
                                                SLog("bundleFlags = ", bundleFlags, ", sparseBundleBitSet = ", sparseBundleBitSet, ", msgSetSize = ", msgSetSize, "\n");

                                        if (sparseBundleBitSet) {
                                                const auto firstMsgSeqNum = *(uint64_t *)p;
                                                uint64_t   lastMsgSeqNum;
                                                p += sizeof(uint64_t);

                                                if (msgSetSize != 1) {
                                                        lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                                                } else {
                                                        lastMsgSeqNum = firstMsgSeqNum;
                                                }

                                                if (trace)
                                                        SLog("sparse bundle, firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum = ", lastMsgSeqNum, "\n");

                                                next = lastMsgSeqNum + 1;
                                        } else
                                                next += msgSetSize;

                                        if (trace)
                                                SLog("bundle msgSetSize = ", msgSetSize, "\n");

                                        p = bundleEnd;
                                        Drequire(p <= e);
                                }

                                Drequire(next > savedNext);
                                Drequire(l->cur.sinceLastUpdate == 0); // not an empty current segment log

                                l->lastAssignedSeqNum = next - 1;

                                if (trace)
                                        SLog(ansifmt::bold, "Set lastAssignedSeqNum = ", l->lastAssignedSeqNum, ansifmt::reset, "\n");
                        }

                        // Just in case
                        lseek64(l->cur.index.fd, 0, SEEK_END);
                        lseek64(l->cur.fdh->fd, 0, SEEK_END);

                        if (const uint32_t max = l->config.maxRollJitterSecs) {
                                std::random_device                      dev;
                                std::mt19937                            rng(dev());
                                std::uniform_int_distribution<uint32_t> distr(0, max);

                                l->cur.rollJitterSecs = distr(rng);
                        } else
                                l->cur.rollJitterSecs = 0;

                        const auto now = time(nullptr);

                        l->cur.createdTS                    = curLogCreateTS ?: now;
                        l->cur.nameEncodesTS                = curLogCreateTS;
                        l->cur.flush_state.pendingFlushMsgs = 0;
                        l->cur.flush_state.nextFlushTS      = config.flushIntervalSecs ? now + config.flushIntervalSecs : UINT32_MAX;

                        if (l->lastAssignedSeqNum >= l->cur.baseSeqNum) {
                                // OK
                        } else {
                                Print("UNEXPECTED lastAssignedSeqNum(", l->lastAssignedSeqNum, ") baseSeqNum(", l->cur.baseSeqNum, ") for ", basePath_, "\n");
                                std::abort();
                        }

                        if (trace)
                                SLog("createdTS(", l->cur.createdTS, ") nameEncodesTS(", l->cur.nameEncodesTS, ")\n");
                } else {
                        // We 'll just create those on demand in append()
                        l->cur.fdh.reset(nullptr);
                        l->cur.fileSize              = UINT32_MAX;
                        l->cur.index.fd              = -1;
                        l->cur.sinceLastUpdate       = UINT32_MAX;
                        l->cur.baseSeqNum            = UINT64_MAX;
                        l->lastAssignedSeqNum        = 0;
                        l->cur.index.haveWideEntries = false;

                        if (l->roSegments && !l->roSegments->empty()) {
                                // We can still use the last immutable segment
                                Print(ansifmt::bold, ansifmt::color_red, "Looks like someone deleted the active segment from ", bp, ansifmt::reset, "\n");
                                l->lastAssignedSeqNum = l->roSegments->back()->lastAvailSeqNum;
                        }
                }

                return partition;
        } catch (const std::exception &e) {
                if (trace)
                        SLog("Exception:", e.what(), "\n");
                throw;
        }
}

static int log_fd;

void init_log() {
#ifndef LEAN_SWITCH
        log_fd = open("/tmp/tank_srv.log", O_WRONLY | O_CREAT | O_LARGEFILE | O_APPEND, 0775);

        if (log_fd != -1) {
                Buffer m;

                m.append(Date::ts_repr(time(nullptr)), " TANK service initialization in progress\n");
                write(log_fd, m.data(), m.size());
        }
#else
        log_fd = -1;
#endif
}

template <typename... T>
void track_shutdown(connection *const c, const size_t line, T &&... args) {
#ifndef LEAN_SWITCH
        if (auto fd{log_fd}; fd != -1) {
                Buffer b;

                b.append(Date::ts_repr(time(nullptr)), ": Unexpected shutdown at ", line, " for connection from ", ip4addr_repr(c ? c->addr4 : INADDR_NONE), ":");
                b.append(std::forward<T>(args)...);
                write(fd, b.data(), b.size());
        }
#endif
}

bool Service::process_consume(connection *const c, const uint8_t *p, const size_t /*len*/) {
        try {
                auto                        respHeader = get_buffer();
                bool                        respondNow{false};
                const auto                  replicaId     = c->replicaId;
                [[maybe_unused]] const auto clientVersion = decode_pod<uint16_t>(p);
                const auto                  requestId     = decode_pod<uint32_t>(p);
                const strwlen8_t            clientId(reinterpret_cast<const char *>(p) + 1, *p);
                p += clientId.size() + sizeof(uint8_t);
                const auto maxWait   = decode_pod<uint64_t>(p);                                   // if we don't get any data within `maxWait`ms, we 'll return nothing for the requested partitions
                const auto minBytes  = Min<uint32_t>(decode_pod<uint32_t>(p), 128 * 1024 * 1024); // keep it sane
                const auto topicsCnt = *p++;

                if (trace)
                        SLog(ansifmt::bold, ansifmt::color_magenta, "New COSNUME request for topicsCnt = ", topicsCnt, ansifmt::reset, "\n");

                respHeader->Serialize(uint8_t(TankAPIMsgType::Consume));
                const auto sizeOffset = respHeader->size();
                respHeader->RoomFor(sizeof(uint32_t));

                // This is an optimization for the client
                // we 'll store the response length header
                const auto headerSizeOffset = respHeader->size();
                respHeader->RoomFor(sizeof(uint32_t));
                respHeader->Serialize(requestId);
                respHeader->Serialize(topicsCnt);

                auto     q = c->outQ;
                size_t   sum{0};
                uint32_t patchListSize{0};
                uint8_t  patchIndices[256];

                // TODO(markp): https://github.com/phaistos-networks/TANK/issues/12
                patchList[0].offset = 0;
                if (!q)
                        q = c->outQ = get_outgoing_queue();

                const auto  qSize         = q->size();
                auto *const headerPayload = q->push_back(respHeader);

                deferList.clear();

                for (uint32_t i{0}; i != topicsCnt; ++i) {
                        const strwlen8_t topicName(reinterpret_cast<const char *>(p) + 1, *p);
                        p += topicName.size() + sizeof(uint8_t);
                        const auto partitionsCnt = *p++;
                        auto       topic         = topic_by_name(topicName);

                        if (trace)
                                SLog("topic [", topicName, "]\n");

                        respHeader->Serialize(topicName.len);
                        respHeader->Serialize(topicName.p, topicName.len);
                        respHeader->Serialize(partitionsCnt);

                        if (!topic) {
                                if (trace)
                                        SLog("Unknown topic [", topicName, "]\n");

                                p += (sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t)) * partitionsCnt;
                                // Absuse scheme so that we won't have another field for this fault
                                // Set next/first partition id to UINT16_MAX
                                respHeader->Serialize<uint16_t>(UINT16_MAX);
                                respondNow = true;
                                continue;
                        }

                        if (trace)
                                SLog(partitionsCnt, " for topic [", topicName, "]\n");

                        for (uint32_t k{0}; k != partitionsCnt; ++k) {
                                const auto partitionId = decode_pod<uint16_t>(p);
                                const auto absSeqNum   = decode_pod<uint64_t>(p);
                                const auto fetchSize   = decode_pod<uint32_t>(p);
                                auto       partition   = topic->partition(partitionId);

                                // TODO(markp): we should probably limit this to say a few MBs at most
                                // so that clients won't be able to abuse/miuse tank
                                //
                                // We just need to make sure no single event is larger than whatever threshold we choose

                                respHeader->Serialize(partitionId);

                                if (!partition) {
                                        if (trace)
                                                SLog("Undefined partition ", partitionId, "\n");

                                        respHeader->Serialize(uint8_t(0xff));
                                        respondNow = true;
                                        continue;
                                }

                                if (trace)
                                        SLog("> REQUEST FOR partition ", partitionId, ", absSeqNum ", absSeqNum, ", fetchSize ", fetchSize, "\n");

                                if (absSeqNum == UINT64_MAX) {
                                        // Fetch starting from whatever bundles are commited from now on
                                        const auto l = respHeader->size();

                                        patchList[patchListSize++].SetEnd(l);
                                        patchIndices[deferList.size()]  = patchListSize++;
                                        patchList[patchListSize].offset = l;
                                        deferList.push_back(partition);
                                } else {
                                        const bool fetchOnlyFromLeader = replicaId != UINT16_MAX; // UINT16_MAX replica is the debug consumer ID
                                        const bool fetchOnlyCommitted  = replicaId == 0;          // for clients, only comitted
                                        auto       res                 = partition->read_from_local(fetchOnlyFromLeader, fetchOnlyCommitted,
                                                                              absSeqNum, fetchSize);
                                        const auto hwMark              = res.highWatermark;
                                        range32_t  range;
                                        bool       firstBundleIsSparse;
                                        uint64_t   start;

                                        switch (res.fault) {
                                                case lookup_res::Fault::NoFault:
                                                        // for promethus metrics
                                                        start               = Timings::Microseconds::Tick();
                                                        firstBundleIsSparse = adjust_range_start(res, absSeqNum);
                                                        range.Set(res.fileOffset, fetchSize);
                                                        if (range.stop() > res.fileOffsetCeiling)
                                                                range.SetEnd(res.fileOffsetCeiling);

                                                        if (trace)
                                                                SLog(ansifmt::bold, "Response:(baseSeqNum = ", res.absBaseSeqNum, ", range ", range, ", firstBundleIsSparse = ", firstBundleIsSparse, ")", ansifmt::reset, "\n");

                                                        if (firstBundleIsSparse) {
                                                                // Set special errorOrFlags to let the client know that we are not going to encode here the seq.num of the first msg of the first bundle, because
                                                                // the first bundle we are streaming is a 'sparse bundle', which means it encodes the absolute sequence number of its first message
                                                                // in the bundle header anyway
                                                                respHeader->Serialize(uint8_t(0xfe)); // errorOrFlags

                                                                if (trace)
                                                                        SLog("Setting errorOrFlags to 0xfe\n");
                                                        } else {
                                                                respHeader->Serialize(uint8_t(0));        // errorOrFlags
                                                                respHeader->Serialize(res.absBaseSeqNum); // absolute first seq.num of the first message of the first bundle in the streamed chunk
                                                        }

                                                        respHeader->Serialize(hwMark);
                                                        respHeader->Serialize(range.len);

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

                                                                if (trace)
                                                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(b)), " for readahead(", range, ") ", size_repr(range.len), "\n");
                                                        }
#endif

                                                        sum += range.len;
                                                        q->push_back({res.fdh.get(), range, start, topic});

                                                        // TODO(markp):
                                                        // if (replicaId) { updateFollowerLogEndOffset(topic, partition, absSeqNum) }
                                                        respondNow = true;
                                                        break;

                                                case lookup_res::Fault::AtEOF: {
                                                        if (trace)
                                                                SLog("Got AtEOF; will wait\n");

                                                        const auto l = respHeader->size();

                                                        patchList[patchListSize++].SetEnd(l);
                                                        patchIndices[deferList.size()]  = patchListSize++;
                                                        patchList[patchListSize].offset = l;
                                                        deferList.push_back(partition);
                                                        break;
                                                }

                                                case lookup_res::Fault::Empty:
                                                        respHeader->Serialize(uint8_t(0));
                                                        respHeader->Serialize(res.absBaseSeqNum);
                                                        respHeader->Serialize(hwMark);
                                                        respHeader->Serialize(uint32_t(0));
                                                        respondNow = true;
                                                        break;

                                                case lookup_res::Fault::BoundaryCheck:
                                                        respHeader->Serialize(uint8_t(1));
                                                        respHeader->Serialize(uint64_t(0));
                                                        respHeader->Serialize(hwMark);
                                                        respHeader->Serialize(uint32_t(0));
                                                        {
                                                                // Only for this specific fault
                                                                respHeader->Serialize<uint64_t>(partition->log_->firstAvailableSeqNum);
                                                        }
                                                        respondNow = true;
                                                        break;
                                        }
                                }
                        }
                }

                if (trace)
                        SLog("respondNow = ", respondNow, ", maxWait = ", maxWait, "\n");

                // TODO(markp): https://github.com/phaistos-networks/TANK/issues/17#issuecomment-236106945
                // (don't respond even if we have any data, amount >= minBytes)
                if (respondNow || maxWait == 0) {
                        // - fetch request does not want to wait
                        // - fetch request does not require any data, or we already have some data to provide to the client
                        // - one or more errors were generated
                        uint32_t   extra{0};
                        const auto n = deferList.size();

                        if (trace)
                                SLog("Responding now, n = ", deferList.size(), "\n");

                        patchList[patchListSize++].SetEnd(respHeader->size());

                        for (uint32_t i{0}; i != n; ++i) {
                                const auto idx = patchIndices[i];
                                const auto o   = respHeader->size();
                                auto       p   = deferList[i];
                                auto       log = p->log_.get();

                                respHeader->Serialize(uint8_t(0));
                                respHeader->Serialize(log->firstAvailableSeqNum);
                                respHeader->Serialize(p->highwater_mark());
                                respHeader->Serialize(uint32_t(0));

                                patchList[idx] = {o, respHeader->size() - o};
                        }

                        *reinterpret_cast<uint32_t *>(respHeader->At(sizeOffset))       = respHeader->size() - sizeOffset - sizeof(uint32_t) + sum + extra;
                        *reinterpret_cast<uint32_t *>(respHeader->At(headerSizeOffset)) = respHeader->size() - headerSizeOffset - sizeof(uint32_t) + extra;

                        if (trace)
                                SLog("respHeader.length = ", respHeader->size(), " ", *(uint32_t *)respHeader->At(sizeOffset), "\n");

                        headerPayload->set_iov(patchList, patchListSize);
                        return try_send_ifnot_blocked(c);
                } else {
                        // Can't respond; we 'll need to wait until we have any data for any of those
                        // topic/partitions first

                        if (trace)
                                SLog("Cannot respond yet (", q->size(), ", ", qSize, ")\n");

                        while (q->size() != qSize) {
                                auto &p = q->back();

                                if (trace)
                                        SLog("Dropping payload ", p.payloadBuf, "\n");

                                if (p.payloadBuf)
                                        put_buffer(p.buf);
                                else
                                        p.file_range.fdh->Release();

                                q->pop_back();
                        }

                        if (q->empty()) {
                                if (trace)
                                        SLog("No longer needed outQ\n");

                                put_outgoing_queue(q);
                                c->outQ = nullptr;
                        }

                        return register_consumer_wait(c, requestId, maxWait, minBytes, deferList.data(), deferList.size());
                }
        } catch (const std::exception &e) {
                if (trace)
                        SLog("Cought exception:", e.what(), "\n");

                track_shutdown(c, __LINE__, "Exception thrown:", e.what(), "\n");
                return shutdown(c, __LINE__);
        }
}

void Service::register_timer(eb64_node *const node) {
        timers_ebtree_next = std::min<uint64_t>(node->key, timers_ebtree_next);
        eb64_insert(&timers_ebtree_root, node);

        if (trace) {
                SLog(ansifmt::color_brown, "Registered timer now ", timers_ebtree_next, ansifmt::reset, "\n");
        }
}

bool Service::cancel_timer(eb64_node *const node) {
        // SLog(ansifmt::color_brown, "Attempting to cancel timer (is linked?:", node->node.leaf_p != nullptr, ")", ansifmt::reset, "\n");

        if (!node->node.leaf_p) {
                // already unlinked
                return false;
        }

        const auto when = node->key;

        eb64_delete(node);
        EXPECT(!node->node.leaf_p);

        if (when <= timers_ebtree_next) {
                // update timers_ebtree_next
                if (const auto eb = eb64_first(&timers_ebtree_root)) {
                        timers_ebtree_next = eb->key;
                } else {
                        timers_ebtree_next = std::numeric_limits<uint64_t>::max();
                }
        }

        return true;
}

bool Service::process_pending_signals(uint64_t mask) {
        do {
                const auto bit = mask & -mask;

                if (bit == (1ull << SIGINT)) {
                        return false;
                } else if (bit == 1ull << SIGUSR1) {
                        // woke up
                }

                mask ^= bit;
        } while (mask);

        return true;
}

void Service::drain_pubsub_queue() {
        if (auto it = mainThreadClosures.drain()) {
                // we don't care about the order those closures are executed
                // but we may as well reverse the list anyway
                mainthread_closure *rh{nullptr};

                while (it) {
                        auto t{it};

                        it      = t->next;
                        t->next = rh;
                        rh      = t;
                }

                do {
                        auto next = rh->next;

                        if (trace) {
                                SLog("Executing\n");
                        }

                        (*rh)();
                        delete rh;
                        rh = next;
                } while (rh);
        }
}

void Service::fire_timer(eb64_node_ctx *const ctx) {
        // SLog(ansifmt::color_brown, "firing timer ", unsigned(ctx->type), ansifmt::reset, "\n");

        switch (ctx->type) {
                case eb64_node_ctx::ContainerType::WaitCtx: {
                        [[maybe_unused]] auto wc = containerof(wait_ctx, exp_tree_node, ctx);

                        abort_wait_ctx(wc);
                } break;

                case eb64_node_ctx::ContainerType::CleanupTracker: {
                        if (cleanupTrackerIsDirty) {
                                // TODO(markp): maybe we should just have another thread to do this
                                // although this is a very infrequent operation and shouldn't take more than a few microseconds
                                IOBuffer b;
                                int      fd;

                                for (auto it : cleanupTracker) {
                                        const auto topic = it->partition->owner->name();

                                        b.Serialize(topic.len);
                                        b.Serialize(topic.p, topic.len);
                                        b.Serialize<uint16_t>(it->partition->idx);
                                        b.Serialize<uint64_t>(it->lastCleanupMaxSeqNum);
                                }

                                fd = open(Buffer::build(basePath_, "/.cleanup.log.int").data(), O_WRONLY | O_TRUNC | O_CREAT, 0775);
                                if (fd == -1)
                                        Print("Failed to update cleanup log:", strerror(errno), "\n");
                                else if (write(fd, b.data(), b.size()) != b.size()) {
                                        Print("Failed to update cleanup log:", strerror(errno), "\n");
                                        close(fd);
                                } else {
                                        close(fd);
                                        if (Rename(Buffer::build(basePath_, "/.cleanup.log.int").data(), Buffer::build(basePath_, "/.cleanup.log").data()) == -1)
                                                Print("Failed to update cleanup log:", strerror(errno));
                                }

                                cleanupTrackerIsDirty = false;
                        }
                } break;

                default:
                        IMPLEMENT_ME();
                        break;
        }
}

void Service::process_timers() {
        eb64_node *it;

        if (trace) {
                SLog(ansifmt::color_brown, "Processing timers, empty root:", eb_is_empty(&timers_ebtree_root), ansifmt::reset, "\n");
        }

        goto l1;
        for (;;) {
                if (!it) {
                        // we may have another registered so try again
                l1:
                        it = eb64_first(&timers_ebtree_root);
                        if (!it) {
                                timers_ebtree_next = std::numeric_limits<uint64_t>::max();
                                return;
                        }
                }

                // SLog(ansifmt::color_brown, "Comparing ", it->key, " ", now_ms, ansifmt::reset, "\n");

                if (const auto key = it->key; key > now_ms) {
                        // not expired yet, try again later
                        // we are done
                        timers_ebtree_next = it->key;
                        return;
                }

                auto ctx  = eb64_entry(it, eb64_node_ctx, node);
                auto next = eb64_next(it);

                // unlink before we get to fire_timer()
                // because within fire_timer() the node may be inserted again
                eb64_delete(it);

                // SLog(ansifmt::color_brown, "deleted node, empty root:", eb_is_empty(&timers_ebtree_root), "\n");

                fire_timer(ctx);
                it = next;
        }
}

bool Service::register_consumer_wait(connection *const c, const uint32_t requestId, const uint64_t maxWait, const uint32_t minBytes, topic_partition **const partitions, const uint32_t totalPartitions) {
        auto ctx = get_waitctx(totalPartitions);

        if (trace) {
                SLog("REGISTERING consumer wait for ", totalPartitions, " partitions, maxWait = ", maxWait, "\n");
        }

        if (maxWait) {
                ctx->exp_tree_node.node.key = now_ms + maxWait;
                ctx->exp_tree_node.type     = eb64_node_ctx::ContainerType::WaitCtx;
                register_timer(&ctx->exp_tree_node.node);
        } else {
                memset(&ctx->exp_tree_node.node, 0, sizeof(ctx->exp_tree_node.node));
        }

        ctx->requestId        = requestId;
        ctx->scheduledForDtor = false;
        ctx->c                = c;
        ctx->partitionsCnt    = totalPartitions;
        ctx->minBytes         = minBytes;
        ctx->capturedSize     = 0;

        switch_dlist_init(&ctx->list);
        switch_dlist_insert_after(&c->waitCtxList, &ctx->list);

        for (uint32_t i{0}; i != totalPartitions; ++i) {
                auto p   = partitions[i];
                auto out = ctx->partitions + i;

                if (trace) {
                        SLog("Partition ", ptr_repr(p), "\n");
                }

                p->waitingList.push_back(ctx);

                out->partition = p;
                out->fdh       = nullptr;
                out->range.reset();
                out->seqNum = 0;
        }

        return true;
}

bool Service::process_create_topic(connection *const c, const uint8_t *p, const size_t len) {
        if (unlikely(len < sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint8_t))) {
                if (trace)
                        SLog("Unexpected len = ", len, "\n");

                track_shutdown(c, __LINE__, "Uenxpected length ", len, "\n");
                return shutdown(c, __LINE__);
        }

        auto       q         = c->outQ;
        auto       resp      = get_buffer();
        const auto requestId = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const strwlen8_t topicName((char *)p + 1, *p);

        p += topicName.len + sizeof(uint8_t);

        partition_config partitionConfig;
        strwlen32_t      config;
        const auto       partitionsCnt = *(uint16_t *)p;
        p += sizeof(uint16_t);

        config.len = Compression::UnpackUInt32(p);
        config.p   = (char *)p;
        p += config.len;

        if (!q)
                q = c->outQ = get_outgoing_queue();

        resp->Serialize(uint8_t(TankAPIMsgType::CreateTopic));
        const auto                     sizeOffset = resp->size();
        std::vector<topic_partition *> list;

        resp->RoomFor(sizeof(uint32_t));

        resp->Serialize(requestId);
        resp->Serialize(topicName.len);
        resp->Serialize(topicName.p, topicName.len);

        config.TrimWS();
        if (config) {
                try {
                        parse_partition_config(config, &partitionConfig);
                } catch (...) {
                        resp->Serialize<uint8_t>(4); // Invalid configuration
                        goto l1;
                }
        }

        if (read_only) {
                resp->pack(uint8_t(5));
        } else if (topic_by_name(topicName)) {
                resp->Serialize<uint8_t>(1); // already exists
        } else {
                char       topicPath[PATH_MAX];
                const auto topicPathLen = Snprint(topicPath, sizeof(topicPath), basePath_, "/", topicName, "/");

                if (mkdir(topicPath, 0775) == -1) {
                        resp->Serialize<uint8_t>(2);
                } else {
                        try {
                                for (uint16_t i{0}; i != partitionsCnt; ++i) {
                                        sprintf(topicPath + topicPathLen, "%u", i);

                                        if (mkdir(topicPath, 0775) == -1) {
                                                resp->Serialize<uint8_t>(2);
                                                goto l1;
                                        }

                                        auto _p = init_local_partition(i, topicPath, partitionConfig).release();

                                        EXPECT(_p->use_count() == 1);
                                        list.emplace_back(_p);
                                }

                                if (config) {
                                        int fd;

                                        strcpy(topicPath + topicPathLen, "config");
                                        fd = open(topicPath, O_WRONLY | O_CREAT | O_LARGEFILE, 0775);
                                        if (fd == -1) {
                                                resp->Serialize<uint8_t>(2);
                                                goto l1;
                                        } else if (write(fd, config.p, config.len) != config.len) {
                                                close(fd);
                                                unlink(topicPath);
                                                resp->Serialize<uint8_t>(2);
                                                goto l1;
                                        } else {
                                                close(fd);
                                        }
                                }

                                auto t = Switch::make_sharedref<topic>(topicName, partitionConfig);

                                EXPECT(t->use_count() == 1);

                                t->register_partitions(list.data(), list.size());
                                list.clear();

                                EXPECT(t->use_count() == 1);

                                register_topic(t.get());
                                resp->Serialize(uint8_t(0));
                        } catch (...) {
                                resp->Serialize<uint8_t>(2);
                        }
                }
        }

l1:
        while (!list.empty()) {
                list.back()->Release();
                list.pop_back();
        }

        *reinterpret_cast<uint32_t *>(resp->At(sizeOffset)) = resp->size() - sizeOffset - sizeof(uint32_t);

        auto payload = q->push_back(resp);

        payload->iovCnt = 1;
        payload->iov[0] = {(void *)resp->data(), resp->size()};

        return try_send_ifnot_blocked(c);
}

bool Service::process_load_conf(connection *const c, const uint8_t *p, const size_t len) {
        if (len < sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint16_t)) {
                track_shutdown(c, __LINE__, "Unexpected length ", len, "\n");
                return shutdown(c, __LINE__);
        }

        const auto e         = p + len;
        const auto req_id    = decode_pod<uint32_t>(p);
        const auto topic_len = decode_pod<uint8_t>(p);

        if (p + topic_len + sizeof(uint16_t) > e) {
                track_shutdown(c, __LINE__, "Unexpected length ", len, "\n");
                return shutdown(c, __LINE__);
        }
        const str_view8 topic_name(reinterpret_cast<const char *>(p), topic_len);

        p += topic_len;

        const auto partition_id = decode_pod<uint16_t>(p);
        auto       q            = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto       resp         = get_buffer();
        const auto topic        = topic_by_name(topic_name);

        resp->pack(static_cast<uint8_t>(TankAPIMsgType::ReloadConf));

        const auto size_offset = resp->size();

        resp->pack(static_cast<uint32_t>(0), req_id, topic_name.size());
        resp->serialize(topic_name.data(), topic_name.size());
        resp->pack(partition_id);

        if (!topic) {
                resp->pack(static_cast<uint8_t>(1));
        } else {
                if (!topic->partitions_ || partition_id >= topic->partitions_->size()) {
                        resp->pack(static_cast<uint8_t>(10));
                } else {
                        auto path = Buffer::build(basePath_.as_s32(), '/', topic->name_, '/', partition_id, "/config"_s32);
                        int  fd   = open(path.c_str(), O_RDONLY);

                        if (-1 == fd) {
                                resp->pack(static_cast<uint8_t>(2));
                        } else if (const auto file_size = lseek(fd, 0, SEEK_END); file_size > 0) {
                                const auto vma_dtor = [file_size](auto ptr) {
                                        if (ptr && ptr != MAP_FAILED) {
                                                munmap(ptr, file_size);
                                        }
                                };
                                std::unique_ptr<void, decltype(vma_dtor)> vma(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0), vma_dtor);
                                auto                                      file_data = vma.get();

                                close(fd);
                                if (MAP_FAILED == file_data) {
                                        resp->pack(static_cast<uint8_t>(2));
                                } else {
                                        madvise(file_data, file_size, MADV_SEQUENTIAL | MADV_DONTDUMP);

                                        try {
                                                partition_config config;
                                                auto             partition = topic->partitions_->at(partition_id);

                                                parse_partition_config(str_view32(reinterpret_cast<const char *>(file_data), file_size), &config);
                                                partition->config = config;
                                                resp->pack(static_cast<uint8_t>(0));
                                        } catch (...) {
                                                resp->pack(static_cast<uint8_t>(3));
                                        }
                                }
                        } else {
                                resp->pack(static_cast<uint8_t>(2));
                                close(fd);
                        }
                }
        }

        *reinterpret_cast<uint32_t *>(resp->At(size_offset)) = resp->size() - size_offset - sizeof(uint32_t);

        auto payload = q->push_back(resp);

        payload->iovCnt = 1;
        payload->iov[0] = {static_cast<void *>(resp->data()), resp->size()};

        return try_send_ifnot_blocked(c);
}

bool Service::process_discover_partitions(connection *const c, const uint8_t *p, const size_t len) {
        if (unlikely(len < sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint8_t))) {
                if (trace)
                        SLog("Unexpected len = ", len, "\n");

                track_shutdown(c, __LINE__, "Unexpected length ", len, "\n");
                return shutdown(c, __LINE__);
        }

        auto       q         = c->outQ;
        auto       resp      = get_buffer();
        const auto requestId = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const strwlen8_t topicName((char *)p + 1, *p);

        p += topicName.len + sizeof(uint8_t);

        if (!q)
                q = c->outQ = get_outgoing_queue();

        resp->Serialize(uint8_t(TankAPIMsgType::DiscoverPartitions));
        const auto sizeOffset = resp->size();

        resp->RoomFor(sizeof(uint32_t));
        resp->Serialize(requestId);

        auto topic = topic_by_name(topicName);

        resp->Serialize(topicName.len);
        resp->Serialize(topicName.p, topicName.len);

        if (!topic)
                resp->Serialize(uint16_t(0));
        else {
                const auto n = topic->partitions_->size();

                resp->Serialize(uint16_t(n));
                resp->reserve(n * (sizeof(uint64_t) + sizeof(uint64_t)));

                for (const auto it : *topic->partitions_) {
                        auto log = it->log_.get();

                        resp->Serialize(log->firstAvailableSeqNum);
                        resp->Serialize(log->lastAssignedSeqNum);
                }
        }

        *reinterpret_cast<uint32_t *>(resp->At(sizeOffset)) = resp->size() - sizeOffset - sizeof(uint32_t);

        auto payload = q->push_back(resp);

        payload->iovCnt = 1;
        payload->iov[0] = {(void *)resp->data(), resp->size()};

        return try_send_ifnot_blocked(c);
}

bool Service::process_replica_reg(connection *const c, const uint8_t *p, const size_t len) {
        if (unlikely(len < sizeof(uint16_t)))
                return shutdown(c, __LINE__);

        c->replicaId = *(uint16_t *)p;

        if (unlikely(c->replicaId == 0))
                return shutdown(c, __LINE__);

        return true;
}

bool Service::process_produce(const TankAPIMsgType msg, connection *const c, const uint8_t *p, const size_t len) {
        if (unlikely(len < sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint8_t))) {
                track_shutdown(c, __LINE__, "Unexpected request\n");
                return shutdown(c, __LINE__);
        }

        const auto *const __end = p + len;

        if (unlikely(p + (*p) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint8_t) >= __end)) {
                track_shutdown(c, __LINE__, "Unexpected request\n");
                return shutdown(c, __LINE__);
        }

        const uint64_t              processBegin  = trace ? now_ms : 0;
        auto *const                 respHeader    = get_buffer();
        auto                        q             = c->outQ;
        [[maybe_unused]] const auto clientVersion = decode_pod<uint16_t>(p);
        const auto                  requestId     = decode_pod<uint32_t>(p);

        const strwlen8_t clientId(reinterpret_cast<const char *>(p) + 1, *p);
        p += clientId.size() + sizeof(uint8_t);
        [[maybe_unused]] const auto requiredAcks = *p++;
        [[maybe_unused]] const auto ackTimeout   = decode_pod<uint32_t>(p);
        const auto                  topicsCnt    = *p++;
        strwlen8_t                  topicName;
        strwlen32_t                 msgContent;

        Drequire(respHeader->empty());

        respHeader->Serialize(uint8_t(TankAPIMsgType::Produce));
        const auto sizeOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint32_t));

        if (!q)
                q = c->outQ = get_outgoing_queue();

        // It is very important that we queue this buffer(respHeader) here, before we may do in wakeup_wait_ctx()
        // so that if a client has issued a produce and a consume request for the same (topic,partition) from the same connection, the client
        // will get the produce response first and the consume later.
        //
        // Also, in this case, to avoid wakeup_wait_ctx() to invoke try_send_ifnot_blocked() which would mean that
        // it would send this produce response respHeader before it has been built, we are going
        // to provide this connection to wakeup_wait_ctx() so that it will check if the connection it need to wake up
        // is this connection, and if so, not wake it up for we will wake it up here.
        auto    payload = q->push_back(respHeader);
        uint8_t error_code;

        respHeader->Serialize(requestId);

        if (trace)
                SLog("Parsing ", topicsCnt, "\n");

        for (uint32_t i{0}; i != topicsCnt; ++i) {
                if (unlikely(p + (*p) >= __end)) {
                        track_shutdown(c, __LINE__, "Unexpected request\n");
                        return shutdown(c, __LINE__);
                }

                topicName.Set((char *)p + 1, *p);
                p += sizeof(uint8_t) + topicName.len;

                auto topic = topic_by_name(topicName);

                if (read_only) {
                        // can't do that
                        error_code = 0xfe;
                        goto skip1;
                }

                if (!topic) {
                        error_code = 0xff;

                        if (trace)
                                SLog("Unknown topic [", topicName, "]\n");

                skip1:
                        for (auto cnt = *p++; cnt; --cnt) {
                                if (unlikely(p + sizeof(uint16_t) >= __end)) {
                                        track_shutdown(c, __LINE__, "Unexpected request\n");
                                        return shutdown(c, __LINE__);
                                }

                                p += sizeof(uint16_t); // skip partition ID

                                if (unlikely(!Compression::UnpackUInt32Check(p, __end))) {
                                        track_shutdown(c, __LINE__, "Unexpected request\n");
                                        return shutdown(c, __LINE__);
                                }

                                p += Compression::UnpackUInt32(p); // skip bundle
                        }

                        respHeader->Serialize(uint8_t(error_code));
                        continue;
                }

                const auto                          partitionsCnt = *p++;
                range_base<const uint8_t *, size_t> msgSetContent;

                if (trace)
                        SLog("partitionsCnt:", partitionsCnt, " for [", topicName, "]\n");

                for (uint32_t k{0}; k != partitionsCnt; ++k) {
                        const auto partitionId = decode_pod<uint16_t>(p);
                        const auto bundleLen   = Compression::UnpackUInt32(p);
                        auto       partition   = topic->partition(partitionId);
                        uint64_t   firstMsgSeqNum{0}, lastMsgSeqNum;

                        if (unlikely(!bundleLen)) {
                                track_shutdown(c, __LINE__, "Unexpected request\n");
                                return shutdown(c, __LINE__);
                        }

                        if (msg == TankAPIMsgType::ProduceWithBaseSeqNum) {
                                // See common.h
                                firstMsgSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

                                if (trace)
                                        SLog("ProduceWithBaseSeqNum request, firstMsgSeqNum = ", firstMsgSeqNum, "\n");
                        }

                        // TODO(markp): Reject appending to internal topics if not allowed

                        if (trace)
                                SLog("partitionId = ", partitionId, ",  bundleLen = ", bundleLen, "\n");

                        // BEGIN: bundle header
                        // See tank_encoding.md
                        const auto *const bundle = p;
                        const auto *const e      = p + bundleLen;

                        if (!partition) {
                                if (trace)
                                        SLog("Undefined topic partition ", partitionId, "\n");

                                p = e;
                                respHeader->Serialize<uint8_t>(1);
                                continue;
                        }

                        const auto bundleFlags        = *p++;
                        const auto codec              = bundleFlags & 3;
                        const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                        auto       msgSetSize         = uint32_t((bundleFlags >> 2) & 0xf);

                        if (!msgSetSize) {
                                // more than 15 messages in the message set; were not able to encode that in the 4 bits reserved
                                // in flags; so that's encoded as a varint here
                                msgSetSize = Compression::UnpackUInt32(p);

                                if (unlikely(msgSetSize == 0)) {
                                        // This is absolutely unacceptable
                                        track_shutdown(c, __LINE__, "Unexpected request\n");
                                        return shutdown(c, __LINE__);
                                }
                        }

                        if (sparseBundleBitSet) {
                                // The abs.sequence number of the first message in the message set of this bundle
                                firstMsgSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

                                if (msgSetSize != 1) {
                                        // multiple messages in the bundle
                                        // compute the last message in the bundle abs.seqNumber from
                                        // the encoded delta (see tank_encoding.md)
                                        lastMsgSeqNum = firstMsgSeqNum + Compression::UnpackUInt32(p) + 1;
                                } else {
                                        lastMsgSeqNum = firstMsgSeqNum;
                                }

                                if (trace)
                                        SLog("sparseBundleBitSet set: firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum ", lastMsgSeqNum, "\n");

                                if (unlikely(lastMsgSeqNum < firstMsgSeqNum || !lastMsgSeqNum)) {
                                        // Invalid; sequence number's base is (1)
                                        track_shutdown(c, __LINE__, "Unexpected request\n");
                                        return shutdown(c, __LINE__);
                                }
                        } else {
                                lastMsgSeqNum = 0;
                        }

                        if (trace)
                                SLog("bundleFlags = ", bundleFlags, ", codec= ", codec, ", message set messages cnt: ", msgSetSize, "\n");
                        // END: bundle header

                        if (trace) {
                                static thread_local IOBuffer decompressionBuf;
                                strwlen8_t                   key;

                                if (codec) {
                                        if (trace)
                                                SLog("codec = ", codec, ", will decompress\n");

                                        decompressionBuf.clear();
                                        if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, e - p, &decompressionBuf)) {
                                                std::abort();
                                        }

                                        msgSetContent.Set(reinterpret_cast<const uint8_t *>(decompressionBuf.data()), decompressionBuf.size());
                                } else
                                        msgSetContent.Set(p, e - p);

                                // iterate bundle's message set
                                uint64_t              msgTs{0};
                                uint64_t              absMsgSeqNum, prevAbsMsgSeqNum;
                                [[maybe_unused]] bool monotonicallyIncreasing { true };
                                uint32_t              msgIdx{0};

                                if (trace)
                                        SLog("Iterating ", msgSetContent.len, " bytes\n");

                                for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e; ++msgIdx, ++absMsgSeqNum) {
                                        // Next message set message
                                        const auto flags = *p++;

                                        if (trace)
                                                SLog("Considering ", msgIdx, " / ", msgSetSize, "\n");

                                        if (sparseBundleBitSet) {
                                                // this is _never_ set for the first nor the last message in the set
                                                // because we already encode that in the bundle header
                                                if (msgIdx == 0)
                                                        absMsgSeqNum = firstMsgSeqNum;
                                                else {
                                                        if (msgIdx == msgSetSize - 1)
                                                                absMsgSeqNum = lastMsgSeqNum;
                                                        else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                                // incremented in for()
                                                        } else {
                                                                const auto delta = Compression::UnpackUInt32(p);

                                                                if (trace)
                                                                        SLog("Delta from previous (", delta, ")\n");

                                                                absMsgSeqNum += delta;
                                                        }

                                                        if (absMsgSeqNum <= prevAbsMsgSeqNum) {
                                                                // Yeah, this is wrong
                                                                monotonicallyIncreasing = false;
                                                        }
                                                }

                                                if (trace)
                                                        SLog("sparseBundleBitSet, absMsgSeqNum = ", absMsgSeqNum, "\n");

                                                prevAbsMsgSeqNum = absMsgSeqNum;
                                        }

                                        if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                                if (trace)
                                                        SLog("New msgTs\n");

                                                msgTs = *(uint64_t *)p;
                                                p += sizeof(uint64_t);
                                        } else if (trace)
                                                SLog("Using last set msgTs\n");

                                        if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                                key.Set((char *)p + 1, *p);
                                                p += key.len + sizeof(uint8_t);

                                                if (trace)
                                                        SLog("have key\n");
                                        } else
                                                key.reset();

                                        const auto msgLen = Compression::UnpackUInt32(p);

                                        if (trace)
                                                SLog("msgLen = ", msgLen, "\n");

                                        (void)flags;
                                        (void)msgTs;
                                        msgContent.Set((char *)p, msgLen);
                                        p += msgLen;

                                        Print("[", key, "] = ", msgContent, " ", msgTs, "(", Date::ts_repr(Timings::Milliseconds::ToSeconds(msgTs)), ")\n");
                                }
                        }

                        //
                        // XXX:
                        // If we append to a fresh partition (i.e firstAvailableSeqNum == 0) 2 msgs
                        // and right after that we append 2 more msgs to an explicit firstMsgSeqNum = 5112 using e.g tank-cli set -S 5112
                        // and then we try to e.g tank-cli get from that partition starting at 0
                        // then we 'll get 4 msgs, _but_ the client won't have a clue that the 2 later messages have a different
                        // id (starting from 5112), and instead will consider them to have ids 3 and 4 respectively, and so, it will
                        // set next.seqNum to 5. (which is wrong). Setting their ids to 3 and 4 is also wrong.
                        //
                        // If the client attempts to request again starting from 5 (because that's what next.seqNum was set to), it will get the same
                        // messages.
                        // So we need to either allow explicit firstMsgSeqNum > 0 only iff the partition is empty, or we need
                        // to explicitly use a sparse bundle if we need to do that (at least for the first bundle!)
                        //
                        // That's why we need to use TankClient::produce_with_base()
                        // Rules:
                        // 1. Always use sparse bundles when compacting
                        // 2. use TankClient::produce_with_base() for mirroring
                        // 3. Expect that everything will work out otherwise if you are just building Tank apps.
                        [[maybe_unused]] const uint64_t b   = trace ? Timings::Microseconds::Tick() : 0;
                        const auto                      res = partition->append_bundle_to_leader(curTime, bundle, bundleLen, msgSetSize, expiredCtxList3, firstMsgSeqNum, lastMsgSeqNum);

                        if (unlikely(!res.fdh)) {
                                if (res.dataRange.size() == UINT32_MAX) {
                                        // Invalid request(offsets)
                                        respHeader->Serialize(uint8_t(2));
                                } else {
                                        // System error
                                        respHeader->Serialize(uint8_t(10));
                                }
                        } else {
                                // success
                                respHeader->Serialize(uint8_t(0));

                                // we deliberatly only care for the actual bundle for bytes_in
                                // i.e we don't account for the header
                                topic->metrics.bytes_in += bundleLen;
                                topic->metrics.msgs_in += msgSetSize;
                        }

                        if (trace)
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(b)), " for ", msgSetSize, " msgs in bundle message set: ", expiredCtxList3.size(), " ", res.fdh ? res.fdh->use_count() : 0, "\n");

                        while (!expiredCtxList3.empty()) {
                                auto ctx = expiredCtxList3.Pop();

                                wakeup_wait_ctx(ctx, res, c);
                        }

                        p = e; // to next partition
                }
        }
        *reinterpret_cast<uint32_t *>(respHeader->At(sizeOffset)) = respHeader->size() - sizeOffset - sizeof(uint32_t);

        payload->iovCnt = 1;
        payload->iov[0] = {(void *)respHeader->data(), respHeader->size()};

        if (trace) {
                // for 100MBs, this takes Took 0.005s
                SLog("Took ", duration_repr(Timings::Microseconds::Since(processBegin)), "\n");
        }

        return try_send_ifnot_blocked(c);
}

bool Service::process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len) {
        if (trace) {
                SLog("New message  type ", msg, ", len ", len, "\n");
        }

        switch (TankAPIMsgType(msg)) {
                case TankAPIMsgType::Produce:
                case TankAPIMsgType::ProduceWithBaseSeqNum:
                        return process_produce(TankAPIMsgType(msg), c, data, len);

                case TankAPIMsgType::Consume:
                        return process_consume(c, data, len);

                case TankAPIMsgType::Ping:
                        return true;

                case TankAPIMsgType::RegReplica:
                        return process_replica_reg(c, data, len);

                case TankAPIMsgType::DiscoverPartitions:
                        return process_discover_partitions(c, data, len);

                case TankAPIMsgType::ReloadConf:
                        return process_load_conf(c, data, len);

                case TankAPIMsgType::CreateTopic:
                        return process_create_topic(c, data, len);

                default:
                        return shutdown(c, __LINE__);
        }
}

void Service::wakeup_wait_ctx(wait_ctx *const wctx, const append_res & /*appendRes*/, connection *const produceConnection) {
        auto    respHeader = get_buffer();
        uint8_t topicsCnt{0};
        auto    c = wctx->c;
        auto    q = c->outQ;
        size_t  sum{0};

        if (!q)
                q = c->outQ = get_outgoing_queue();

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_green, "Waking up wait ctx ", ptr_repr(wctx), ", ", wctx->requestId, ansifmt::reset, ansifmt::reset, "\n");

        auto payload = q->push_back(respHeader);

        respHeader->Serialize(uint8_t(2));
        const auto sizeOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint32_t));
        const auto headerSizeOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint32_t));
        respHeader->Serialize(wctx->requestId);
        const auto topicsCntOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint8_t));

        for (uint32_t i{0}; i != wctx->partitionsCnt;) {
                auto        it        = wctx->partitions + i;
                const auto *p         = it->partition;
                const auto  t         = p->owner;
                const auto  topicName = t->name();
                uint8_t     partitionsCnt{0};

                ++topicsCnt;
                respHeader->Serialize(topicName.len);
                respHeader->Serialize(topicName.p, topicName.len);

                if (trace)
                        SLog("Topic [", topicName, "]\n");

                const auto partitionsCntOffset = respHeader->size();
                respHeader->RoomFor(sizeof(uint8_t));

                do {
                        if (trace)
                                SLog("partition ", p->idx, "\n");

                        respHeader->Serialize(p->idx);
                        respHeader->Serialize(uint8_t(0));
                        if (it->fdh) {

                                if (trace)
                                        SLog("HAVE data for ", topicName, ".", p->idx, ", seqNum = ", it->seqNum, ", range ", it->range, " (", it->range.len, ") ", ptr_repr(it->fdh), " ", it->fdh->use_count(), "\n");

                                respHeader->Serialize(it->seqNum);
                                respHeader->Serialize(p->highwater_mark());
                                respHeader->Serialize(it->range.len);

                                sum += it->range.len;

                                const auto __v = it->fdh->use_count();

                                q->push_back({it->fdh, it->range, Timings::Microseconds::Tick(), t});

                                require(it->fdh->use_count() == __v + 1);

                                it->fdh->Release(); // was retained in consider_append_res()
                                it->fdh = nullptr;
                        } else {
                                respHeader->Serialize(uint64_t(0));
                                respHeader->Serialize(p->highwater_mark());
                                respHeader->Serialize(uint32_t(0));
                        }

                        ++partitionsCnt;
                } while (++i != wctx->partitionsCnt && (p = (it = wctx->partitions + i)->partition)->owner == t);

                *reinterpret_cast<uint8_t *>(respHeader->At(partitionsCntOffset)) = partitionsCnt;
        }

        *reinterpret_cast<uint8_t *>(respHeader->At(topicsCntOffset))   = topicsCnt;
        *reinterpret_cast<uint32_t *>(respHeader->At(sizeOffset))       = respHeader->size() - sizeOffset - sizeof(uint32_t) + sum;
        *reinterpret_cast<uint32_t *>(respHeader->At(headerSizeOffset)) = respHeader->size() - headerSizeOffset - sizeof(uint32_t);

        destroy_wait_ctx(wctx);

        payload->iovCnt = 1;
        payload->iov[0] = {static_cast<void *>(respHeader->data()), respHeader->size()};

        if (c != produceConnection) {
                // see process_produce()
                try_send_ifnot_blocked(c);
        }
}

void Service::abort_wait_ctx(wait_ctx *const wctx) {
        if (wctx->scheduledForDtor)
                return;

        auto    respHeader = get_buffer();
        uint8_t topicsCnt{0};
        auto    c = wctx->c;

        if (trace)
                SLog("Aborting wait ctx ", ptr_repr(wctx), ", ", wctx->requestId, "\n");

        respHeader->Serialize(uint8_t(2));
        const auto sizeOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint32_t));
        const auto headerSizeOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint32_t));
        respHeader->Serialize(wctx->requestId);
        const auto topicsCntOffset = respHeader->size();
        respHeader->RoomFor(sizeof(uint8_t));

        for (uint32_t i{0}; i != wctx->partitionsCnt;) {
                auto        it        = wctx->partitions + i;
                const auto *p         = it->partition;
                const auto  t         = p->owner;
                const auto  topicName = t->name();
                uint8_t     partitionsCnt{0};

                ++topicsCnt;
                respHeader->Serialize(topicName.len);
                respHeader->Serialize(topicName.p, topicName.len);

                if (trace)
                        SLog("Topic [", topicName, "]\n");

                const auto partitionsCntOffset = respHeader->size();
                respHeader->RoomFor(sizeof(uint8_t));

                do {
                        if (trace)
                                SLog("partition ", p->idx, "\n");

                        if (it->fdh) {
                                it->fdh->Release();
                                it->fdh = nullptr;
                        }

                        respHeader->Serialize(p->idx);
                        respHeader->Serialize(uint8_t(0));
                        respHeader->Serialize(uint64_t(0));
                        respHeader->Serialize(p->highwater_mark());
                        respHeader->Serialize(uint32_t(0));
                        ++partitionsCnt;
                } while (++i != wctx->partitionsCnt && (p = (it = wctx->partitions + i)->partition)->owner == t);

                *reinterpret_cast<uint8_t *>(respHeader->At(partitionsCntOffset)) = partitionsCnt;
        }

        *reinterpret_cast<uint8_t *>(respHeader->At(topicsCntOffset))   = topicsCnt;
        *reinterpret_cast<uint32_t *>(respHeader->At(sizeOffset))       = respHeader->size() - sizeOffset - sizeof(uint32_t);
        *reinterpret_cast<uint32_t *>(respHeader->At(headerSizeOffset)) = respHeader->size() - headerSizeOffset - sizeof(uint32_t);

        auto q = c->outQ;

        if (!q)
                q = c->outQ = get_outgoing_queue();

        auto payload = q->push_back(respHeader);

        payload->iovCnt = 1;
        payload->iov[0] = {static_cast<void *>(respHeader->data()), respHeader->size()};

        // XXX: hypotethetical scenario where this could fail
        // 1. client sets wait time for consume request e.g 10ms
        // 2. tank accepts the connection, sets NeedOutAvail and add with (POLLIN|POLLOUT)
        // 3. tank reads the request, processes it, and because there is no content available
        // 	register_consumer_wait() to be fired in about 10ms
        // 4. (now_ms > nextExpWaitCtxCheck) is true, and wait context is now aborted
        //  but this try_send_ifnot_blocked() won't schedule data immediately
        // because NeedOutAvail is still set, because POLLOUT hasn't yet become available.
        //
        // If for some reason we don't get POLLOUT, and thus, invoke try_send()
        // then we 'll never get to respond to the client

        destroy_wait_ctx(wctx);
        try_send_ifnot_blocked(c);
}

void Service::destroy_wait_ctx(wait_ctx *const wctx) {
        if (wctx->scheduledForDtor) {
                return;
        }

        switch_dlist_del(&wctx->list);

        if (trace) {
                SLog("destroying ", ptr_repr(wctx), " ", wctx->partitionsCnt, "\n");
        }

        for (uint32_t i{0}; i != wctx->partitionsCnt; ++i) {
                auto &it = wctx->partitions[i];
                auto  p  = it.partition;

                if (it.fdh) {
                        if (trace) {
                                SLog("Releasing ", ptr_repr(it.fdh), " ", it.fdh->use_count(), "\n");
                        }

                        it.fdh->Release();
                        it.fdh = nullptr;
                }

                p->waitingList.RemoveByValue(wctx);
        }

        cancel_timer(&wctx->exp_tree_node.node);

        // Defer put_waitctx() until the next iteration
        wctx->scheduledForDtor = true;
        waitCtxDeferredGC.push_back(wctx);
}

void Service::cleanup_connection(connection *const c) {
        const bool prom_connection = c->src_prometheus();

        poller.erase(c->fd);
        close(c->fd);
        c->fd = -1;

        if (!prom_connection) {
                // For simplicity, place in a vector and drain it instead
                expiredCtxList.clear();
                for (auto it = c->waitCtxList.next; it != &c->waitCtxList; it = it->next) {
                        expiredCtxList.push_back(switch_list_entry(wait_ctx, list, it));
                }

                while (!expiredCtxList.empty()) {
                        destroy_wait_ctx(expiredCtxList.Pop());
                }
        }

        switch_dlist_del(&c->connectionsList);

        if (auto b = std::exchange(c->inB, nullptr)) {
                b->clear();
                put_buffer(b);
        }

        if (auto q = std::exchange(c->outQ, nullptr)) {
                put_outgoing_queue(q);
        }

        put_connection(c);
}

bool Service::shutdown(connection *const c, const uint32_t ref) {
        if (trace) {
                SLog("SHUTDOWN at ", ref, " ", time(nullptr), "\n");
        }

        EXPECT(c->fd != -1);
        cleanup_connection(c);
        return false;
}

void Service::introduce_self(connection *const c, bool &haveCork) {
        uint8_t b[sizeof(uint8_t) + sizeof(uint32_t)];
        auto    q  = c->outQ;
        int     fd = c->fd;

        b[0]                                               = uint8_t(TankAPIMsgType::Ping); // msg = ping
        *reinterpret_cast<uint32_t *>(b + sizeof(uint8_t)) = 0;                             // no payload

        if (trace)
                SLog("PINGING\n");

        if (q && !q->empty()) {
                if (trace)
                        SLog("Activating Cork\n");

                haveCork = true;
                Switch::SetTCPCork(fd, 1);
        }

        if (write(fd, b, sizeof(b)) != sizeof(b)) {
                RFLog("Failed to ping client:", strerror(errno), "\n");
                throw Switch::system_error("Failed to ping client");
        }

        c->state.flags &= ~(1u << uint8_t(connection::State::Flags::PendingIntro));
}

void Service::stop_poll_outavail(connection *c) {
        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))) {
                poller.set_data_events(c->fd, c, POLLIN);
                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::NeedOutAvail));
        }
}

void Service::poll_outavail(connection *const c) {
        if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))) {
                if (trace)
                        SLog("Polling out availability\n");

                c->state.flags |= (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                poller.set_data_events(c->fd, c, POLLIN | POLLOUT);
        }
}

bool Service::flush_iov(connection *const c, struct iovec *const iov, const uint32_t iovCnt, const bool have_cork) {
        const auto fd = c->fd;
        auto       q  = c->outQ;

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_magenta, "FLUSHING ", iovCnt, ansifmt::reset, "\n");

        for (;;) {
                auto r = writev(fd, iov, iovCnt);

                if (r == -1) {
                        if (errno == EINTR)
                                continue;
                        else if (errno == EAGAIN) {
                                if (trace)
                                        SLog("Deactivating Cork\n");

                                if (have_cork)
                                        Switch::SetTCPCork(fd, 0);
                                poll_outavail(c);
                                return true;
                        } else {
                                track_shutdown(c, __LINE__, "writev() failed for ", iovCnt, ": ", strerror(errno), "\n");
                                return shutdown(c, __LINE__);
                        }
                }

                for (;;) {
                next_gather:
                        auto &it  = q->front();
                        auto  ptr = it.iov + it.iovIdx;

                        while (r >= ptr->iov_len) {
                                r -= ptr->iov_len;

                                if (++it.iovIdx == it.iovCnt) {
                                        put_buffer(it.buf);
                                        q->pop_front();

                                        if (!r) {
                                                // done
                                                if (have_cork)
                                                        Switch::SetTCPCork(fd, 0);

                                                if (c->state.flags & (1 << unsigned(connection::State::Flags::DrainingForShutdown))) {
                                                        if (trace)
                                                                SLog("drained outgoing data\n");
                                                        return shutdown(c, __LINE__);
                                                } else
                                                        return true;
                                        } else {
                                                // flushed data for next payload
                                                goto next_gather;
                                        }
                                } else
                                        ++ptr;
                        }

                        ptr->iov_base = reinterpret_cast<char *>(ptr->iov_base) + r;
                        ptr->iov_len -= r;

                        if (trace)
                                SLog("Deactivating Cork\n");

                        if (have_cork)
                                Switch::SetTCPCork(fd, 0);
                        poll_outavail(c);
                        return true;
                }

                break;
        }
}

// This method's implementation is somewhat more complex that it perhaps ought to be, but we need to
// keep the syscalls count down to minimum and coallesce data to write
//
// Related: https://github.com/phaistos-networks/TANK/issues/41
bool Service::try_send(connection *const c) {
        const auto   fd{c->fd};
        auto         q{c->outQ};
        bool         haveCork{false};
        struct iovec iov[1024];
        uint32_t     iovCnt{0};

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::PendingIntro)))
                introduce_self(c, haveCork);

        if (!q) {
                if (trace)
                        SLog("Nothing to send\n");

                stop_poll_outavail(c);
                return true;
        }

        if (trace)
                SLog("Attempting to send ", q->size(), " payloads\n");

        const auto                   end{q->backIdx};
        [[maybe_unused]] std::size_t transmitted { 0 };
        // TODO(markp): transmit_trheshold should probably be computed dynamically based on current approximate load and other signals
        static constexpr std::size_t transmit_trheshold{24 * 1024 * 1024};
        uint32_t                     next;

        for (auto idx = q->frontIdx; idx != end; idx = next) {
                auto &it = q->A[idx];

                next = q->next(idx); // idx is the last one if (next == end)
                if (it.payloadBuf) {
                        const auto n = std::min<uint32_t>(it.iovCnt - it.iovIdx, sizeof_array(iov) - iovCnt);

                        if (trace)
                                SLog("payload holds buffer {", ptr_repr(it.buf), ", ", it.iovCnt, "}\n");

                        EXPECT(it.iovCnt);
                        EXPECT(it.iovIdx < it.iovCnt);

                        memcpy(iov + iovCnt, it.iov + it.iovIdx, sizeof(struct iovec) * n);
                        iovCnt += n;

                        if (unlikely(iovCnt == sizeof_array(iov))) {
                                // local iov[] is full, need to flush it now
                                if (!haveCork && next != end) {
                                        // only cork if we need to
                                        if (trace)
                                                SLog("Activating Cork\n");

                                        haveCork = true;
                                        Switch::SetTCPCork(fd, 1);
                                }

                                if (!flush_iov(c, iov, iovCnt, haveCork)) {
                                        return false;
                                } else {
                                        iovCnt = 0;
                                }
                        }
                } else {
                        if (trace) {
                                SLog("payload holds content range (iovCnt = ", iovCnt, ")\n");
                        }

                        if (iovCnt) {
                                if (!haveCork) {
                                        // we really need to cork here
                                        // because we have pending iovecs to stream before we stream the file range
                                        if (trace) {
                                                SLog("Activating Cork\n");
                                        }

                                        haveCork = true;
                                        Switch::SetTCPCork(fd, 1);
                                }

                                // flush iov[]
                                for (;;) {
                                        auto r = writev(fd, iov, iovCnt);

                                        if (r == -1) {
                                                if (errno == EINTR)
                                                        continue;
                                                else if (errno == EAGAIN) {
                                                        if (trace)
                                                                SLog("Deactivating Cork\n");

                                                        Switch::SetTCPCork(fd, 0);
                                                        poll_outavail(c);
                                                        return true;
                                                } else {
                                                        track_shutdown(c, __LINE__, "writev() failed:", strerror(errno), " for ", iovCnt, "\n");
                                                        return shutdown(c, __LINE__);
                                                }
                                        }

                                        for (;;) {
                                        next:
                                                auto &it  = q->front();
                                                auto  ptr = it.iov + it.iovIdx;

                                                while (r >= ptr->iov_len) {
                                                        r -= ptr->iov_len;

                                                        if (++it.iovIdx == it.iovCnt) {
                                                                put_buffer(it.buf);
                                                                q->pop_front();

                                                                if (!r) {
                                                                        // done
                                                                        goto flushed_iov;
                                                                } else {
                                                                        // flushed data for next payload
                                                                        goto next;
                                                                }
                                                        } else
                                                                ++ptr;
                                                }

                                                ptr->iov_base = reinterpret_cast<char *>(ptr->iov_base) + r;
                                                ptr->iov_len -= r;

                                                if (trace)
                                                        SLog("Deactivating Cork\n");

                                                Switch::SetTCPCork(fd, 0);
                                                poll_outavail(c);
                                                return true;
                                        }

                                        break;
                                }

                        flushed_iov:
                                iovCnt = 0;
                        }

                        // https://github.com/phaistos-networks/TANK/issues/14
                        //
                        // If only FreeBSD's great sendfile() syscall was available on Linux, with support for the
                        // extra flags based on NGINX's and Netflix's work, that'd make everything so much simpler.
                        // We 'd just use the SF_NODISKIO flag and the SF_READAHEAD macro, and check for EBUSY
                        // and optionally use readahead() and try again later(we could also mmap() the log and use mincore() to determine if
                        // all pages are cached)
                        for (size_t maxSpan{128 * 1024}, sum{0};; maxSpan = 640 * 1024) {
                                auto &         range  = it.file_range.range;
                                const uint64_t before = Timings::Microseconds::Tick();
                                // This is probably a good idea; break this down into multiple requests; syscall overhead should be low. Give readahead() a chance to page-in data
                                // in order to reduce the likelihood of blocking here waiting for that
                                //
                                // We are also now paying attention to how long we have spent in this function so that we can abort early if we have neglected
                                // other connections for too long.
                                //
                                // https://github.com/phaistos-networks/TANK/issues/14#issuecomment-301000261
                                const auto outLen = std::min<size_t>(range.len, maxSpan);

#ifdef HAVE_SENDFILE64
                                off64_t    offset = range.offset;
                                const auto r      = sendfile64(fd, it.file_range.fdh->fd, &offset, outLen);
#else
                                off_t offset = range.offset;
                                const auto r = sendfile(fd, it.file_range.fdh->fd, &offset, outLen);
#endif

                                sum += Timings::Microseconds::Since(before);

                                if (trace) {
                                        // See https://github.com/phaistos-networks/TANK/issues/14 for measurements
                                        SLog("Sending contents ", range, " => ", size_repr(r), " ", size_repr(range.size()), " ", duration_repr(Timings::Microseconds::Since(before)), "\n");
                                }

                                if (r == -1) {
                                        if (errno == EINTR)
                                                continue;
                                        else if (errno == EAGAIN) {
                                                if (trace)
                                                        SLog("EAGAIN (haveCork = ", haveCork, ", needOutAvail = ", (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))), ")\n");

                                                if (haveCork) {
                                                        if (trace)
                                                                SLog("Deactivating Cork\n");

                                                        Switch::SetTCPCork(fd, 0);
                                                }

                                                poll_outavail(c);
                                                return true;
                                        } else {
                                                track_shutdown(c, __LINE__, "sendfile() failed: ", strerror(errno), " for ", outLen, "\n");
                                                return shutdown(c, __LINE__);
                                        }
                                } else {
                                        range.len -= r;
                                        range.offset += r;

                                        if (it.tracker.since) {
                                                it.tracker.src_topic->metrics.bytes_out += r;
                                        }

                                        if (0 == range.len) {
                                                if (trace)
                                                        SLog("SENT-releasing ", ansifmt::bold, ansifmt::color_blue, ptr_repr(it.file_range.fdh), " ", it.file_range.fdh->use_count(), ansifmt::reset, "\n");

                                                if (const auto since = it.tracker.since) {
                                                        const auto delta = Timings::Microseconds::ToMillis(Timings::Microseconds::Since(since));
                                                        auto       topic{it.tracker.src_topic};

                                                        if (trace)
                                                                SLog("Took ", duration_repr(delta), " to send\n");

                                                        require(topic);
                                                        topic->metrics.latency.reg_sample(delta);
                                                }

                                                it.file_range.fdh->Release();
                                                q->pop_front();
                                                break;
                                        }

                                        if (trace)
                                                SLog("r = ", r, ", outLen = ", outLen, "\n");

                                        if (r != outLen) {
                                                // if we didn't get to sendfile() all the data we wanted to write
                                                // it means that the socket buffer is now full, and it will almost definitely
                                                // not be drained by the time we call sendfile() again in this loop; we 'd insted
                                                // get EAGAIN.
                                                // So we 'll just consider the socket buffer full now and execute the EAGAIN logic
                                                if (haveCork) {
                                                        if (trace)
                                                                SLog("Deactivating Cork\n");

                                                        Switch::SetTCPCork(fd, 0);
                                                }

                                                poll_outavail(c);
                                                return true;
                                        }

                                        transmitted += r;
                                        if (sum > 2'000) // over 0.002s?
                                        {
                                                // Spent too long here, looks like we are reading data not currently in the kernel VM cache
                                                // so, give readhead() a fair chance to work for us, and return control to the loop so that other
                                                // clients and their connections can be served.
                                                // https://github.com/phaistos-networks/TANK/issues/14#issuecomment-301442619
                                                if (trace)
                                                        SLog("Bailing, transmitted ", transmitted, " but spent ", sum, " ", duration_repr(sum), "\n");

                                                goto l2;
                                        }

                                        if (unlikely(transmitted > transmit_trheshold)) {
                                        // Be fair to all other connections
                                        // We tansferred too much data already
                                        //
                                        // The only downside is the overhead epoll_ctl() call(see poll_outavail() impl.)
                                        // and that if there are no other consumers/producers this is not going to produce any benefits.
                                        // https://github.com/phaistos-networks/TANK/issues/14#issuecomment-301000261
                                        l2:
                                                if (haveCork) {
                                                        if (trace)
                                                                SLog("Deactivating cork\n");
                                                        Switch::SetTCPCork(fd, 0);
                                                }

                                                poll_outavail(c);
                                                return true;
                                        }
                                }
                        }
                }
        }

        if (iovCnt) {
                if (trace)
                        SLog("Sending pending outgoing iov[] ", iovCnt, "\n");

                for (;;) {
                        auto r = writev(fd, iov, iovCnt);

                        if (trace)
                                SLog("r = ", r, " for ", iovCnt, "\n");

                        if (r == -1) {
                                if (errno == EINTR) {
                                        continue;
                                } else if (errno == EAGAIN) {
                                        if (haveCork) {
                                                if (trace)
                                                        SLog("Deactivating Cork\n");

                                                Switch::SetTCPCork(fd, 0);
                                        }

                                        poll_outavail(c);
                                        return true;
                                } else {
                                        track_shutdown(c, __LINE__, "writev() failed for ", iovCnt, ": ", strerror(errno), "\n");
                                        return shutdown(c, __LINE__);
                                }
                        }

                        for (;;) {
                        next2:
                                auto &it  = q->front();
                                auto  ptr = it.iov + it.iovIdx;

                                if (trace)
                                        SLog("Payload iovIdx = ", it.iovIdx, " ", it.iovCnt, "\n");

                                while (r >= ptr->iov_len) {
                                        r -= ptr->iov_len;

                                        if (++it.iovIdx == it.iovCnt) {
                                                put_buffer(it.buf);
                                                q->pop_front();

                                                if (trace)
                                                        SLog("done with payload, r now = ", r, ", close on flush = ", c->close_on_flush(), ", haveCork = ", haveCork, "\n");

                                                if (!r) {
                                                        // done with all data we we need to flush for the buffers
                                                        require(q->empty()); // sanity check

                                                        if (haveCork) {
                                                                if (trace)
                                                                        SLog("Deactivating cork\n");

                                                                Switch::SetTCPCork(fd, 0);
                                                                haveCork = false;
                                                        }

                                                        if (trace)
                                                                SLog("Drained queue\n");

                                                        if (c->close_on_flush()) {
                                                                if (trace)
                                                                        SLog("Drained\n");

                                                                return shutdown(c, __LINE__);
                                                        } else {
                                                                // done with the queue
                                                                goto l150;
                                                        }
                                                } else {
                                                        // there's definitely more
                                                        goto next2;
                                                }
                                        } else
                                                ++ptr;
                                }

                                ptr->iov_base = reinterpret_cast<char *>(ptr->iov_base) + r;
                                ptr->iov_len -= r;

                                if (haveCork) {
                                        if (trace)
                                                SLog("Deactivating Cork\n");

                                        Switch::SetTCPCork(fd, 0);
                                }

                                poll_outavail(c);
                                return true;
                        }
                        break;
                }
        }

        if (haveCork) {
                if (trace)
                        SLog("Deactivating Cork\n");

                Switch::SetTCPCork(fd, 0);
        }

l150:
        if (trace)
                SLog("Releasing queue\n");

        put_outgoing_queue(q);
        c->outQ = nullptr;

        stop_poll_outavail(c);
        return true;
}

Service::~Service() {
        while (switch_dlist_any(&allConnections)) {
                cleanup_connection(switch_list_entry(connection, connectionsList, allConnections.next));
        }

        while (!bufs.empty()) {
                delete bufs.Pop();
        }

        while (!outgoingQueuesPool.empty()) {
                delete outgoingQueuesPool.Pop();
        }

        for (auto it : waitCtxDeferredGC) {
                put_waitctx(it);
        }

        waitCtxDeferredGC.clear();

        while (!connsPool.empty()) {
                delete connsPool.Pop();
        }

        for (auto &it : topics) {
                for (auto p : *it.second->partitions_) {
                        for (auto it : p->waitingList)
                                std::free(it);

                        p->waitingList.clear();
                }
        }

        for (auto &it : waitCtxPool) {
                while (!it.empty()) {
                        std::free(it.Pop());
                }
        }
}

int Service::try_accept(int fd) {
        sockaddr_in sa;
        socklen_t   saLen = sizeof(sa);
        int         newFd = accept4(fd, reinterpret_cast<sockaddr *>(&sa), &saLen, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (newFd == -1) {
                if (errno == EMFILE || errno == ENFILE) {
                        // TODO(markp): Ideally, we 'd poller.erase(listenFd);
                        // set listening = false; and if a connection's closed, check
                        // if (listening == false)  { poller.insert(listenFd, POLLIN, &listenFd); listening  = true; }
                        // so that we won't waste any cycles attempting to accept4() only to fail because of this here reason
                        // This would probably be useful in case someone puprosely tried to harm the service, or some badly written client(s)
                        // are acting up.
                        //
                        // For now, just do nothing
                        // Reported @by @rkrambovitis
                        return 0;
                } else if (errno != EINTR && errno != EAGAIN) {
                        RFLog("accept4(): ", strerror(errno), "\n");
                        return 1;
                }
        } else {
                static const auto rcvBufSize = strwlen32_t(getenv("TANK_BROKER_SOCKBUF_RCV_SIZE") ?: "1048576").AsUint32();
                static const auto sndBufSize = strwlen32_t(getenv("TANK_BROKER_SOCKBUF_SND_SIZE") ?: "1048576").AsUint32();

                require(saLen == sizeof(sockaddr_in));

                auto c = get_connection();

                require(c);

                // Kafka's default is 1mb for both buffers
                if (rcvBufSize) {
                        if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&rcvBufSize, sizeof(rcvBufSize)) == -1)
                                Print("WARNING: unable to set socket receive buffer size:", strerror(errno), "\n");
                }

                if (sndBufSize) {
                        if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&sndBufSize, sizeof(sndBufSize)) == -1)
                                Print("WARNING: unable to set socket send buffer size:", strerror(errno), "\n");
                }

                c->addr4     = *reinterpret_cast<const uint32_t *>(&sa.sin_addr.s_addr);
                c->fd        = newFd;
                c->replicaId = 0;
                switch_dlist_init(&c->connectionsList);
                switch_dlist_init(&c->waitCtxList);
                switch_dlist_insert_after(&allConnections, &c->connectionsList);

                Switch::SetNoDelay(c->fd, 1);

                // We are going to ping as soon as we can so that the client will know we have been accepted
                // but we can't write(fd, ..) now; so we 'll need to wait for POLLOUT and then ping
                c->state.flags = (1u << uint8_t(connection::State::Flags::PendingIntro)) | (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                poller.insert(c->fd, POLLIN | POLLOUT, c);
        }

        return 0;
}

int Service::try_accept_prom(int fd) {
        sockaddr_in sa;
        socklen_t   len    = sizeof(sa);
        auto        new_fd = accept4(fd, reinterpret_cast<sockaddr *>(&sa), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (new_fd == -1) {
                if (errno == EMFILE || errno == ENFILE) {
                        // TODO(markp): do something appropriate
                        // shutdown idle connections, and try accept4() again
                        return 0;
                } else if (errno != EINTR && errno != EAGAIN) {
                        Print("accept4() failed:", strerror(errno), "\n");
                        return 1;
                }
        } else {
                auto c = get_connection();

                c->fd          = new_fd;
                c->state.flags = 1 << unsigned(connection::State::Flags::TypePrometheus);
                require(c->src_prometheus());

                switch_dlist_init(&c->connectionsList);
                switch_dlist_init(&c->waitCtxList); // no need for this, it's a prom connection, but that's OK
                switch_dlist_insert_after(&allConnections, &c->connectionsList);

                poller.insert(c->fd, POLLIN, c);
        }

        return 0;
}

bool Service::try_recv_prom(connection *const c) {
        static constexpr const bool trace{false};
        auto *const                 b = c->inB;
        const auto *p = b->data() + b->offset(), *const e = b->end();

        if (trace)
                SLog("From prometheus [", b->as_s32(), "]\n");

        for (;;) {
                // process next HTTP request, if we have one
                while (p != e && isspace(*p))
                        ++p;

                if (p == e) {
                        if (trace)
                                SLog("No other HTTP requests to process\n");

                        put_buffer(b);
                        c->inB = nullptr;
                        return true;
                }

                b->set_offset(p - b->data());

                strwlen32_t method;

                for (method.p = p; p != e && !isblank(*p); ++p)
                        continue;

                if (p == e)
                        return true;

                method.SetEnd(p);

                if (trace)
                        SLog("Method [", method, "]\n");

                for (++p; p != e && isblank(*p); ++p)
                        continue;

                strwlen32_t path;

                for (path.p = p;; ++p) {
                        if (p == e)
                                return true;
                        else if (*p == '\n') {
                                path.SetEnd(p);
                                ++p;
                                break;
                        } else if (*p == '\r' && p + 1 < e && p[1] == '\n') {
                                path.SetEnd(p);
                                p += 2;
                                break;
                        }
                }

                if (trace)
                        SLog("Path [", path, "]\n");

                // parse headers
                strwlen32_t n, v;
                uint64_t    content_length{std::numeric_limits<uint64_t>::max()};

                for (;;) {
                        for (n.p = p;; ++p) {
                                if (p == e)
                                        return true;
                                else if (*p == ':') {
                                        n.SetEnd(p);

                                        for (++p; p != e && isblank(*p); ++p)
                                                continue;

                                        v.p = p;
                                        for (;; ++p) {
                                                if (p == e)
                                                        return true;
                                                else if (*p == '\n') {
                                                        v.SetEnd(p);
                                                        ++p;
                                                        break;
                                                } else if (*p == '\r' && p + 1 < e && p[1] == '\n') {
                                                        v.SetEnd(p);
                                                        p += 2;
                                                        break;
                                                }
                                        }

                                        break;
                                } else if (*p == '\n') {
                                        n.SetEnd(p);
                                        ++p;
                                        v.reset();
                                        break;
                                } else if (*p == '\r' && p + 1 < e && p[1] == '\n') {
                                        n.SetEnd(p);
                                        p += 2;
                                        v.reset();
                                        break;
                                }
                        }

                        if (trace)
                                SLog("[", n, "] [", v, "]\n");

                        if (!n) {
                                // done with headers
                                auto                    q = c->outQ;
                                outgoing_queue::payload payload;

                                if (!q)
                                        q = c->outQ = get_outgoing_queue();

                                payload.iovCnt     = 0;
                                payload.iovIdx     = 0;
                                payload.payloadBuf = true; // even if we don't have a buf

                                const auto http_error = [&](auto c, const auto resp, const bool close_on_flush = false) {
                                        if (trace)
                                                SLog("Failed:", resp, "\n");

                                        payload.append("HTTP/1.1 "_s32);
                                        payload.append(resp);
                                        payload.append("\r\nServer: TANK\r\nContent-Length: 0\r\n\r\n"_s32);
                                        q->push_back(std::move(payload));
                                };

                                b->set_offset(p - b->data());

                                if (content_length != std::numeric_limits<uint64_t>::max()) {
                                        http_error(c, "413 Payload Too Large"_s32, true);
                                        // shutdown immediately
                                        return true;
                                }
                                if (!method.Eq(_S("GET")))
                                        http_error(c, "405 Method Not Allowed"_s32);
                                else if (!path.BeginsWith(_S("/metrics")))
                                        http_error(c, "404 Not Found"_s32);
                                else {
                                        auto b = get_buffer();

                                        payload.set_buf(b);
                                        payload.append("HTTP/1.1 200 OK\r\nServer: TANK\r\nContent-Type: text/plain\r\nContent-Length: "_s32);
                                        const auto content_len_o{b->size()};
                                        b->append("                     \r\n\r\n"_s32);

                                        const auto body_o{b->size()};
#pragma mark Prometheus Metrics Response
                                        b->append("# HELP tanksrv_topic_latency Consumer latency\n"_s32);
                                        b->append("# TYPE tanksrv_topic_latency histogram\n"_s32);
                                        b->append("# HELP tanksrv_topic_produced_bytes Total bytes of all accepted new messages\n"_s32);
                                        b->append("# TYPE tanksrv_topic_produced_bytes counter\n"_s32);
                                        b->append("# HELP tanksrv_topic_produced_msgs Total accepted messages\n"_s32);
                                        b->append("# TYPE tanksrv_topic_produced_msgs counter\n"_s32);
                                        b->append("# HELP tanksrv_topic_consumed_bytes Total bytes of all outgoing messages\n"_s32);
                                        b->append("# TYPE tanksrv_topic_consumed_bytes counter\n"_s32);

                                        for (const auto &it : topics) {
                                                const auto [name, topic] = it;

                                                if (const auto v = topic->metrics.bytes_in)
                                                        b->append(R"(tanksrv_topic_produced_bytes{m=")", name, R"("} )", v, "\n");
                                                if (const auto v = topic->metrics.msgs_in)
                                                        b->append(R"(tanksrv_topic_produced_msgs{m=")", name, R"("} )", v, "\n");
                                                if (const auto v = topic->metrics.bytes_out)
                                                        b->append(R"(tanksrv_topic_consumed_bytes{m=")", name, R"("} )", v, "\n");

                                                if (const auto cnt = topic->metrics.latency.cnt) {
                                                        uint64_t total{0};

                                                        for (uint32_t i{0}; i != sizeof_array(topic::metrics_struct::latency_struct::histogram_scale); ++i) {
                                                                total += topic->metrics.latency.hist_buckets[i];
                                                                if (total)
                                                                        b->append(R"(tanksrv_topic_latency_bucket{le=")", topic::metrics_struct::latency_struct::histogram_scale[i], R"(", n=")", name, R"("} )", total, "\n"_s32);
                                                        }

                                                        b->append(R"(tanksrv_topic_latency_bucket{le="+Inf", n=")", name, R"("} )", cnt, "\n"_s32);
                                                        b->append(R"(tanksrv_topic_latency_sum{n=")", name, R"("} )", topic->metrics.latency.sum, "\n"_s32);
                                                        b->append(R"(tanksrv_topic_latency_count{n=")", name, R"("} )", cnt, "\n\n"_s32);
                                                }
                                        }

                                        b->data()[content_len_o + sprintf(b->data() + content_len_o, "%u", b->size() - body_o)] = ' ';
                                        payload.append(b->as_s32());
                                        q->push_back(std::move(payload));
                                }

                                if (!try_send_ifnot_blocked(c)) {
                                        return false;
                                }

                                break;
                        } else {
                                if (n.EqNoCase(_S("content-length"))) {
                                        content_length = v.as_uint64();
                                }
                        }
                }
        }

        return true;
}

bool Service::try_recv_tank(connection *const c) {
        auto *const b = c->inB;

        for (const auto *e = reinterpret_cast<uint8_t *>(b->end());;) {
                const auto *p = reinterpret_cast<uint8_t *>(b->data_at_offset());

                if (e - p >= sizeof(uint8_t) + sizeof(uint32_t)) {
                        const auto     msg    = *p++;
                        const uint32_t msgLen = *(uint32_t *)p;
                        p += sizeof(uint32_t);

                        if (unlikely(msgLen > 256 * 1024 * 1024)) {
                                Print("** TOO large incoming packet of length ", size_repr(msgLen), "\n");
                                track_shutdown(c, __LINE__, "Too large incoming packet of length ", msgLen, "\n");
                                return shutdown(c, __LINE__);
                        }

                        if (0 == (c->state.flags & (1u << uint8_t(connection::State::Flags::ConsideredReqHeader)))) {
                                // So that ingestion of future incoming data will not require buffer reallocations
                                const auto o = (char *)p - b->data();

                                if (trace)
                                        SLog("Need to reserve(", msgLen, ")\n");

                                b->reserve(msgLen);

                                p = reinterpret_cast<uint8_t *>(b->At(o));
                                e = reinterpret_cast<uint8_t *>(b->end());

                                c->state.flags |= 1u << uint8_t(connection::State::Flags::ConsideredReqHeader);
                        }

                        if (p + msgLen > e) {
                                if (trace)
                                        SLog("Need more data for ", msg, "\n");

                                return true;
                        }

                        c->state.flags &= ~(1u << uint8_t(connection::State::Flags::ConsideredReqHeader));
                        if (!process_msg(c, msg, reinterpret_cast<const uint8_t *>(p), msgLen)) {
                                return false;
                        }

                        p += msgLen;
                        if (p == e) {
                                b->clear();
                                c->inB = nullptr;
                                put_buffer(b);
                                break;
                        } else {
                                b->set_offset((char *)p);
                        }

                } else {
                        break;
                }
        }

        return true;
}

bool Service::try_recv(connection *const c) {
        int         fd = c->fd, n;
        auto *const b  = c->inB ?: (c->inB = get_buffer());

        if (unlikely(ioctl(fd, FIONREAD, &n) == -1)) {
                std::abort();
        }
        b->reserve(n);

        if (const auto r = read(fd, b->end(), n); - 1 == r) {
                if (errno == EINTR || errno == EAGAIN)
                        return true;
                else {
                        if (trace)
                                SLog("Failed:", strerror(errno), "\n");

                        track_shutdown(c, __LINE__, "read() failed: ", strerror(errno), "\n");
                        return shutdown(c, __LINE__);
                }
        } else if (0 == r) {
                return shutdown(c, __LINE__);
        } else {
                b->advance_size(r);
                c->state.lastInputTS = now_ms;
        }

        if (c->src_prometheus()) {
                if (!try_recv_prom(c)) {
                        return false;
                }
        } else {
                if (!try_recv_tank(c)) {
                        return false;
                }
        }

        if (auto b = c->inB) {
                if (b->offset() == b->size()) {
                        put_buffer(b);
                        c->inB = nullptr;
                } else if (b->offset() > 4 * 1024 * 1024) {
                        b->DeleteChunk(0, b->offset());
                        b->SetOffset(uint64_t(0));
                }
        }

        return true;
}

int Service::process_io(const int r) {
        for (const auto it : poller.new_events(r)) {
                auto *const c      = static_cast<connection *>(it->data.ptr);
                const auto  events = it->events;
                int         fd     = c->fd;

                if (fd == listenFd) {
                        if (const auto res = try_accept(fd)) {
                                return r;
                        }

                        continue;
                } else if (fd == prom_listen_fd) {
                        if (const auto res = try_accept_prom(fd)) {
                                return r;
                        }

                        continue;
                }

                if (events & (POLLHUP | POLLERR)) {
                        shutdown(c, __LINE__);
                        continue;
                }

                if (events & POLLIN) {
                        if (!try_recv(c)) {
                                continue;
                        }
                }

                if (events & POLLOUT) {
                        try_send(c);
                }
        }

        return 0;
}

void Service::schedule_cleanup() {
        if (!cleanupTrackerIsDirty) {
                cleanupTrackerIsDirty = true;

                cleanup_tracker.node.key = now_ms + 128;
                register_timer(&cleanup_tracker.node);
        }
}

void Service::wakeup_reactor() {
        pthread_kill(main_thread_id, SIGUSR1);
}

void Service::maybe_wakeup_reactor() {
        std::atomic_signal_fence(std::memory_order_seq_cst);
        if (sleeping.load(std::memory_order_relaxed)) {
                sleeping.store(false, std::memory_order_relaxed);
                wakeup_reactor();
        }
}

int Service::reactor_main() {
        for (;;) {
                // Deferred , see waitCtxDeferredGC decl. comments
                for (auto wctx : waitCtxDeferredGC) {
                        EXPECT(wctx->scheduledForDtor);

                        put_waitctx(wctx);
                }
                waitCtxDeferredGC.clear();

                // determine how long to wait, account for the next timer/event timestamp
                now_ms = Timings::Milliseconds::Tick();

                auto wait_until = std::min(now_ms + 30 * 1000, timers_ebtree_next);

                sleeping.store(true, std::memory_order_relaxed);
                const auto                  r           = poller.poll(wait_until - now_ms);
                [[maybe_unused]] const auto saved_errno = errno; // in case it's updated before we use it
                sleeping.store(false, std::memory_order_relaxed);

                now_ms  = Timings::Milliseconds::Tick();
                curTime = time(nullptr);

                drain_pubsub_queue();

                if (auto mask = pending_signals.load(std::memory_order_relaxed)) {
                        pending_signals.fetch_and(~mask, std::memory_order_relaxed);
                        if (!process_pending_signals(mask)) {
                                break;
                        }
                }

                if (-1 == r) {
                        if (errno != EINTR && errno != EAGAIN) {
                                IMPLEMENT_ME();
                        }
                } else {
                        if (const auto res = process_io(r)) {
                                return res;
                        }
                }

                if (now_ms >= timers_ebtree_next) {
                        process_timers();
                }
        }

        // notify the thread to exit so that we can join()
        mboxLock.lock();
        mbox.emplace_back(-1, 1);
        mboxLock.unlock();
        mbox_cv.notify_one();

        if (auto t = sync_thread.release()) {
                t->join();
                delete t;
        }

        Print("TANK terminated\n");
        return 0;
}

int Service::verify(char *paths[], const int cnt) {
        size_t n{0}, tot{0};
        bool   anyFailed{false};

        for (int i{0}; i < cnt; ++i) {
                const char *const path = paths[i];
                const strwlen32_t fullPath(path);
                const auto        ext = fullPath.Extension();

                if (ext.Eq(_S("ilog")) || ext.Eq(_S("log")) || ext.Eq(_S("index"))) {
                        int fd = open(path, O_RDONLY | O_LARGEFILE);

                        if (fd == -1) {
                                RFLog("Failed to access ", fullPath, ": ", strerror(errno), "\n");
                                return 1;
                        }

                        DEFER({ close(fd); });

                        Print("Verifying ", fullPath, " (", size_repr(lseek64(fd, 0, SEEK_END)), ") ..\n");

                        try {
                                if (ext.Eq(_S("ilog")) || ext.Eq(_S("log"))) {
                                        const auto r = Service::verify_log(fd);

                                        Print("> ", dotnotation_repr(r), " msgs\n");
                                        tot += r;
                                } else if (ext.Eq(_S("index"))) {
                                        auto name = fullPath;

                                        if (const auto p = name.SearchR('/'))
                                                name = name.SuffixFrom(p + 1);

                                        Service::verify_index(fd, name.Divided('_').second.Eq(_S("64")));
                                }
                        } catch (const std::exception &e) {
                                Print(ansifmt::bold, ansifmt::color_red, "Failed to verify (", fullPath, ansifmt::reset, ")\n");
                                anyFailed = true;
                        }

                        ++n;
                } else {
                        Print("Ignoring ", fullPath, "\n");
                }
        }

        if (!anyFailed) {
                Print(ansifmt::color_green, "All ", dotnotation_repr(n), " files verified OK, ", dotnotation_repr(tot), " messages", ansifmt::reset, "\n");
                return 0;
        } else {
                return 1;
        }
}

// TODO(markp): https://github.com/phaistos-networks/TANK/issues/7
int Service::start(int argc, char **argv) {
        sockaddr_in      sa;
        int              r;
        struct stat64    st;
        size_t           totalPartitions{0};
        Switch::endpoint listenAddr, prom_endpoint;

#ifndef LEAN_SWITCH
        // See: https://github.com/markpapadakis/BacktraceResolver
        Switch::trapCommonUnexpectedSignals();
#endif

        if (argc > 1 && !strcmp(argv[1], "verify")) {
                // https://github.com/phaistos-networks/TANK/wiki/Managing-Segments-files
                return verify(argv + 2, argc - 2);
        }

        prom_endpoint.unset();

        while ((r = getopt(argc, argv, "p:l:hvP:r")) != -1) {
                switch (r) {
                        case 'r':
                                read_only = true;
                                break;

                        case 'P':
                                prom_endpoint = Switch::ParseSrvEndpoint({optarg}, "http"_s8, 9102);
                                if (!prom_endpoint) {
                                        Print("Failed to parse endpoint for prometheus metrics from ", optarg, "\n");
                                        return 1;
                                }
                                break;

                        case 'p':
                                basePath_.clear();
                                basePath_.append(strwlen32_t(optarg, strlen(optarg)));
                                break;

                        case 'l':
                                listenAddr = Switch::ParseSrvEndpoint({optarg}, _S8("tank"), 11011);
                                if (!listenAddr) {
                                        Print("Failed to parse endpoint from ", optarg, "\n");
                                        return 1;
                                }
                                break;

                        case 'h':
                                Print("-p path: Specifies the base path where all topic exist. Used in standalone mode\n");
                                Print("-l endpoint: Specifies that the service will run in standalone mode, listening for connections to that address\n");
                                Print("-v : displays Tank version and exits\n");
                                Print("-P endpoint: Enables Prometheus Endpoint exporter. Requests to http://<endpoint>/metrics will return metrics prometheus can scrape and consume\n");
                                Print("-h : this help message\n");
                                return 0;

                        case 'v':
                                Print("TANK v", TANK_VERSION / 100, ".", TANK_VERSION % 100, ", (C) Phaistos Networks, S.A | http://phaistosnetworks.gr/\n");
                                Print("You can always get the latest release from the GitHub hosted repository at https://github.com/phaistos-networks/TANK\n");
                                return 0;

                        default:
                                return 1;
                }
        }

        if (!listenAddr) {
                Print("Listen address not specified. Use -l address to specify it\n");
                return 1;
        }

        if (basePath_.empty()) {
                Print("Base path not specified. Use -p path to specify it\n");
                return 1;
        } else if (stat64(basePath_.data(), &st) == -1) {
                Print("Failed to stat(", basePath_, "): ", strerror(errno), ". Please verify base path\n");
                return 1;
        } else if (!(st.st_mode & S_IFDIR)) {
                Print(basePath_, " is not a directory\n");
                return 1;
        } else if (opMode == OperationMode::Standalone) {
                try {
                        // We will parallelize this across multiple threads so that we can support many thousands of topics and partitions
                        // without incurring a long startup-sequence time
                        const auto                                                           basePathLen = basePath_.size();
                        std::vector<std::pair<topic *, size_t>>                              pendingPartitions;
                        uint64_t                                                             before;
                        simple_allocator                                                     a{8192};
                        std::vector<strwlen8_t>                                              collectedTopics;
                        std::vector<std::future<void>>                                       futures;
                        std::mutex                                                           collectLock;
                        Switch::vector<std::pair<std::pair<strwlen8_t, uint16_t>, uint64_t>> cleanupCheckpoints;
                        char                                                                 fullPath[PATH_MAX];

                        if (trace)
                                before = Timings::Microseconds::Tick();

                        for (const auto &&name : DirectoryEntries(basePath_.data())) {
                                if (name.Eq(_S(".cleanup.log"))) {
                                        int fd = open(Buffer::build(basePath_, "/", name).data(), O_RDONLY | O_LARGEFILE);

                                        if (fd == -1) {
                                                RFLog("Failed to access ", name, ":", strerror(errno), "\n");
                                                return 1;
                                        }

                                        const auto fileSize = lseek64(fd, 0, SEEK_END);

                                        if (!fileSize) {
                                                close(fd);
                                                continue;
                                        }

                                        auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                                        close(fd);
                                        if (fileData == MAP_FAILED) {
                                                RFLog("mmap() failed:", strerror(errno), "\n");
                                                return 1;
                                        }

                                        DEFER(
                                            {
                                                    munmap(fileData, fileSize);
                                            });

                                        madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

                                        strwlen8_t topicName;

                                        for (const auto *p = static_cast<uint8_t *>(fileData), *const e = p + fileSize; p != e;) {
                                                topicName.Set((char *)p + 1, *p);

                                                p += topicName.len + sizeof(uint8_t);

                                                const auto partition = *(uint16_t *)p;
                                                p += sizeof(uint16_t);
                                                const auto seqNum = *(uint64_t *)p;
                                                p += sizeof(uint64_t);

                                                //SLog(topicName, " ", partition, " ", seqNum, "\n");

                                                if (seqNum)
                                                        cleanupCheckpoints.push_back({{{a.CopyOf(topicName.p, topicName.len), topicName.len}, partition}, seqNum});
                                        }
                                } else if (*name.p != '.') {
                                        struct stat64 st;

                                        basePath_.ToCString(fullPath, sizeof(fullPath));
                                        fullPath[basePath_.size()] = '/';
                                        name.ToCString(fullPath + basePath_.size() + 1);

                                        if (stat64(fullPath, &st) == -1)
                                                throw Switch::system_error("Failed to stat(", fullPath, "): ", strerror(errno));
                                        else if (!S_ISDIR(st.st_mode)) {
                                                // Just ignore whatever isn't a directory
                                        } else {
                                                collectedTopics.emplace_back(a.CopyOf(name.p, name.len), name.len);
                                        }
                                }
                        }

                        if (trace)
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for initial walk ", collectedTopics.size(), "\n");

                        for (const auto &it : collectedTopics) {
                                futures.push_back(std::async(std::launch(std::launch::async), [&collectLock, this, &pendingPartitions](const strwlen8_t name) {
                                        char          path[PATH_MAX];
                                        struct stat64 st;
                                        const auto    len = Snprint(path, sizeof(path), basePath_, "/", name, "/");

                                        if (stat64(path, &st) == -1)
                                                throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
                                        else if (st.st_mode & S_IFDIR) {
                                                uint32_t         partitionsCnt{0}, min{UINT32_MAX}, max{0};
                                                partition_config partitionConfig;

                                                for (const auto &&name : DirectoryEntries(path)) {
                                                        name.ToCString(path + len);

                                                        if (name.Eq(_S("config"))) {
                                                                // topic overrides defaults
                                                                parse_partition_config(path, &partitionConfig);
                                                        } else if (name.IsDigits()) {
                                                                if (stat64(path, &st) == -1)
                                                                        throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
                                                                else if (st.st_mode & S_IFDIR) {
                                                                        const auto id = name.AsUint32();

                                                                        min = std::min<uint32_t>(min, id);
                                                                        max = std::max<uint32_t>(max, id);
                                                                        ++partitionsCnt;
                                                                }
                                                        }
                                                }

                                                if (partitionsCnt) {
                                                        if (min != 0 && max != partitionsCnt - 1) {
                                                                throw Switch::system_error("Unexpected partitions list; expected [0, ", partitionsCnt - 1, "]");
                                                        }

                                                        auto t = Switch::make_sharedref<topic>(name, partitionConfig);

                                                        EXPECT(t->use_count() == 1);

                                                        collectLock.lock();
                                                        pendingPartitions.emplace_back(t.get(), partitionsCnt);
                                                        register_topic(t.get());
                                                        collectLock.unlock();
                                                }
                                        }
                                },
                                                             it));
                        }

                        for (auto &it : futures) {
                                it.get();
                        }

                        basePath_.resize(basePathLen);

                        if (trace)
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for topics\n");

                        if (!pendingPartitions.empty()) {
                                static constexpr bool                                                   trace{false};
                                std::vector<std::pair<topic *, Switch::shared_refptr<topic_partition>>> list;

                                if (trace)
                                        SLog("pendingPartitions.size() = ", pendingPartitions.size(), "\n");

                                futures.clear();
                                for (auto &it : pendingPartitions) {
                                        auto t = it.first;

                                        for (uint16_t i{0}; i != it.second; ++i) {
                                                futures.push_back(std::async(std::launch(std::launch::async), [&list, &collectLock, this](topic *t, const uint16_t partition) {
                                                        char path[PATH_MAX];

                                                        Snprint(path, sizeof(path), basePath_.data(), "/", t->name_, "/", partition, "/");
                                                        auto p = init_local_partition(partition, path, t->partitionConf);

                                                        //SLog("Initializing ", path, "\n"); p->log_->compact(path); exit(0);

                                                        collectLock.lock();
                                                        list.emplace_back(t, std::move(p));
                                                        collectLock.unlock();
                                                },
                                                                             t, i));
                                        }
                                }

                                if (trace) {
                                        SLog("futures.size() = ", futures.size(), "\n");
                                        before = Timings::Microseconds::Tick();
                                }

                                for (auto &it : futures)
                                        it.get();

                                if (trace)
                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for all partitions\n");

                                std::sort(list.begin(), list.end(), [](const auto &a, const auto &b) noexcept {
                                        return uintptr_t(a.first) < uintptr_t(b.first);
                                });

                                const auto                     n = list.size();
                                std::vector<topic_partition *> partitions;

                                if (trace)
                                        before = Timings::Microseconds::Tick();

                                for (uint32_t i{0}; i != n;) {
                                        auto t = list[i].first;

                                        do {
                                                partitions.push_back(list[i].second.release());
                                        } while (++i != n && list[i].first == t);

                                        t->register_partitions(partitions.data(), partitions.size());
                                        totalPartitions += partitions.size();
                                        partitions.clear();
                                }

                                for (const auto &it : cleanupCheckpoints) {
                                        const auto topic     = it.first.first;
                                        const auto partition = it.first.second;
                                        auto       seqNum    = it.second;

                                        if (auto t = topic_by_name(topic)) {
                                                if (partition < t->partitions_->size()) {
                                                        auto log = t->partitions_->at(partition)->log_.get();

                                                        if (seqNum < log->firstAvailableSeqNum)
                                                                seqNum = log->firstAvailableSeqNum - 1;

                                                        log->lastCleanupMaxSeqNum = seqNum;
                                                        cleanupTracker.push_back(log); // TODO(markp): If we support topic/partitions deletes, we need to remove from cleanupTracker
                                                }
                                        }
                                }

                                if (trace)
                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " to initialize all partitions\n");
                        }
                } catch (const std::exception &e) {
                        Print("Failed to initialize topics and partitions:", e.what(), "\n");
                        return 1;
                }
        } else if (opMode == OperationMode::Clustered) {
                IMPLEMENT_ME();
        }

        Print(ansifmt::bold, "<=TANK=>", ansifmt::reset, " v", TANK_VERSION / 100, ".", TANK_VERSION % 100, " ", dotnotation_repr(topics.size()), " topics registered, ", dotnotation_repr(totalPartitions), " partitions; will listen for new connections at ", listenAddr, "\n");
        if (prom_endpoint)
                Print("> Prometheus Exporter enabled, will respond to http://", prom_endpoint, "/metrics\n");
        if (read_only)
                Print("> Read-Only mode; some functionality/APIs will be unavailable\n");
        Print("(C) Phaistos Networks, S.A. - ", ansifmt::color_green, "http://phaistosnetworks.gr/", ansifmt::reset, ". Licensed under the Apache License\n\n");

        if (topics.empty()) {
                Print("No topics found in ", basePath_, ". You may want to create a few, like so:\n");
                Print("mkdir -p ", basePath_, "/events/0 ", basePath_, "/orders/0 \n");
                Print("This will create topics events and orders and define one partition with id 0 for each of them.\nRestart Tank after you have created a few topics/partitions\n");
                Print(R"EOF(Or, you can use tank-cli's "create topic" command to create new topics instead)EOF", "\n");
        }

        listenFd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

        if (-1 == listenFd) {
                Print("socket() failed:", strerror(errno), "\n");
                return 1;
        }

        memset(&sa, 0, sizeof(sa));
        sa.sin_addr.s_addr = listenAddr.addr4;
        sa.sin_port        = htons(listenAddr.port);
        sa.sin_family      = AF_INET;

        if (Switch::SetReuseAddr(listenFd, 1) == -1) {
                Print("SO_REUSEADDR: ", strerror(errno), "\n");
                return 1;
        } else if (bind(listenFd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa))) {
                Print("bind() failed:", strerror(errno), "\n");
                return 1;
        } else if (listen(listenFd, 128)) {
                Print("listen() failed:", strerror(errno), "\n");
                return 1;
        }

        if (prom_endpoint) {
                prom_listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

                if (-1 == prom_listen_fd) {
                        Print("socket() failed:", strerror(errno), "\n");
                        return 1;
                }

                memset(&sa, 0, sizeof(sa));
                sa.sin_addr.s_addr = prom_endpoint.addr4;
                sa.sin_port        = htons(prom_endpoint.port);
                sa.sin_family      = AF_INET;

                if (Switch::SetReuseAddr(prom_listen_fd, 1) == -1) {
                        Print("SO_REUSEADDR: ", strerror(errno), "\n");
                        return 1;
                } else if (bind(prom_listen_fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) == -1) {
                        Print("bind() failed:", strerror(errno), ". Cannot accept Prometheus connections.\n");
                        return 1;
                } else if (listen(prom_listen_fd, 128)) {
                        Print("listen() failed:", strerror(errno), "\n");
                        return 1;
                }
        }

        sync_thread.reset(new std::thread([] {
                std::vector<std::pair<int, int>> local;
                sigset_t                         mask;

                sigfillset(&mask);
                pthread_sigmask(SIG_SETMASK, &mask, nullptr);
                for (;;) {
                        {
                                std::unique_lock<std::mutex> g(mboxLock);

                                while (mbox.empty()) {
                                        mbox_cv.wait(g);
                                }

                                local = std::move(mbox);
                                EXPECT(mbox.empty());
                        }

                        for (auto &it : local) {
                                if (-1 == it.first) {
                                        return;
                                }

                                fsync(it.first);
                                fsync(it.second);
                        }

                        local.clear();
                }
        }));

        poller.insert(listenFd, POLLIN, &listenFd);
        if (prom_listen_fd != -1) {
                poller.insert(prom_listen_fd, POLLIN, &prom_listen_fd);
        }

        main_thread_id = pthread_self();
        this_service   = this;

        // setup signals delivery
        sigset_t   mask;
        const auto signal_handler = [](int sig) { pending_signals.fetch_or(1ull << sig); };

        signal(SIGINT, signal_handler);
        signal(SIGUSR1, signal_handler);

        sigemptyset(&mask);
        sigaddset(&mask, SIGPIPE);
        sigaddset(&mask, SIGHUP);
        sigdelset(&mask, SIGUSR1);
        if (-1 == pthread_sigmask(SIG_BLOCK, &mask, nullptr)) {
                Print("pthread_sigmask():", strerror(errno), "\n");
                return 1;
        }
        return reactor_main();
}

topic *Service::topic_by_name(const strwlen8_t name) const {
        if (const auto it = topics.find(name); it != topics.end()) {
                return it->second.get();
        } else {
                return nullptr;
        }
}

int main(int argc, char *argv[]) {
        init_log();

        return Service{}.start(argc, argv);
}

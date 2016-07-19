#include "service.h"
#include <ansifmt.h>
#include <compress.h>
#include <date.h>
#include <fcntl.h>
#include <fs.h>
#include <future>
#include <random>
#include <set>
#include <signal.h>
#include <switch_mallocators.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <text.h>
#include <text.h>
#include <thread>
#include <timings.h>
#include <unistd.h>

// From SENDFILE(2): The original Linux sendfile() system call was not designed to handle large file offsets.
// Consequently, Linux 2.4 added sendfile64(), with a wider type for the offset argument.
// The glibc sendfile() wrapper function transparently deals with the kernel differences.
#define HAVE_SENDFILE64 1

static constexpr bool trace{false};

static Switch::mutex mboxLock;
static Switch::vector<std::pair<int, int>> mbox;

ro_segment::ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const strwlen32_t base, const uint32_t creationTS, const bool wideEntries)
    : baseSeqNum{absSeqNum}, lastAvailSeqNum{lastAbsSeqNum}, createdTS{creationTS}, haveWideEntries{wideEntries}
{
        int fd, indexFd;
        struct stat64 st;

        index.data = nullptr;
        if (createdTS)
                fd = open(Buffer::build(base, "/", absSeqNum, "-", lastAbsSeqNum, "_", createdTS, ".ilog").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        else
                fd = open(Buffer::build(base, "/", absSeqNum, "-", lastAbsSeqNum, ".ilog").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);

        if (fd == -1)
                throw Switch::system_error("Failed to access log file:", strerror(errno));

        fdh.reset(new fd_handle(fd));
        Drequire(fdh.use_count() == 2);
        fdh->Release();

        if (fstat64(fd, &st) == -1)
                throw Switch::system_error("Failed to fstat():", strerror(errno));

        auto size = st.st_size;

        if (unlikely(size == (off64_t)-1))
                throw Switch::system_error("lseek64() failed: ", strerror(errno));

        Drequire(size < std::numeric_limits<std::remove_reference<decltype(fileSize)>::type>::max());

        fileSize = size;
        lastModTS = st.st_mtime;

        if (trace)
                SLog(ansifmt::bold, "fileSize = ", fileSize, ", lastModTS = ", Date::ts_repr(lastModTS), ", createdTS = ", Date::ts_repr(creationTS), ansifmt::reset, "\n");

        if (haveWideEntries)
                indexFd = open(Buffer::build(base, "/", absSeqNum, "_64.index").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        else
                indexFd = open(Buffer::build(base, "/", absSeqNum, ".index").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);

        Defer({ if (indexFd != -1) close(indexFd); });

        if (indexFd == -1)
        {
                if (haveWideEntries)
                        indexFd = open(Buffer::build(base, "/", absSeqNum, "_64.index").data(), O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME);
                else
                        indexFd = open(Buffer::build(base, "/", absSeqNum, ".index").data(), O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME);

                if (indexFd == -1)
                        throw Switch::system_error("Failed to rebuild index file:", strerror(errno));

                Service::rebuild_index(fdh->fd, indexFd);
        }

        size = lseek64(indexFd, 0, SEEK_END);

        if (unlikely(size == (off64_t)-1))
                throw Switch::system_error("lseek64() failed: ", strerror(errno));

        Drequire(size < std::numeric_limits<std::remove_reference<decltype(index.fileSize)>::type>::max());

        // TODO: if (haveWideEntries), index.lastRecorded.relSeqNum should be a union, and we should
        // properly set index.lastRecorded here
        require(haveWideEntries == false); // not implemented yet

        index.fileSize = size;
        index.lastRecorded.relSeqNum = index.lastRecorded.absPhysical = 0;

        if (size)
        {
                auto data = mmap(nullptr, index.fileSize, PROT_READ, MAP_SHARED, indexFd, 0);

                if (unlikely(data == MAP_FAILED))
                        throw Switch::system_error("Failed to access the index file. mmap() failed:", strerror(errno));

                index.data = static_cast<const uint8_t *>(data);

                if (likely(index.fileSize >= sizeof(uint32_t) + sizeof(uint32_t)))
                {
                        // last entry in the index; very handy
                        const auto *const p = (uint32_t *)(index.data + index.fileSize - sizeof(uint32_t) - sizeof(uint32_t));

                        index.lastRecorded.relSeqNum = p[0];
                        index.lastRecorded.absPhysical = p[1];

                        if (trace)
                                SLog("lastRecorded = ", index.lastRecorded.relSeqNum, "(", index.lastRecorded.relSeqNum + baseSeqNum, "), ", index.lastRecorded.absPhysical, "\n");
                }
        }
        else
                index.data = nullptr;
}

std::pair<uint32_t, uint32_t> ro_segment::snapDown(const uint64_t absSeqNum) const
{
        require(haveWideEntries == false); // not implemented yet
        const auto relSeqNum = uint32_t(absSeqNum - baseSeqNum);
        const auto *const all = reinterpret_cast<const index_record *>(index.data);
        const auto *const end = all + index.fileSize / sizeof(index_record);
        const auto it = std::upper_bound_or_match(all, end, relSeqNum, [](const auto &a, const auto num) {
                return TrivialCmp(num, a.relSeqNum);
        });

        if (it == end)
        {
                // no index record where record.relSeqNum <= relSeqNum
                // that is, the first index record.relSeqNum > relSeqNum
                return {0, 0};
        }
        else
                return {it->relSeqNum, it->absPhysical};
}

std::pair<uint32_t, uint32_t> ro_segment::snapUp(const uint64_t absSeqNum) const
{
        require(haveWideEntries == false); // not implemented yet
        const auto relSeqNum = uint32_t(absSeqNum - baseSeqNum);
        const auto *const all = reinterpret_cast<const index_record *>(index.data);
        const auto *const end = all + index.fileSize / sizeof(index_record);
        const auto it = std::lower_bound(all, end, relSeqNum, [](const auto &a, const auto num) {
                return a.relSeqNum < num;
        });

        if (it == end)
        {
                // no index record where record.relSeqNum >= relSeqNum
                // that is, the last index record.relSeqNum < relSeqNum
                return {UINT32_MAX, UINT32_MAX};
        }
        else
                return {it->relSeqNum, it->absPhysical};
}

// Searches forward starting from `fileOffset` until it finds a message(set) with absSeqNum > `maxAbsSeqNum`, and
// returns the file offset of that message(which is the end of file before
// that message); also respects maxSize
// TODO: read in 4k chunks/time or something more appropriate
static uint32_t search_before_offset(uint64_t baseSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum, int fd, const uint32_t fileSize, uint32_t fileOffset)
{
        uint8_t buf[64];
        auto o = fileOffset;
        const auto limit = maxSize != UINT32_MAX ? Min<uint32_t>(fileSize, fileOffset + maxSize) : fileSize;

        if (trace)
                SLog("Searching for offset ", maxAbsSeqNum, ", limit = ", limit, "(maxSize = ", maxSize, ", baseSeqNum = ", baseSeqNum, ", fileOffset = ", fileOffset, ")\n");

        while (fileOffset < limit)
        {
                const auto r = pread64(fd, buf, sizeof(buf), fileOffset);

                if (unlikely(r == -1))
                        throw Switch::system_error("pread() failed:", strerror(errno));

                const auto *p = buf;
                const auto bundleLen = Compression::UnpackUInt32(p);
                const auto encodedBundleLenLen = p - buf;
                const auto bundleFlags = *p++;
                uint32_t msgSetSize = (bundleFlags >> 2) & 0xf;

                if (!msgSetSize)
                        msgSetSize = Compression::UnpackUInt32(p);

                if (trace)
                        SLog("abs = ", baseSeqNum, "(msgSetSize = ", msgSetSize, ") VS ", maxAbsSeqNum, " at ", o, "\n");

                if (baseSeqNum > maxAbsSeqNum)
                        break;
                else
                {
                        baseSeqNum += msgSetSize;
                        fileOffset += bundleLen + encodedBundleLenLen;
                }
        }

        if (trace)
                SLog("Returnign fileOffset = ", fileOffset, "\n");

        return fileOffset;
}

// We are operating on index boundaries, so our res.fileOffset is aligned on an index boundary, which means
// we may stream (0, partition_config::indexInterval] excess bytes.
// This is probably fine, but we may as well
// scan ahead from that fileOffset until we find an more appropriate file offset to begin streaming for, and adjust
// res.fileOffset and res.baseSeqNum accordidly if we can.
//
// Kafka does something similar(see it's FileMessageSet.scala#searchFor() impl.)
//
// If we don't adjust_range_start(), we 'll avoid the scanning I/O cost, which should be minimal anyway, but we can potentially send
// more data at the expense of network I/O and transfer costs
//
// This will require some effort once we support sparse logs (e.g due to compaction), but we 'll figure out the appropriate impl. then
// XXX: make sure this reflects the latest encoding scheme
static void adjust_range_start(lookup_res &res, const uint64_t absSeqNum)
{
        uint64_t baseSeqNum = res.absBaseSeqNum;

        if (baseSeqNum == absSeqNum || absSeqNum <= 1)
        {
                // No need for any ajustements
                if (trace)
                        SLog("No need for any adjustments\n");

                return;
        }

        int fd = res.fdh->fd;
        uint8_t tinyBuf[sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint64_t)];
        const auto baseOffset = res.fileOffset;
        auto o = baseOffset;
        const auto fileOffsetCeiling = res.fileOffsetCeiling;
        uint64_t before = trace ? Timings::Microseconds::Tick() : 0;

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_brown, "About to adjust range fileOffset ", baseOffset, ", absBaseSeqNum(", baseSeqNum, "), absSeqNum(", absSeqNum, ")", ansifmt::reset, "\n");

        while (o < fileOffsetCeiling)
        {
                const auto r = pread64(fd, tinyBuf, sizeof(tinyBuf), o);

                if (unlikely(r == -1))
                        throw Switch::system_error("pread64() failed:", strerror(errno));

                const auto *const baseBuf = tinyBuf;
                const uint8_t *p = baseBuf;
                const auto bundleLen = Compression::UnpackUInt32(p);

                require(bundleLen);

                const auto encodedBundleLenLen = p - baseBuf;
                const auto bundleFlags = *p++;
                uint32_t msgSetSize = (bundleFlags >> 2) & 0xf;

                if (!msgSetSize)
                        msgSetSize = Compression::UnpackUInt32(p);

                if (trace)
                        SLog("Now at bundle(", baseSeqNum, "), msgSetSize(", msgSetSize, "), bundleFlags(", bundleFlags, ")\n");

                const auto nextBundleBaseSeqNum = baseSeqNum + msgSetSize;

                if (absSeqNum >= nextBundleBaseSeqNum)
                {
                        // Our target is in later bundle
                        if (trace)
                                SLog("Target in later bundle\n");

                        o += bundleLen + encodedBundleLenLen;
                        baseSeqNum = nextBundleBaseSeqNum;
                }
                else
                {
                        // Our target is in this bundle
                        if (trace)
                                SLog("Target in this bundle(", o, ")\n");

                        res.fileOffset = o;
                        res.absBaseSeqNum = baseSeqNum;
                        break;
                }
        }

        if (trace)
                SLog(ansifmt::color_blue, "After adjustement, fileOffset ", res.fileOffset, ", absBaseSeqNum(", res.absBaseSeqNum, "), took  ", duration_repr(Timings::Microseconds::Since(before)), ansifmt::reset, "\n");
}

lookup_res topic_partition_log::read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum)
{
        // lock is expected to be locked
        require(absSeqNum >= cur.baseSeqNum);

        lookup_res res;
        const auto highWatermark = lastAssignedSeqNum;
        bool inSkiplist;
        const auto relSeqNum = uint32_t(absSeqNum - cur.baseSeqNum);
        const auto &skipList = cur.index.skipList;
        const auto end = skipList.end();
        const auto it = std::upper_bound_or_match(skipList.begin(), end, relSeqNum, [](const auto &a, const auto seqNum) {
                return TrivialCmp(seqNum, a.first);
        });

        res.fdh = cur.fdh;

        if (it != end)
        {
                if (trace)
                        SLog("Found in skiplist\n");

                res.absBaseSeqNum = cur.baseSeqNum + it->first;
                res.fileOffset = it->second;

                inSkiplist = true;
        }
        else
        {
                require(cur.index.ondisk.span); // we checked if it's in this current segment

                if (trace)
                        SLog("Considering ondisk index (cur.fileSize = ", cur.fileSize, ")\n");

                const auto size = cur.index.ondisk.span;
                const auto *const all = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                const auto *const e = all + size / sizeof(index_record);
                const auto i = std::upper_bound_or_match(all, e, relSeqNum, [](const auto &a, const auto seqNum) {
                        return TrivialCmp(seqNum, a.relSeqNum);
                });

                if (i != e)
                {
                        if (trace)
                                SLog("In ondisk index (relSeqNum:", i->relSeqNum, ", absPhysical:", i->absPhysical, ")\n");

                        res.absBaseSeqNum = cur.baseSeqNum + i->relSeqNum;
                        res.fileOffset = i->absPhysical;
                }
                else
                {
                        res.absBaseSeqNum = cur.baseSeqNum;
                        res.fileOffset = 0;
                }

                inSkiplist = false;
        }

        if (maxAbsSeqNum != UINT64_MAX)
        {
                index_record ref;

                if (inSkiplist)
                {
                        const auto it = std::upper_bound_or_match(skipList.begin(), skipList.end(), uint32_t(maxAbsSeqNum - cur.baseSeqNum), [](const auto &a, const auto seqNum) {
                                return TrivialCmp(seqNum, a.first);
                        });

                        ref.relSeqNum = it->first;
                        ref.absPhysical = it->second;
                }
                else
                {
                        const auto size = cur.index.ondisk.span;
                        const auto *const all = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                        const auto *const e = all + size / sizeof(index_record);
                        const auto it = std::upper_bound_or_match(all, e, uint32_t(maxAbsSeqNum - cur.baseSeqNum), [](const auto &a, const uint32_t seqNum) {
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
        }
        else
        {
                res.fileOffsetCeiling = cur.fileSize;

                if (trace)
                        SLog("res.fileOffsetCeiling = ", res.fileOffsetCeiling, "\n");
        }

        res.highWatermark = highWatermark;
        return res;
}

// Aligns to indices boundaries
lookup_res topic_partition_log::range_for(uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum)
{
        if (trace)
                SLog("Read for absSeqNum = ", absSeqNum, ", lastAssignedSeqNum = ", lastAssignedSeqNum, ", firstAvailableSeqNum = ", firstAvailableSeqNum, "\n");

        // (UINT64_MAX) is reserved: fetch only newly produced bundles
        // see process_consume()
        // (0) is also reserved; start from the first available sequence
        if (absSeqNum == 0)
        {
                if (trace)
                        SLog("Asked to fetch starting from first available sequence number ", firstAvailableSeqNum, "\n");

                if (firstAvailableSeqNum)
                        absSeqNum = firstAvailableSeqNum;
                else
                {
                        // No content at all
                        absSeqNum = 1;

                        if (trace)
                                SLog("No content, will start from ", absSeqNum, " (lastAssignedSeqNum = ", lastAssignedSeqNum, ")\n");
                }
        }

        const auto highWatermark = lastAssignedSeqNum;

        if (maxSize == 0)
        {
                // Should be handled by the client
                if (trace)
                        SLog("maxSize == 0\n");

                return {lookup_res::Fault::Empty, highWatermark};
        }
        else if (maxAbsSeqNum < absSeqNum)
        {
                // Should be handled by the client
                if (trace)
                        SLog("Past maxAbsSeqNum = ", maxAbsSeqNum, "\n");

                return {lookup_res::Fault::Empty, highWatermark};
        }
        else if (absSeqNum == lastAssignedSeqNum + 1)
        {
                // return empty
                // UPDATE: let's wait instead
                if (trace)
                        SLog("Empty\n");

                return {lookup_res::Fault::AtEOF, highWatermark};
        }
        else if (absSeqNum > lastAssignedSeqNum)
        {
                // past last assigned sequence number? nope
                // throw an error
                if (trace)
                        SLog("PAST absSeqNum(", absSeqNum, ") > lastAssignedSeqNum(", lastAssignedSeqNum, ")\n");

                return {lookup_res::Fault::BoundaryCheck, highWatermark};
        }
        else if (absSeqNum < firstAvailableSeqNum)
        {
                if (trace)
                        SLog("< firstAvailableSeqNum\n");

                return {lookup_res::Fault::BoundaryCheck, highWatermark};
        }
        else if (absSeqNum >= cur.baseSeqNum)
        {
                // Great, definitely in the current segment
                if (trace)
                        SLog("absSeqNum >= cur.baseSeqNum(", cur.baseSeqNum, "). Will get from current\n");

                return read_cur(absSeqNum, maxSize, maxAbsSeqNum);
        }

        auto prevSegments = roSegments;

        // TODO: https://github.com/phaistos-networks/TANK/issues/2
        const auto end = prevSegments->end();
        const auto it = std::upper_bound_or_match(prevSegments->begin(), end, absSeqNum, [](const auto s, const auto absSeqNum) {
                return TrivialCmp(absSeqNum, s->baseSeqNum);
        });

        if (it != end)
        {
                const auto f = *it;
                const auto res = f->translateDown(absSeqNum, UINT32_MAX);
                uint32_t offsetCeil;

                if (trace)
                        SLog("Found in RO segment (", f->baseSeqNum, ", ", f->lastAvailSeqNum, ")\n");

                if (maxAbsSeqNum != UINT64_MAX)
                {
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
                }
                else
                        offsetCeil = f->fileSize;

                return {f->fdh, offsetCeil, f->baseSeqNum + res.record.relSeqNum, res.record.absPhysical, highWatermark};
        }

        if (trace)
                SLog("No segment suitable for absSeqNum\n");

        // No segment, either one of the immutalbe segments or the current segment, that holds
        // a message with seqNum >= absSeqNum
        //
        // so just point to the first(oldest) segment
        if (prevSegments->size())
        {
                const auto f = prevSegments->front();

                if (trace)
                        SLog("Will use first R/O segment\n");

                return {f->fdh, f->fileSize, f->baseSeqNum, 0, highWatermark};
        }
        else
        {
                if (trace)
                        SLog("Will use current segment\n");

                return {cur.fdh, cur.fileSize, cur.baseSeqNum, 0, highWatermark};
        }
}

void topic_partition_log::consider_ro_segments()
{
        size_t sum{0};
        const uint32_t nowTS = Timings::Seconds::SysTime();

        for (auto it : *roSegments)
                sum += it->fileSize;

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_blue, "Considering segments sum=", sum, ", total = ", roSegments->size(), " limits { roSegmentsCnt ", config.roSegmentsCnt, ", roSegmentsSize ", config.roSegmentsSize, "}", ansifmt::reset, "\n");

        while (roSegments->size() && ((config.roSegmentsCnt && roSegments->size() > config.roSegmentsCnt) || (config.roSegmentsSize && sum > config.roSegmentsSize) || (roSegments->front()->createdTS && config.lastSegmentMaxAge && roSegments->front()->createdTS + config.lastSegmentMaxAge < nowTS)))
        {
                auto segment = roSegments->front();
                const auto basePathLen = basePath.length();

                if (trace)
                        SLog(ansifmt::bold, ansifmt::color_red, "Removing ", segment->baseSeqNum, ansifmt::reset, "\n");

                basePath.append("/", segment->baseSeqNum, "-", segment->lastAvailSeqNum, "_", segment->createdTS, ".ilog");
                if (unlink(basePath.data()) == -1)
			Print("Failed to unlink ", basePath, ": ", strerror(errno), "\n");
		else if (trace)
			SLog("Removed ", basePath, "\n");

                basePath.SetLength(basePathLen);
                basePath.append("/", segment->baseSeqNum, ".index");
                if (unlink(basePath.data()) == -1)
			Print("Failed to unlink ", basePath, ": ", strerror(errno), "\n");
		else if (trace)
			SLog("Removed ", basePath, "\n");

                basePath.SetLength(basePathLen);

                segment->fdh.reset(nullptr);

                sum -= segment->fileSize;
		delete segment;
                roSegments->pop_front();
        }

	if (roSegments->size())
        	firstAvailableSeqNum = roSegments->front()->baseSeqNum;
	else
		firstAvailableSeqNum = cur.baseSeqNum;

        if (trace)
                SLog("firstAvailableSeqNum now = ", firstAvailableSeqNum, "\n");
}

bool topic_partition_log::should_roll(const uint32_t now) const
{
        if (cur.fileSize)
        {
                if (trace)
                        SLog(ansifmt::color_green, basePath, " Consider roll:cur.fileSize(", cur.fileSize, "), config.maxSegmentSize(", config.maxSegmentSize, "), skipList.size(", cur.index.skipList.size(), "), config.curSegmentMaxAge (", config.curSegmentMaxAge, "), ", Timings::Seconds::SysTime() - cur.createdTS, " old,  cur.rollJitterSecs = ", cur.rollJitterSecs, ansifmt::reset, "\n");

                if (cur.fileSize > config.maxSegmentSize)
                {
                        if (trace)
                                SLog(ansifmt::bold, "Should roll: cur.fileSize(", cur.fileSize, ") > config.maxSegmentSize(", config.maxSegmentSize, ")", ansifmt::reset, "\n");

                        return true;
                }

                const size_t skipListCapacity = Min<size_t>(65536, config.maxIndexSize / (sizeof(uint32_t) + sizeof(uint32_t)));

                if (cur.index.skipList.size() > skipListCapacity)
                {
                        // index is full
                        if (trace)
                                SLog(ansifmt::bold, "cur.index.skipList.size(", cur.index.skipList.size(), ") > skipListCapacity(", skipListCapacity, ")", ansifmt::reset, "\n");

                        return true;
                }

                if (const auto v = config.curSegmentMaxAge)
                {
                        if (now - cur.createdTS > v - cur.rollJitterSecs)
                        {
                                // Soft limit
                                if (trace)
                                        SLog(ansifmt::bold, "now - cur.createdTS(", now - cur.createdTS, ") > (", v - cur.rollJitterSecs, ")", ansifmt::reset, "\n");

                                return true;
                        }
                }
        }

        return false;
}

append_res topic_partition_log::append_bundle(const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt)
{
        const auto savedLastAssignedSeqNum = lastAssignedSeqNum;
        const auto absSeqNum = lastAssignedSeqNum + 1;
        const auto now = Timings::Seconds::SysTime();

        lastAssignedSeqNum += bundleMsgsCnt;

        if (should_roll(now))
        {
                const auto basePathLen = basePath.length();

                if (trace)
                        SLog("Need to switch to another commit log (", cur.fileSize, "> ", config.maxSegmentSize, ") ", cur.index.skipList.size(), "\n");

                if (cur.fileSize != UINT32_MAX)
                {
                        auto newROFiles = std::make_unique<Switch::vector<ro_segment *>>();
                        auto newROFile = std::make_unique<ro_segment>(cur.baseSeqNum, savedLastAssignedSeqNum, cur.createdTS);

                        const auto n = cur.fdh.use_count();

                        require(n >= 1);
                        newROFile->fdh = cur.fdh;
                        require(cur.fdh.use_count() == n + 1);
                        newROFile->fileSize = cur.fileSize;

                        newROFile->index.fileSize = lseek64(cur.index.fd, 0, SEEK_END);
                        newROFile->index.lastRecorded.relSeqNum = newROFile->index.lastRecorded.absPhysical = 0;

                        // We now encode the [first,last] range into the filename for simplicity and future-proofing; we 'd like to
                        // support sparse sequence numbers space
                        if (cur.nameEncodesTS)
                        {
                                if (rename(Buffer::build(basePath, "/", cur.baseSeqNum, "_", cur.createdTS, ".log").data(),
                                           Buffer::build(basePath, "/", cur.baseSeqNum, "-", savedLastAssignedSeqNum, "_", cur.createdTS, ".ilog").data()) == -1)
                                {
                                        throw Switch::system_error("Failed to rename():", strerror(errno));
                                }
                        }
                        else if (rename(Buffer::build(basePath, "/", cur.baseSeqNum, ".log").data(),
                                        Buffer::build(basePath, "/", cur.baseSeqNum, "-", savedLastAssignedSeqNum, "_", cur.createdTS, ".ilog").data()) == -1)
                        {
                                throw Switch::system_error("Failed to rename():", strerror(errno));
                        }

                        if (newROFile->index.fileSize)
                        {
                                newROFile->index.data = (uint8_t *)mmap(nullptr, newROFile->index.fileSize, PROT_READ, MAP_SHARED, cur.index.fd, 0);

                                if (unlikely(newROFile->index.data == MAP_FAILED))
                                        throw Switch::system_error("mmap() failed:", strerror(errno));

                                if (newROFile->index.fileSize >= sizeof(uint32_t) + sizeof(uint32_t))
                                {
                                        const auto *const p = (uint32_t *)(newROFile->index.data + newROFile->index.fileSize - sizeof(uint32_t) - sizeof(uint32_t));

                                        newROFile->index.lastRecorded.relSeqNum = p[0];
                                        newROFile->index.lastRecorded.absPhysical = p[1];

                                        if (trace)
                                                SLog("set lastRecorded to ", newROFile->index.lastRecorded.relSeqNum, ", ", newROFile->index.lastRecorded.absPhysical, "\n");
                                }
                        }
                        else
                                newROFile->index.data = nullptr;

                        newROFiles->Append(roSegments->values(), roSegments->size());
                        newROFiles->push_back(newROFile.release());

                        roSegments.reset(newROFiles.release());
                        consider_ro_segments();
                }
                else
                {
                        // first segment for this partition
                        firstAvailableSeqNum = absSeqNum;
                }

                cur.baseSeqNum = absSeqNum;
                cur.fileSize = 0;
                // It is very important than the first bundle's recorded immediately in the index
                // so we set cur.sinceLastUpdate = UINT32_MAX to force that
                cur.sinceLastUpdate = UINT32_MAX;
                cur.createdTS = Timings::Seconds::SysTime();
                cur.nameEncodesTS = true;

                cur.index.skipList.clear();
		fdatasync(cur.index.fd);
                close(cur.index.fd);

                if (cur.index.ondisk.data != nullptr && cur.index.ondisk.data != MAP_FAILED)
                {
                        munmap((void *)cur.index.ondisk.data, cur.index.ondisk.span);
                        cur.index.ondisk.data = nullptr;
                }

                basePath.append(cur.baseSeqNum, "_", cur.createdTS, ".log");

                cur.fdh.reset(new fd_handle(open(basePath.data(), O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME|O_APPEND, 0775)));
                require(cur.fdh->use_count() == 2);
                cur.fdh->Release();

                if (cur.fdh->fd == -1)
                        throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot load segment log");

                basePath.SetLength(basePathLen);
                basePath.append(cur.baseSeqNum, ".index");
                cur.index.fd = open(basePath.data(), O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND, 0775);
                basePath.SetLength(basePathLen);

                if (cur.index.fd == -1)
                        throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot load segment index");

                if (const uint32_t max = config.maxRollJitterSecs)
                {
                        std::random_device dev;
                        std::mt19937 rng(dev());
                        std::uniform_int_distribution<uint32_t> distr(0, max);

                        cur.rollJitterSecs = distr(rng);
                }
                else
                        cur.rollJitterSecs = 0;

                cur.flush_state.pendingFlushMsgs = 0;
                cur.flush_state.nextFlushTS = config.flushIntervalSecs ? now + config.flushIntervalSecs : UINT32_MAX;

                if (trace)
                        SLog("Switched\n");
        }

        // XXX: maybe we should update the index _after_ the log
        if (cur.sinceLastUpdate > config.indexInterval)
        {
                const uint32_t out[] = {uint32_t(absSeqNum - cur.baseSeqNum), cur.fileSize};

                cur.index.skipList.push_back({out[0], out[1]});

                if (trace)
                        SLog(">> ", out[0], ", ", out[1], "\n");

                if (unlikely(write(cur.index.fd, out, sizeof(out)) != sizeof(out)))
                {
                        RFLog("Failed to write():", strerror(errno), "\n");
                        return {nullptr, {}, {}};
                }

                cur.sinceLastUpdate = 0;
        }

        require(cur.fdh.use_count() >= 1);

        uint8_t varint[8];
        const uint8_t varintLen = Compression::PackUInt32(bundleSize, varint) - varint;
        auto fd = cur.fdh->fd;
        const struct iovec iov[] =
            {
                {(void *)varint, varintLen},
                {(void *)bundle, bundleSize}};
        const auto entryLen = iov[0].iov_len + iov[1].iov_len;
        const range32_t fileRange(cur.fileSize, entryLen);
        const auto before = cur.fdh.use_count();
        Switch::shared_refptr<fd_handle> fdh(cur.fdh);
        const auto b = trace ? Timings::Microseconds::Tick() : uint64_t(0);

        require(cur.fdh.use_count() == before + 1);

        // https://github.com/phaistos-networks/TANK/issues/14
        if (writev(fd, iov, sizeof_array(iov)) != entryLen)
        {
                RFLog("Failed to writev():", strerror(errno), "\n");
        }
        else
        {
                if (trace)
                        SLog("writev() took ", duration_repr(Timings::Microseconds::Since(b)), "\n");

                cur.fileSize += entryLen;
                cur.sinceLastUpdate += entryLen;

                cur.flush_state.pendingFlushMsgs += bundleMsgsCnt;

                if (trace)
                        SLog("cur.flush_state.pendingFlushMsgs = ", cur.flush_state.pendingFlushMsgs, ", config.flushIntervalMsgs = ", config.flushIntervalMsgs, ", config.flushIntervalMsgs = ", config.flushIntervalMsgs, "\n");

                if (config.flushIntervalMsgs && cur.flush_state.pendingFlushMsgs >= config.flushIntervalMsgs)
                {
                        if (trace)
                                SLog("Scheduling flush\n");

                        schedule_flush(now);
                }
                else if (now >= cur.flush_state.nextFlushTS)
                {
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

void topic_partition_log::schedule_flush(const uint32_t now)
{
        cur.flush_state.pendingFlushMsgs = 0;
        cur.flush_state.nextFlushTS = now + config.flushIntervalSecs;

        // This is obviously not optimal; we should have used a bounded queue, or some lock/wait-free construct
        // but given this is a rare event, it's not worth it yet
        mboxLock.lock();
        mbox.push_back({cur.fdh->fd, cur.index.fd});
        mboxLock.unlock();
}

// XXX: this works great for standalone mode, but when the highwater mark is based on an ISR, which means
// it doesn't get incremented immediately as soon as the leader appends the bundle to its local segment, waking up
// any waiting consumers is not going to work. We likely need to do this only iff running in standalone mode, otherwise
// only when the highwater mark is updated.
// i.e opMode == OperationMode::Standalone
void topic_partition::consider_append_res(append_res &res, Switch::vector<wait_ctx *> &waitCtxWorkL)
{
        bool newLogFile{false};

        if (trace)
                SLog(ansifmt::color_blue, " waitingList.size() = ", waitingList.size(), ansifmt::reset, "\n");

        for (uint32_t i{0}; i < waitingList.size();)
        {
                auto it = waitingList[i];

                for (uint8_t i{0}; i != it->partitionsCnt; ++i)
                {
                        auto &ctxP = it->partitions[i];

                        if (ctxP.partition == this)
                        {
                                if (!ctxP.fdh)
                                {
                                        ctxP.fdh = res.fdh.get();
                                        ctxP.fdh->Retain();
                                        ctxP.range = res.dataRange;
                                        ctxP.seqNum = res.msgSeqNumRange.offset;
                                        it->capturedSize = res.dataRange.len;

                                        if (trace)
                                                SLog("Just registered fdh for wait ctx, capturedSize(", it->capturedSize, "), range = ", ctxP.range, "\n");
                                }
                                else if (res.fdh.get() != ctxP.fdh)
                                {
                                        // Switched to another log file
                                        newLogFile = true;

                                        if (trace)
                                                SLog("Switched to a new fdh for wait ctx\n");
                                }
                                else
                                {
                                        // extend the range
                                        ctxP.range.len += res.dataRange.len;
                                        it->capturedSize += res.dataRange.len;

                                        if (trace)
                                                SLog("Extending range, capturedSize(", it->capturedSize, "), range ", ctxP.range, "\n");
                                }

                                break;
                        }
                }

                if (newLogFile || it->capturedSize >= it->minBytes)
                {
                        if (trace)
                                SLog("Go either newLogFile(", newLogFile, "), or capturedSize(", it->capturedSize, ") >= minBytes(", it->minBytes, ")\n");

                        waitCtxWorkL.push_back(it);
                        waitingList.PopByIndex(i);
                }
                else
                        ++i;
        }
}

append_res topic_partition::append_bundle_to_leader(const uint8_t *const bundle, const size_t bundleLen, const uint8_t bundleMsgsCnt, Switch::vector<wait_ctx *> &waitCtxWorkL)
{
        // TODO: route to leader
        try
        {
                auto res = log_->append_bundle(bundle, bundleLen, bundleMsgsCnt);

                if (trace)
                        SLog("Appended to ", ptr_repr(this), "\n");

                consider_append_res(res, waitCtxWorkL);
                return res;
        }
        catch (const std::exception &e)
        {
                RFLog("Failed, cought exception:", e.what(), "\n");
                return {nullptr, {}, {}};
        }
}

lookup_res topic_partition::read_from_local(const bool fetchOnlyFromLeader, const bool fetchOnlyComittted, const uint64_t absSeqNum, const uint32_t fetchSize)
{
        // TODO:
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

        return std::move(log_->range_for(absSeqNum, fetchSize, maxAbsSeqNum));
}

static uint32_t parse_duration(strwlen32_t in)
{
        uint32_t sum{0};

        do
        {
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

static uint64_t parse_size(strwlen32_t in)
{
        uint64_t sum{0};

        do
        {
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

bool Service::parse_partition_config(const char *const path, partition_config *const l)
{
        int fd = open(path, O_RDONLY | O_LARGEFILE | O_NOATIME);

        if (fd == -1)
                throw Switch::system_error("Failed to access topic/partition config file(", path, "):", strerror(errno));
        else if (const auto fileSize = lseek64(fd, 0, SEEK_END))
        {
                require(fileSize != off64_t(-1));

                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                close(fd);
                if (fileData == MAP_FAILED)
                        throw Switch::system_error("Failed to access topic/partition config file(", path, ") of size ", fileSize, ":", strerror(errno));

                madvise(fileData, fileSize, MADV_SEQUENTIAL);
                Defer({ munmap(fileData, fileSize); });

                for (auto &&line : strwlen32_t((char *)fileData, fileSize).Split('\n'))
                {
                        strwlen32_t k, v;

                        if (auto p = line.Search('#'))
                                line.SetEnd(p);

                        std::tie(k, v) = line.Divided('=');
                        k.TrimWS();
                        v.TrimWS();

                        if (!IsBetweenRange<size_t>(v.len, 1, 128))
                                throw Switch::data_error("Unexpected value for ", k);

                        // We are now using Kafka's configuration keys and semantics - for the most part - for simplicity
                        // Keep it Simple.
                        if (k && v)
                        {
                                if (k.EqNoCase(_S("retention.segments.count")))
                                {
                                        l->roSegmentsCnt = v.AsUint32();
                                        if (l->roSegmentsCnt < 2 && l->roSegmentsCnt)
                                                throw Switch::range_error("Invalid value for ", k);
                                }
                                else if (k.EqNoCase(_S("log.retention.secs")))
                                {
                                        l->lastSegmentMaxAge = parse_duration(v);
                                }
                                else if (k.EqNoCase(_S("log.retention.bytes")))
                                {
                                        l->roSegmentsSize = parse_size(v);
                                        if (l->roSegmentsSize < 128 && l->roSegmentsSize)
                                                throw Switch::range_error("Invalid value for ", k);
                                }
                                else if (k.EqNoCase(_S("log.segment.bytes")))
                                {
                                        l->maxSegmentSize = parse_size(v);
                                        if (l->maxSegmentSize < 128)
                                                throw Switch::range_error("Invalid value for ", k);
                                }
                                else if (k.EqNoCase(_S("log.index.interval.bytes")))
                                {
                                        l->indexInterval = parse_size(v);
                                        if (l->indexInterval < 128)
                                                throw Switch::range_error("Invalid value for ", k);
                                }
                                else if (k.EqNoCase(_S("log.index.size.max.bytes")))
                                {
                                        l->maxIndexSize = parse_size(v);
                                        if (l->maxIndexSize < 128)
                                                throw Switch::range_error("Invalid value for ", k);
                                }
                                else if (k.EqNoCase(_S("log.roll.jitter.secs")))
                                {
                                        l->maxRollJitterSecs = parse_duration(v);
                                }
                                else if (k.EqNoCase(_S("log.roll.secs")))
                                {
                                        l->curSegmentMaxAge = parse_duration(v);
                                }
                                else if (k.EqNoCase(_S("flush.messages")))
                                {
                                        // The number of messages accumulated on a log partition before messages are flushed on disk
                                        l->flushIntervalMsgs = v.AsUint32();
                                }
                                else if (k.EqNoCase(_S("flush.secs")))
                                {
                                        // The amount of time the log can have dirty data before a flush is forced
                                        l->flushIntervalSecs = parse_duration(v);
                                }
                                else
                                        Print("Unknown topic/partition configuration key '", k, "'\n");
                        }
                }
        }

        return true;
}

void Service::rebuild_index(int logFd, int indexFd)
{
        const auto fileSize = lseek64(logFd, 0, SEEK_END);
        auto *const fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, logFd, 0);
        IOBuffer b;
        uint32_t relSeqNum{0};
        static constexpr size_t step{8192};

        if (fileData == MAP_FAILED)
                throw Switch::system_error("Unable to mmap():", strerror(errno));

        Defer({ munmap(fileData, fileSize); });
        madvise(fileData, fileSize, MADV_SEQUENTIAL);

	Print("Rebuilding index of log of size ", size_repr(fileSize), " ..\n");
        for (const auto *p = reinterpret_cast<const uint8_t *>(fileData), *const e = p + fileSize, *const base = p, *next = p; p != e;)
        {
                const auto bundleBase = p;
                const auto bundleLen = Compression::UnpackUInt32(p);
                const auto nextBundle = p + bundleLen;

		expect(p < e);

                const auto bundleFlags = *p++;
                uint32_t msgsSetSize = (bundleFlags >> 2) & 0xf;

                if (!msgsSetSize)
                        msgsSetSize = Compression::UnpackUInt32(p);

		expect(p <= e);

                if (p >= next)
                {
                        b.Serialize<uint32_t>(relSeqNum);
                        b.Serialize<uint32_t>(bundleBase - base);
                        next = bundleBase + step;
                }

                relSeqNum += msgsSetSize;
                p = nextBundle;

		expect(p <= e);
        }

        if (trace)
                SLog("Rebuilt index ", size_repr(b.length()), ", last ", relSeqNum, ", ", b.length(), "\n");

        if (pwrite64(indexFd, b.data(), b.length(), 0) != b.length())
                throw Switch::system_error("Failed to store index:", strerror(errno));
        if (ftruncate(indexFd, b.length()))
                throw Switch::system_error("Failed to truncate index file:", strerror(errno));

        fdatasync(indexFd);
	Print("Exiting\n"); _exit(0);
}

void Service::verify_index(int fd)
{
        const auto fileSize = lseek64(fd, 0, SEEK_END);

	if (!fileSize)
		return;
	else if (fileSize & 7)
		throw Switch::system_error("Unexpected index filesize");

        auto *const fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

        if (fileData == MAP_FAILED)
                throw Switch::system_error("Unable to mmap():", strerror(errno));

        Defer({ munmap(fileData, fileSize); });
        madvise(fileData, fileSize, MADV_SEQUENTIAL);

	for (const auto *p = reinterpret_cast<const uint32_t *>(fileData), 
		*const e = p + (fileSize / sizeof(uint32_t)), *const b = p; p != e; p+=2)
        {
		//SLog(p[0], " ", p[1], "\n");

                if (p != b)
                {
                        expect(p[0] > p[-2]);
                        expect(p[1] > p[-1]);
                }
		else
		{
			expect(p[0] == 0);
			expect(p[1] == 0);
		}
        }
}

uint32_t Service::verify_log(int fd)
{
        const auto fileSize = lseek64(fd, 0, SEEK_END);

	if (!fileSize)
		return 0;

        auto *const fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
	strwlen8_t key;
	strwlen32_t msgContent;
        uint32_t relSeqNum{0};
	IOBuffer cb;
        range_base<const uint8_t *, size_t> msgSetContent;

        if (fileData == MAP_FAILED)
                throw Switch::system_error("Unable to mmap():", strerror(errno));

        Defer({ munmap(fileData, fileSize); });
        madvise(fileData, fileSize, MADV_SEQUENTIAL);

        for (const auto *p = (uint8_t *)fileData, *const e = p + fileSize, *const base = p; p != e;)
        {
		const auto *const bundleBase = p;
                const auto bundleLen = Compression::UnpackUInt32(p);
                const auto nextBundle = p + bundleLen;

		expect(p < e);
		expect(bundleLen);

                const auto bundleFlags = *p++;
		const auto codec = bundleFlags & 3;
                uint32_t msgsSetSize = (bundleFlags >> 2) & 0xf;

                if (!msgsSetSize)
                        msgsSetSize = Compression::UnpackUInt32(p);

		expect(p <= e);
		expect(msgsSetSize);
		expect(codec == 0 || codec == 1);

		if (0)
			Print(relSeqNum, " => OFFSET ", bundleBase - base, "\n");

		if (codec)
		{
			cb.clear();
			if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, nextBundle - p, &cb))
				throw Switch::system_error("Failed to decompress content");
			msgSetContent.Set((uint8_t *)cb.data(), cb.length());
		}
		else
			msgSetContent.Set(p, nextBundle - p);

                uint64_t msgTs{0};

                for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e;)
                {
                        // Next message set message
                        const auto flags = *p++;

                        if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS)))
                        {
                                msgTs = *(uint64_t *)p;
                                p += sizeof(uint64_t);
                        }

                        if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey))
                        {
                                key.Set((char *)p + 1, *p);
                                p += key.len + sizeof(uint8_t);
                        }
                        else
                                key.Unset();

                        const auto msgLen = Compression::UnpackUInt32(p);

                        msgContent.Set((char *)p, msgLen);
                        p += msgLen;
                }

                relSeqNum += msgsSetSize;
                p = nextBundle;

		expect(p <= e);
        }

	return relSeqNum;
} 

Switch::shared_refptr<topic_partition> Service::init_local_partition(const uint16_t idx, const char *const bp, const partition_config &partitionConf)
{
        char basePath[PATH_MAX];
        size_t basePathLen = strlen(bp);

        require(basePathLen < PATH_MAX - 2);
        memcpy(basePath, bp, basePathLen);
        basePath[basePathLen] = '/';
        basePath[++basePathLen] = '\0';

        if (trace)
                SLog("Initializing partition ", bp, "\n");

#ifndef LEAN_SWITCH
        if (!SwitchFS::BuildPath(basePath))
                throw Switch::system_error("Failed to create or access directory ", basePath);
#endif

        try
        {
                struct rosegment_ctx
                {
                        uint64_t firstAvailableSeqNum;
                        uint64_t lastAvailSeqNum;
                        uint32_t creationTS;
                };

                // TODO: reuse roLogs and wideEntyRoLogIndices
                Switch::vector<rosegment_ctx> roLogs;
                std::set<uint64_t> wideEntyRoLogIndices;
                auto partition = Switch::make_sharedref(new topic_partition());
                uint64_t curLogSeqNum{0};
                uint32_t curLogCreateTS{0};
                const strwlen32_t b(basePath, basePathLen);
                int fd;
                auto l = new topic_partition_log();

                l->config = partitionConf;
                partition->distinctId = ++nextDistinctPartitionId;

                partition->log_.reset(l);
                partition->idx = idx;

                l->basePath.Append(basePath, basePathLen);
                l->roSegments = nullptr;
                l->cur.index.ondisk.data = nullptr;
                l->cur.index.ondisk.span = 0;
                l->cur.index.ondisk.lastRecorded.relSeqNum = 0;
                l->cur.index.ondisk.lastRecorded.absPhysical = 0;

                for (auto &&name : DirectoryEntries(basePath))
                {
                        if (*name.p == '.')
                                continue;

                        if (name.Eq(_S("config")))
                        {
                                // override
                                parse_partition_config(Buffer::build(basePath, "/", name).data(), &l->config);
                                continue;
                        }

                        const auto r = name.Divided('.');

                        if (r.second.Eq(_S("index")))
                        {
                                // accept
                                const auto v = r.first.Divided('_');

                                if (v.second.Eq(_S("64")))
                                {
                                        // Just so ro_segment::ro_segment() won't have to try different names until it gets it right
                                        wideEntyRoLogIndices.insert(v.first.AsUint64());
                                }
                        }
                        else if (r.second.Eq(_S("swap")))
                        {
                                // TODO:
                        }
                        else if (r.second.Eq(_S("ilog")))
                        {
                                // Immutable log
                                auto s = r.first;
                                uint32_t creationTS{0};

                                if (const auto *const p = s.Search('_'))
                                {
                                        // Creation TS is encoded in the name
                                        creationTS = s.SuffixFrom(p + 1).AsUint32();
                                        s = s.PrefixUpto(p);
                                }

                                const auto repr = s.Divided('-');
                                const auto baseSeqNum = repr.first.AsUint64(), lastAvailSeqNum = repr.second.AsUint64();

                                if (trace)
                                        SLog("(	", baseSeqNum, ", ", lastAvailSeqNum, ", ", creationTS, ") ", r.first, "\n");

                                roLogs.push_back({baseSeqNum, lastAvailSeqNum, creationTS});
                        }
                        else if (r.second.Eq(_S("log")))
                        {
                                // current, mutable log(segment)
                                // Either num.log or
                                // num_ts.log
                                // the later encodes the creation timestamp in the path, which is useful because
                                // we 'd like to know when this was created, when we restart the service and get to continue using the selected log
                                if (const auto *const p = r.first.Search('_'))
                                {
                                        const auto seqRepr = r.first.PrefixUpto(p);
                                        const auto tsRepr = r.first.SuffixFrom(p + 1);

                                        if (!seqRepr.IsDigits() || !tsRepr.IsDigits())
                                                throw Switch::system_error("Unexpected name ", name);
                                        else
                                        {
                                                const auto seq = seqRepr.AsUint64();

                                                require(seq);
                                                require(!curLogSeqNum);
                                                curLogSeqNum = seq;
                                                curLogCreateTS = tsRepr.AsUint32();

                                                if (trace)
                                                        SLog("curLogSeqNum = ", curLogSeqNum, ", curLogCreateTS = ", curLogCreateTS, " from ", r.first, "\n");
                                        }
                                }
                                else
                                {
                                        if (unlikely(!r.first.IsDigits()))
                                                throw Switch::system_error("Unexpected name ", name);
                                        else
                                        {
                                                const auto seq = r.first.AsUint64();

                                                require(seq);
                                                require(!curLogSeqNum);
                                                curLogSeqNum = seq;
                                        }
                                }
                        }
                        else
                                Print("Unexpected name ", name, " in ", basePath, "\n");
                }

                if (trace)
                        SLog("roLogs.size() = ", roLogs.size(), ", curLogSeqNum = ", curLogSeqNum, ", curLogCreateTS = ", curLogCreateTS, "\n");

                if (roLogs.size())
                {
                        std::sort(roLogs.begin(), roLogs.end(), [](const auto &a, const auto &b) {
                                return a.firstAvailableSeqNum < b.firstAvailableSeqNum;
                        });

                        l->firstAvailableSeqNum = roLogs.front().firstAvailableSeqNum;
                        l->roSegments.reset(new Switch::vector<ro_segment *>());

                        for (const auto &it : roLogs)
                        {
                                if (trace)
                                        SLog("Initializing [", it.firstAvailableSeqNum, ",", it.lastAvailSeqNum, "]\n");

                                l->roSegments->push_back(new ro_segment(it.firstAvailableSeqNum, it.lastAvailSeqNum, b, it.creationTS, wideEntyRoLogIndices.count(it.firstAvailableSeqNum)));
                        }
                }
                else
                {
                        l->firstAvailableSeqNum = curLogSeqNum;
                        l->roSegments.reset(new Switch::vector<ro_segment *>());
                }

                if (curLogSeqNum)
                {
                        // Have a current segment
                        if (curLogCreateTS)
                                Snprint(basePath, sizeof(basePath), b, curLogSeqNum, "_", curLogCreateTS, ".log");
                        else
                                Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".log");

                        fd = open(basePath, O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME, 0775);
                        if (fd == -1)
                                throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot open current segment log");

                        l->cur.fdh.reset(new fd_handle(fd));
                        require(l->cur.fdh->use_count() == 2);
                        l->cur.fdh->Release();
                        l->cur.baseSeqNum = curLogSeqNum;
                        l->cur.fileSize = lseek64(fd, 0, SEEK_END);

                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".index");
                        fd = open(basePath, O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND, 0775);

			if (trace)
				SLog("Considering ", basePath, "\n");

                        if (fd == -1)
                                throw Switch::system_error("open(", basePath_, ") failed:", strerror(errno), ". Cannot open current segment index");
                        else if (lseek64(fd, 0, SEEK_END) == 0 && l->cur.fileSize)
                                Service::rebuild_index(l->cur.fdh->fd, fd);

                        l->cur.index.fd = fd;
                        // if this an empty commit log, need to update the index immediately
                        l->cur.sinceLastUpdate = l->cur.fileSize == 0 ? UINT32_MAX : 0;

                        if (const auto size = lseek64(fd, 0, SEEK_END))
                        {
                                // we don't want to deserialize the skiplist for faster startup
                                // we 'll mmap the region though
                                l->cur.index.ondisk.span = size;
                                l->cur.index.ondisk.data = static_cast<const uint8_t *>(mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0));

                                if (unlikely(l->cur.index.ondisk.data == MAP_FAILED))
                                        throw Switch::system_error("mmap() failed:", strerror(errno));

                                const auto *const p = (uint32_t *)(l->cur.index.ondisk.data + l->cur.index.ondisk.span - sizeof(uint32_t) - sizeof(uint32_t));

                                l->cur.index.ondisk.lastRecorded.relSeqNum = p[0];
                                l->cur.index.ondisk.lastRecorded.absPhysical = p[1];

                                if (trace)
                                {
                                        SLog("Have cur.index.ondisk.span = ", l->cur.index.ondisk.span, " lastRecorded =  ( relSeqNum = ", l->cur.index.ondisk.lastRecorded.relSeqNum, ", absPhysical = ", l->cur.index.ondisk.lastRecorded.absPhysical, ")\n");

#if 1
                                        for (const auto *it = (uint32_t *)l->cur.index.ondisk.data, *const base = it, *const e = it + l->cur.index.ondisk.span / sizeof(uint32_t); it != e; it+=2)
                                        {
						//SLog(it[0], " => ", it[1], "\n");
						if (it != base)
						{
							Drequire(it[0] != it[-2]);
						}
                                        }

#endif
                                }
                        }



                        l->lastAssignedSeqNum = 0;

                        if (const auto s = l->cur.fileSize)
                        {
                                const auto o = l->cur.index.ondisk.lastRecorded.absPhysical;
                                // This is somewhat expensive; but we only need to do this once
                                // We just start from the last tracked-recorded (relSeqNum, absPhysical) and skip bundles until EOF
                                // keeping track of offsets as we go.
                                const auto span = s - o;
                                uint8_t *const data = (uint8_t *)malloc(span);
                                // first message in the first bundle we 'll parse
                                uint64_t next = l->cur.index.ondisk.lastRecorded.relSeqNum + l->cur.baseSeqNum;
                                int fd = l->cur.fdh->fd;

                                if (trace)
                                {
                                        SLog("From lastRecorded.absPhysical = ", o, ", cur.baseSeqNum = ", l->cur.baseSeqNum, "\n");
                                        SLog("span = ", span, ", start from ", next, "\n");
                                }

                                if (unlikely(!data))
                                        throw Switch::system_error("malloc(", span, ") failed");

                                Defer({ free(data); });

                                Drequire(fd != -1);

                                const auto res = pread64(fd, data, span, o);

                                if (trace)
                                        SLog("Attempting to read ", span, " bytes at ", o, ": ", res, "\n");

                                if (res != span)
                                        throw Switch::system_error("pread64() failed:", strerror(errno));

                                for (const auto *p = data, *const e = p + span; p != e;)
                                {
                                        const auto bundleLen = Compression::UnpackUInt32(p);
                                        const auto *const bundleEnd = p + bundleLen;

                                        require(bundleLen);

                                        const auto bundleFlags = *p++;
                                        uint32_t msgSetSize = (bundleFlags >> 2) & 0xf;

                                        if (!msgSetSize)
                                                msgSetSize = Compression::UnpackUInt32(p);

                                        if (trace)
                                                SLog("bundle msgSetSize = ", msgSetSize, "\n");

                                        next += msgSetSize;
                                        p = bundleEnd;
                                        Drequire(p <= e);
                                }

                                l->lastAssignedSeqNum = next - 1;

                                if (trace)
                                        SLog("Set lastAssignedSeqNum = ", l->lastAssignedSeqNum, "\n");
                        }

                        // Just in case
                        lseek64(l->cur.index.fd, 0, SEEK_END);
                        lseek64(l->cur.fdh->fd, 0, SEEK_END);

                        if (const uint32_t max = l->config.maxRollJitterSecs)
                        {
                                std::random_device dev;
                                std::mt19937 rng(dev());
                                std::uniform_int_distribution<uint32_t> distr(0, max);

                                l->cur.rollJitterSecs = distr(rng);
                        }
                        else
                                l->cur.rollJitterSecs = 0;

                        const auto now = Timings::Seconds::SysTime();

                        l->cur.createdTS = curLogCreateTS ?: now;
                        l->cur.nameEncodesTS = curLogCreateTS;
                        l->cur.flush_state.pendingFlushMsgs = 0;
                        l->cur.flush_state.nextFlushTS = config.flushIntervalSecs ? now + config.flushIntervalSecs : UINT32_MAX;

                        if (trace)
                                SLog("createdTS(", l->cur.createdTS, ") nameEncodesTS(", l->cur.nameEncodesTS, ")\n");
                }
                else
                {
                        // We 'll just create those on demand in append()
                        l->cur.fdh.reset(nullptr);
                        l->cur.fileSize = UINT32_MAX;
                        l->cur.index.fd = -1;
                        l->cur.sinceLastUpdate = UINT32_MAX;
                        l->cur.baseSeqNum = UINT64_MAX;
                        l->lastAssignedSeqNum = 0;

                        if (l->roSegments && l->roSegments->size())
                        {
                                // We can still use the last immutable segment
                                Print("Looks like someone deleted the active segment from ", bp, "\n");
                                l->lastAssignedSeqNum = l->roSegments->back()->lastAvailSeqNum;
                        }
                }

                return partition;
        }
        catch (const std::exception &e)
        {
                if (trace)
                        SLog("Exception:", e.what(), "\n");

                throw;
        }
}

bool Service::process_consume(connection *const c, const uint8_t *p, const size_t len)
{
        try
        {
                auto respHeader = get_buffer();
                bool respondNow{false};
                const auto replicaId = c->replicaId;

                const auto clientVersion = *(uint16_t *)p;
                p += sizeof(uint16_t);
                const auto requestId = *(uint32_t *)p;
                p += sizeof(uint32_t);
                const strwlen8_t clientId((char *)(p + 1), *p);
                p += clientId.len + sizeof(uint8_t);
                const auto maxWait = *(uint64_t *)p; // if we don't get any data within `maxWait`ms, we 'll return nothing for the requested partitions
                p += sizeof(uint64_t);
                const auto minBytes = Min<uint32_t>(*(uint32_t *)p, 64 * 1024 * 1024); // keep it sane
                p += sizeof(uint32_t);
                const auto topicsCnt = *p++;

                (void)clientVersion;
                if (trace)
                        SLog("topicsCnt = ", topicsCnt, "\n");

                respHeader->Serialize(uint8_t(TankAPIMsgType::Consume));
                const auto sizeOffset = respHeader->length();
                respHeader->MakeSpace(sizeof(uint32_t));

                // This is an optimization for the client
                // we 'll store the response length header
                const auto headerSizeOffset = respHeader->length();
                respHeader->MakeSpace(sizeof(uint32_t));
                respHeader->Serialize(requestId);
                respHeader->Serialize(topicsCnt);

                auto q = c->outQ;
                size_t sum{0};

                if (!q)
                        q = c->outQ = get_outgoing_queue();

                const auto qSize = q->size();

                q->push_back(respHeader);
                deferList.clear();

                for (uint32_t i{0}; i != topicsCnt; ++i)
                {
                        const strwlen8_t topicName((char *)(p + 1), *p);
                        p += topicName.len + sizeof(uint8_t);
                        const auto partitionsCnt = *p++;
                        auto topic = topic_by_name(topicName);

                        if (trace)
                                SLog("topic [", topicName, "]\n");
                        respHeader->Serialize(topicName.len);
                        respHeader->Serialize(topicName.p, topicName.len);
                        respHeader->Serialize(partitionsCnt);

                        if (!topic)
                        {
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

                        for (uint32_t k{0}; k != partitionsCnt; ++k)
                        {
                                const auto partitionId = *(uint16_t *)p;
                                p += sizeof(uint16_t);
                                const auto absSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);
                                const auto fetchSize = *(uint32_t *)p;
                                p += sizeof(uint32_t);
                                auto partition = topic->partition(partitionId);

                                respHeader->Serialize(partitionId);

                                if (!partition)
                                {
                                        if (trace)
                                                SLog("Undefined partition ", partitionId, "\n");
                                        respHeader->Serialize(uint8_t(0xff));
                                        respondNow = true;
                                        continue;
                                }

                                if (trace)
                                        SLog("> ", partitionId, ", ", absSeqNum, ", ", fetchSize, "\n");

                                if (absSeqNum == UINT64_MAX)
                                {
                                        // Fetch starting from whatever bundles are commited from now on
                                        // This should be the default
                                        deferList.push_back(partition);
                                }
                                else
                                {
                                        const bool fetchOnlyFromLeader = replicaId != UINT16_MAX; // UINT16_MAX replica is the debug consumer ID
                                        const bool fetchOnlyCommitted = replicaId == 0;           // for clients, only comitted
                                        auto res = partition->read_from_local(fetchOnlyFromLeader, fetchOnlyCommitted,
                                                                              absSeqNum, fetchSize);
                                        const auto hwMark = res.highWatermark;
                                        range32_t range;

                                        switch (res.fault)
                                        {
                                                case lookup_res::Fault::NoFault:
                                                        adjust_range_start(res, absSeqNum);
                                                        range.Set(res.fileOffset, fetchSize);
                                                        if (range.End() > res.fileOffsetCeiling)
                                                                range.SetEnd(res.fileOffsetCeiling);

                                                        if (trace)
                                                                SLog(ansifmt::bold, "Response:(baseSeqNum = ", res.absBaseSeqNum, ", range ", range, ")", ansifmt::reset, "\n");

                                                        respHeader->Serialize(uint8_t(0));        // error
                                                        respHeader->Serialize(res.absBaseSeqNum); // absolute first seq.num of the first message of the first bundle in the streamed chunk
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

                                                        if (1)
                                                        {
                                                                // See https://github.com/phaistos-networks/TANK/issues/14 for measurements
                                                                const uint64_t b = trace ? Timings::Microseconds::Tick() : 0;

                                                                readahead(res.fdh->fd, range.offset, range.len);

                                                                if (trace)
                                                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(b)), " for readahead(", range, ") ", size_repr(range.len), "\n");
                                                        }
#endif

                                                        sum += range.len;
                                                        q->push_back({res.fdh.get(), range});

                                                        // TODO:
                                                        // if (replicaId) { updateFollowerLogEndOffset(topic, partition, absSeqNum) }
                                                        respondNow = true;
                                                        break;

                                                case lookup_res::Fault::AtEOF:
                                                        if (trace)
                                                                SLog("Got AtEOF; will wait\n");

                                                        deferList.push_back(partition);
                                                        break;

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
                if (respondNow || maxWait == 0)
                {
                        // - fetch request does not want to wait
                        // - fetch request does not require any data, or we already have some data to provide to the client
                        // - one or more errors were generated
                        *(uint32_t *)respHeader->At(sizeOffset) = respHeader->length() - sizeOffset - sizeof(uint32_t) + sum;
                        *(uint32_t *)respHeader->At(headerSizeOffset) = respHeader->length() - headerSizeOffset - sizeof(uint32_t);

                        if (trace)
                                SLog("respHeader.length = ", respHeader->length(), " ", *(uint32_t *)respHeader->At(sizeOffset), "\n");

                        return try_send_ifnot_blocked(c);
                }
                else
                {
                        // Can't respond; we 'll need to wait until we have any data for any of those
                        // topic/partitions first
                        while (q->size() != qSize)
                        {
                                auto &p = q->back();

                                if (p.payloadBuf)
                                        put_buffer(p.buf);
                                else
                                        p.file_range.fdh->Release();

                                q->pop_back();
                        }

                        if (q->empty())
                        {
                                if (trace)
                                        SLog("No longer needed outQ\n");

                                put_outgoing_queue(q);
                                c->outQ = nullptr;
                        }

                        return register_consumer_wait(c, requestId, maxWait, minBytes, deferList.values(), deferList.size());
                }
        }
        catch (const std::exception &e)
        {
                return shutdown(c, __LINE__);
        }
}

bool Service::register_consumer_wait(connection *const c, const uint32_t requestId, const uint64_t maxWait, const uint32_t minBytes, topic_partition **const partitions, const uint32_t totalPartitions)
{
        auto ctx = get_waitctx(totalPartitions);

        if (trace)
                SLog("REGISTERING consumer wait for ", totalPartitions, " partitions, maxWait = ", maxWait, "\n");

        switch_dlist_init(&ctx->list);
        switch_dlist_init(&ctx->expList);
        ctx->requestId = requestId;
        ctx->c = c;
        ctx->partitionsCnt = totalPartitions;
        ctx->minBytes = minBytes;
        ctx->capturedSize = 0;
        switch_dlist_insert_after(&c->waitCtxList, &ctx->list);

        if (maxWait)
        {
                ctx->expiration = Timings::Milliseconds::Tick() + maxWait;
                switch_dlist_insert_after(&waitExpList, &ctx->expList);
        }
        else
                ctx->expiration = 0;

        for (uint32_t i{0}; i != totalPartitions; ++i)
        {
                auto p = partitions[i];
                auto out = ctx->partitions + i;

                if (trace)
                        SLog("Partition ", ptr_repr(p), "\n");

                p->waitingList.push_back(ctx);

                out->partition = p;
                out->fdh = nullptr;
                out->range.Unset();
                out->seqNum = 0;
        }

        return true;
}

bool Service::process_replica_reg(connection *const c, const uint8_t *p, const size_t len)
{
        if (unlikely(len < sizeof(uint16_t)))
                return shutdown(c, __LINE__);

        c->replicaId = *(uint16_t *)p;

        if (unlikely(c->replicaId == 0))
                return shutdown(c, __LINE__);

        return true;
}

bool Service::process_produce(connection *const c, const uint8_t *p, const size_t len)
{
        const uint64_t processBegin = trace ? Timings::Microseconds::Tick() : 0;
        auto *const respHeader = get_buffer();
        auto q = c->outQ;
        const auto clientVersion = *(uint16_t *)p;
        p += sizeof(uint16_t);
        const auto requestId = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const strwlen8_t clientId((char *)(p + 1), *p);
        p += clientId.len + sizeof(uint8_t);
        const auto requiredAcks = *p++;
        const auto ackTimeout = *(uint32_t *)p;
        p += sizeof(uint32_t);
        const auto topicsCnt = *p++;
        strwlen8_t topicName;
        strwlen32_t msgContent;

        Drequire(!respHeader->length());

        respHeader->Serialize(uint8_t(TankAPIMsgType::Produce));
        const auto sizeOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint32_t));

        (void)clientVersion;
        (void)requiredAcks;
        (void)ackTimeout;

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
        q->push_back(respHeader);

        respHeader->Serialize(requestId);

        if (trace)
                SLog("Parsing ", topicsCnt, "\n");

        for (uint32_t i{0}; i != topicsCnt; ++i)
        {
                topicName.Set((char *)p + 1, *p);
                p += sizeof(uint8_t) + topicName.len;

                auto topic = topic_by_name(topicName);

                if (!topic)
                {
                        if (trace)
                                SLog("Unknown topic [", topicName, "]\n");

                        for (auto cnt = *p++; cnt; --cnt)
                        {
                                p += sizeof(uint16_t);             // skip partition ID
                                p += Compression::UnpackUInt32(p); // skip bundle
                        }

                        respHeader->Serialize(uint8_t(0xff));
                        continue;
                }

                const auto partitionsCnt = *p++;
                range_base<const uint8_t *, size_t> msgSetContent;

                if (trace)
                        SLog("partitionsCnt:", partitionsCnt, " for [", topicName, "]\n");

                for (uint32_t k{0}; k != partitionsCnt; ++k)
                {
                        const auto partitionId = *(uint16_t *)p;
                        p += sizeof(uint16_t);
                        const auto bundleLen = Compression::UnpackUInt32(p);
                        const auto *const bundle = p;
                        const auto *const e = p + bundleLen;
                        auto partition = topic->partition(partitionId);

                        if (!partition)
                        {
                                if (trace)
                                        SLog("Undefined topic partition ", partitionId, "\n");

                                p = e;
                                respHeader->Serialize<uint8_t>(1);
                                continue;
                        }

                        if (unlikely(!bundleLen))
                                return shutdown(c, __LINE__);

                        // TODO: Reject appending to internal topics if not alowed

                        if (trace)
                                SLog("partitionId = ", partitionId, ",  bundleLen = ", bundleLen, "\n");

                        // BEGIN: bundle header
                        const auto bundleFlags = *p++;
                        const auto codec = bundleFlags & 3;
                        auto msgSetSize = uint32_t((bundleFlags >> 2) & 0xf);

                        if (!msgSetSize)
                        {
                                // more than 15 messages in the message set; were not able to encode that in the 4 bits reserved
                                // in flags; so that's encoded as a varint here
                                msgSetSize = Compression::UnpackUInt32(p);
                        }

                        if (trace)
                                SLog("bundleFlags = ", bundleFlags, ", codec= ", codec, ", message set messages cnt: ", msgSetSize, "\n");
                        // END: bundle header

                        if (trace)
                        {
                                static thread_local IOBuffer decompressionBuf;
                                strwlen8_t key;

                                if (codec)
                                {
                                        decompressionBuf.clear();
                                        Compression::UnCompress(Compression::Algo::SNAPPY, p, e - p, &decompressionBuf);
                                        msgSetContent.Set(reinterpret_cast<const uint8_t *>(decompressionBuf.data()), decompressionBuf.length());
                                }
                                else
                                        msgSetContent.Set(p, e - p);

                                // iterate bundle's message set
                                uint64_t msgTs{0};

                                for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e;)
                                {
                                        // Next message set message
                                        const auto flags = *p++;

                                        if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS)))
                                        {
                                                msgTs = *(uint64_t *)p;
                                                p += sizeof(uint64_t);
                                        }

                                        if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey))
                                        {
                                                key.Set((char *)p + 1, *p);
                                                p += key.len + sizeof(uint8_t);
                                        }
                                        else
                                                key.Unset();

                                        const auto msgLen = Compression::UnpackUInt32(p);

                                        (void)flags;
                                        (void)msgTs;
                                        msgContent.Set((char *)p, msgLen);
                                        p += msgLen;

                                        Print("[", key, "] = ", msgContent, "\n");
                                }
                        }

                        const auto b = Timings::Microseconds::Tick();
                        const auto res = partition->append_bundle_to_leader(bundle, bundleLen, msgSetSize, expiredCtxList);

                        if (unlikely(!res.fdh))
                        {
                                Print("Failed; will shutdown\n");
                                _exit(1);
                        }

                        if (trace)
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(b)), " for ", msgSetSize, " msgs in bundle message set: ", expiredCtxList.size(), "\n");

#ifdef LEAN_SWITCH
                        (void)b;
#endif

                        respHeader->Serialize(uint8_t(0));

                        while (expiredCtxList.size())
                        {
                                auto ctx = expiredCtxList.Pop();

                                wakeup_wait_ctx(ctx, res, c);
                        }
                }
        }
        *(uint32_t *)respHeader->At(sizeOffset) = respHeader->length() - sizeOffset - sizeof(uint32_t);

        if (trace)
        {
                // for 100MBs, this takes Took 0.005s
                SLog("Took ", duration_repr(Timings::Microseconds::Since(processBegin)), "\n");
        }

        return try_send_ifnot_blocked(c);
}

bool Service::process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len)
{
        if (trace)
                SLog("New message  type ", msg, ", len ", len, "\n");

        switch (TankAPIMsgType(msg))
        {
                case TankAPIMsgType::Produce:
                        return process_produce(c, data, len);

                case TankAPIMsgType::Consume:
                        return process_consume(c, data, len);

                case TankAPIMsgType::Ping:
			return true;

                case TankAPIMsgType::RegReplica:
                        return process_replica_reg(c, data, len);

                default:
                        return shutdown(c, __LINE__);
        }
}

void Service::wakeup_wait_ctx(wait_ctx *const wctx, const append_res &appendRes, connection *const produceConnection)
{
        auto respHeader = get_buffer();
        uint8_t topicsCnt{0};
        auto c = wctx->c;
        auto q = c->outQ;
        size_t sum{0};

        if (!q)
                q = c->outQ = get_outgoing_queue();

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_green, "Waking up wait ctx ", ptr_repr(wctx), ", ", wctx->requestId, ansifmt::reset, ansifmt::reset, "\n");

        q->push_back(respHeader);

        respHeader->Serialize(uint8_t(2));
        const auto sizeOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint32_t));
        const auto headerSizeOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint32_t));
        respHeader->Serialize(wctx->requestId);
        const auto topicsCntOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint8_t));

        for (uint32_t i{0}; i != wctx->partitionsCnt;)
        {
                auto &it = wctx->partitions[i];
                const auto *p = it.partition;
                const auto t = p->owner;
                const auto topicName = t->name();
                uint8_t partitionsCnt{0};

                ++topicsCnt;
                respHeader->Serialize(topicName.len);
                respHeader->Serialize(topicName.p, topicName.len);

                if (trace)
                        SLog("Topic [", topicName, "]\n");

                const auto partitionsCntOffset = respHeader->length();
                respHeader->MakeSpace(sizeof(uint8_t));

                do
                {
                        if (trace)
                                SLog("partition ", p->idx, "\n");

                        respHeader->Serialize(p->idx);
                        respHeader->Serialize(uint8_t(0));
                        if (it.fdh)
                        {

                                if (trace)
                                        SLog("HAVE data for ", topicName, ".", p->idx, ", seqNum = ", it.seqNum, ", range ", it.range, " (", it.range.len, ")\n");

                                respHeader->Serialize(it.seqNum);
                                respHeader->Serialize(p->highwater_mark());
                                respHeader->Serialize(it.range.len);

                                sum += it.range.len;
                                q->push_back({it.fdh, it.range});
                                it.fdh->Release();
                                it.fdh = nullptr;
                        }
                        else
                        {
                                respHeader->Serialize(uint64_t(0));
                                respHeader->Serialize(p->highwater_mark());
                                respHeader->Serialize(uint32_t(0));
                        }

                        ++partitionsCnt;
                } while (++i != wctx->partitionsCnt && (p = (it = wctx->partitions[i]).partition)->owner == t);

                *(uint8_t *)respHeader->At(partitionsCntOffset) = partitionsCnt;
        }

        *(uint8_t *)respHeader->At(topicsCntOffset) = topicsCnt;
        *(uint32_t *)respHeader->At(sizeOffset) = respHeader->length() - sizeOffset - sizeof(uint32_t) + sum;
        *(uint32_t *)respHeader->At(headerSizeOffset) = respHeader->length() - headerSizeOffset - sizeof(uint32_t);

        destroy_wait_ctx(wctx);

        if (c != produceConnection)
        {
                // see process_produce()
                try_send_ifnot_blocked(c);
        }
}

void Service::abort_wait_ctx(wait_ctx *const wctx)
{
        auto respHeader = get_buffer();
        uint8_t topicsCnt{0};
        auto c = wctx->c;

        if (trace)
                SLog("Aborting wait ctx ", ptr_repr(wctx), ", ", wctx->requestId, "\n");

        respHeader->Serialize(uint8_t(2));
        const auto sizeOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint32_t));
        const auto headerSizeOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint32_t));
        respHeader->Serialize(wctx->requestId);
        const auto topicsCntOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint8_t));

        for (uint32_t i{0}; i != wctx->partitionsCnt;)
        {
                auto &it = wctx->partitions[i];
                const auto *p = it.partition;
                const auto t = p->owner;
                const auto topicName = t->name();
                uint8_t partitionsCnt{0};

                ++topicsCnt;
                respHeader->Serialize(topicName.len);
                respHeader->Serialize(topicName.p, topicName.len);

                if (trace)
                        SLog("Topic [", topicName, "]\n");

                const auto partitionsCntOffset = respHeader->length();
                respHeader->MakeSpace(sizeof(uint8_t));

                do
                {
                        if (trace)
                                SLog("partition ", p->idx, "\n");

                        if (it.fdh)
                        {
                                it.fdh->Release();
                                it.fdh = nullptr;
                        }

                        respHeader->Serialize(p->idx);
                        respHeader->Serialize(uint8_t(0));
                        respHeader->Serialize(uint64_t(0));
                        respHeader->Serialize(p->highwater_mark());
                        respHeader->Serialize(uint32_t(0));
                        ++partitionsCnt;
                } while (++i != wctx->partitionsCnt && (p = (it = wctx->partitions[i]).partition)->owner == t);

                *(uint8_t *)respHeader->At(partitionsCntOffset) = partitionsCnt;

                if (it.fdh)
                {
                        it.fdh->Release();
                        it.fdh = nullptr;
                }
        }

        *(uint8_t *)respHeader->At(topicsCntOffset) = topicsCnt;
        *(uint32_t *)respHeader->At(sizeOffset) = respHeader->length() - sizeOffset - sizeof(uint32_t);
        *(uint32_t *)respHeader->At(headerSizeOffset) = respHeader->length() - headerSizeOffset - sizeof(uint32_t);

        auto q = c->outQ;

        if (!q)
                q = c->outQ = get_outgoing_queue();

        q->push_back(respHeader);
        destroy_wait_ctx(wctx);
        try_send_ifnot_blocked(c);
}

void Service::destroy_wait_ctx(wait_ctx *const wctx)
{
        switch_dlist_del(&wctx->list);

        if (trace)
                SLog("destroying ", ptr_repr(wctx), " ", wctx->partitionsCnt, "\n");

        for (uint32_t i{0}; i != wctx->partitionsCnt; ++i)
        {
                auto &it = wctx->partitions[i];
                auto p = it.partition;

                if (it.fdh)
                {
                        it.fdh->Release();
                        it.fdh = nullptr;
                }

                p->waitingList.RemoveByValue(wctx);
        }

        if (switch_dlist_any(&wctx->expList))
                switch_dlist_del(&wctx->expList);

        put_waitctx(wctx);
}

void Service::cleanup_connection(connection *const c)
{
        poller.DelFd(c->fd);
        close(c->fd);
        c->fd = -1;

        expiredCtxList.clear();
        for (auto it = c->waitCtxList.next; it != &c->waitCtxList; it = it->next)
                expiredCtxList.push_back(switch_list_entry(wait_ctx, list, it));

        while (expiredCtxList.size())
                destroy_wait_ctx(expiredCtxList.Pop());

        switch_dlist_del(&c->connectionsList);

        if (auto b = std::exchange(c->inB, nullptr))
        {
                b->clear();
                put_buffer(b);
        }

        if (auto q = std::exchange(c->outQ, nullptr))
                put_outgoing_queue(q);

        put_connection(c);
}

bool Service::shutdown(connection *const c, const uint32_t ref)
{
        if (trace)
                SLog("SHUTDOWN at ", ref, " ", Timings::Microseconds::SysTime(), "\n");

        require(c->fd != -1);
        cleanup_connection(c);

        return false;
}

bool Service::try_send(connection *const c)
{
        int fd = c->fd;
        auto q = c->outQ;
        bool haveCork{false};

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::PendingIntro)))
        {
                uint8_t b[sizeof(uint8_t) + sizeof(uint32_t)];

                b[0] = uint8_t(TankAPIMsgType::Ping);          // msg = ping
                *(uint32_t *)(b + sizeof(uint8_t)) = 0; 	// no payload

                if (trace)
                        SLog("PINGING\n");

                if (q && !q->empty())
                {
                        if (trace)
                                SLog("Enabling cork\n");

                        haveCork = true;
                        Switch::SetTCPCork(fd, 1);
                }

                if (write(fd, b, sizeof(b)) != sizeof(b))
                {
                        RFLog("Failed to ping client:", strerror(errno), "\n");
                        throw Switch::system_error("Failed to ping client");
                }

                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::PendingIntro));
        }

        if (!q)
        {
                if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))
                {
                        poller.SetDataAndEvents(fd, c, POLLIN);
                        c->state.flags &= ~(1u << uint8_t(connection::State::Flags::NeedOutAvail));
                }

                return true;
        }

        if (trace)
                SLog("Attempting to send ", q->size(), "\n");

        while (!q->empty())
        {
                auto &it = q->front();

                if (it.payloadBuf)
                {
                        if (trace)
                                SLog("Buf = ", ptr_repr(it.buf), "\n");

                        if (!haveCork)
                        {
                                if (trace)
                                        SLog("Enabling cork\n");
                                haveCork = true;
                                Switch::SetTCPCork(fd, 1);
                        }

                        for (auto b = it.buf;;)
                        {
                                const auto range = b->SuffixFromOffset();
                                int r = write(fd, range.p, range.len);

                                if (r == -1)
                                {
                                        if (errno == EINTR)
                                                continue;
                                        else if (errno == EAGAIN)
                                        {
                                        set_need_outavail:
                                                if (haveCork)
                                                {
                                                        if (trace)
                                                                SLog("Removing cork\n");
                                                        Switch::SetTCPCork(fd, 0);
                                                }

                                                if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))))
                                                {
                                                        c->state.flags |= (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                                                        poller.SetDataAndEvents(fd, c, POLLIN | POLLOUT);
                                                }

                                                return true;
                                        }
                                        else
                                                return shutdown(c, __LINE__);
                                }

                                b->AdvanceOffset(r);

                                if (b->IsAtEnd())
                                {
                                        if (trace)
                                                SLog("Done with buf\n");

                                        put_buffer(b);
                                        q->pop_front();
                                        break;
                                }

                                if (r != range.len)
                                {
                                        // if we didn't get to write() all the data we wanted to write
					// it means that the socket buffer is now full, and it will almost definitely
					// not be drained by the time we call write() again in this loop; we 'd insted
					// get EAGAIN.
					// So we 'll just consider the socket buffer full now and execute the EAGAIN logic
                                        goto set_need_outavail;
                                }
                        }
                }
                else
                {
                        // https://github.com/phaistos-networks/TANK/issues/14
			// if only FreeBSD's great sendfile() syscall was available on Linux, with support for the
			// extra flags based on NGINX's and Netflix's work, that'd make everything so much simpler.
			// We 'd just use the SF_NODISKIO flag and the SF_READAHEAD macro, and check for EBUSY
			// and optionally use readahead() and try again later(we could also mmap() the log and use mincore() to determine if
			// all pages are cached)
                        for (;;)
                        {
                                auto &range = it.file_range.range;
                                const uint64_t before = trace ? Timings::Microseconds::Tick() : 0;
                                const auto outLen = range.len;

#ifdef HAVE_SENDFILE64
                                off64_t offset = range.offset;
                                const auto r = sendfile64(fd, it.file_range.fdh->fd, &offset, outLen);
#else
                                off_t offset = range.offset;
                                const auto r = sendfile(fd, it.file_range.fdh->fd, &offset, outLen);
#endif

                                if (trace)
                                {
                                        // See https://github.com/phaistos-networks/TANK/issues/14 for measurements
                                        SLog("Sending contents ", range, " => ", r, " ", duration_repr(Timings::Microseconds::Since(before)), "\n");
                                }

                                if (r == -1)
                                {
                                        if (errno == EINTR)
                                                continue;
                                        else if (errno == EAGAIN)
                                        {
                                        set_need_outavail2:
                                                if (trace)
                                                        SLog("EAGAIN (haveCork = ", haveCork, ", needOutAvail = ", (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))), ")\n");

                                                if (haveCork)
                                                {
                                                        if (trace)
                                                                SLog("Unsetting cork\n");

                                                        Switch::SetTCPCork(fd, 0);
                                                }

                                                if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))))
                                                {
                                                        if (trace)
                                                                SLog("Now NeedOutAvail\n");

                                                        c->state.flags |= (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                                                        poller.SetDataAndEvents(fd, c, POLLIN | POLLOUT);
                                                }

                                                return true;
                                        }
                                        else
                                                return shutdown(c, __LINE__);
                                }
                                else
                                {
                                        range.len -= r;
                                        range.offset += r;

                                        if (!range)
                                        {
                                                it.file_range.fdh->Release();
                                                q->pop_front();
                                                break;
                                        }

                                        if (r != outLen)
                                        {
                                                // if we didn't get to sendfile() all the data we wanted to write
                                                // it means that the socket buffer is now full, and it will almost definitely
                                                // not be drained by the time we call sendfile() again in this loop; we 'd insted
                                                // get EAGAIN.
                                                // So we 'll just consider the socket buffer full now and execute the EAGAIN logic
                                                goto set_need_outavail2;
                                        }
                                }
                        }
                }
        }

        put_outgoing_queue(q);
        c->outQ = nullptr;

        if (haveCork)
        {
                if (trace)
                        SLog("Removing cork\n");

                Switch::SetTCPCork(fd, 0);
        }

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))
        {
                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::NeedOutAvail));
                poller.SetDataAndEvents(fd, c, POLLIN);
        }

        return true;
}

Service::~Service()
{
        while (switch_dlist_any(&allConnections))
                cleanup_connection(switch_list_entry(connection, connectionsList, allConnections.next));

        while (bufs.size())
                delete bufs.Pop();

        while (outgoingQueuesPool.size())
                delete outgoingQueuesPool.Pop();

        while (connsPool.size())
                delete connsPool.Pop();

        for (auto &it : waitCtxPool)
        {
                while (it.size())
                        free(it.Pop());
        }

#ifdef LEAN_SWITCH
        for (auto &it : topics)
                it.second->Release();
#endif
}

static bool running{true};

static void sig_handler(int)
{
        running = false;
}

// TODO: https://github.com/phaistos-networks/TANK/issues/7
int Service::start(int argc, char **argv)
{
        sockaddr_in sa;
        uint64_t nextIdleCheck{0}, nextExpWaitCtxCheck{0};
        int r;
        struct stat64 st;
        size_t totalPartitions{0};
        Switch::endpoint listenAddr;

	if (argc > 1 && !strcmp(argv[1], "verify"))
        {
                // https://github.com/phaistos-networks/TANK/wiki/Managing-Segments-files
                size_t n{0};
                bool anyFailed{false};

                for (uint32_t i{2}; i < argc; ++i)
                {
                        const char *const path = argv[i];
                        const strwlen32_t fullPath(path);
                        const auto ext = fullPath.Extension();

                        if (ext.Eq(_S("ilog")) || ext.Eq(_S("log")) || ext.Eq(_S("index")))
                        {
                                int fd = open(path, O_RDONLY | O_LARGEFILE);

                                if (fd == -1)
                                {
                                        Print("Failed to access ", fullPath, ": ", strerror(errno), "\n");
                                        return 1;
                                }

                                Defer({ close(fd); });

                                Print("Verifying ", fullPath, " (", size_repr(lseek64(fd, 0, SEEK_END)), ") ..\n");

                                try
                                {
                                        if (ext.Eq(_S("ilog")) || ext.Eq(_S("log")))
                                        {
                                                const auto r = Service::verify_log(fd);

                                                Print("> ", dotnotation_repr(r), " msgs\n");
                                        }
                                        else if (ext.Eq(_S("index")))
                                        {
                                                Service::verify_index(fd);
                                        }
                                }
                                catch (const std::exception &e)
                                {
                                        Print(ansifmt::bold, ansifmt::color_red, "Failed to verify (", fullPath, ansifmt::reset, ")\n");
                                        anyFailed = true;
                                }

                                ++n;
                        }
                        else
                                Print("Ignoring ", fullPath, "\n");
                }

                if (!anyFailed)
                        Print(ansifmt::color_green, "All ", dotnotation_repr(n), " files verified OK", ansifmt::reset, "\n");
                return 0;
        }

        signal(SIGPIPE, SIG_IGN);
        signal(SIGHUP, SIG_IGN);
        while ((r = getopt(argc, argv, "p:l:hv")) != -1)
        {
                switch (r)
                {
                        case 'p':
                                basePath_.clear();
                                basePath_.append(strwlen32_t(optarg, strlen(optarg)));
                                break;

                        case 'l':
                                listenAddr = Switch::ParseSrvEndpoint({optarg}, _S8("tank"), 11011);
                                if (!listenAddr)
                                {
                                        Print("Failed to parse endpoint from ", optarg, "\n");
                                        return 1;
                                }
                                break;

                        case 'h':
                                Print("-p path: Specifies the base path where all topic exist. Used in standalone mode\n");
                                Print("-b endpoint: Specifies that the service will run in standalone mode, listening for connections to that address\n");
                                Print("-v : displays Tank version and exits\n");
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

        if (!listenAddr)
        {
                Print("Listen address not specified. Use -l address to specify it\n");
                return 1;
        }

        if (!basePath_.length())
        {
                Print("Base path not specified. Use -p path to specify it\n");
                return 1;
        }
        else if (stat64(basePath_.data(), &st) == -1)
        {
                Print("Failed to stat(", basePath_, "): ", strerror(errno), ". Please verify base path\n");
                return 1;
        }
        else if (!(st.st_mode & S_IFDIR))
        {
                Print(basePath_, " not a directory\n");
                return 1;
        }
        else if (opMode == OperationMode::Standalone)
        {
                try
                {
                        // We will parallelize this across multiple threads so that we can support many thousands of topics and partitions
                        // without incurring a long startup-sequence time
                        const auto basePathLen = basePath_.length();
                        std::vector<std::pair<topic *, size_t>> pendingPartitions;
                        uint64_t before;
                        simple_allocator a{8192};
                        std::vector<strwlen8_t> collectedTopics;
                        std::vector<std::future<void>> futures;
                        std::mutex collectLock;

                        if (trace)
                                before = Timings::Microseconds::Tick();

                        for (const auto &&name : DirectoryEntries(basePath_.data()))
                        {
                                if (*name.p != '.')
                                        collectedTopics.push_back({a.CopyOf(name.p, name.len), name.len});
                        }

                        if (trace)
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for initial walk ", collectedTopics.size(), "\n");

                        for (const auto &it : collectedTopics)
                        {
                                futures.push_back(std::async([&collectLock, &basePath_ = basePath_, this, &pendingPartitions ](const strwlen8_t name) {
                                        char path[PATH_MAX];
                                        struct stat64 st;
                                        const auto len = Snprint(path, sizeof(path), basePath_, "/", name, "/");

                                        if (stat64(path, &st) == -1)
                                                throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
                                        else if (st.st_mode & S_IFDIR)
                                        {
                                                uint32_t partitionsCnt{0}, min{UINT32_MAX}, max{0};
                                                partition_config partitionConfig;

                                                for (const auto &&name : DirectoryEntries(path))
                                                {
                                                        name.ToCString(path + len);

                                                        if (name.Eq(_S("config")))
                                                        {
                                                                // topic overrides defaults
                                                                parse_partition_config(path, &partitionConfig);
                                                        }
                                                        else if (name.IsDigits())
                                                        {
                                                                if (stat64(path, &st) == -1)
                                                                        throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
                                                                else if (st.st_mode & S_IFDIR)
                                                                {
                                                                        const auto id = name.AsUint32();

                                                                        min = std::min<uint32_t>(min, id);
                                                                        max = std::max<uint32_t>(max, id);
                                                                        ++partitionsCnt;
                                                                }
                                                        }
                                                }

                                                if (partitionsCnt)
                                                {
                                                        if (min != 0 && max != partitionsCnt - 1)
                                                                throw Switch::system_error("Unexpected partitions list; expected [0, ", partitionsCnt - 1, "]");

                                                        auto t = Switch::make_sharedref(new topic(name, partitionConfig));

                                                        collectLock.lock();
                                                        pendingPartitions.push_back({t.get(), partitionsCnt});
                                                        register_topic(t.release());
                                                        collectLock.unlock();
                                                }
                                        }

                                },
                                                             it));
                        }

                        for (auto &it : futures)
                                it.get();

                        basePath_.SetLength(basePathLen);

                        if (trace)
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for topics\n");

                        if (pendingPartitions.size())
                        {
                                static constexpr bool trace{false};
                                std::vector<std::pair<topic *, Switch::shared_refptr<topic_partition>>> list;

                                if (trace)
                                        SLog("pendingPartitions.size() = ", pendingPartitions.size(), "\n");

                                futures.clear();
                                for (auto &it : pendingPartitions)
                                {
                                        auto t = it.first;

                                        for (uint16_t i{0}; i != it.second; ++i)
                                        {
                                                futures.push_back(std::async([&list, &collectLock, &basePath_ = basePath_, this ](topic * t, const uint16_t partition) {
                                                        char path[PATH_MAX];

                                                        Snprint(path, sizeof(path), basePath_.data(), "/", t->name_, "/", partition, "/");
                                                        auto p = init_local_partition(partition, path, t->partitionConf);

                                                        collectLock.lock();
                                                        list.push_back({t, std::move(p)});
                                                        collectLock.unlock();

                                                },
                                                                             t, i));
                                        }
                                }

                                if (trace)
                                {
                                        SLog("futures.size() = ", futures.size(), "\n");
                                        before = Timings::Microseconds::Tick();
                                }

                                for (auto &it : futures)
                                        it.get();

                                if (trace)
                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for all partitions\n");

                                std::sort(list.begin(), list.end(), [](const auto &a, const auto &b) {
                                        return uintptr_t(a.first) < uintptr_t(b.first);
                                });

                                const auto n = list.size();
                                std::vector<topic_partition *> partitions;

                                if (trace)
                                        before = Timings::Microseconds::Tick();

                                for (uint32_t i{0}; i != n;)
                                {
                                        auto t = list[i].first;

                                        do
                                        {
                                                partitions.push_back(list[i].second.release());
                                        } while (++i != n && list[i].first == t);

                                        t->register_partitions(partitions.data(), partitions.size());
                                        totalPartitions += partitions.size();
                                        partitions.clear();
                                }

                                if (trace)
                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " to initialize all partitions\n");
                        }
                }
                catch (const std::exception &e)
                {
                        Print("Failed to initialize topics and partitions:", e.what(), "\n");
                        return 1;
                }
        }
        else if (opMode == OperationMode::Clustered)
        {
                IMPLEMENT_ME();
        }

        if (topics.empty())
        {
                Print("No topics found in ", basePath_, ". You may want to create a few, like so:\n");
                Print("mkdir -p ", basePath_, "/events/0 ", basePath_, "/orders/0 \n");
                Print("This will create topics events and orders and define one partition with id 0 for each of them. Restart Tank after you have created a few topics/partitions\n");
                return 1;
        }

        Print(ansifmt::bold, "<=TANK=>", ansifmt::reset, " v", TANK_VERSION / 100, ".", TANK_VERSION % 100, " ", dotnotation_repr(topics.size()), " topics registered, ", dotnotation_repr(totalPartitions), " partitions; will listen for new connections at ", listenAddr, "\n");
        Print("(C) Phaistos Networks, S.A. - ", ansifmt::color_green, "http://phaistosnetworks.gr/", ansifmt::reset, ". Licensed under the Apache License\n");

        listenFd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

        if (listenFd == -1)
        {
                Print("socket() failed:", strerror(errno), "\n");
                return 1;
        }

        require(listenFd != -1);

        memset(&sa, 0, sizeof(sa));
        sa.sin_addr.s_addr = listenAddr.addr4;
        sa.sin_port = htons(listenAddr.port);
        sa.sin_family = AF_INET;

        if (Switch::SetReuseAddr(listenFd, 1) == -1)
        {
                Print("SO_REUSEADDR: ", strerror(errno), "\n");
                return 1;
        }
        else if (bind(listenFd, (sockaddr *)&sa, sizeof(sa)))
        {
                Print("bind() failed:", strerror(errno), "\n");
                return 1;
        }
        else if (listen(listenFd, 128))
        {
                Print("listen() failed:", strerror(errno), "\n");
                return 1;
        }

        std::thread([] {
                Switch::vector<std::pair<int, int>> local;

                for (;;)
                {
                        Timings::Seconds::Sleep(1);

                        mboxLock.lock();
                        std::swap(local, mbox);
                        require(mbox.empty());
                        mboxLock.unlock();

                        for (auto &it : local)
                        {
                                fdatasync(it.first);
                                fdatasync(it.second);
                        }

                        local.clear();
                }

        }).detach();

        poller.AddFd(listenFd, POLLIN, &listenFd);

        signal(SIGINT, sig_handler);
        while (likely(running))
        {
                const auto r = poller.Poll(1000);

                if (r == -1)
                {
                        if (errno == EINTR || errno == EAGAIN)
                                continue;

                        RFLog("epoll_wait(): ", strerror(errno), "\n");
                        return 1;
                }

                const auto nowMS = Timings::Milliseconds::Tick();

                for (const auto *it = poller.Events(), *const e = it + r; it != e; ++it)
                {
                        auto *const c = (connection *)it->data.ptr;
                        const auto events = it->events;
                        int fd = c->fd;

                        if (fd == listenFd)
                        {
                                socklen_t saLen = sizeof(sa);
                                int newFd = accept4(fd, (sockaddr *)&sa, &saLen, SOCK_NONBLOCK | SOCK_CLOEXEC);

                                if (newFd == -1)
                                {
                                        if (errno != EINTR && errno != EAGAIN)
                                        {
                                                Print("accept4(): ", strerror(errno), "\n");
                                                return 1;
                                        }
                                }
                                else
                                {
                                        require(saLen == sizeof(sockaddr_in));

                                        auto c = get_connection();

#if 0
                                        {
						// Default (socket.send.buffer and socket.receive.buffer) from Kafka
                                                int rcvBufSize{102400}, sndBufSize{102400};

                                                if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&rcvBufSize, sizeof(rcvBufSize)) == -1)
                                                        Print("WARNING: unable to set socket receive buffer size:", strerror(errno), "\n");
                                                if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&sndBufSize, sizeof(sndBufSize)) == -1)
                                                        Print("WARNING: unable to set socket send buffer size:", strerror(errno), "\n");
                                        }
#endif

                                        c->fd = newFd;
                                        c->replicaId = 0;
                                        switch_dlist_init(&c->connectionsList);
                                        switch_dlist_init(&c->waitCtxList);
                                        switch_dlist_insert_after(&allConnections, &c->connectionsList);

                                        Switch::SetNoDelay(c->fd, 1);

                                        // We are going to ping as soon as we can so that the client will know we have been accepted
                                        // but we can't write(fd, ..) now; so we 'll need to wait for POLLOUT and then ping
                                        c->state.flags = (1u << uint8_t(connection::State::Flags::PendingIntro)) | (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                                        poller.AddFd(c->fd, POLLIN | POLLOUT, c);
                                }

                                continue;
                        }

                        if (events & (POLLHUP | POLLERR))
                        {
                                shutdown(c, __LINE__);
                                continue;
                        }

                        if (events & POLLIN)
                        {
                                auto b = c->inB;
                                int n, r;

                                if (!b)
                                        b = c->inB = get_buffer();

                                if (unlikely(ioctl(fd, FIONREAD, &n) == -1))
                                {
                                        Print("ioctl():", strerror(errno), "\n");
                                        return 1;
                                }
                                b->reserve(n);
                                r = read(fd, b->End(), n);

                                if (r == -1)
                                {
                                        if (errno == EINTR || errno == EAGAIN)
                                                goto l1;
                                        else
                                        {
                                                if (trace)
                                                        SLog("Failed:", strerror(errno), "\n");
                                                shutdown(c, __LINE__);
                                                continue;
                                        }
                                }
                                else if (!r)
                                {
                                        shutdown(c, __LINE__);
                                        continue;
                                }
                                else
                                {
                                        b->AdvanceLength(r);
                                        c->state.lastInputTS = Timings::Milliseconds::Tick();
                                }

                                for (const auto *e = (uint8_t *)b->End(); !(c->state.flags & (1u << uint8_t(connection::State::Flags::Busy)));)
                                {
                                        const auto *p = (uint8_t *)b->AtOffset();

                                        if (e - p >= sizeof(uint8_t) + sizeof(uint32_t))
                                        {
                                                const auto msg = *p++;
                                                const uint32_t msgLen = *(uint32_t *)p;
                                                p += sizeof(uint32_t);

                                                if (unlikely(msgLen > 256 * 1024 * 1024))
                                                {
                                                        Print("** TOO large incoming packet of length ", size_repr(msgLen), "\n");
                                                        shutdown(c, __LINE__);
                                                        goto nextEvent;
                                                }

                                                if (0 == (c->state.flags & (1u << uint8_t(connection::State::Flags::ConsideredReqHeader))))
                                                {
                                                        // So that ingestion of future incoming data will not require buffer reallocations
                                                        const auto o = (char *)p - b->data();

                                                        if (trace)
                                                                SLog("Need to reserve(", msgLen, ")\n");

                                                        b->reserve(msgLen);

                                                        p = (uint8_t *)b->At(o);
                                                        e = (uint8_t *)b->End();

                                                        c->state.flags |= 1u << uint8_t(connection::State::Flags::ConsideredReqHeader);
                                                }

                                                if (p + msgLen > e)
                                                {
                                                        if (trace)
                                                                SLog("Need more data for ", msg, "\n");

                                                        goto l1;
                                                }

                                                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::ConsideredReqHeader));
                                                if (!process_msg(c, msg, reinterpret_cast<const uint8_t *>(p), msgLen))
                                                        goto nextEvent;

                                                p += msgLen;
                                                if (p == e)
                                                {
                                                        b->clear();
                                                        Drequire(b->Offset() == 0);
                                                        c->inB = nullptr;
                                                        put_buffer(b);
                                                        break;
                                                }
                                                else
                                                        b->SetOffset((char *)p);
                                        }
                                        else
                                                break;
                                }

                                if (auto b = c->inB)
                                {
                                        if (b->Offset() > 4 * 1024 * 1024)
                                        {
                                                b->DeleteChunk(0, b->Offset());
                                                b->SetOffset(uint64_t(0));
                                        }
                                }
                        }

                l1:
                        if (events & POLLOUT)
                                try_send(c);

                nextEvent:;
                }

                if (nowMS > nextIdleCheck)
                {
                        nextIdleCheck = nowMS + 800;

                        for (auto it = allConnections.next; it != &allConnections;)
                        {
                                auto next = it->next;
                                auto c = switch_list_entry(connection, connectionsList, it);

                                (void)c;
                                it = next;
                        }
                }

                if (nowMS > nextExpWaitCtxCheck)
                {
                        nextExpWaitCtxCheck = nowMS + 512;

                        for (auto it = waitExpList.prev; it != &waitExpList; it = it->prev)
                        {
                                auto ctx = switch_list_entry(wait_ctx, expList, it);

                                if (nowMS > ctx->expiration)
                                        expiredCtxList.push_back(ctx);
                                else
                                        break;
                        }

                        while (expiredCtxList.size())
                                abort_wait_ctx(expiredCtxList.Pop());
                }
        }

        Print("TANK terminated\n");
        return true;
}

topic *Service::topic_by_name(const strwlen8_t name) const
{
#ifdef LEAN_SWITCH
        auto it = topics.find(name);

        if (it != topics.end())
                return it->second;
#else
        return topics[name];
#endif

        return nullptr;
}

int main(int argc, char *argv[])
{
        return Service{}.start(argc, argv);
}

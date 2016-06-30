#include "service.h"
#include <fs.h>
#include <sys/stat.h>
#include <text.h>
#include <unistd.h>

static constexpr bool trace{false};

ro_segment::ro_segment(const uint64_t absSeqNum, const uint64_t lastAbsSeqNum, const strwlen32_t base)
    : baseSeqNum{absSeqNum}, lastAvailSeqNum{lastAbsSeqNum}
{
        index.data = nullptr;
        int fd = open(Buffer::build(base, "/", absSeqNum, "-", lastAbsSeqNum, ".ilog").data(), O_RDONLY | O_LARGEFILE);

        if (fd == -1)
                throw Switch::system_error("Failed to access log file");

        fdh.reset(new fd_handle(fd));
        Drequire(fdh.use_count() == 2);
        fdh->Release();

        auto size = lseek64(fd, 0, SEEK_END);

        if (unlikely(size == (off64_t)-1))
                throw Switch::system_error("lseek64() failed: ", strerror(errno));

        Drequire(size < std::numeric_limits<std::remove_reference<decltype(fileSize)>::type>::max());
        fileSize = size;

        int indexFd = open(Buffer::build(base, "/", absSeqNum, ".index").data(), O_RDONLY | O_LARGEFILE);

        if (indexFd == -1)
        {
                // TODO: rebuild it
                throw Switch::system_error("Failed to access the index file");
        }

        Defer({ close(indexFd); });

        size = lseek64(indexFd, 0, SEEK_END);

        if (unlikely(size == (off64_t)-1))
                throw Switch::system_error("lseek64() failed: ", strerror(errno));

        Drequire(size < std::numeric_limits<std::remove_reference<decltype(index.fileSize)>::type>::max());

        index.fileSize = size;
        index.lastRecorded.relSeqNum = index.lastRecorded.absPhysical = 0;

        if (size)
        {
                auto data = mmap(nullptr, index.fileSize, PROT_READ, MAP_SHARED, indexFd, 0);

                if (unlikely(data == MAP_FAILED))
                        throw Switch::system_error("Failed to access the index file");

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
static uint32_t search_before_offset(const uint64_t baseSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum, int fd, const uint32_t fileSize, uint32_t fileOffset)
{
        uint8_t buf[sizeof(uint32_t) + sizeof(uint32_t) + 4];
        auto o = fileOffset;
        const auto limit = maxSize != UINT32_MAX ? Min<uint32_t>(fileSize, fileOffset + maxSize) : fileSize;

        if (trace)
                SLog("Searching for offset ", maxAbsSeqNum, ", limit = ", limit, "(maxSize = ", maxSize, ", baseSeqNum = ", baseSeqNum, ", fileOffset = ", fileOffset, ")\n");

        while (o < limit)
        {
                const auto r = pread64(fd, buf, sizeof(buf), o);
                const auto *p = buf;
                const auto abs = baseSeqNum + *(uint32_t *)p;

                (void)r;
                if (trace)
                        SLog("abs = ", abs, "(", *(uint32_t *)p, ") VS ", maxAbsSeqNum, " at ", o, "\n");

                if (abs > maxAbsSeqNum)
                        break;
                else
                {
                        p += sizeof(uint32_t);

                        const auto len = Compression::UnpackUInt32(p);

                        fileOffset = o;
                        o += (p - buf) + len;
                }
        }

        return fileOffset;
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
        const auto *const end = skipList.end();
        const auto it = std::upper_bound_or_match(skipList.begin(), end, relSeqNum, [](const auto &a, const auto seqNum) {
                return TrivialCmp(seqNum, a.first);
        });

        res.fdh = cur.fdh;

        if (it != end)
        {
                if (trace)
                        SLog("Found in skiplist\n");

                res.absBaseSeqNum = cur.baseSeqNum + it->first;
                res.range.Set(it->second, cur.fileSize - it->second);
                lock.unlock_shared();

                inSkiplist = true;
        }
        else
        {
                lock.unlock_shared();

                require(cur.index.ondisk.span); // we checked if it's in this current segment

                if (trace)
                        SLog("Considering ondisk index\n");

                const auto size = cur.index.ondisk.span;
                const auto *const all = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                const auto *const e = all + size / sizeof(index_record);
                const auto i = std::upper_bound_or_match(all, e, relSeqNum, [](const auto &a, const auto seqNum) {
                        return TrivialCmp(seqNum, a.relSeqNum);
                });

                if (i != e)
                {
                        res.absBaseSeqNum = cur.baseSeqNum + i->relSeqNum;
                        res.range.Set(i->absPhysical, cur.fileSize - i->absPhysical);
                }
                else
                {
                        res.absBaseSeqNum = cur.baseSeqNum;
                        res.range.Set(0, cur.fileSize);
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

                res.range.SetEnd(search_before_offset(cur.baseSeqNum, maxSize, maxAbsSeqNum, cur.fdh->fd, cur.fileSize, ref.absPhysical));
        }

        if (res.range.len > maxSize)
                res.range.len = maxSize;

        res.highWatermark = highWatermark;
        return res;
}

lookup_res topic_partition_log::range_for(uint64_t absSeqNum, const uint32_t maxSize, const uint64_t maxAbsSeqNum)
{
        if (trace)
                SLog("Read for absSeqNum = ", absSeqNum, ", lastAssignedSeqNum = ", lastAssignedSeqNum, ", firstAvailableSeqNum = ", firstAvailableSeqNum, "\n");

        lock.lock_shared();

        // UINT64_MAX is reserved: fetch only newly produced bundles
        // see process_consume()
        // UINT64_MAX -1 is also reserved; start from the first available sequence
        if (absSeqNum == UINT64_MAX - 1)
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
                lock.unlock_shared();

                if (trace)
                        SLog("maxSize == 0\n");
                return {lookup_res::Fault::Empty, highWatermark};
        }
        else if (maxAbsSeqNum < absSeqNum)
        {
                // Should be handled by the client
                lock.unlock_shared();

                if (trace)
                        SLog("Past maxAbsSeqNum = ", maxAbsSeqNum, "\n");
                return {lookup_res::Fault::Empty, highWatermark};
        }
        else if (absSeqNum == lastAssignedSeqNum + 1)
        {
                // return empty
                // UPDATE: let's wait instead
                lock.unlock_shared();

                if (trace)
                        SLog("Empty\n");
                return {lookup_res::Fault::AtEOF, highWatermark};
        }
        else if (absSeqNum > lastAssignedSeqNum)
        {
                // past last assigned sequence number? nope
                // throw an error
                lock.unlock_shared();

                if (trace)
                        SLog("PAST absSeqNum(", absSeqNum, ") > lastAssignedSeqNum(", lastAssignedSeqNum, ")\n");
                return {lookup_res::Fault::BoundaryCheck, highWatermark};
        }
        else if (absSeqNum < firstAvailableSeqNum)
        {
                lock.unlock_shared();

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

        lock.unlock_shared();

	// TODO: https://github.com/phaistos-networks/TANK/issues/2
        const auto end = prevSegments->end();
        const auto it = std::upper_bound_or_match(prevSegments->begin(), end, absSeqNum, [](const auto s, const auto absSeqNum) {
                return TrivialCmp(absSeqNum, s->baseSeqNum);
        });

        if (it != end)
        {
                const auto f = *it;
                const auto res = f->translateDown(absSeqNum, UINT32_MAX);
                range32_t range(res.record.absPhysical, res.span);

                if (trace)
                        SLog("Found in RO segment\n");

                if (maxAbsSeqNum != UINT64_MAX)
                {
                        // Scan forward for the last message with absSeqNum < maxAbsSeqNum
                        const auto r = f->snapDown(maxAbsSeqNum);

                        if (trace)
                                SLog("maxAbsSeqNum = ", maxAbsSeqNum, " => ", r, "\n");

                        range.SetEnd(search_before_offset(f->baseSeqNum, maxSize, maxAbsSeqNum, f->fdh->fd, f->fileSize, r.second));
                }

                if (range.len > maxSize)
                        range.len = maxSize;

                return {f->fdh, f->baseSeqNum + res.record.relSeqNum, range, highWatermark};
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
                return {f->fdh, f->baseSeqNum, {0, Min<uint32_t>(maxSize, f->fileSize)}, highWatermark};
        }
        else
        {
                if (trace)
                        SLog("Will use current segment\n");
                return {cur.fdh, cur.baseSeqNum, {0, Min<uint32_t>(maxSize, cur.fileSize)}, highWatermark};
        }
}

void topic_partition_log::consider_ro_segments()
{
        size_t sum{0};

        for (auto it : *roSegments)
                sum += it->fileSize;

        if (trace)
                SLog(ansifmt::bold, "Considering segments sum=", sum, ", total = ", roSegments->size(), " limits { roSegmentsCnt ", limits.roSegmentsCnt, ", roSegmentsSize ", limits.roSegmentsSize, "}", ansifmt::reset, "\n");

        while (((limits.roSegmentsCnt && roSegments->size() > limits.roSegmentsCnt) || (limits.roSegmentsSize && sum > limits.roSegmentsSize)) && roSegments->size())
        {
                auto segment = roSegments->front();
                const auto basePathLen = basePath.length();

                if (trace)
                        SLog(ansifmt::color_red, "Removing ", segment->baseSeqNum, ansifmt::reset, "\n");

                basePath.append("/", segment->baseSeqNum, "-", segment->lastAvailSeqNum, ".ilog");
                unlink(basePath.data());
                basePath.SetLength(basePathLen);
                basePath.append("/", segment->baseSeqNum, ".index");
                unlink(basePath.data());
                basePath.SetLength(basePathLen);

                segment->fdh.reset(nullptr);

                sum -= segment->fileSize;
                roSegments->pop_front();
        }

        firstAvailableSeqNum = roSegments->front()->baseSeqNum;
        if (trace)
                SLog("firstAvailableSeqNum now = ", firstAvailableSeqNum, "\n");
}

append_res topic_partition_log::append_bundle(const void *bundle, const size_t bundleSize, const uint32_t bundleMsgsCnt)
{
        lock.lock();

	const auto savedLastAssignedSeqNum = lastAssignedSeqNum;
        const auto absSeqNum = lastAssignedSeqNum + 1;

        lastAssignedSeqNum += bundleMsgsCnt;

        if (cur.fileSize > limits.maxSegmentSize || cur.index.skipList.size() > 65536)
        {
                const auto basePathLen = basePath.length();

                if (trace)
                        SLog("Need to switch to another commit log (", cur.fileSize, "> ", limits.maxSegmentSize, ")\n");

                if (cur.fileSize != UINT32_MAX)
                {
                        auto newROFiles = std::make_unique<Switch::vector<ro_segment *>>();
                        auto newROFile = std::make_unique<ro_segment>(cur.baseSeqNum, savedLastAssignedSeqNum);

                        const auto n = cur.fdh.use_count();

                        require(n >= 1);
                        newROFile->fdh = cur.fdh;
                        require(cur.fdh.use_count() == n + 1);
                        newROFile->fileSize = cur.fileSize;

                        newROFile->index.fileSize = lseek64(cur.index.fd, 0, SEEK_END);
                        newROFile->index.lastRecorded.relSeqNum = newROFile->index.lastRecorded.absPhysical = 0;

			// We now encode the [first,last] range into the filename for simplicity and future-proofing; we 'd like to
			// support sparse sequence numbers space
			if (rename(Buffer::build(basePath, "/", cur.baseSeqNum, ".log").data(), 
				Buffer::build(basePath, "/", cur.baseSeqNum, "-", savedLastAssignedSeqNum, ".ilog").data()) == -1)
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

                cur.index.skipList.clear();
                close(cur.index.fd);

                if (cur.index.ondisk.data != nullptr && cur.index.ondisk.data != MAP_FAILED)
                {
                        munmap((void *)cur.index.ondisk.data, cur.index.ondisk.span);
                        cur.index.ondisk.data = nullptr;
                }

                basePath.append(cur.baseSeqNum, ".log");

                cur.fdh.reset(new fd_handle(open(basePath.data(), O_RDWR | O_LARGEFILE | O_CREAT, 0775)));
                require(cur.fdh->use_count() == 2);
                cur.fdh->Release();

                if (cur.fdh->fd == -1)
                        throw Switch::system_error("open() failed:", strerror(errno));

                basePath.SetLength(basePathLen);
                basePath.append(cur.baseSeqNum, ".index");
                cur.index.fd = open(basePath.data(), O_RDWR | O_LARGEFILE | O_CREAT, 0775);
                basePath.SetLength(basePathLen);

                if (cur.index.fd == -1)
                        throw Switch::system_error("open() failed:", strerror(errno));

                if (trace)
                        SLog("Switched\n");
        }

        if (cur.sinceLastUpdate > limits.indexInterval)
        {
                const uint32_t out[] = {uint32_t(absSeqNum - cur.baseSeqNum), cur.fileSize};

                cur.index.skipList.push_back({out[0], out[1]});

                if (trace)
                        SLog(">> ", out[0], ", ", out[1], "\n");

                if (unlikely(write(cur.index.fd, out, sizeof(out)) != sizeof(out)))
                {
                        lock.unlock();
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

        require(cur.fdh.use_count() == before + 1);
        if (writev(fd, iov, sizeof_array(iov)) != entryLen)
        {
                lock.unlock();
                return {nullptr, {}, {}};
        }
        else
        {
                cur.fileSize += entryLen;
                cur.sinceLastUpdate += entryLen;

                lock.unlock();
                return {fdh, fileRange, {absSeqNum, uint16_t(bundleMsgsCnt)}};
        }
}

void topic_partition::consider_append_res(append_res &res, Switch::vector<wait_ctx *> &waitCtxWorkL)
{
        bool newLogFile{false};

        if (trace)
                SLog(ansifmt::color_blue, " waitingList.size() = ", waitingList.size(), ansifmt::reset, "\n");

        waitingListLock.lock();
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
        waitingListLock.unlock();
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
                if (trace)
                        SLog("Failed:", e.what(), "\n");

                return {nullptr, {}, {}};
        }
}

lookup_res topic_partition::read_from_local(const bool fetchOnlyFromLeader, const bool fetchOnlyComittted, const uint64_t absSeqNum, const uint32_t fetchSize)
{
	// TODO:
	// For a multi-node configurations, maxAbsSeqNum should be set to the last committed absolute sequence number (i.e highwater mark)
	// range_for() will make sure it won't return any data for messages with id >= maxAbsSeqNum
	//
	// search_before_offset() will need to access the segment log file though, and that may not be optimal; instead
	// we may just want to rely on the client stopping processing chunk bundles if it reaches message id > maxAbsSeqNum(provided 
	// in the fetch response), which it already takes into account
	// reaches messages id >= maxAbsSeqNum
        const uint64_t maxAbsSeqNum{UINT64_MAX};

        if (trace)
                SLog("Fetching log segment for partition ", idx, ", abs.sequence number ", absSeqNum, ", fetchSize ", fetchSize, ", maxAbsSeqNum = ", maxAbsSeqNum, "\n");

        return std::move(log_->range_for(absSeqNum, fetchSize, maxAbsSeqNum));
}

Switch::shared_refptr<topic_partition> Service::init_local_partition(const uint16_t idx, const char *const bp)
{
        char basePath[PATH_MAX];
        size_t basePathLen = strlen(bp);

        require(basePathLen < PATH_MAX - 2);
        memcpy(basePath, bp, basePathLen);
        basePath[basePathLen] = '/';
        basePath[++basePathLen] = '\0';

        if (trace)
                SLog("Initializing partition ", bp, "\n");

        if (!SwitchFS::BuildPath(basePath))
                throw Switch::system_error("Failed to create or access directory ", basePath);

        try
        {
                auto partition = Switch::make_sharedref(new topic_partition());
		Switch::vector<std::pair<uint64_t, uint64_t>> roLogs;
		uint64_t curLogSeqNum{0};
                const strwlen32_t b(basePath, basePathLen);
                int fd;
                auto l = new topic_partition_log();

                partition->distinctId = _atomic_inc_relaxed(&nextDistinctPartitionId);

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
                                int fd = open(Buffer::build(basePath, "/", name).data(), O_RDONLY | O_LARGEFILE);

                                if (fd == -1)
                                        throw Switch::system_error("Failed to access topic/partition config file:", strerror(errno));
                                else if (const auto fileSize = lseek64(fd, 0, SEEK_END))
                                {
                                        auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                                        close(fd);
                                        if (fileData == MAP_FAILED)
                                                throw Switch::system_error("Failed to access topic/partition config file:", strerror(errno));

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

                                                if (k && v)
                                                {
                                                        if (k.EqNoCase(_S("limits.segments.count")))
                                                        {
                                                                l->limits.roSegmentsCnt = v.AsUint32();
                                                                if (l->limits.roSegmentsCnt < 2 && l->limits.roSegmentsCnt)
                                                                        throw Switch::range_error("Invalid limits.segments.count");
                                                        }
                                                        else if (k.EqNoCase(_S("limits.segments.size")))
                                                        {
                                                                l->limits.roSegmentsSize = v.AsUint32();
                                                                if (l->limits.roSegmentsSize < 128 && l->limits.roSegmentsSize)
                                                                        throw Switch::range_error("Invalid limits.segments.size");
                                                        }
                                                        else if (k.EqNoCase(_S("limits.segment.size")))
                                                        {
                                                                l->limits.maxSegmentSize = v.AsUint32();
                                                                if (l->limits.maxSegmentSize < 128)
                                                                        throw Switch::range_error("Invalid limits.segment.size");
                                                        }
                                                        else if (k.EqNoCase(_S("index.interval")))
                                                        {
                                                                l->limits.indexInterval = v.AsUint32();
                                                                if (l->limits.indexInterval < 128)
                                                                        throw Switch::range_error("Invalid index.interval");
                                                        }
                                                        else
                                                                Print("Unknown topic/partition configuration key '", k, "'\n");
                                                }
                                        }
                                }
                                else
                                        close(fd);

                                continue;
                        }

                        const auto r = name.Divided('.');

                        if (r.second.Eq(_S("index")))
                        {
                                // accept
                        }
                        else if (r.second.Eq(_S("swap")))
                        {
				// TODO:
                        }
			else if (r.second.Eq(_S("ilog")))
			{
				// Immutable log
				const auto repr = r.first.Divided('-');
				const auto baseSeqNum = repr.first.AsUint64(), lastAvailSeqNum = repr.second.AsUint64();

				roLogs.push_back({baseSeqNum, lastAvailSeqNum});
			}
                        else if (r.second.Eq(_S("log")))
                        {
				// current, mutable log(segment)
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
                        else
                                Print("Unexpected name ", name, "\n");
                }

		if (trace)
			SLog("roLogs.size() = ", roLogs.size(), ", curLogSeqNum = ", curLogSeqNum, "\n");

		if (roLogs.size())
		{
                        std::sort(roLogs.begin(), roLogs.end(), [](const auto &a, const auto &b) {
                                return a.first < b.first;
                        });

			l->firstAvailableSeqNum = roLogs.front().first;
			l->roSegments.reset(new Switch::vector<ro_segment *>());

			for (const auto &it : roLogs)
			{
				if (trace)
					SLog("Initializing [", it.first, ",", it.second, "]\n");

				l->roSegments->push_back(new ro_segment(it.first, it.second, b));
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
                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".log");
                        fd = open(basePath, O_RDWR | O_LARGEFILE | O_CREAT, 0775);

                        if (fd == -1)
                                throw Switch::system_error("open() failed:", strerror(errno));

                        l->cur.fdh.reset(new fd_handle(fd));
                        require(l->cur.fdh->use_count() == 2);
                        l->cur.fdh->Release();
                        l->cur.baseSeqNum = curLogSeqNum;
                        l->cur.fileSize = lseek64(fd, 0, SEEK_END);

                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".index");
                        fd = open(basePath, O_RDWR | O_LARGEFILE | O_CREAT, 0775);

                        if (fd == -1)
                                throw Switch::system_error("open() failed:", strerror(errno));

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
                                        throw Switch::system_error("open() failed:", strerror(errno));

                                const auto *const p = (uint32_t *)(l->cur.index.ondisk.data + l->cur.index.ondisk.span - sizeof(uint32_t) - sizeof(uint32_t));

                                l->cur.index.ondisk.lastRecorded.relSeqNum = p[0];
                                l->cur.index.ondisk.lastRecorded.absPhysical = p[1];

                                if (trace)
                                        SLog("Have cur.index.ondisk.span = ", l->cur.index.ondisk.span, " lastRecorded =  ( relSeqNum = ", l->cur.index.ondisk.lastRecorded.relSeqNum, ", absPhysical = ", l->cur.index.ondisk.lastRecorded.absPhysical, ")\n");
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

                                if (pread64(fd, data, span, o) != span)
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
                                }

                                l->lastAssignedSeqNum = next - 1;

                                if (trace)
                                        SLog("Set lastAssignedSeqNum = ", l->lastAssignedSeqNum, "\n");
                        }

                        // Just in case
                        lseek64(l->cur.index.fd, 0, SEEK_END);
                        lseek64(l->cur.fdh->fd, 0, SEEK_END);
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
                const auto minBytes = *(uint32_t *)p;
                p += sizeof(uint32_t);
                const auto topicsCnt = *p++;

                (void)clientVersion;
                if (trace)
                        SLog("topicsCnt = ", topicsCnt, "\n");

                respHeader->Serialize(uint8_t(2));
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
                        auto topic = topics[topicName];

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

                                        switch (res.fault)
                                        {
                                                case lookup_res::Fault::NoFault:
                                                        respHeader->Serialize(uint8_t(0));        // error
                                                        respHeader->Serialize(res.absBaseSeqNum); // absolute first seq.num of the first message of the first bundle in the streamed chunk
                                                        respHeader->Serialize(hwMark);
                                                        respHeader->Serialize(res.range.len);

                                                        sum += res.range.len;
                                                        q->push_back({res.fdh.get(), res.range});

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

                p->waitingListLock.lock();
                p->waitingList.push_back(ctx);
                p->waitingListLock.unlock();

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

bool Service::process_publish(connection *const c, const uint8_t *p, const size_t len)
{
        auto respHeader = get_buffer();
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

        respHeader->Serialize(uint8_t(1));
        const auto sizeOffset = respHeader->length();
        respHeader->MakeSpace(sizeof(uint32_t));

        (void)clientVersion;
        (void)requiredAcks;
        (void)ackTimeout;

        if (!q)
                q = c->outQ = get_outgoing_queue();
        q->push_back(respHeader);

        respHeader->Serialize(requestId);

        if (trace)
                SLog("Parsing ", topicsCnt, "\n");
        for (uint32_t i{0}; i != topicsCnt; ++i)
        {
                topicName.Set((char *)p + 1, *p);
                p += sizeof(uint8_t) + topicName.len;

                auto topic = topics[topicName];

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
                                for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e;)
                                {
                                        // Next message set message
                                        const auto flags = *p++;
                                        const auto msgTs = *(uint64_t *)p;
                                        p += sizeof(uint64_t);

					if (flags&0x1)
					{
						key.Set((char *)p + 1, *p);
						p+=key.len + sizeof(uint8_t);
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

                        respHeader->Serialize(uint8_t(0));

                        while (expiredCtxList.size())
                        {
                                auto ctx = expiredCtxList.Pop();

                                wakeup_wait_ctx(ctx, res);
                        }
                }
        }
        *(uint32_t *)respHeader->At(sizeOffset) = respHeader->length() - sizeOffset - sizeof(uint32_t);

        return try_send_ifnot_blocked(c);
}

bool Service::process_msg(connection *const c, const uint8_t msg, const uint8_t *const data, const size_t len)
{
        if (trace)
                SLog("New message  type ", msg, ", len ", len, "\n");

        switch (msg)
        {
                case 1:
                        return process_publish(c, data, len);

                case 2:
                        return process_consume(c, data, len);

                case 3: // ping
                        return true;
                        break;

                case 4:
                        return process_replica_reg(c, data, len);

                default:
                        return shutdown(c, __LINE__);
        }
}

void Service::wakeup_wait_ctx(wait_ctx *const wctx, const append_res &appendRes)
{
        auto respHeader = get_buffer();
        uint8_t topicsCnt{0};
        auto c = wctx->c;
        auto q = c->outQ;

        if (!q)
                q = c->outQ = get_outgoing_queue();

        if (trace)
                SLog(ansifmt::bold, ansifmt::color_green, "Waking up wait ctx ", ptr_repr(wctx), ", ", wctx->requestId, ansifmt::reset, "\n");

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
                                        SLog("HAVE data for ", topicName, ".", p->idx, ", seqNum = ", it.seqNum, ", range ", it.range, "\n");

                                respHeader->Serialize(it.seqNum);
                                respHeader->Serialize(p->highwater_mark());
                                respHeader->Serialize(it.range.len);

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
        *(uint32_t *)respHeader->At(sizeOffset) = respHeader->length() - sizeOffset - sizeof(uint32_t);
        *(uint32_t *)respHeader->At(headerSizeOffset) = respHeader->length() - headerSizeOffset - sizeof(uint32_t);

        destroy_wait_ctx(wctx);
        try_send_ifnot_blocked(c);
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

                p->waitingListLock.lock();
                p->waitingList.RemoveByValue(wctx);
                p->waitingListLock.unlock();
        }

        if (switch_dlist_any(&wctx->expList))
                switch_dlist_del(&wctx->expList);

        put_waitctx(wctx);
}

bool Service::shutdown(connection *const c, const uint32_t ref)
{
        if (trace)
                SLog("SHUTDOWN at ", ref, "\n");

        require(c->fd != -1);
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

                b[0] = 3;                               // msg = ping
                *(uint32_t *)(b + sizeof(uint8_t)) = 0; // no payload

                if (trace)
                        SLog("PINGING\n");

                if (q && !q->empty())
                {
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
                                                if (haveCork)
                                                        Switch::SetTCPCork(fd, 0);

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
                        }
                }
                else
                {
                        for (;;)
                        {
                                auto &range = it.file_range.range;
                                off_t offset = range.offset;
                                auto r = sendfile(fd, it.file_range.fdh->fd, &offset, range.len);

                                if (trace)
                                        SLog("Sending contents ", range, "\n");

                                if (r == -1)
                                {
                                        if (errno == EINTR)
                                                continue;
                                        else if (errno == EAGAIN)
                                        {
                                                if (haveCork)
                                                        Switch::SetTCPCork(fd, 0);

                                                if (!(c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail))))
                                                {
                                                        c->state.flags |= (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                                                        poller.SetDataAndEvents(fd, c, POLLIN | POLLOUT);
                                                }
                                        }
                                        else
                                                return shutdown(c, __LINE__);
                                }

                                range.len -= offset - range.offset;

                                if (!range)
                                {
                                        it.file_range.fdh->Release();
                                        q->pop_front();
                                        break;
                                }
                        }
                }
        }

        put_outgoing_queue(q);
        c->outQ = nullptr;

        if (haveCork)
        {
                if (trace)
                        SLog("Releasing cork\n");
                Switch::SetTCPCork(fd, 0);
        }

        if (c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)))
        {
                c->state.flags &= ~(1u << uint8_t(connection::State::Flags::NeedOutAvail));
                poller.SetDataAndEvents(fd, c, POLLIN);
        }

        return true;
}

int Service::start(int argc, char **argv)
{
        sockaddr_in sa;
        uint64_t nextIdleCheck{0}, nextExpWaitCtxCheck{0};
        int r;
        struct stat64 st;
        Switch::endpoint listenAddr;

        while ((r = getopt(argc, argv, "b:l:")) != -1)
        {
                switch (r)
                {
                        case 'b':
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

                        default:
                                return 1;
                }
        }

        if (!listenAddr)
        {
                Print("Listen address not specified\n");
                return 1;
        }

        if (!basePath_.length())
        {
                Print("Base path not specified\n");
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
        else
        {
                size_t totalPartitions{0};

                try
                {
                        const auto basePathLen = basePath_.length();

                        for (const auto &&name : DirectoryEntries(basePath_.data()))
                        {
                                if (*name.p == '.')
                                        continue;

                                basePath_.SetLength(basePathLen);
                                basePath_.append("/", name);
                                if (stat64(basePath_.data(), &st) == -1)
                                        throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
                                else if (st.st_mode & S_IFDIR)
                                {
                                        const auto topicBasePathLen = basePath_.length();
                                        uint32_t partitionsCnt{0}, sum{0};

                                        for (const auto &&name : DirectoryEntries(basePath_.data()))
                                        {
                                                if (name.IsDigits())
                                                {
                                                        if (stat64(basePath_.data(), &st) == -1)
                                                                throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
                                                        else if (st.st_mode & S_IFDIR)
                                                        {
                                                                sum += name.AsUint32();
                                                                ++partitionsCnt;
                                                        }
                                                }
                                        }

                                        if (partitionsCnt)
                                        {
                                                if (sum != partitionsCnt - 1)
                                                        throw Switch::system_error("Unexpected partitions list; expected [0, ", partitionsCnt - 1, "]");

                                                auto t = Switch::make_sharedref(new topic(name));

                                                for (uint16_t i{0}; i != partitionsCnt; ++i)
                                                {
                                                        basePath_.append("/", i);
                                                        auto partition = init_local_partition(i, basePath_.data());

                                                        t->register_partition(partition.release());
                                                        basePath_.SetLength(topicBasePathLen);
                                                        ++totalPartitions;
                                                }

                                                register_topic(t.release());
                                        }
                                }
                        }
                        basePath_.SetLength(basePathLen);
                }
                catch (const std::exception &e)
                {
                        Print("Failed to initialize topics and partitions:", e.what(), "\n");
                        return 1;
                }

                Print(ansifmt::bold, "<=TANK=>", ansifmt::reset, " ", dotnotation_repr(topics.size()), " topics registered, ", dotnotation_repr(totalPartitions), " partitions; will listen for new connections at ", listenAddr, "\n");
        }

        if (topics.empty())
        {
                Print("No topics registered\n");
                return 1;
        }

        listenFd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

        require(listenFd != -1);

        memset(&sa, 0, sizeof(sa));
        sa.sin_addr.s_addr = listenAddr.addr4;
        sa.sin_port = htons(listenAddr.port);
        sa.sin_family = AF_INET;

        Switch::SetReuseAddr(listenFd, 1);
        if (bind(listenFd, (sockaddr *)&sa, sizeof(sa)))
        {
                Print("bind() failed:", strerror(errno), "\n");
                return 1;
        }

        if (listen(listenFd, 128))
        {
                Print("listen() failed:", strerror(errno), "\n");
                return 1;
        }

        poller.AddFd(listenFd, POLLIN, &listenFd);

        for (;;)
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

                                require(saLen == sizeof(sockaddr_in));

                                auto c = get_connection();

                                c->fd = newFd;
                                c->replicaId = 0;
                                switch_dlist_init(&c->connectionsList);
                                switch_dlist_init(&c->waitCtxList);
                                switch_dlist_insert_after(&allConnections, &c->connectionsList);

                                // We are going to ping as soon as we can so that the client will know we have been accepted
                                // but we can't write(fd, ..) now; so we 'll need to wait for POLLOUT and then ping
                                c->state.flags = (1u << uint8_t(connection::State::Flags::PendingIntro)) | (1u << uint8_t(connection::State::Flags::NeedOutAvail));
                                poller.AddFd(c->fd, POLLIN | POLLOUT, c);
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

                                ioctl(fd, FIONREAD, &n);
                                b->EnsureCapacity(n);
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

                                for (const auto *const e = b->End(); !(c->state.flags & (1u << uint8_t(connection::State::Flags::Busy)));)
                                {
                                        const auto *p = b->AtOffset();

                                        if (e - p >= sizeof(uint8_t) + sizeof(uint32_t))
                                        {
                                                const auto msg = *p++;
                                                const uint32_t msgLen = *(uint32_t *)p;
                                                p += sizeof(uint32_t);

                                                if (unlikely(msgLen > 128 * 1024 * 1024))
                                                        return shutdown(c, __LINE__);
                                                else if (p + msgLen > e)
                                                        goto l1;
                                                else if (!process_msg(c, msg, reinterpret_cast<const uint8_t *>(p), msgLen))
                                                        goto nextEvent;

                                                p += msgLen;

                                                if (p == e)
                                                {
                                                        b->clear();
                                                        c->inB = nullptr;
                                                        put_buffer(b);
                                                        break;
                                                }
                                                else
                                                {
                                                        b->SetOffset(p);

                                                        if (b->Offset() > 4 * 1024)
                                                        {
                                                                b->DeleteChunk(0, b->Offset());
                                                                b->SetOffset(uint32_t(0));
                                                        }
                                                }
                                        }
                                        else
                                                break;
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
}

int main(int argc, char *argv[])
{
        return Service{}.start(argc, argv);
}

#include "service_common.h"

int Rename(const char *oldpath, const char *newpath);
int Unlink(const char *pathname);

bool topic_partition_log::should_roll(const uint32_t now) const {
        enum {
                trace     = false,
                trace_yes = false,
        };

        cur.sanity_checks();

        if (cur.fileSize == UINT32_MAX) {
                return true;
        }

        if (0 == cur.fileSize) {
                return false;
        }

        TANK_EXPECT(this_service);

        if (now < this_service->no_roll_until) {
                // if we run out of FDs, we don't want to keep trying in every produce request
                return false;
        }

        if (trace) {
                SLog(ansifmt::color_green, " Consider roll:cur.fileSize(", cur.fileSize,
                     "), config.maxSegmentSize(", config.maxSegmentSize,
                     "), skipList.size(", cur.index.skipList.size(),
                     "), config.curSegmentMaxAge (", config.curSegmentMaxAge,
                     "), ", Timings::Seconds::SysTime() - cur.createdTS,
                     " old,  cur.rollJitterSecs = ", cur.rollJitterSecs, ansifmt::reset, "\n");
        }

#if 1
        const std::size_t max_seg_size_ceil = config.maxSegmentSize;
#else
        const std::size_t max_seg_size_ceil = 10;
#endif

        if (cur.fileSize > max_seg_size_ceil) [[unlikely]] {
                if (trace == true || trace_yes == true) {
                        SLog(ansifmt::bold, "Should roll: cur.fileSize(", cur.fileSize, ") > config.maxSegmentSize(", config.maxSegmentSize, ")", ansifmt::reset, "\n");
                }

                return true;
        }

        const size_t curIndexSizeBytes = cur.index.ondisk.span + (cur.index.skipList.size() * (sizeof(uint32_t) + sizeof(uint32_t)));

        if (curIndexSizeBytes > config.maxIndexSize) [[unlikely]] {
                // index is full
                if (trace == true || trace_yes == true) {
                        SLog(ansifmt::bold, "curIndexSizeBytes(", curIndexSizeBytes, ") > config.maxIndexSize(", config.maxIndexSize, ")", ansifmt::reset, "\n");
                }

                return true;
        }

        cur.sanity_checks();

        if (const auto v = cur.rollJitterSecs) {
                // XXX: it is important to use time32_delta() to guard against the race that stems from _now updates semantics
                if (const auto since_seconds = TANKUtil::time32_delta(cur.createdTS, now); since_seconds > cur.rollJitterSecs) {
                        // Soft limit
                        if (trace == true or trace_yes == true) {
                                SLog(ansifmt::bold, "now - cur.createdTS(",
                                     since_seconds, "s ago) > (", v - cur.rollJitterSecs,
                                     ") cur.rollJitterSecs = ", cur.rollJitterSecs,
                                     ",  cur.createdTS = ", cur.createdTS,
                                     "(", Date::ts_repr(cur.createdTS),
                                     "), now = ", now,
                                     " (", Date::ts_repr(now), ")", ansifmt::reset, "\n");
                        }

                        return true;
                }
        }

        return false;
}

bool topic_partition_log::may_switch_index_wide(const uint64_t lastMsgSeqNum) {
        // This is required for proper support for sparse segments
        // maybe instead of transforming the index we should instead roll?
        static constexpr bool trace{false};

        if (trace) {
                SLog("considering switch to wide index, lastMsgSeqNum = ", lastMsgSeqNum, ", cur.baseSeqNum = ", cur.baseSeqNum, "\n");
        }

        if (unlikely(lastMsgSeqNum - cur.baseSeqNum > INT32_MAX)) {
                // arbitrary
                IMPLEMENT_ME();
                cur.index.haveWideEntries = true;
        }

        return false;
}

void topic_partition_log::roll(const uint64_t absSeqNum, const uint64_t saved_last_assigned_seqnum) {
        static constexpr bool trace{false};
        Buffer                basePath;
        int                   fd;

        basePath.append(basePath_, "/", partition->owner->name(), "/", partition->idx, "/");

        const auto basePathLen = basePath.size();
        const auto _now        = time(nullptr);

        if (_now == ((time_t)-1)) {
                // Yes, this _can_ fail
                Print("Failed ", strerror(errno), "\n");
                std::abort();
        }

        if (trace) {
                SLog("Need to switch to another commit log (cur.fileSize = ", cur.fileSize,
                     "> config.maxSegmentSize = ", config.maxSegmentSize,
                     ") cur.index.skipList.size = ", cur.index.skipList.size(),
                     ", lastAssignedSeqNum = ", lastAssignedSeqNum,
                     " basePath = ", basePath, "\n");
        }

        cur.sanity_checks();

        if (cur.fileSize != UINT32_MAX) {
                // See ro_segment::createdTS declaration comments
                struct stat st;

                if (fstat(cur.fdh->fd, &st) == -1) {
                        if (errno == ENFILE || errno == EMFILE || errno == ENOSPC || errno == EDQUOT) {
                                TANK_EXPECT(this_service);
                                this_service->no_roll_until = time(nullptr) + 60;
                        }

                        throw Switch::system_error("fstat() failed:", strerror(errno));
                }

                const uint64_t freezeTs   = st.st_mtime;
                auto           newROFiles = std::make_unique<std::vector<ro_segment *>>();
                auto           newROFile  = std::make_unique<ro_segment>(cur.baseSeqNum, saved_last_assigned_seqnum, freezeTs);
                const auto     n          = cur.fdh.use_count();

                TANK_EXPECT(n >= 1);

                newROFile->fdh = cur.fdh;
                TANK_EXPECT(cur.fdh.use_count() == n + 1);
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
                                   Buffer::build(basePath, "/", cur.baseSeqNum, "-", saved_last_assigned_seqnum, "_", freezeTs, ".ilog").data()) == -1) {
                                throw Switch::system_error("Failed to Rename():", strerror(errno));
                        }
                } else if (Rename(Buffer::build(basePath, "/", cur.baseSeqNum, ".log").data(),
                                  Buffer::build(basePath, "/", cur.baseSeqNum, "-", saved_last_assigned_seqnum, "_", freezeTs, ".ilog").data()) == -1) {
                        throw Switch::system_error("Failed to Rename():", strerror(errno));
                }

                if (newROFile->index.fileSize) {
                        newROFile->index.data = static_cast<uint8_t *>(mmap(nullptr, newROFile->index.fileSize, PROT_READ, MAP_SHARED, cur.index.fd, 0));

                        if (unlikely(newROFile->index.data == MAP_FAILED)) {
                                throw Switch::system_error("mmap() failed:", strerror(errno));
                        }

                        madvise((void *)newROFile->index.data, newROFile->index.fileSize, MADV_DONTDUMP);

                        if (newROFile->index.fileSize >= sizeof(uint32_t) + sizeof(uint32_t)) {
                                const auto *const p = (uint32_t *)(newROFile->index.data + newROFile->index.fileSize - sizeof(uint32_t) - sizeof(uint32_t));

                                newROFile->index.lastRecorded.relSeqNum   = p[0];
                                newROFile->index.lastRecorded.absPhysical = p[1];

                                if (trace) {
                                        SLog("set lastRecorded to ", newROFile->index.lastRecorded.relSeqNum, ", ", newROFile->index.lastRecorded.absPhysical, "\n");
                                }
                        }
                } else {
                        newROFile->index.data = nullptr;
                }

                const auto prevSize = newROFiles->size();

                newROFiles->insert(newROFiles->end(), roSegments->begin(), roSegments->end());
                TANK_EXPECT(newROFiles->size() == prevSize + roSegments->size());
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
        cur.sinceLastUpdate = UINT32_MAX;

        cur.createdTS             = time(nullptr); // will not assign to _now, to reduce likelihood of race conditions
        cur.nameEncodesTS         = true;
        cur.index.haveWideEntries = false;

        cur.index.skipList.clear();
        if (cur.index.fd != -1) {
                fsync(cur.index.fd);
                TANKUtil::safe_close(cur.index.fd);
                cur.index.fd = -1;
        }
        cur.reset_cache();

        if (cur.index.ondisk.data != nullptr && cur.index.ondisk.data != MAP_FAILED) {
                auto ptr = reinterpret_cast<void *>(const_cast<uint8_t *>(cur.index.ondisk.data));

                madvise(ptr, cur.index.ondisk.span, MADV_DONTNEED);
                munmap(ptr, cur.index.ondisk.span);
                cur.index.ondisk.data = nullptr;
        }

        [[maybe_unused]] const auto saved = cur.createdTS;

        basePath.append(cur.baseSeqNum, "_", cur.createdTS, ".log");

        if (trace) {
                SLog("New active:", basePath, " ", cur.createdTS, " ", Date::ts_repr(cur.createdTS), "\n");
        }

        cur.sanity_checks();

        // XXX:
        // Should open() .log.transient and .index.transient
        // and if both are successful, rename and use
        // as opposed to throwing an exception here and getting stuck in limbo
        fd = this_service->safe_open(basePath.c_str(), read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND), 0775);

        if (-1 == fd) {
                if (errno == ENFILE || errno == EMFILE || errno == ENOSPC || errno == EDQUOT) {
                        TANK_EXPECT(this_service);
                        this_service->no_roll_until = time(nullptr) + 60;
                }

                throw Switch::system_error("open(", basePath, ") failed:", strerror(errno), ". Cannot load segment log");
        }

        partition->flags &= ~unsigned(topic_partition::Flags::NoDataFiles);

        cur.fdh.reset(new fd_handle(fd));

        basePath.resize(basePathLen);
        basePath.append(cur.baseSeqNum, ".index");

        fd = this_service->safe_open(basePath.c_str(), read_only ? O_RDWR : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND), 0775);

        if (-1 == fd) {
                const auto saved_errno = errno;

                if (errno == ENFILE || errno == EMFILE || errno == ENOSPC || errno == EDQUOT) {
                        TANK_EXPECT(this_service);
                        this_service->no_roll_until = time(nullptr) + 60;
                }

                unlink(basePath.c_str());
                basePath.resize(basePathLen);
                basePath.append(cur.baseSeqNum, "_", cur.createdTS, ".log");
                unlink(basePath.c_str());

                throw Switch::system_error("open(", basePath, ") failed:", strerror(saved_errno), ". Cannot load segment index");
        }

        cur.index.fd = fd;
        cur.ra_proxy.ra.reset_to(cur.fdh.get()->fd);
        basePath.resize(basePathLen);

        cur.sanity_checks();

        if (const uint32_t max = config.maxRollJitterSecs) {
                std::random_device                      dev;
                std::mt19937                            rng(dev());
                std::uniform_int_distribution<uint32_t> distr(0, max);

                // make sure this is not low
                cur.rollJitterSecs = std::max<uint32_t>(distr(rng),
                                                        Timings::Hours::ToSeconds(1));
        } else {
                // something sensible
                cur.rollJitterSecs = std::max<uint32_t>(Timings::Hours::ToSeconds(1), Timings::Weeks::ToSeconds(1));
        }

        cur.flush_state.pendingFlushMsgs = 0;
        cur.flush_state.nextFlushTS      = config.flushIntervalSecs
                                               ? time(nullptr) + config.flushIntervalSecs
                                               : UINT32_MAX;

        if (trace) {
                SLog("Switched\n");
        }

        cur.sanity_checks();

        // it's important that we do this as soon as we roll
        cur.triangulation_cache.clear();
        cur.reset_cache();
        cur.sanity_checks();

        TANK_EXPECT(saved == cur.createdTS);
}

void topic_partition_log::flush_index_skiplist() {
        // Implements https://github.com/phaistos-networks/TANK/issues/27
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, "Emptying skiplist", ansifmt::reset, "\n");
        }

        if (cur.index.ondisk.data != nullptr && cur.index.ondisk.data != MAP_FAILED) {
                auto ptr = reinterpret_cast<void *>(const_cast<uint8_t *>(cur.index.ondisk.data));

                madvise(ptr, cur.index.ondisk.span, MADV_DONTNEED);
                munmap(ptr, cur.index.ondisk.span);
        }

        cur.index.ondisk.span     = lseek64(cur.index.fd, 0, SEEK_END);
        cur.index.ondisk.data     = static_cast<const uint8_t *>(mmap(nullptr, cur.index.ondisk.span, PROT_READ, MAP_SHARED, cur.index.fd, 0));
        cur.index.haveWideEntries = false;

        if (cur.index.ondisk.data == MAP_FAILED) {
                throw Switch::system_error("mmap() failed:", strerror(errno));
        }

        madvise(reinterpret_cast<void *>(const_cast<uint8_t *>(cur.index.ondisk.data)), cur.index.ondisk.span, MADV_DONTDUMP);
        cur.index.skipList.clear();
}

// if (firstMsgSeqNum != 0 && lastMsgSeqNum != 0), we have expicitly specified message sequence numbers for the bundle first/last message
append_res topic_partition_log::append_bundle(const time_t   now,
                                              const void    *bundle,
                                              const size_t   bundleSize,
                                              const uint32_t bundleMsgsCnt,
                                              const uint64_t firstMsgSeqNum,
                                              const uint64_t lastMsgSeqNum) {
        static constexpr bool trace{false};
        const auto            saved_last_assigned_seqnum = lastAssignedSeqNum;
        const auto            absSeqNum                  = firstMsgSeqNum ?: lastAssignedSeqNum + 1;

        if (unlikely(absSeqNum < cur.baseSeqNum && cur.baseSeqNum != std::numeric_limits<uint64_t>::max())) {
                // This is odd
                throw Switch::data_error("Unexpected absSeqNum(", absSeqNum, ") < cur.baseSeqNum(", cur.baseSeqNum, ") for ", partition->owner->name(), "/", partition->idx);
        }

        TANK_EXPECT(bundleMsgsCnt);

        cur.sanity_checks();

        if (lastMsgSeqNum) {
                // Sparse bundle; last message seqNum encoded in the bundle header
                lastAssignedSeqNum = lastMsgSeqNum;
        } else if (firstMsgSeqNum) {
                // either sparse bundle(first message encoded in the bundle header), or bundle in
                // a TankAPIMsgType::ProduceWithSeqnum request, where the bundle is encoded in the partition header
                lastAssignedSeqNum = firstMsgSeqNum + bundleMsgsCnt - 1;
        } else {
                lastAssignedSeqNum += bundleMsgsCnt;
        }

        if (trace) {
                SLog("firstMsgSeqNum(", firstMsgSeqNum,
                     "), lastMsgSeqNum(", lastMsgSeqNum,
                     "), bundleMsgsCnt(", bundleMsgsCnt,
                     ") => lastAssignedSeqNum = ", lastAssignedSeqNum, "\n");
        }

        if (unlikely(should_roll(now))) {
                roll(absSeqNum, saved_last_assigned_seqnum);
        } else {
                if (lastMsgSeqNum && !cur.index.haveWideEntries) {
                        // This may be necessary
                        may_switch_index_wide(lastMsgSeqNum);
                }

                if (unlikely(cur.index.skipList.size() > 65536)) {
                        flush_index_skiplist();
                }
        }

        TANK_EXPECT(cur.fdh.use_count() >= 1);

        uint8_t            varint[8];
        const uint8_t      varintLen = Compression::PackUInt32(bundleSize, varint) - varint;
        auto               fd        = cur.fdh->fd;
        const struct iovec iov[] =
            {
                {(void *)varint, varintLen},
                {const_cast<void *>(bundle), bundleSize},
            };
        const auto                 entryLen = iov[0].iov_len + iov[1].iov_len;
        const range32_t            fileRange(cur.fileSize, entryLen);
        const auto                 before = cur.fdh.use_count();
        std::shared_ptr<fd_handle> fdh(cur.fdh);
        const auto                 b = trace ? Timings::Microseconds::Tick() : uint64_t(0);

        TANK_EXPECT(cur.fdh.use_count() == before + 1);

        // https://github.com/phaistos-networks/TANK/issues/14
        if (unlikely(writev(fd, iov, sizeof_array(iov)) != entryLen)) {
                if (EDQUOT == errno || ENOSPC == errno) {
                        TANK_EXPECT(this_service);
                        this_service->no_roll_until = this_service->curTime + 60;
                }

                lastAssignedSeqNum = saved_last_assigned_seqnum;
                this_service->track_io_fail(partition);
                return {nullptr, {}, {}};
        } else {
                if (trace) {
                        SLog("writev() took ", duration_repr(Timings::Microseconds::Since(b)),
                             ", entryLen = ", entryLen,
                             ", cur.sinceLastUpdate = ", cur.sinceLastUpdate,
                             ", config.indexInterval = ", config.indexInterval, "\n");
                }

                // Even if we fail to update the index, that's not a big deal because
                // 1. we can always rebuild the index 2. we use the index to locate the closest bundle to the target sequence number
                if (cur.sinceLastUpdate > config.indexInterval) {
                        TANK_EXPECT(absSeqNum >= cur.baseSeqNum); // sanity check
                        const uint32_t out[] = {static_cast<uint32_t>(absSeqNum - cur.baseSeqNum),
                                                cur.fileSize};

                        cur.index.skipList.push_back({out[0], out[1]});

                        if (trace) {
                                SLog(">> ", out[0], ", ", out[1], " ", cur.index.skipList.size(), "\n");
                        }

                        if (unlikely(write(cur.index.fd, out, sizeof(out)) != sizeof(out))) {
                                // don't restore neither lastAssignedSeqNum from saved_last_assigned_seqnum,  nor fileSize
                                // because this has been accepted
                                if (EDQUOT == errno || ENOSPC == errno) {
                                        TANK_EXPECT(this_service);
                                        this_service->no_roll_until = this_service->curTime + 60;
                                }

                                this_service->track_io_fail(partition);
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

                if (trace) {
                        SLog("cur.flush_state.pendingFlushMsgs = ", cur.flush_state.pendingFlushMsgs,
                             ", config.flushIntervalMsgs = ", config.flushIntervalMsgs,
                             ", config.flushIntervalMsgs = ", config.flushIntervalMsgs, "\n");
                }

                if (config.flushIntervalMsgs && cur.flush_state.pendingFlushMsgs >= config.flushIntervalMsgs) {
                        if (trace) {
                                SLog("Scheduling flush\n");
                        }

                        schedule_flush(now);
                } else if (now >= cur.flush_state.nextFlushTS) {
                        if (trace) {
                                SLog("Scheduling flush\n");
                        }

                        schedule_flush(now);
                }

                this_service->track_io_success(partition);
                return {fdh, fileRange, {absSeqNum, uint16_t(bundleMsgsCnt)}};
        }

        // return here and not in (writev() ! entryLen) check because some older compilers warn about
        // a path with no return from a non-void function. Sigh
        return {nullptr, {}, {}};
}

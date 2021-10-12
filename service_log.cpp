#include "service_common.h"

int Rename(const char *oldpath, const char *newpath);
int Unlink(const char *pathname);

uint32_t Service::verify_log(int fd) {
        const auto fileSize = lseek64(fd, 0, SEEK_END);

        if (!fileSize) {
                return 0;
        }

        auto *const                         fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
        strwlen8_t                          key;
        strwlen32_t                         msgContent;
        IOBuffer                            cb;
        range_base<const uint8_t *, size_t> msgSetContent;
        uint64_t                            msgSeqNum{1}, firstMsgSeqNum, lastMsgSeqNum;
        static constexpr bool               trace{false};

        if (fileData == MAP_FAILED) {
                throw Switch::system_error("Unable to mmap():", strerror(errno));
        }

        DEFER(
            {
                    madvise(fileData, fileSize, MADV_DONTNEED);
                    munmap(fileData, fileSize);
            });
        madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

        for (const auto *p = static_cast<uint8_t *>(fileData), *const e = p + fileSize, *const base = p; p != e;) {
                const auto *const bundleBase = p;
                const auto        bundleLen  = Compression::decode_varuint32(p);
                const auto        nextBundle = p + bundleLen;

                TANK_EXPECT(p < e);
                TANK_EXPECT(bundleLen);

                const auto bundleFlags        = *p++;
                const auto codec              = bundleFlags & 3;
                const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                const auto msgsSetSize        = ((bundleFlags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                if (trace) {
                        SLog("Bundle, flags = ", bundleFlags, ", codec = ", codec, ", sparseBundleBitSet = ", sparseBundleBitSet, ", msgsSetSize = ", msgsSetSize, "\n");
                }

                if (sparseBundleBitSet) {
                        firstMsgSeqNum = decode_pod<uint64_t>(p);

                        if (msgsSetSize != 1) {
                                lastMsgSeqNum = firstMsgSeqNum + Compression::decode_varuint32(p) + 1;
                        } else {
                                lastMsgSeqNum = firstMsgSeqNum;
                        }

                        if (trace) {
                                SLog("sparse bundle - firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum = ", lastMsgSeqNum, "\n");
                        }

                        TANK_EXPECT(firstMsgSeqNum && lastMsgSeqNum >= firstMsgSeqNum);
                }

                TANK_EXPECT(p <= e);
                TANK_EXPECT(msgsSetSize);
                TANK_EXPECT(codec == 0 || codec == 1);

                if (false) {
                        Print(msgSeqNum, " => OFFSET ", bundleBase - base, "\n");
                }

                if (codec) {
                        cb.clear();
                        if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, nextBundle - p, &cb)) {
                                throw Switch::system_error("Failed to decompress content");
                        }

                        msgSetContent.set(reinterpret_cast<uint8_t *>(cb.data()), cb.size());
                } else {
                        msgSetContent.set(p, nextBundle - p);
                }

                [[maybe_unused]] uint64_t msgTs{0};
                uint32_t                  msgIdx{0};

                for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.len; p != e; ++msgIdx, ++msgSeqNum) {
                        // Next message set message
                        const auto flags = *p++;

                        if (sparseBundleBitSet) {
                                if (msgIdx == 0 || msgIdx == msgsSetSize - 1) {
                                        msgSeqNum = firstMsgSeqNum;
                                } else if (msgIdx == msgsSetSize - 1) {
                                        msgSeqNum = lastMsgSeqNum;
                                } else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                        // incremented in for()
                                } else {
                                        // delta from prev - 1
                                        msgSeqNum += Compression::decode_varuint32(p);
                                }
                        }

                        if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                msgTs = decode_pod<uint64_t>(p);
                        }

                        if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                p += key.size() + sizeof(uint8_t);
                        } else {
                                key.reset();
                        }

                        const auto msgLen = Compression::decode_varuint32(p);

                        msgContent.set(reinterpret_cast<const char *>(p), msgLen);
                        p += msgLen;

                        if (trace) {
                                Print("MSG:", msgSeqNum, ", size = ", msgLen, "\n");
                        }
                }

                p = nextBundle;
                TANK_EXPECT(p <= e);
        }

        return msgSeqNum;
}

bool Service::close_partition_log(topic_partition *partition) {
        static constexpr bool trace{false};
        TANK_EXPECT(partition);
        auto log = partition->_log.get();

        // need to make sure we are no longer tracking this
        if (partition->access.ll.try_detach_and_reset() && active_partitions.empty()) {
                next_active_partitions_check = std::numeric_limits<uint64_t>::max();
        }

        if (!log) {
                // already closed
		// we shouldn't ever reach this path, but we may as well account for it
                return false;
        }

        if (log->compacting.load()) {
                // can't touch this while a compaction is on-going
                // XXX: may need to use CAS to set this to some other magic value
                // so that we won't be attempting to compacting them while its closed
                // i.e tri-state(open, close, open-compacting)
                return false;
        }


        auto topic = partition->owner;

        TANK_EXPECT(topic);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "closing PARTITION ", topic->name(), "/", partition->idx, ansifmt::reset, "\n");
        }

        partition->_log.reset(nullptr);

        if (std::exchange(partition->open_ok, false)) {
                --total_open_partitions;
        }

        return true;
}

int Service::reset_partition_log(topic_partition *partition) {
        [[maybe_unused]] static constexpr bool trace{false};
        TANK_EXPECT(partition);
        const auto topic = partition->owner;
        TANK_EXPECT(topic);
        TANK_EXPECT(!partition->_log); // log must be closed or not opened
        TANK_EXPECT(partition->access.ll.empty());

        if (partition->flags & unsigned(topic_partition::Flags::NoDataFiles)) {
                // there is nothing to do here
                return 0;
        }

	TANK_EXPECT(false == read_only);

        char       basePath[PATH_MAX];
        const auto basePathLen = snprintf(basePath, sizeof(basePath), "%.*s/%.*s/%u/",
                                          static_cast<int>(::basePath_.size()), ::basePath_.data(),
                                          static_cast<int>(topic->name_.size()),
                                          topic->name_.data(), partition->idx);

        try {
                for (auto name : DirectoryEntries(basePath)) {
                        if (name.front() == '.') {
                                continue;
                        }

			TANK_EXPECT(false == read_only);

                        name.ToCString(basePath + basePathLen);
                        if (-1 == unlink(basePath)) {
                                Print("Failed to unlink(", basePath, "): ", strerror(errno), "\n");
                                return 1;
                        }
                }
        } catch (...) {
                // maybe gone anyway
        }

        partition->flags |= unsigned(topic_partition::Flags::NoDataFiles);

        return 0;
}

// XXX: if running in cluster-aware mode, we shouldn't open_partition_log() for each discovered partition if TANK_SRV_LAZY_PARTITION_INIT is defined
// TODO:
void Service::open_partition_log(topic_partition *partition, const partition_config &conf) {
	enum {
		trace = false,
	};
        const auto            before = Timings::Microseconds::Tick();
        char                  basePath[PATH_MAX];
        auto                  topic       = partition->owner;
        const auto            basePathLen = snprintf(basePath, sizeof(basePath), "%.*s/%s%.*s/%u/",
                                          static_cast<int>(::basePath_.size()), ::basePath_.data(),
                                          (topic->flags & unsigned(topic::Flags::under_construction)) ? "." : "",
                                          static_cast<int>(topic->name_.size()),
                                          topic->name_.data(), partition->idx);

        TANK_EXPECT(not partition->_log); // already initialized?

        track_accessed_partition(partition, curTime);

        if (trace) {
		puts("");
                SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "OPENING PARTITION ", topic->name(),
                     "/", partition->idx, " ", ptr_repr(this), ", basePath:", basePath,  ansifmt::reset, "\n");
		puts("");
        }

	const auto __before = Timings::Milliseconds::Tick();

        DEFER({
                open_partitions_time += Timings::Milliseconds::Since(__before);
        });

        try {
                struct rosegment_ctx final {
                        uint64_t firstAvailableSeqNum;
                        uint64_t lastAvailSeqNum;
                        uint32_t creationTS;
                };

                // TODO(markp): reuse roLogs, wideEntyRoLogIndices, swapped and the allocator
                std::vector<rosegment_ctx>   roLogs;
                std::unordered_set<uint64_t> wideEntyRoLogIndices;
                simple_allocator             allocator{1024};
                std::vector<strwlen32_t>     swapped;
                uint64_t                     curLogSeqNum{0};
                uint32_t                     curLogCreateTS{0};
                const strwlen32_t            b(basePath, basePathLen);
                int                          fd;
                bool                         processSwapped{true};
                struct stat                  st;
                char                         _base_path[PATH_MAX];
                auto                         l = new topic_partition_log();
                bool                         any_files{false};

                l->partition = partition;
                l->config    = conf;
                partition->_log.reset(l);

                l->roSegments                                = nullptr;
                l->cur.index.ondisk.data                     = nullptr;
                l->cur.index.ondisk.span                     = 0;
                l->cur.index.haveWideEntries                 = false;
                l->cur.index.ondisk.lastRecorded.relSeqNum   = 0;
                l->cur.index.ondisk.lastRecorded.absPhysical = 0;

                *b.CopyTo(_base_path) = '\0';

                // Scan the partitiond directory
                for (auto &&name : DirectoryEntries(basePath)) {
			if (name.front() == '.') {
                                continue;
                        }

                        any_files = true;

                        if (not cluster_aware() and  name.Eq(_S("config"))) {
                                // override
                                parse_partition_config(Buffer::build(basePath, "/", name).data(), &l->config);
                                continue;
                        }

                        auto r = name.divided('.');

                        if (r.second.StripSuffix(_S(".cleaned"))) {
                                // Compaction failed mid-way
                                // remove this file and make sure any .swap file files are also deleted
                                auto fullPath = Buffer::build(basePath, "/", name);

                                if (Unlink(fullPath.data()) == -1) {
                                        throw Switch::system_error("Failed to Unlink(", fullPath, "):", strerror(errno));
                                }

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

                                        if (Unlink(fullPath.data()) == -1) {
                                                throw Switch::system_error("Failed to Unlink(", fullPath, "):", strerror(errno));
                                        }
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
                                if (Rename(Buffer::build(basePath, "/", name).data(),
                                           Buffer::build(basePath, "/", strwlen8_t(name.p, name.len - STRLEN(".old"))).data()) == -1) {
                                        throw Switch::system_error("Unable to rename .old file:", strerror(errno));
                                }
                        }

                        if (r.second.Eq(_S("index"))) {
                                // accept
                                const auto v = r.first.divided('_');

                                if (v.second.Eq(_S("64"))) {
                                        // Just so that ro_segment::ro_segment() won't have to try different names until it gets it right
                                        wideEntyRoLogIndices.insert(v.first.as_uint64());
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

                                const auto repr       = s.divided('-');
                                const auto baseSeqNum = repr.first.as_uint64(), lastAvailSeqNum = repr.second.as_uint64();

                                if (trace) {
                                        SLog("collected ROLog (	baseSeqNum = ", baseSeqNum, ", lastAvailSeqNum = ", lastAvailSeqNum, ", creationTS = ", creationTS, ") r.first = ", r.first, "\n");
                                }

                                roLogs.push_back({
                                    baseSeqNum,
                                    lastAvailSeqNum,
                                    creationTS,
                                });
                        } else if (r.second.Eq(_S("log"))) {
                                // current, mutable log(segment)
                                //
                                // Either num.log or
                                // num_ts.log
                                // the later encodes the creation timestamp in the path, which is useful because
                                // we 'd like to know when this was created, when we restart the service and get to continue using the selected log
                                if (const auto *const p = r.first.Search('_')) {
                                        *name.CopyTo(_base_path + b.size() + 1) = '\0';

                                        if (-1 == stat(_base_path, &st)) {
                                                Print("Failed to stat ", _base_path, ": ", strerror(errno), "\n");
                                                std::abort();
                                        }

                                        if (0 == st.st_size) {
                                                // This is a stray - whatever the reason this is here, it needs to go
                                                Print("Found a stray ", _base_path, ", deleting it\n");
						TANK_EXPECT(false == read_only);
                                                unlink(_base_path);
                                                continue;
                                        }

                                        const auto seqRepr = r.first.PrefixUpto(p);
                                        const auto tsRepr  = r.first.SuffixFrom(p + 1);

                                        if (!seqRepr.IsDigits() || !tsRepr.IsDigits()) {
                                                throw Switch::system_error("Unexpected name ", name);
                                        } else {
                                                const auto seq = seqRepr.as_uint64();

                                                TANK_EXPECT(seq);

                                                curLogSeqNum   = seq;
                                                curLogCreateTS = tsRepr.AsUint32();

                                                if (!curLogSeqNum) {
                                                        Print(ansifmt::bold, ansifmt::color_red, "Unexpected, curLogSeqNum == 0 from ", _base_path, ansifmt::reset, " name='", name, "'\n");
                                                        std::abort();
                                                }

                                                if (trace) {
                                                        SLog("curLogSeqNum = ", curLogSeqNum, ", curLogCreateTS = ", curLogCreateTS, " from ", r.first, "\n");
                                                }
                                        }
                                } else {
                                        if (unlikely(!r.first.all_of_digits())) {
                                                throw Switch::system_error("Unexpected name ", name);
                                        } else {
                                                const auto seq = r.first.as_uint64();

                                                TANK_EXPECT(seq);
                                                TANK_EXPECT(!curLogSeqNum);

                                                curLogSeqNum = seq;
                                        }
                                }
                        } else {
                                Print("Unexpected name ", name, " in ", basePath, "\n");
                        }
                }

                if (any_files) {
                        partition->flags &= ~unsigned(topic_partition::Flags::NoDataFiles);
                }

                // Finish merge process if needed and if it's possible
                if (processSwapped == false) {
                        if (trace) {
                                SLog("Cannot process any swapped files\n");
                        }

                        while (!swapped.empty()) {
                                auto it = swapped.back();

				TANK_EXPECT(false == read_only);
                                if (Unlink(Buffer::build(basePath, "/", it).data()) == -1) {
                                        throw Switch::system_error("Failed to unlink swapped file:", strerror(errno));
                                }

                                swapped.pop_back();
                        }
                } else {
                        if (trace) {
                                SLog("Can process swapped files\n");
                        }

                        while (not swapped.empty()) {
                                auto name = swapped.back();

                                name.StripSuffix(".swap"_len);

                                if (trace) {
                                        SLog("Processing swapped ", name, "\n");
                                }

                                if (Rename(Buffer::build(basePath, "/", name, ".swap").data(),
                                           Buffer::build(basePath, "/", name).data()) == -1) {
                                        throw Switch::system_error("Failed to rename swapped file:", strerror(errno));
                                }

                                const auto r = name.divided('.');

                                if (r.second.Eq(_S("index"))) {
                                        const auto v = r.first.divided('_');

                                        if (v.second.Eq(_S("64"))) {
                                                // Just so that ro_segment::ro_segment() won't have to try different names until it gets it right
                                                wideEntyRoLogIndices.insert(v.first.as_uint64());
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
                                        const auto baseSeqNum = repr.first.as_uint64(), lastAvailSeqNum = repr.second.as_uint64();

                                        if (trace) {
                                                SLog("Collected ROLog (	", baseSeqNum, ", ", lastAvailSeqNum, ", ", creationTS, ") ", r.first, "\n");
                                        }

                                        roLogs.push_back({
                                            baseSeqNum,
                                            lastAvailSeqNum,
                                            creationTS,
                                        });
                                }

                                swapped.pop_back();
                        }
                }

                if (trace) {
                        SLog("roLogs.size() = ", roLogs.size(), 
				", curLogSeqNum = ", curLogSeqNum, 
				", curLogCreateTS = ", curLogCreateTS, "\n");
                }

                // Process read only segments
                if (const auto total = roLogs.size()) {
                        std::sort(roLogs.begin(), roLogs.end(), [](const auto &a, const auto &b) noexcept {
                                return a.firstAvailableSeqNum < b.firstAvailableSeqNum;
                        });

                        l->firstAvailableSeqNum = roLogs.front().firstAvailableSeqNum;
                        l->roSegments.reset(new std::vector<ro_segment *>());

                        for (const auto &it : roLogs) {
                                if (trace) {
                                        SLog("Initializing [firstAvailableSeqNum=", it.firstAvailableSeqNum,
                                             ",lastAvailSeqNum=", it.lastAvailSeqNum, ", base = ", b, "]\n");
                                }

                                try {
                                        auto s = std::make_unique<ro_segment>(it.firstAvailableSeqNum,
                                                                              it.lastAvailSeqNum,
                                                                              b,
                                                                              it.creationTS,
                                                                              wideEntyRoLogIndices.count(it.firstAvailableSeqNum));

					assert(not s->fdh);
                                        l->roSegments->emplace_back(s.release());
                                } catch (const std::exception &e) {
                                        if (trace) {
                                                SLog("Failed:", e.what(), "\n");
                                        }

                                        throw;
                                }
                        }

			if (trace) {
				SLog("All RO segments\n");
				for (const auto s : *l->roSegments) {
					SLog("Segment ", ptr_repr(s), " with baseSeqNum ", s->baseSeqNum, "\n");
				}
			}

                        TANK_EXPECT(std::is_sorted(l->roSegments->begin(), l->roSegments->end(), [](const auto a, const auto b) noexcept {
                                return a->baseSeqNum < b->baseSeqNum;
                        }));

			for (auto seg : *l->roSegments) {
				assert(not seg->fdh);
			}
                } else {
                        l->firstAvailableSeqNum = curLogSeqNum;
                        l->roSegments.reset(new std::vector<ro_segment *>());
                }

                // Have a current segment?
                if (curLogSeqNum) {
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
                        l->cur.baseSeqNum = curLogSeqNum;
                        l->cur.fileSize   = lseek64(fd, 0, SEEK_END);

                        // TODO(markp): check if index has wideEntries
                        // and set l->cur.index.haveWideEntries accordingly
                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".index");
                        fd = open(basePath, read_only ? O_RDWR : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME | O_APPEND), 0775);

                        if (trace) {
                                SLog("Considering index ", basePath, " cur.fileSize = ", l->cur.fileSize, ", with cur.baseSeqNum = ", l->cur.baseSeqNum, "\n");
                        }

                        if (fd == -1) {
                                throw Switch::system_error("open(", basePath_, ") failed:", strerror(errno), ". Cannot open current segment index");
                        } else if (lseek64(fd, 0, SEEK_END) == 0 && l->cur.fileSize) {
                                if (trace) {
                                        SLog("Empty index, but datafile is not empty: will need to rebuild\n");
                                }

                                Service::rebuild_index(l->cur.fdh->fd, fd, curLogSeqNum);
                        } else if (l->cur.fileSize == 0) {
                                if (trace) {
                                        SLog("Got a dummy/empty current segment, will reset\n");
                                }

				TANK_EXPECT(false == read_only);
                                if (-1 == unlink(basePath)) {
                                        throw Switch::system_error("Failed to unlink(", basePath, "): ", strerror(errno));
                                }

                                if (curLogCreateTS) {
                                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, "_", curLogCreateTS, ".log");
                                } else {
                                        Snprint(basePath, sizeof(basePath), b, curLogSeqNum, ".log");
                                }

                                if (-1 == unlink(basePath)) {
                                        throw Switch::system_error("Failed to unlink(", basePath, "): ", strerror(errno));
                                }

                                goto new_cur_segment;
                        }

                        l->cur.index.fd        = fd;
                        l->cur.sinceLastUpdate = l->cur.fileSize == 0 ? UINT32_MAX : 0;
                        l->cur.ra_proxy.ra.reset_to(l->cur.fdh->fd);

                        assert(l->cur.fdh);
                        assert(l->cur.fdh->fd > 2);
                        assert(l->cur.ra_proxy.ra.get_fd() > 2);
                        assert(l->cur.ra_proxy.ra.get_fd() == l->cur.fdh->fd);

                        if (const auto size = lseek64(fd, 0, SEEK_END); size > 0) {
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

                                        if (l->cur.index.haveWideEntries) {
                                                IMPLEMENT_ME();
                                        } else {
                                                for (const auto *      it   = (uint32_t *)l->cur.index.ondisk.data,
                                                                *const base = it,
                                                                *const e    = it + l->cur.index.ondisk.span / sizeof(uint32_t);
                                                     it != e; it += 2) {
                                                        if (it != base) {
                                                                TANK_EXPECT(it[0] != it[-2]);
                                                        }
                                                }
                                        }
                                }
                        } else if (size < 0) {
                                Print("lseek() failed:", strerror(errno), "\n");
                                std::abort();
                        }

                        l->lastAssignedSeqNum = 0;

                        if (const auto s = l->cur.fileSize) {
                                const auto o = l->cur.index.ondisk.lastRecorded.absPhysical;
                                // This is somewhat expensive; but we only need to do this whenever we open_partition_log(), usually
                                // once during startuo
                                //
                                // We just start from the last tracked-recorded (relSeqNum, absPhysical) and skip bundles until EOF
                                // keeping track of offsets as we go.
                                if (unlikely(s < o)) {
                                        Print(ansifmt::bold, ansifmt::color_red, "Dataset corruption", ansifmt::reset, ": Unexpected file size ", s, "(", size_repr(s), ") < last checkpoint in index at ", o, ". Did you copy the data while the partition was actively being updated?\n");
					Print("You can force repair it by deleting ", basePath, " and restarting TANK. This will not salvage whatever data was lost, but will rebuild the index and you should be able to access the partition\n");
					Print("Aborting\n");
					exit(1);
                                }

                                TANK_EXPECT(s >= o);

                                const auto  span = s - o;
                                auto *const data = static_cast<uint8_t *>(malloc(span));
                                // first message in the first bundle we 'll parse
                                uint64_t   next = l->cur.index.ondisk.lastRecorded.relSeqNum + l->cur.baseSeqNum;
                                const auto savedNext{next};
                                int        fd = l->cur.fdh->fd;

                                if (trace) {
                                        SLog("From lastRecorded.relSeqNum = ",
                                             l->cur.index.ondisk.lastRecorded.relSeqNum,
                                             ", lastRecorded.absPhysical = ", o, ", cur.baseSeqNum = ", l->cur.baseSeqNum, "\n");

                                        SLog("span = ", span, " (file_size - last message bundle offset recorded), start from abs sequence number ", next, "\n");
                                }

                                if (unlikely(!data)) {
                                        throw Switch::system_error("malloc(", span, ") failed");
                                }

                                DEFER({ free(data); });

				assert(fd > 2);

                                const auto     res = pread(fd, data, span, o);
                                const uint8_t *lastCheckpoint{data};

                                if (trace) {
                                        SLog("Attempting to read ", span, " bytes at ", o, ": ", res, "\n");
                                }

                                if (res != span) {
                                        throw Switch::system_error("pread64() failed:", strerror(errno));
                                }

                                for (const auto *p = data, *const e = p + span; p < e;) {
                                        const auto *      saved{p};
                                        const auto        bundleLen = Compression::decode_varuint32(p);
                                        const auto *const bundleEnd = p + bundleLen;

                                        if (unlikely(bundleLen == 0 || bundleEnd > e)) {
                                                const auto ckpt = (lastCheckpoint - data) + o;

                                                Print("Likely corrupt TANK partition segment(ran out of disk space?).\n");
                                                if (getenv("TANK_FORCE_SALVAGE_CURSEGMENT")) {
                                                        if (ftruncate(l->cur.fdh->fd, ckpt) == -1) {
                                                                Print("Failed to truncate:", strerror(errno), "\n");
                                                                exit(1);
                                                        } else if (l->cur.index.fd != -1 && ftruncate(l->cur.index.fd, 0) == -1) {
                                                                Print("Failed to truncate:", strerror(errno), "\n");
                                                                exit(1);
                                                        } else {
                                                                Print("Salvaged segment. Please restart Tank\n");
                                                        }
                                                } else {
                                                        Print("Set TANK_FORCE_SALVAGE_CURSEGMENT=1 and restart Tank so that it will _delete_ the current segment index and truncate the current segment file so that it will salvage whatever's possible\n");
                                                        Print("Can save up to ", size_repr(ckpt), ", will lose ", size_repr(s - ckpt), "\n");
                                                        Print("Aborting\n");
                                                }
                                                exit(1);
                                        }

                                        TANK_EXPECT(bundleLen);

                                        lastCheckpoint = saved;

                                        const auto     bundleFlags        = *p++;
                                        const bool     sparseBundleBitSet = bundleFlags & (1u << 6);
                                        const uint32_t msgSetSize         = ((bundleFlags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                                        if (trace) {
                                                SLog("bundleFlags = ", bundleFlags, ", sparseBundleBitSet = ", sparseBundleBitSet, ", msgSetSize = ", msgSetSize, "\n");
                                        }

                                        if (sparseBundleBitSet) {
                                                const auto firstMsgSeqNum = decode_pod<uint64_t>(p);
                                                uint64_t   lastMsgSeqNum;

                                                if (msgSetSize != 1) {
                                                        lastMsgSeqNum = firstMsgSeqNum + Compression::decode_varuint32(p) + 1;
                                                } else {
                                                        lastMsgSeqNum = firstMsgSeqNum;
                                                }

                                                if (trace) {
                                                        SLog("sparse bundle, firstMsgSeqNum = ", firstMsgSeqNum, ", lastMsgSeqNum = ", lastMsgSeqNum, "\n");
                                                }

                                                next = lastMsgSeqNum + 1;
                                        } else {
                                                next += msgSetSize;
                                        }

                                        if (trace) {
                                                SLog("bundle msgSetSize = ", msgSetSize, "\n");
                                        }

                                        p = bundleEnd;

                                        TANK_EXPECT(p <= e);
                                }

                                TANK_EXPECT(next > savedNext);
                                TANK_EXPECT(l->cur.sinceLastUpdate == 0); // not an empty current segment log

                                l->lastAssignedSeqNum = next - 1;
                                set_hwmark(partition, l->lastAssignedSeqNum);

                                if (trace) {
                                        SLog(ansifmt::bold, "Set lastAssignedSeqNum = ", l->lastAssignedSeqNum, ansifmt::reset, "\n");
                                }
                        }

                        // Just in case
                        lseek(l->cur.index.fd, 0, SEEK_END);
                        lseek(l->cur.fdh->fd, 0, SEEK_END);

                        if (const uint32_t max = l->config.maxRollJitterSecs) {
                                std::random_device                      dev;
                                std::mt19937                            rng(dev());
                                std::uniform_int_distribution<uint32_t> distr(0, max);

                                l->cur.rollJitterSecs = std::max<uint32_t>(distr(rng),
                                                                           Timings::Hours::ToSeconds(1));
                        } else {
                                // something sensible
                                l->cur.rollJitterSecs = std::max<uint32_t>(Timings::Hours::ToSeconds(1), Timings::Weeks::ToSeconds(1));
                        }

                        const auto now = time(nullptr);

                        if (now == ((time_t)-1)) {
                                // Yes, this is possible
                                IMPLEMENT_ME();
                        }

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

                        if (trace) {
                                SLog("createdTS(", l->cur.createdTS, ") nameEncodesTS(", l->cur.nameEncodesTS, ")\n");
                        }
                } else {
                        // We 'll just create those on demand in append() (lazily)
                new_cur_segment:
                        l->cur.fdh.reset();
                        l->cur.fileSize              = UINT32_MAX;
                        l->cur.index.fd              = -1;
                        l->cur.sinceLastUpdate       = UINT32_MAX;
                        l->cur.baseSeqNum            = UINT64_MAX;
                        l->lastAssignedSeqNum        = 0;
                        l->cur.index.haveWideEntries = false;
                        l->cur.reset_cache();

                        if (l->roSegments and not l->roSegments->empty()) {
                                // We can still use the last immutable segment
                                Print(ansifmt::bold, ansifmt::color_red, "Looks like someone deleted the active segment from ", basePath, ansifmt::reset, "\n");

                                l->lastAssignedSeqNum = l->roSegments->back()->lastAvailSeqNum;
                                set_hwmark(partition, l->lastAssignedSeqNum);
                        } else {
                                set_hwmark(partition, 0);
                        }
                }

		partition->open_ok = true;		
		++total_open_partitions;
        } catch (const std::exception &e) {
                // we need to _close_ the partition explicitly because we couldn't open it
                // this should reclaim resources, and partition will no longer be considered open, because it is not
                if (trace) {
                        SLog("Exception:", e.what(), "\n");
                }

		close_partition_log(partition);
                throw;
        }

        if (trace) {
                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " to open_partition_log()\n");
        }
}

topic_partition_log *Service::partition_log(topic_partition *const p) {
	enum {
		trace = false,
	};
        TANK_EXPECT(p);

	if (not p->_log) {
                open_partition_log(p, p->owner->partitionConf);
        } else {
                TANK_EXPECT(p->access.ll.empty() == false);
                p->access.last_access = curTime;
        }

        return p->_log.get();
}

void Service::track_io_fail(topic_partition *const p) {
        TANK_EXPECT(p);

#if 1
        partitions_io_failed_cnt = 1;
#else
        if (0 == (p->flags & unsigned(topic_partition::Flags::IOFailed))) {
                p->flags |= unsigned(topic_partition::Flags::IOFailed);
                ++partitions_io_failed_cnt;

                // see can_accept_any_messages()
                // if we want to account for different volume/partition, we should
                // invalidate_replicated_partitions_from_peer_cache_by_partition()p) here
        }
#endif
}

void Service::track_io_success(topic_partition *p) {
        TANK_EXPECT(p);
#if 1
        partitions_io_failed_cnt = 0;
#else
        if (p->flags & unsigned(topic_partition::Flags::IOFailed)) {
                p->flags ^= unsigned(topic_partition::Flags::IOFailed);

                TANK_EXPECT(partitions_io_failed_cnt);
                --partitions_io_failed_cnt;

                // see can_accept_any_messages()
                // if we want to account for different volume/partition, we should
                // invalidate_replicated_partitions_from_peer_cache_by_partition()p) here
        }
#endif
}

bool Service::can_accept_any_messages() const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        if (read_only) {
                return false;
        }

        if (partitions_io_failed_cnt) {
                // see track_io_success() and track_io_fail()
                // we could just black-list and remove from blacklist specific partitions because
                // we could say have persisted partition data to different volumes so that if
                // we fail to update one partition's data files because the volume ran out of disk space, we could
                // likely still continue to accept messages for other partitions. (if we do that, we need to invalidate cluster_node::leadership::local_replication_list)
                //
                // For now, we just assume that all partitions are stored in the same volume.
                return false;
        }

        return true;
}

bool Service::can_accept_messages(const topic_partition *p) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        TANK_EXPECT(p);

        if (!can_accept_any_messages()) {
                return false;
        }

        if (!p->enabled()) {
                return false;
        }

        if (p->flags & unsigned(topic_partition::Flags::IOFailed)) {
                return false;
        }

        return true;
}

append_res topic_partition_log::append_msg(const time_t ts, const strwlen8_t key, const str_view32 msg) {
        const std::size_t required = (key.size() + msg.size()) * 24;
        const uint8_t     msgFlags = key.size() ? uint8_t(TankFlags::BundleMsgFlags::HaveKey) : 0;
        IOBuffer          _msgBuf; // XXX: Service instance
        auto             &b{_msgBuf};

        b.clear();
        b.reserve(required);
        b.pack(uint8_t(1 << 2)); // bundle flags: 1 message in the bundle
        b.pack(msgFlags, ts);    // message header: flags and timestamp

        if (key) {
                b.pack(key.size());
                b.serialize(key.data(), key.size());
        }

        b.encode_varuint32(msg.size());
        b.serialize(msg.data(), msg.size());

        return append_bundle(ts, b.data(), b.size(), 1);
}

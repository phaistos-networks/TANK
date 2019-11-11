#include "service_common.h"

bool ro_segment::prepare_access(const topic_partition *const partition) {
        static constexpr const bool trace{false};
        TANK_EXPECT(partition);

        // TODO:
        // if we were unable to prepare_access() earlier, we need to fail explicitly for sometime and retry e.g 1 hour later
	// otherwise we 'll be retrying to access(open, mmmap, etc) the files/VMAs for every other request to this ro segment
        if (fdh) {
                // already initialised
                if (trace) {
                        SLog(ansifmt::color_green, "Already Initialised", ansifmt::reset, "\n");
                }

                return true;
        }

        // lazily initialise RO Segments for faster startup
        // and reduced resources pressure
        int        fd, indexFd{-1};
        char       path[PATH_MAX];
        const auto cleanup = [&]() {
                fdh.reset(nullptr);
                return false;
        };
        const auto    topic         = partition->owner;
        const auto    absSeqNum     = this->baseSeqNum;
        const auto    lastAbsSeqNum = this->lastAvailSeqNum;
        const auto    creationTS    = this->createdTS;
        struct stat64 st;

        TANK_EXPECT(topic);

        const auto path_base_len = sprintf(path, "%.*s/%s%.*s/%u/",
                                           basePath_.size(), basePath_.data(),
                                           (topic->flags & unsigned(topic::Flags::under_construction)) ? "." : "",
                                           topic->name_.size(), topic->name_.data(),
                                           partition->idx);

        if (createdTS) {
                sprintf(path + path_base_len, "%" PRIu64 "-%" PRIu64 "_%" PRIu32 ".ilog", absSeqNum, lastAbsSeqNum, createdTS);
        } else {
                sprintf(path + path_base_len, "%" PRIu64 "-%" PRIu64 ".ilog", absSeqNum, lastAbsSeqNum);
        }

        if (trace) {
                SLog("Will access [", path, "\n");
        }

        fd = this_service->safe_open(path, O_RDONLY | O_LARGEFILE | O_NOATIME);

        if (fd == -1) {
                const auto saved = errno;

                if (EPOLLIN == saved) {
                        // provide some guidance because this may result in a lot of wasted time
                        Print(ansifmt::color_red, ansifmt::bold, "Unable to access ", ansifmt::reset, path,
                              ": The effective UID of the calleer does not match the owner of the file, and the caller is not privilleged\n");
                }

                Print("Failed to access log file ", path, ":", strerror(saved));
                return false;
        }

        fdh.reset(new fd_handle(fd));
        TANK_EXPECT(fdh.use_count() == 2);
        fdh->Release();

        if (fstat64(fd, &st) == -1) {
                Print("Failed to fstat():", strerror(errno));
                return cleanup();
        }

        auto size = st.st_size;

        if (unlikely(size == (off64_t)-1)) {
                Print("lseek64() failed: ", strerror(errno));
                return cleanup();
        }

        TANK_EXPECT(size < std::numeric_limits<std::remove_reference<decltype(fileSize)>::type>::max());

        fileSize = size;

        if (trace) {
                SLog(ansifmt::bold, "fileSize = ", fileSize,
                     ", createdTS = ", Date::ts_repr(creationTS), ansifmt::reset, "\n");
        }

        if (haveWideEntries) {
                indexFd = this_service->safe_open(Buffer::build(str_view32(path, path_base_len), "/", absSeqNum, "_64.index").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        } else {
                indexFd = this_service->safe_open(Buffer::build(str_view32(path, path_base_len), "/", absSeqNum, ".index").data(), O_RDONLY | O_LARGEFILE | O_NOATIME);
        }

        const auto saved_errno = errno;

        DEFER({
                if (indexFd != -1) {
                        TANKUtil::safe_close(indexFd);
                }
        });

        if (indexFd == -1) {
                if (saved_errno == ENFILE || saved_errno == EMFILE) {
                        Print("open() failed: too many open files\n");
                        return cleanup();
                }

                if (haveWideEntries) {
                        indexFd = this_service->safe_open(Buffer::build(str_view32(path, path_base_len), "/", absSeqNum, "_64.index").data(),
                                                          read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME), 0775);
                } else {
                        indexFd = this_service->safe_open(Buffer::build(str_view32(path, path_base_len), "/", absSeqNum, ".index").data(),
                                                          read_only ? O_RDONLY : (O_RDWR | O_LARGEFILE | O_CREAT | O_NOATIME), 0775);
                }

                if (indexFd == -1) {
                        Print("Failed to access index file: ", strerror(errno));
                        return cleanup();
                }

                if (haveWideEntries) {
                        IMPLEMENT_ME();
                }

                Service::rebuild_index(fdh->fd, indexFd, absSeqNum);

                if (trace) {
                        SLog("Had to Rebuild the Index\n");
                }
        } else if (trace) {
                SLog("open() index OK\n");
        }

        TANK_EXPECT(indexFd > 2);

        size = lseek64(indexFd, 0, SEEK_END);

        if (unlikely(size == (off64_t)-1)) {
                Print("lseek64() failed: ", strerror(errno));
                return cleanup();
        }

        TANK_EXPECT(size < std::numeric_limits<std::remove_reference<decltype(index.fileSize)>::type>::max());

        // TODO(markp): if (haveWideEntries), index.lastRecorded.relSeqNum should be a union, and we should
        // properly set index.lastRecorded here
        assert(haveWideEntries == false); // not implemented yet

        index.fileSize               = size;
        index.lastRecorded.relSeqNum = index.lastRecorded.absPhysical = 0;

        if (size) {
                auto data = mmap(nullptr, index.fileSize, PROT_READ, MAP_SHARED, indexFd, 0);

                if (unlikely(data == MAP_FAILED)) {
                        Print("Failed to access the index file. mmap() failed:", strerror(errno),
                              " for ", size_repr(index.fileSize), " ", path);
                        return cleanup();
                }

                madvise(data, index.fileSize, MADV_DONTDUMP);

                index.data = static_cast<const uint8_t *>(data);

                if (likely(index.fileSize >= sizeof(uint32_t) + sizeof(uint32_t))) {
                        // last entry in the index; very handy
                        const auto *const p = (uint32_t *)(index.data + index.fileSize - sizeof(uint32_t) - sizeof(uint32_t));

                        index.lastRecorded.relSeqNum   = p[0];
                        index.lastRecorded.absPhysical = p[1];

                        if (trace) {
                                SLog("lastRecorded = ", index.lastRecorded.relSeqNum,
                                     "(", index.lastRecorded.relSeqNum + baseSeqNum,
                                     "), ", index.lastRecorded.absPhysical, "\n");
                        }
                }
        } else {
                index.data = nullptr;
        }

        return true;
}

ro_segment::ro_segment(const uint64_t    absSeqNum,
                       uint64_t          lastAbsSeqNum,
                       const strwlen32_t base,
                       const uint32_t    creationTS,
                       const bool        wideEntries)
    : baseSeqNum{absSeqNum}, createdTS{creationTS}, haveWideEntries{wideEntries} {
        static constexpr const bool trace{false};

        if (trace) {
                SLog("New ro_segment(this = ", ptr_repr(this),
                     ", baseSeqNum = ", baseSeqNum,
                     ", lastAbsSeqNum = ", lastAbsSeqNum,
                     ", createdTS = ", createdTS,
                     ", haveWideEntries = ", haveWideEntries, " ", base, "/", absSeqNum, "\n");
        }

        TANK_EXPECT(lastAbsSeqNum >= baseSeqNum);

        index.data     = nullptr;
        index.fileSize = 0;
        fileSize       = 0;

        // This is no longer enabled during ro_segment::ro_segment()
        // TODO: come up with a new CLI option for verifying all RO segments
#if 0
        bool _verified{false};

        if (std::exchange(_verified, true) == false && []() {
                    static const bool _verify = getenv("TANK_VERIFY_ROLOGS");

                    return _verify;
            }()) {
                const auto l = Service::segment_lastmsg_seqnum(fd, absSeqNum);

                if (l != lastAbsSeqNum) {
                        Buffer new_path;

                        Print(ansifmt::bold, ansifmt::color_red, "Unexpected last message abs.seqnum (", lastAbsSeqNum, ") for ", path, ", expected ", l, ansifmt::reset, "\n");

                        lastAbsSeqNum = l;

                        if (createdTS) {
                                new_path.append(base, "/", absSeqNum, "-", lastAbsSeqNum, "_", createdTS, ".ilog");
                        } else {
                                new_path.append(base, "/", absSeqNum, "-", lastAbsSeqNum, ".ilog");
                        }

                        TANKUtil::safe_close(fd);
                        if (-1 == rename(path.c_str(), new_path.c_str())) {
                                throw Switch::system_error("Failed to rename()");
                        }

                        Print(ansifmt::bold, ansifmt::color_green, "Renamed ", path, " to ", new_path, ansifmt::reset, "\n");
                }
        }
#endif

        this->lastAvailSeqNum = lastAbsSeqNum;
}

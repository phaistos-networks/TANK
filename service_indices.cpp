#include "service_common.h"

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
                const auto bundleLen  = Compression::decode_varuint32(p);
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

                TANK_EXPECT(p < e);

                const auto bundleFlags        = *p++;
                const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                const auto msgsSetSize        = ((bundleFlags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                TANK_EXPECT(p <= e);

                if (trace)
                        SLog("New bundle bundleFlags = ", bundleFlags, ", msgSetSize = ", msgsSetSize, "\n");

                if (sparseBundleBitSet) {
                        uint64_t lastMsgSeqNum;

                        firstMsgSeqNum = *(uint64_t *)p;
                        p += sizeof(uint64_t);

                        if (msgsSetSize != 1) {
                                lastMsgSeqNum = firstMsgSeqNum + Compression::decode_varuint32(p) + 1;
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
                TANK_EXPECT(p <= e);
        }

        if (trace)
                SLog("Rebuilt index ", size_repr(b.size()), ", last ", relSeqNum, ", ", b.size(), "\n");

        if (pwrite64(indexFd, b.data(), b.size(), 0) != b.size()) {
                throw Switch::system_error("Failed to store index:", strerror(errno));
	}	

        if (ftruncate(indexFd, b.size())) {
                throw Switch::system_error("Failed to truncate index file:", strerror(errno));
	}

        fsync(indexFd);

        if (trace) {
                SLog("REBUILT INDEX\n");
	}
}

void Service::verify_index(int fd, const bool wideEntries) {
        static constexpr bool trace{false};
        const auto            fileSize = lseek64(fd, 0, SEEK_END);

        if (!fileSize) {
                return;
	} else if (fileSize & 7) {
                throw Switch::system_error("Unexpected index filesize");
	}

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
                if (trace) {
                        SLog("Entry(", *p, ", ", p[1], ")\n");
		}

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


#include "service_common.h"

int Rename(const char *oldpath, const char *newpath) {
	static constexpr bool trace{false};

        if (trace) {
                SLog("rename(", oldpath, ", ", newpath, ")\n");
        }

        return rename(oldpath, newpath);
}

int Unlink(const char *pathname) {
	static constexpr bool trace{false};

        if (trace) {
                SLog("unlink(", pathname, ")\n");
        }

        return unlink(pathname);
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
                        const auto bundleLen          = Compression::decode_varuint32(p);
                        const auto nextBundle         = p + bundleLen;
                        const auto bundleFlags        = *p++;
                        const auto codec              = bundleFlags & 3;
                        const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                        uint32_t   msgsSetSize        = (bundleFlags >> 2) & 0xf;

                        if (!msgsSetSize)
                                msgsSetSize = Compression::decode_varuint32(p);

                        if (trace)
                                SLog("New bundle msgSetSize = ", msgsSetSize, ", bundleFlags = ", bundleFlags, ", codec = ", codec, "\n");

                        if (sparseBundleBitSet) {
                                firstMsgSeqNum = *(uint64_t *)p;
                                p += sizeof(uint64_t);

                                if (msgsSetSize != 1) {
                                        lastMsgSeqNum = firstMsgSeqNum + Compression::decode_varuint32(p) + 1;
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
                                                msgSeqNum += Compression::decode_varuint32(p);

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

                                const auto msgLen = Compression::decode_varuint32(p);

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

                        if (iovLen) {
                                flush();
                        }

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
                        if (Rename(logPath, Buffer::build(destPartitionPath, baseSeqNum, "-", lastAvailSeqNum, "_", curSegment->createdTS, ".ilog.cleaned")) == -1) {
                                throw Switch::system_error("Failed to rename segment:", strerror(errno));
			}

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

                        TANK_EXPECT(it != roSegments->end());

                        // remove segments from current roSegments[]
                        std::for_each(it, it + segments.size(), [](auto ptr) { delete ptr; });
                        roSegments->erase(it, it + segments.size());

                        // replace removed segments with new segments
                        roSegments->insert(it, newSegments.begin(), newSegments.end());

                        log->compacting = false;
                        if (!log->lastCleanupMaxSeqNum) {
                                this_service->track_log_cleanup(log);
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
	static constexpr bool trace{false};
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

        if (trace) {
                SLog("Compaction for [", basePartitionPath, "]\n");
	}

        TANK_EXPECT(compaction->prevSegments.size());

        std::call_once(onceFlag, [] {
                std::thread([]() {
                        std::vector<pending_compaction *> localWork;
                        sigset_t                          mask;

                        sigfillset(&mask);
                        pthread_sigmask(SIG_SETMASK, &mask, nullptr);
                        for (;;) {
                                std::unique_lock<std::mutex> lock(workLock);

                                workCond.wait(lock, [] { return pendingCompactions.any(); });
                                for (auto it = pendingCompactions.drain(); it; it = it->next) {
                                        localWork.push_back(it);
                                }

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

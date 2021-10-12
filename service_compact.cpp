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

        enum {
                trace      = false,
                trace_msgs = false,
        };
        uint64_t                               firstMsgSeqNum, lastMsgSeqNum, msgSeqNum;
        range_base<const uint8_t *, size_t>    msgSetContent;
        strwlen8_t                             key;
        [[maybe_unused]] strwlen32_t           msgContent, msgValue;
        bool                                   anyDropped{false};
        std::vector<std::unique_ptr<IOBuffer>> pool;
        IOBuffer *                             cur{nullptr};
        size_t                                 base{0};
        static constexpr const uint64_t        ptrBit{uint64_t(1) << (sizeof(uintptr_t) * 8 - 1)};

        const auto compact = [&anyDropped](std::vector<msg> &msgs) {
                std::sort(msgs.begin(), msgs.end(), [](const auto &a, const auto &b) noexcept {
                        return a.key.Cmp(b.key) < 0;
                });

                auto *out = msgs.data();

                for (const auto *it = out, *const e = it + msgs.size(); it != e;) {
                        const auto k = it->key;

                        if (!k) {
                                do {
                                        *out++ = *it;
                                } while (++it < e && !it->key);
                        } else {
                                auto last = it->seqNum;
                                auto sel  = it;

                                for (++it; it < e && it->key == k; ++it) {
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

        std::vector<msg>                       msgs;
        std::vector<std::pair<void *, size_t>> vmas;
        const auto                             before = Timings::Microseconds::Tick();

        DEFER(
            {
                    while (!vmas.empty()) {
                            auto it = vmas.back();

                            madvise(it.first, it.second, MADV_DONTNEED);
                            munmap(it.first, it.second);
                            vmas.pop_back();
                    }
            });

        if (trace) {
                SLog(prevSegments.size(), " segments\n");
        }

        for (auto it : prevSegments) {
                TANK_EXPECT(it->fdh);
                int         fd         = it->fdh->fd;
                const auto  fileSize   = it->fileSize;
                const auto  baseSeqNum = it->baseSeqNum;
                auto *const fileData   = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                if (fileData == MAP_FAILED) {
                        throw Switch::system_error("mmap() failed:", strerror(errno));
                }

                madvise(fileData, fileSize, MADV_DONTDUMP);
                vmas.emplace_back(fileData, fileSize);

                if (trace_msgs) {
                        SLog("baseSeqNum for segment ", baseSeqNum, "\n");
                }

                msgSeqNum = baseSeqNum;
                for (const auto *p = static_cast<const uint8_t *>(fileData), *const e = p + fileSize; p < e;) {
                        const auto     bundleLen          = Compression::decode_varuint32(p);
                        const auto     nextBundle         = p + bundleLen;
                        const auto     bundleFlags        = *p++; // header flags
                        const auto     codec              = bundleFlags & 3;
                        const bool     sparseBundleBitSet = bundleFlags & (1u << 6);
                        const uint32_t msgsSetSize        = ((bundleFlags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                        if (trace_msgs) {
                                SLog("New bundle msgSetSize = ", msgsSetSize, ", bundleFlags = ", bundleFlags, ", codec = ", codec, "\n");
                        }

                        if (sparseBundleBitSet) {
                                firstMsgSeqNum = decode_pod<uint64_t>(p);

                                if (msgsSetSize != 1) {
                                        lastMsgSeqNum = firstMsgSeqNum + Compression::decode_varuint32(p) + 1;
                                } else {
                                        lastMsgSeqNum = firstMsgSeqNum;
                                }

                                if (trace_msgs) {
                                        SLog("sparse bundle (first ", firstMsgSeqNum, ", last ", lastMsgSeqNum, ")\n");
                                }

				//next_seqnum = lastMsgSeqNum + 1;
                        } 

                        if (codec) {
                                if (!cur || cur->Reserved() > 64 * 1024 * 1024) { // XXX: arbitrary
                                        auto       cur_data = cur ? cur->data() : nullptr;
                                        const auto n        = msgs.size();

                                        while (base < n) {
                                                auto &     it  = msgs[base++];
                                                const auto ptr = reinterpret_cast<uintptr_t>(it.content.data());

                                                if (const auto to = ptr & ~ptrBit; ptr != to) {
                                                        it.content.p = cur_data + to;
                                                }

                                                if (it.key) {
                                                        const auto ptr = reinterpret_cast<uintptr_t>(it.key.data());

                                                        if (const auto to = ptr & ~ptrBit; ptr != to) {
                                                                it.key.p = cur_data + to;
                                                        }
                                                }
                                        }


					// TODO:  https://github.com/phaistos-networks/TANK/issues/72
					// spill to disk?
                                        auto owner = std::make_unique<IOBuffer>();

                                        cur = owner.get();
                                        pool.emplace_back(std::move(owner));
                                }

                                const auto len  = cur->size();
                                const auto span = std::distance(p, nextBundle);

                                if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, span, cur)) {
                                        throw Switch::system_error("failed to decompress message set");
                                } else if (trace_msgs) {
                                        SLog("Decompressed ", size_repr(span), " ", size_repr(cur->size() - len), " at ", len, "\n");
                                }

                                msgSetContent.set(reinterpret_cast<const uint8_t *>(cur->data() + len), cur->size() - len);
                        } else {
                                msgSetContent.set(p, std::distance(p, nextBundle));
                        }

                        p = nextBundle; // advance p past this bundle

                        uint64_t    msgTs{0};
                        uint32_t    msgIdx{0};
                        const char *ptrBase = cur ? cur->data() : nullptr;

                        if (trace_msgs) {
                                SLog("Parsing Message Set(codec = ", codec, ") of size ", size_repr(msgSetContent.size()), "\n");
                        }

                        for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.size(); p < e; ++msgIdx, ++msgSeqNum) {
                                const auto flags = decode_pod<uint8_t>(p);

                                if (trace_msgs) {
                                        SLog("Message ", msgIdx, ", flags ", flags, "\n");
                                }

                                if (sparseBundleBitSet) {
                                        if (msgIdx == 0) {
                                                msgSeqNum = firstMsgSeqNum;
                                        } else if (msgIdx == msgsSetSize - 1) {
                                                msgSeqNum = lastMsgSeqNum;
                                        } else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                // incremented in for() (in previous loop iteration)
                                                if (trace_msgs) {
                                                        SLog("SeqNumPrevPlusOne set\n");
                                                }
                                        } else {
                                                // we encode delta from last - 1, but we already ++msgSeqNum in for() (in previous iteration)
                                                msgSeqNum += Compression::decode_varuint32(p);

                                                if (trace_msgs) {
                                                        SLog("Adjusting delta\n");
                                                }
                                        }
                                }

                                if (trace_msgs) {
                                        SLog("SeqNum = ", msgSeqNum, "\n");
                                }

                                if (0 == (flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                        msgTs = decode_pod<uint64_t>(p);
                                }

                                if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                        key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                        p += key.size() + sizeof(uint8_t);

                                        if (trace_msgs) {
                                                SLog("MSG ", msgSeqNum, ", key [", key, "] ", Date::ts_repr(Timings::Milliseconds::ToSeconds(msgTs)), "\n");
                                        }

                                        if (codec) {
                                                key.p = reinterpret_cast<const char *>(static_cast<uintptr_t>(std::distance(ptrBase, key.data())) | ptrBit);
                                        }
                                } else {
                                        key.reset();
                                }

                                const auto msgLen = Compression::decode_varuint32(p);

                                if (msgLen || key) {
                                        msgValue.set(reinterpret_cast<const char *>(p), msgLen);
                                        p += msgLen;

                                        if (trace_msgs) {
                                                SLog("value [", msgValue, "]\n");
                                        }

                                        if (codec) {
                                                msgValue.p = reinterpret_cast<const char *>(static_cast<uintptr_t>(std::distance(ptrBase, msgValue.data())) | ptrBit);
                                        }

                                        msgs.push_back({key,
                                                        msgSeqNum,
                                                        msgTs,
                                                        msgValue});
                                } else {
                                        // Drop deleted messages(messages with a key and no content)
                                        anyDropped = true;
                                }
                        }
                }

                if (trace) {
                        SLog("Preocessing segment of size ", size_repr(fileSize), "\n");
                }
        }

        if (cur) {
                // patch
                const auto n        = msgs.size();
                const auto cur_data = cur->data();

                while (base < n) {
                        auto &     it  = msgs[base++];
                        const auto ptr = reinterpret_cast<uintptr_t>(it.content.data());

                        if (const auto to = ptr & ~ptrBit; ptr != to) {
                                it.content.p = cur_data + to;
                        }

                        if (it.key) {
                                const auto ptr = reinterpret_cast<uintptr_t>(it.key.data());

                                if (const auto to = ptr & ~ptrBit; ptr != to) {
                                        it.key.p = cur_data + to;
                                }
                        }
                }
        }

        if (trace) {
                SLog("Done collecting segments. Took ", duration_repr(Timings::Microseconds::Since(before)), ", ", dotnotation_repr(msgs.size()), " msgs\n");
        }

        if (!compact(msgs)) {
		// TODO: https://github.com/phaistos-networks/TANK/issues/72
		// maybe just do this anyway if we can reduce the number of RO Logs
                if (trace) {
                        SLog("No need for compaction, prevSegments.size() = ", prevSegments.size(), "\n");
                }

                run_on_main_thread([log]() {
                        log->compacting.store(false);
                        Print("Did not need to compact log\n");
                });

                return;
        }

        if (trace_msgs) {
                for (const auto &it : msgs) {
                        Print(it.seqNum, " [", it.key, "] [", it.content, "]\n");
                }
        }

        if (trace) {
                SLog(dotnotation_repr(msgs.size()), " messages\n");
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
                for (uint32_t i{0}; i < iovLen; ++i) {
                        auto &it  = iov[i];
                        auto  ptr = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(it.iov_base));

                        if (ptr & (1u << 31)) {
                                ptr ^= (1u << 31);
                                it.iov_base = out.data() + ptr;
                        } else {
                                ptr &= ~(1u << 30);
                                it.iov_base = cbuf.data() + ptr;
                        }
                }

                const auto r = writev(fd, iov, iovLen);

                if (unlikely(r == -1)) {
                        throw Switch::system_error("writev() failed:", strerror(errno));
                }

                out.clear();
                cbuf.clear();
                iovLen = 0;
        };

        try {
                const char *destPartitionPath;
                uint32_t    curSegmentIdx{0};

                destPartitionPath = basePartitionPath;

                //destPartitionPath = "/tmp/foo/0/"; // XXX:

                // Process all collected messages, pack into segments
                // XXX: this is not ideal
                // we are now generating as many new segments as we have previously
                // because we process messages for one one previous segment/time in order to produce a new matching segment
                for (uint32_t i{0}; i < n;) {
                        uint8_t      bundleFlags;
                        size_t       outFileSize{0};
                        size_t       sinceLastUpdateBytes{UINT32_MAX}, sinceLastUpdateMsgsCnt{UINT32_MAX};
                        auto         curSegment = prevSegments[curSegmentIdx];
                        const auto   baseSeqNum{all[i].seqNum};
                        auto         curSegmentLastAvailSeqNum = curSegment->lastAvailSeqNum;
                        index_record indexLastRecorded;
                        auto         expected = all[i].seqNum;

                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_blue, "Now processing segment ", curSegmentIdx, "/", prevSegments.size(), " (", baseSeqNum, ", ", curSegment->lastAvailSeqNum, ")", ansifmt::reset, "\n");
                        }

                        // new segment
                        index.clear();
                        out.clear();
                        cbuf.clear();
                        iovLen = 0;

                        Snprint(logPath, sizeof(logPath), destPartitionPath, baseSeqNum, "-", 0, "_", 0, ".ilog.cleaned");
                        fd = open(logPath, O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC, 0775);
                        TANK_EXPECT(fd != -1);

                        DEFER({
                                if (fd != -1) {
                                        TANKUtil::safe_close(fd);
                                }
                        });

                        for (;;) {
                                // new bundle
                                const auto base{i};
                                const auto upto = std::min<size_t>(n, i + maxBundleMsgsSetSize);
                                bool       asSparse{false};
                                size_t     sum{0};

                                do {
                                        const auto n = all[i].seqNum;

                                        asSparse |= (n != expected);

                                        if (trace_msgs) {
                                                SLog("consider ", i, " ", all[i].seqNum, "\n");
                                        }

                                        expected = n + 1;
                                        sum += all[i].key.size() + all[i].content.size() + 8;
                                } while (++i < upto &&
                                         sum < maxBundleMsgsSetSizeBytes &&
                                         all[i].seqNum <= curSegmentLastAvailSeqNum);

                                if (trace_msgs) {
                                        SLog("expected = ", expected, ", asSparse = ", asSparse, "\n");
                                }

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
                                                out.encode_varuint32(msgSetSize);
                                        }

                                        const auto first = all[base].seqNum, last = all[i - 1].seqNum;

                                        if (trace_msgs) {
                                                SLog("Sparse bundle first = ", first, ", last = ", last, ", set size = ", msgSetSize, "\n");
                                        }

                                        out.Serialize<uint64_t>(first);
                                        if (msgSetSize != 1) {
                                                out.encode_varuint32(last - first - 1);
                                        }
                                } else {
                                        bundleFlags = 0;

                                        if (msgSetSize < 16) {
                                                bundleFlags |= (msgSetSize << 2);
                                                out.Serialize(bundleFlags);
                                        } else {
                                                out.Serialize(bundleFlags);
                                                out.encode_varuint32(msgSetSize);
                                        }
                                }

                                const auto savedOutFileSize   = outFileSize;
                                const auto bundleHeaderLength = out.size() - bundleHeaderFlagsOffset;
                                uint64_t   lastTS{0};
                                const auto msgSetOffset = out.size();

                                sinceLastUpdateMsgsCnt += msgSetSize;
                                outFileSize += bundleHeaderLength;

                                iov[iovLen++] = {(void *)uintptr_t(bundleHeaderFlagsOffset | (1u << 31)), bundleHeaderLength};

                                if (trace_msgs) {
                                        SLog("Encoding Messages Set asSparse = ", asSparse, "\n");
                                }

                                for (uint32_t k{base}; k < i; ++k) {
                                        const auto &m        = all[k];
                                        uint8_t     msgFlags = m.key ? uint8_t(TankFlags::BundleMsgFlags::HaveKey) : uint8_t(0);
                                        bool        encodeTS, encodeSparseDelta;

                                        if (asSparse && k != base && k != i - 1) {
                                                if (m.seqNum == all[k - 1].seqNum + 1) {
                                                        msgFlags |= uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                                        encodeSparseDelta = false;
                                                } else {
                                                        encodeSparseDelta = true;
                                                }
                                        } else {
                                                encodeSparseDelta = false;
                                        }

                                        if (trace_msgs) {
                                                SLog("message ", k - base, " ", m.seqNum, ", asSparse = ", asSparse, ", encodeSparseDelta = ", encodeSparseDelta, "\n");
                                        }

                                        if (m.ts == lastTS && k != base) {
                                                msgFlags |= uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                                encodeTS = false;
                                        } else {
                                                lastTS   = m.ts;
                                                encodeTS = true;
                                        }

                                        out.Serialize(msgFlags);

                                        if (encodeSparseDelta) {
                                                out.encode_varuint32(m.seqNum - all[k - 1].seqNum - 1);
                                                if (trace_msgs) {
                                                        SLog("Serializing delta ", m.seqNum - all[k - 1].seqNum - 1, "\n");
                                                }
                                        }

                                        if (encodeTS) {
                                                out.Serialize<uint64_t>(m.ts);
                                        }

                                        if (m.key) {
                                                out.Serialize(m.key.size());
                                                out.Serialize(m.key.data(), m.key.size());
                                        }

                                        out.encode_varuint32(m.content.size());
                                        out.Serialize(m.content.data(), m.content.size());
                                }

                                const auto msgSetLen = out.size() - msgSetOffset;

                                if (trace_msgs) {
                                        SLog("msgSetLen = ", msgSetLen, ", bundleFlags = ", bundleFlags, "\n");
                                }

                                if (msgSetLen > 1024) { // XXX: arbitrary
                                        const auto offset = cbuf.size();

                                        if (!Compression::Compress(Compression::Algo::SNAPPY, out.data() + msgSetOffset, msgSetLen, &cbuf)) {
                                                throw Switch::system_error("Compression failed");
                                        }

                                        const auto span = cbuf.size() - offset;

                                        if (span >= msgSetLen) {
                                                // not worth it
                                                if (trace_msgs) {
                                                        SLog("Not worth compressing bundle msgs set\n");
                                                }

                                                cbuf.resize(offset);
                                                goto l10;
                                        } else {
                                                iov[iovLen++] = {(void *)uintptr_t(offset | (1u << 30)), span};
                                                out.resize(msgSetOffset);

                                                *reinterpret_cast<uint8_t *>(out.data() + bundleHeaderFlagsOffset) |= 1; // set codec
                                                outFileSize += span;

                                                if (trace_msgs) {
                                                        SLog(">> Compressed ", msgSetLen, " ", span, "\n");
                                                }
                                        }
                                } else {
                                l10:
                                        iov[iovLen++] = {(void *)uintptr_t(msgSetOffset | (1u << 31)), msgSetLen};
                                        outFileSize += msgSetLen;
                                }

                                const auto bundleLength = outFileSize - savedOutFileSize;
                                const auto _l           = out.size();

                                out.encode_varuint32(bundleLength);
                                const auto bundleLengthReprLen = out.size() - _l;
                                iov[bundleLengthIOVIdx]        = {(void *)uintptr_t(_l | (1u << 31)), bundleLengthReprLen};

                                outFileSize += bundleLengthReprLen;

                                sinceLastUpdateBytes += bundleLength;
                                if (iovLen > sizeof_array(iov) - 16) {
                                        flush();
                                }

                                if (i == n) {
                                        // done and done
                                        break;
                                } else if (all[i].seqNum > curSegmentLastAvailSeqNum) {
                                        if (trace_msgs) {
                                                SLog("Consumed current segment ", curSegmentIdx, "\n");
                                        }

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
                                TANKUtil::safe_close(logFd);
                                throw Switch::system_error("Failed to access new segment's index:", strerror(errno));
                        }

                        if (write(fd, index.data(), index.size()) != index.size()) {
                                TANKUtil::safe_close(logFd);
                                TANKUtil::safe_close(fd);
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
                        newSegment->fileSize           = outFileSize;
                        newSegment->index.data         = reinterpret_cast<const uint8_t *>(mmap(nullptr, index.size(), PROT_READ, MAP_SHARED, fd, 0));
                        newSegment->index.fileSize     = index.size();
                        newSegment->index.lastRecorded = indexLastRecorded;

                        if (trace) {
                                SLog(newSegment->fileSize, " ", lseek64(newSegment->fdh->fd, 0, SEEK_END), "\n");
                        }

                        TANK_EXPECT(newSegment->index.fileSize == lseek64(fd, 0, SEEK_END));
                        TANK_EXPECT(newSegment->fileSize == lseek64(newSegment->fdh->fd, 0, SEEK_END));

                        TANKUtil::safe_close(fd);
                        fd = -1;

                        if (newSegment->index.data == MAP_FAILED) {
                                throw Switch::system_error("mmap() failed:", strerror(errno));
                        }

                        madvise((void *)newSegment->index.data, index.size(), MADV_DONTDUMP);

                        TANK_EXPECT(newSegment->fdh.use_count() == 1);
                        newSegments.push_back(newSegment.release());

                        if (trace) {
                                SLog("Out segment, output ", outFileSize, "(", size_repr(outFileSize), ") ", dotnotation_repr(index.size()), " index entries\n");
                        }
                }

                if (trace) {
                        SLog("Done scanning RO segments\n");
                }

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

                        if (const auto createdTS = it->createdTS) {
                                pathLen = Snprint(path, sizeof(path), basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, "_", createdTS, ".ilog.old");
                        } else {
                                pathLen = Snprint(path, sizeof(path), basePartitionPath, "/", it->baseSeqNum, "-", it->lastAvailSeqNum, ".ilog.old");
                        }

                        if (Unlink(path) == -1) {
                                throw Switch::system_error("Failed to remove ", strwlen32_t(path, pathLen), ": ", strerror(errno));
                        }

                        Snprint(path, sizeof(path), basePartitionPath, "/", it->baseSeqNum, ".index.old");
                        if (Unlink(path) == -1) {
                                throw Switch::system_error("Failed to unlink file:", strerror(errno));
                        }
                }

                // Replace segments
                run_on_main_thread([log, segments = std::move(prevSegments), newSegments = std::move(newSegments)]() {
                        auto       roSegments = log->roSegments.get();
                        auto       it         = std::find(roSegments->begin(), roSegments->end(), segments.front());
                        const auto upto       = segments.back()->lastAvailSeqNum;

                        TANK_EXPECT(it != roSegments->end());

                        if (trace) {
                                SLog("Now running on main thread\n");
                        }

                        // remove segments from current roSegments[]
                        std::for_each(it, it + segments.size(), [](auto ptr) { delete ptr; });
                        roSegments->erase(it, it + segments.size());

                        // replace removed segments with new segments
                        roSegments->insert(it, newSegments.begin(), newSegments.end());

                        if (trace) {
                                SLog("roSegments->size() now = ", roSegments->size(), "\n");
                        }

                        log->compacting.store(false);
                        if (!log->lastCleanupMaxSeqNum) {
                                this_service->track_log_cleanup(log);
                        }
                        log->lastCleanupMaxSeqNum = upto;

                        if (trace) {
                                SLog("Scheduling cleanup\n");
                        }

                        this_service->schedule_cleanup();

                        if (trace && false) {
                                for (auto it : *roSegments) {
                                        SLog("rosegment (base_seqnum:", it->baseSeqNum, ", last_avail_seqnum:", it->lastAvailSeqNum, ") fdh.use_count()", it->fdh.use_count(), " fd:", it->fdh->fd, "\n");
                                }
                        }

                        if (trace) {
                                Print("Compacted partition segments\n");
                        }
                });
        } catch (...) {
                while (!newSegments.empty()) {
                        delete newSegments.back();
                        newSegments.pop_back();
                }

                run_on_main_thread([log]() {
                        Print("Failed to compact partition segments\n");
                        log->compacting.store(false);
                });
        }
}

void Service::schedule_compaction(std::unique_ptr<pending_compaction> &&compaction) {
        static std::once_flag onceFlag;

        std::call_once(onceFlag, [this] {
                compactions.compaction_thread.reset(new std::thread([this]() {
                        std::vector<pending_compaction *> localWork;
                        sigset_t                          mask;

                        sigfillset(&mask);
                        pthread_sigmask(SIG_SETMASK, &mask, nullptr);
                        for (bool done = false; !done;) {
                                std::unique_lock<std::mutex> lock(compactions.workLock);

                                compactions.workCond.wait(lock, [this] { return compactions.pendingCompactions.any(); });
                                for (auto it = compactions.pendingCompactions.drain(); it; it = it->next) {
                                        localWork.push_back(it);
                                }
                                lock.unlock();

                                while (!localWork.empty()) {
                                        auto c = localWork.back();

                                        if (c->log) {
                                                try {
                                                        compact_partition(c->log, c->basePartitionPath, std::move(c->prevSegments));
                                                } catch (...) {
                                                        //
                                                }
                                        } else {
                                                done = true;
                                        }

                                        localWork.pop_back();
                                        delete c;
                                }
                        }
                }));
        });

        compactions.pendingCompactions.push_back(compaction.release());
        compactions.workCond.notify_one();
}

void Service::schedule_compaction(const char *base_partitition_path, topic_partition_log *log) {
	TANK_EXPECT(base_partitition_path);
	TANK_EXPECT(log);
        static constexpr bool     trace{false};
        std::vector<ro_segment *> prevSegments;

        if (bool expected{false}; !log->compacting.compare_exchange_weak(expected, true,
                                                                         std::memory_order_release,
                                                                         std::memory_order_relaxed)) {
                // TODO: do something that makes more sense here
                std::abort();
        }

        auto       compaction = std::make_unique<pending_compaction>();
        const auto l          = strlen(base_partitition_path);

        TANK_EXPECT(l < sizeof(compaction->basePartitionPath));

        compaction->log = log;
        strwlen32_t(base_partitition_path, l).ToCString(compaction->basePartitionPath);
        compaction->prevSegments.reserve(log->roSegments->size());

        for (auto it : *log->roSegments) {
                TANK_EXPECT(it->fdh);
                compaction->prevSegments.emplace_back(it);
        }

        if (trace) {
                SLog("Compaction for [", base_partitition_path, "]\n");
        }

        TANK_EXPECT(compaction->prevSegments.size());
        schedule_compaction(std::move(compaction));
}

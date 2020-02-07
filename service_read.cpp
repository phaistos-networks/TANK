#include "service_common.h"

static TANKUtil::range_start determine_consume_file_range_start(const uint64_t                                             abs_seqnum,
                                                                const uint64_t                                             max_abs_seq_num,
                                                                int                                                        fd,
                                                                const uint32_t                                             file_size,
                                                                uint32_t                                                   file_offset,
                                                                TANKUtil::read_ahead<TANKUtil::read_ahead_default_stride> &ra,
                                                                const uint64_t                                             consume_req_seqnum) {

        enum {
                trace = false,
        };

        TANK_EXPECT(fd != -1);
        TANK_EXPECT(ra.get_fd() != -1);
        TANK_EXPECT(ra.get_fd() == fd);

        auto                  o                      = file_offset;
        const auto            ceiling                = file_size;
        bool                  first_bundle_is_sparse = false;
        auto                  base_seqnum            = abs_seqnum;
        uint64_t              last_msg_seqnum;
        TANKUtil::range_start rs;

        rs.first_bundle_is_sparse = false;
        rs.abs_seqnum             = abs_seqnum;
        rs.file_offset            = file_offset;

        // even if (consume_req_seqnum == abs_seqnum)
        // we need to proceed, because we want to know if the first bundle is sparse

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, ansifmt::inverse, "determine_consume_file_range_start(abs_seqnum = ", abs_seqnum,
                     ", max_abs_seq_num = ", max_abs_seq_num,
                     ", file_size = ", file_size,
                     ", file_offset = ", file_offset,
                     ", consume_req_seqnum = ", consume_req_seqnum, ansifmt::reset, "\n");
        }

        while (o < ceiling) {
                const auto data = ra.read(o, sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));

                if (data.size() < 32) {
                        // TODO:
                }

                const auto        tiny_buf   = data.offset;
                const auto *const base_buf   = tiny_buf;
                const uint8_t *   p          = base_buf;
                const auto        bundle_len = Compression::decode_varuint32(p);

                TANK_EXPECT(bundle_len);

                const auto     encoded_bundle_len_len     = std::distance(base_buf, p); // how many bytes used to varint encode the bundle length
                const auto     bundleheader_flags         = decode_pod<uint8_t>(p);
                const bool     bundleheader_sparsebit_set = bundleheader_flags & (1u << 6);
                const uint32_t msgset_size                = ((bundleheader_flags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                if (bundleheader_sparsebit_set) {
                        const auto first_msg_seqnum = decode_pod<uint64_t>(p);

                        if (msgset_size != 1) {
                                last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1;
                        } else {
                                last_msg_seqnum = first_msg_seqnum;
                        }

                        first_bundle_is_sparse = true;
                } else {
                        first_bundle_is_sparse = false;
                }

                const auto next_bundle_base_seqnum = bundleheader_sparsebit_set
                                                         ? last_msg_seqnum + 1
                                                         : base_seqnum + msgset_size;

                if (consume_req_seqnum >= next_bundle_base_seqnum) {
                        // Our target is in later bundle
                        o += bundle_len + encoded_bundle_len_len;
                        base_seqnum = next_bundle_base_seqnum;

                        if (trace) {
                                SLog("Target consume_req_seqnum(", consume_req_seqnum,
                                     ") >= next_bundle_base_seqnum(", next_bundle_base_seqnum,
                                     ") in later bundle, advanced offset by ", bundle_len + encoded_bundle_len_len,
                                     " to = ", o, "\n");
                        }
                } else {
                        // Our target is in this bundle
                        if (trace) {
                                SLog("Target ", consume_req_seqnum, " in this bundle(offset ", o, ")\n");
                        }

                        rs.file_offset = o;
                        rs.abs_seqnum  = base_seqnum;
                        break;
                }
        }

        rs.first_bundle_is_sparse = first_bundle_is_sparse;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "{.first_bundle_is_sparse = ", rs.first_bundle_is_sparse,
                     ", .abs_seqnum = ", rs.abs_seqnum,
                     ", .file_offset = ", rs.file_offset, "}", ansifmt::reset, "\n");
        }

        return rs;
}

// Searches forward starting from `file_offset`, in order to determine how far ahead it should read based on `max_abs_seq_num` (usually, the highwater mark for the partition)
// `max_size` (how much data the client is interested in), and `file_size`
// in order to reduce the size of the chunk to be streamed.
//
// Now using a read_ahead
// This is important, because, e.g if indexInterval is high (e.g, over 4k) and/or
// message bundles are short (e.g containing 1 message with very small values)
// then we 'd need to pread() potentially dozens if not 100s of times for each message header
// until we get to where we need.
//
// With a read ahead, backed by a cache, we just read 8k and operate on that buffer directly
// which may sound expensive but it is not because:
// 1. we will reuse the read-ahead data in subsequent consume requests if possible
// 2. the cost of many reads for a few bytes over 1 read for a larger file segment is likely way higher
uint32_t determine_consume_file_range_end(uint64_t                                                   base_seqnum,
                                          const uint32_t                                             max_size,
                                          const uint64_t                                             max_abs_seq_num,
                                          int                                                        fd,
                                          const uint32_t                                             file_size,
                                          uint32_t                                                   file_offset,
                                          TANKUtil::read_ahead<TANKUtil::read_ahead_default_stride> &ra,
                                          const uint64_t                                             consume_req_seqnum) {
        enum {
                trace = false,
        };
        uint64_t   last_msg_seqnum;
        int        r;
        const auto limit = max_size != std::numeric_limits<uint32_t>::max()
                               ? std::min<uint32_t>(file_size, std::max<uint32_t>(file_offset + max_size, 32))
                               : file_size;
	[[maybe_unused]] const auto start_ = file_offset;

        TANK_EXPECT(fd != -1);
        TANK_EXPECT(ra.get_fd() != -1);
        TANK_EXPECT(ra.get_fd() == fd);

        if (trace) {
                SLog(ansifmt::color_brown, "Searching forward starting from file_offset(", file_offset,
                     ") in order to determine how far ahead we should read based on max_abs_seq_num(", max_abs_seq_num,
                     ") and file_size(", file_size,
                     ") in order to reduce the size of the chunk to be streamed", ansifmt::reset, "\n");

                SLog("Searching for max_abs_seq_num =  ",
                     max_abs_seq_num,
                     ", limit = ", limit,
                     "(max_size = ", max_size, "< ", size_repr(max_size), ">",
                     ", base_seqnum = ", base_seqnum,
                     ", file_offset = ", file_offset,
                     ", file_size = ", file_size, "), consume_req_seqnum = ", consume_req_seqnum, "\n");
        }

        for (;;) {
                if (file_offset >= file_size) {
                        if (trace) {
                                SLog("Stopping; past file size\n");
                        }

                        break;
                }

                if (file_offset >= limit) {
                        // only if we have crossed past our target
                        if (base_seqnum > consume_req_seqnum) {
                                if (trace) {
                                        SLog("Stopping: file_offset(", file_offset,
                                             ") >= limit(", limit,
                                             ") and base_seqnum(", base_seqnum,
                                             ") > consume_req_seqnum(", consume_req_seqnum, ")\n");
                                }

                                break;
                        }
                }

                // read bundle header
                // we may wind up reading a partial header, and that's OK
                const auto data = ra.read(file_offset, 4);

                if (unlikely(data.size() < 4)) {
                        // we didn't get to read enough
                        // point to the end of file
                        if (trace) {
                                SLog("ODD, read just ", r, " bytes, expected at least 4\n");
                        }

                        break;
                }

                const auto     buf                     = data.offset;
                const auto *   p                       = buf;
                const auto     bundle_len              = Compression::decode_varuint32(p);
                const auto     encoded_bundle_len_len  = std::distance(buf, const_cast<const uint8_t *>(p)); // how many bytes used to varint encode the bundle length
                const auto     bundle_hdr_flags        = decode_pod<uint8_t>(p);
                const bool     bundlehdr_sparsebit_set = bundle_hdr_flags & (1u << 6);
                const uint32_t msgset_size             = ((bundle_hdr_flags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                if (bundlehdr_sparsebit_set) {
                        const auto first_msg_seqnum = decode_pod<uint64_t>(p);

                        if (msgset_size != 1) {
                                last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1;
                        } else {
                                last_msg_seqnum = first_msg_seqnum;
                        }

                        if (trace) {
                                SLog("bundlehdr_sparsebit_set is set, first_msg_seqnum = ", first_msg_seqnum, ", last_msg_seqnum = ", last_msg_seqnum, "\n");
                        }
                }

                if (trace) {
                        SLog("Now at abs = ", base_seqnum,
                             "(msgset_size = ", msgset_size,
                             ") VS ", max_abs_seq_num,
                             " at ", file_offset,
                             ", encoded_bundle_len_len = ", encoded_bundle_len_len, "\n");
                }

                if (base_seqnum > max_abs_seq_num) {
                        // stop at bundle serialized at `file_offset`
                        if (trace) {
                                SLog("Stopping at bundle serialized at ", file_offset, " with first msg.seqnum = ", base_seqnum, "\n");
                        }

                        break;
                }

                // skip past this bundle
                if (trace) {
                        SLog(ansifmt::bold, "Skipping past this bundle, because base_seqnum(", base_seqnum,
                             " <= ", max_abs_seq_num,
                             "), to advance by ", bundle_len + encoded_bundle_len_len, ansifmt::reset, "\n");
                }

                if (bundlehdr_sparsebit_set) {
                        base_seqnum = last_msg_seqnum + 1;
                } else {
                        base_seqnum += msgset_size;
                }

                // Skip to the next message set
                file_offset += bundle_len + encoded_bundle_len_len;
        }

        if (trace) {
                SLog("Returning file_offset = ", file_offset, ", start = ", start_, " ", size_repr(file_offset - start_), "\n");
        }

        return file_offset;
}

// This is perhaps the hottest function in TANK
// We should trade performance for complexitity here
lookup_res topic_partition_log::read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t max_abs_seq_num) {
        // lock is expected to be locked
        static constexpr bool trace{false};
        TANK_EXPECT(absSeqNum >= cur.baseSeqNum);

        if (trace) {
                puts("\n\n\n\n\n");
                SLog(ansifmt::color_brown, ansifmt::underline, "absSeqNum = ", absSeqNum,
                     ", maxSize = ", maxSize,
                     ", max_abs_seq_num = ", max_abs_seq_num, ansifmt::reset, "\n");
        }

        if (cur.index.haveWideEntries) {
                // need to use the appropriate skipList64 and a different index encoding format
                IMPLEMENT_ME();
        }

        lookup_res  res;
        const auto  relSeqNum     = static_cast<uint32_t>(absSeqNum - cur.baseSeqNum);
        const auto &skipList      = cur.index.skipList;
        const auto  skiplist_data = skipList.data();
        const auto  skiplist_size = static_cast<int32_t>(skipList.size());
        int32_t     top           = skiplist_size - 1;
        bool        in_skiplist;

        for (int32_t btm = 0; btm <= top;) {
                const auto mid = btm + (top - btm) / 2;
                const auto it  = skiplist_data[mid].first;

                if (relSeqNum == it) {
                        top = mid;
                        break;
                } else if (relSeqNum < it) {
                        top = mid - 1;
                } else {
                        btm = mid + 1;
                }
        }

        res.fdh = cur.fdh;
	TANK_EXPECT(res.fdh.get());

        if (trace) {
                SLog("Got top = ", top, " / ", skiplist_size, "\n");
        }

#pragma mark Determine first message bundle (bundle first msg seqnum and file offset)
        if (top >= 0) {
                // Found offset of the message with the highest sequence number that is (<= relSeqNum)
                const auto it = skiplist_data + top;

                if (trace) {
                        SLog(ansifmt::color_green, "Memory Resident SL Hit:", ansifmt::reset, " Found ", absSeqNum,
                             "(", relSeqNum,
                             ") in SL snapped to ", cur.baseSeqNum + it->first,
                             " ", it->second, "\n");
                }

                res.absBaseSeqNum = cur.baseSeqNum + it->first;
                res.fileOffset    = it->second;

                in_skiplist = true;
        } else {
                // Unable to look it up in the memory resident skiplist; likely not recent, or
                // we just flushed it earlier in append_bundle()
                TANK_EXPECT(cur.index.ondisk.span); // we checked if it's in this current segment

                if (trace) {
                        SLog("Considering ondisk index (cur.fileSize = ", cur.fileSize, ")\n");
                }

                const auto        size          = cur.index.ondisk.span;
                const auto *const skiplist_data = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                const auto        skiplist_size = static_cast<int32_t>(size / sizeof(index_record));

                if (trace) {
                        const auto upto = std::min<size_t>(5, skiplist_size);

                        SLog("First ", upto, " skiplist checkpoints\n");
                        for (unsigned i = 0; i < upto; ++i) {
                                SLog(i, " {.relSeqNum = ", skiplist_data[i].relSeqNum, ", .absPhysical = ", skiplist_data[i].absPhysical, "}\n");
                        }
                }

                top = skiplist_size - 1;
                for (int32_t btm = 0; btm <= top;) {
                        const auto mid = btm + (top - btm) / 2;
                        const auto it  = skiplist_data[mid].relSeqNum;

                        if (relSeqNum == it) {
                                top = mid;
                                break;
                        } else if (relSeqNum < it) {
                                top = mid - 1;
                        } else {
                                btm = mid + 1;
                        }
                }

                if (trace) {
                        SLog("Got top = ", top, " / ", skiplist_size, "\n");
                }

                if (top >= 0) {
                        // Found offset of the message with the highest sequence number that is (<= relSeqNum)
                        const auto i = skiplist_data + top;

                        if (trace) {
                                SLog("In ondisk index (relSeqNum:", i->relSeqNum,
                                     ", absPhysical:", i->absPhysical, ")\n");
                        }

                        res.absBaseSeqNum = cur.baseSeqNum + i->relSeqNum;
                        res.fileOffset    = i->absPhysical;
                } else {
                        // We 'll start from the beginning because
                        // our skiplist's first message sequence number is > relSeqNum
                        res.absBaseSeqNum = cur.baseSeqNum;
                        res.fileOffset    = 0;
                        top               = 0; // XXX: important
                }

                in_skiplist = false;
        }

        const auto sl_index = top;

#pragma mark Seek to messages bundle that includes the message identified by the consume_req_seqnum for the beginning of the file range
        {
                // XXX: it is important that determine_consume_file_range_start() before we
                // attempt to determine_consume_file_range_end(). This is because
                // min_fetch_size in the consume request is epxressed in number of bytes from the bundle that includes the message
                // whereas we initially snap to the the closest message bundle in the index, and this can lead to all kind of issues
#if 1
                auto &file_range_start_cache = cur.file_range_start_cache;

                if (unlikely(file_range_start_cache.size() > 1024)) {
                        file_range_start_cache.clear();
                }

                const auto emplace_res = file_range_start_cache.emplace(absSeqNum,
                                                                        TANKUtil::range_start{});

                if (emplace_res.second) {
                        // We can safely cache the results here, unlike
                        // with determine_consume_file_range_end() which is a non-trivial matter
                        emplace_res.first->second = determine_consume_file_range_start(res.absBaseSeqNum,
                                                                                       max_abs_seq_num,
                                                                                       cur.fdh->fd,
                                                                                       cur.fileSize,
                                                                                       res.fileOffset,
                                                                                       cur.ra_proxy.ra,
                                                                                       absSeqNum);
                        if (trace) {
                                SLog(ansifmt::color_red, "Cache miss:", ansifmt::reset, " file_range_start_cache for ", res.absBaseSeqNum, "\n");
                        }
                } else if (trace) {
                        SLog(ansifmt::color_green, "Cache hit:", ansifmt::reset, " file_range_start_cache for ", res.absBaseSeqNum, "\n");

                        for (const auto &it : file_range_start_cache) {
                                SLog("Cache ", it.first, " {", it.second.abs_seqnum, ", ", it.second.file_offset, "}\n");
                        }
                }

                const auto &rs = emplace_res.first->second;
#else
                const auto rs = determine_consume_file_range_start(res.absBaseSeqNum,
                                                                   max_abs_seq_num,
                                                                   cur.fdh->fd,
                                                                   cur.fileSize,
                                                                   res.fileOffset,
                                                                   cur.ra_proxy.ra,
                                                                   absSeqNum);
#endif

                res.absBaseSeqNum          = rs.abs_seqnum;
                res.fileOffset             = rs.file_offset;
                res.first_bundle_is_sparse = rs.first_bundle_is_sparse;

                if (trace) {
                        SLog("res.absBaseSeqNum = ", res.absBaseSeqNum,
                             ", res.fileOffset = ", res.fileOffset,
                             ", res.first_bundle_is_sparse = ", res.first_bundle_is_sparse, "\n");
                }
        }

        if (trace) {
                SLog("Determined fileOffset = ", res.fileOffset, " / ", cur.fileSize, "\n");
                SLog("max_abs_seq_num = ", max_abs_seq_num, ", lastAssignedSeqNum = ", lastAssignedSeqNum, "\n");
        }

        if (max_abs_seq_num >= lastAssignedSeqNum) {
                const auto rem = cur.fileSize - res.fileOffset;

                if (trace) {
                        SLog("max_abs_seq_num(", max_abs_seq_num, ") >= lastAssignedSeqNum(", lastAssignedSeqNum, ")\n");
                }

                if (rem <= maxSize) {
                        // fast-path:
                        // we can safely extend range end to the end of the file
                        if (trace) {
                                SLog(ansifmt::color_red, "fast-path:", ansifmt::reset, " rem(", rem, ") <= maxSize(", maxSize, "), setting res.fileOffsetCeiling to cur.fileSize(", cur.fileSize, ")\n");
                        }

                        res.fileOffsetCeiling = cur.fileSize;
                        return res;
                } else if (trace) {
                        SLog(ansifmt::color_red, "slow-path: need to determine end of file range", ansifmt::reset, " rem(", rem, ") > maxSize(", maxSize, ")\n");
                }
        }

#pragma mark Fast-Path: Determine the end of the file range
        if (true) {
                // Turns out, we can just use the skip-lists to find the last indexed message bundle
                // where its seqnum <= max_abs_seq_num
                // and its file_offset <= cur.fileSize + maxSize
                // in order to avoid using determine_consume_file_range_end() unless we really need to
                const auto file_offset_ceiling = std::min<size_t>(cur.fileSize, res.fileOffset + maxSize);
                const auto abs_seqnum_ceiling  = max_abs_seq_num;

                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_red, ansifmt::inverse, "file_offset_ceiling = ", file_offset_ceiling,
                             ", abs_seqnum_ceiling = ", abs_seqnum_ceiling, ", in_skiplist = ", in_skiplist, ansifmt::reset, "\n");
                }

                if (in_skiplist) {
                        int32_t top = skiplist_size - 1;

                        if (trace) {
                                SLog("in-skiplist: top = ", top, ", btm = ", sl_index, "\n");
                        }

                        TANK_EXPECT(sl_index >= 0);
                        for (int32_t btm = sl_index; btm <= top;) {
                                const auto  mid            = btm + (top - btm) / 2;
                                const auto &it             = skiplist_data[mid];
                                const auto  msg_abs_seqnum = cur.baseSeqNum + it.first;

                                if (abs_seqnum_ceiling < msg_abs_seqnum || file_offset_ceiling < it.second) {
                                        top = mid - 1;
                                } else {
                                        btm = mid + 1;
                                }
                        }

                        TANK_EXPECT(top >= 0);
                        TANK_EXPECT(top < skiplist_size);

                        res.fileOffsetCeiling = skiplist_data[top].second;

                        if (trace) {
                                SLog("Ceiling msg.seqnum = ", skiplist_data[top].first + cur.baseSeqNum,
                                     ", offset = ", skiplist_data[top].second, "\n");
                        }

                        if (res.fileOffsetCeiling > res.fileOffset) {
                                if (trace) {
                                        SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "in-memory skiplist: Determined ceiling to be ", res.fileOffsetCeiling, ansifmt::reset, "\n");
                                }

                                return res;
                        } else if (trace) {
                                SLog("Ignoring ceiling from in-memory skiplist\n");
                        }

                } else {
                        // use the on-disk index
                        const auto        size           = cur.index.ondisk.span;
                        const auto        skip_list_size = size / sizeof(index_record);
                        const auto *const skip_list_data = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                        int32_t           top            = skip_list_size - 1;

                        if (trace) {
                                SLog("Starting from ", sl_index, ", top = ", top, "\n");
                        }

                        TANK_EXPECT(sl_index >= 0);
                        for (int32_t btm = sl_index; btm <= top;) {
                                const auto  mid            = btm + (top - btm) / 2;
                                const auto &it             = skip_list_data[mid];
                                const auto  msg_abs_seqnum = it.relSeqNum + cur.baseSeqNum;

                                if (abs_seqnum_ceiling < msg_abs_seqnum || file_offset_ceiling < it.absPhysical) {
                                        top = mid - 1;
                                } else {
                                        btm = mid + 1;
                                }
                        }

                        TANK_EXPECT(top >= 0);
                        TANK_EXPECT(top < skip_list_size);

                        res.fileOffsetCeiling = skip_list_data[top].absPhysical;

                        if (trace) {
                                SLog("Ceiling msg.seqnum = ", skip_list_data[top].relSeqNum + cur.baseSeqNum,
                                     ", offset = ", skip_list_data[top].absPhysical, "\n");
                        }

                        if (res.fileOffsetCeiling > res.fileOffset) {
                                if (trace) {
                                        SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "on-disk skiplist: Determined ceiling to be ", res.fileOffsetCeiling, ansifmt::reset, "\n");
                                }

                                return res;
                        } else if (trace) {
                                SLog(ansifmt::color_red, "Ignoring on-disk skiplist ceiling", ansifmt::reset, "\n");
                        }
                }
        }

#pragma mark Slow-Path: Determine the end of the file range
        // We weren't able to upper bound file offset via the skiplists
        // XXX: we can't cache the result of determine_consume_file_range_end()
        // unless the cache is invalidated as soon as cur.fileSize or max_abs_seq_num is accounted for
        // because determine_consume_file_range_end() depends on (max_abs_seq_num, file_size)
        // It doesn't really matter that much anyway; most of the time we 'll be able to snap to a message by consulting the ondisk index(see above)
        // and max_abs_seq_num will be >= lastAssignedSeqNum
        const auto lookup_file_offset = res.fileOffset;
        const auto lookup_seq_num     = res.absBaseSeqNum;

        if (trace) {
                SLog(ansifmt::color_red, "slow-path:", ansifmt::reset,
                     " Will determine_consume_file_range_end() for maxSize = ", maxSize, ", max_abs_seq_num = ", max_abs_seq_num, ", file_size = ", cur.fileSize, "\n");
        }

        res.fileOffsetCeiling = determine_consume_file_range_end(lookup_seq_num,
                                                                 maxSize,
                                                                 max_abs_seq_num,
                                                                 cur.fdh->fd, cur.fileSize,
                                                                 lookup_file_offset,
                                                                 cur.ra_proxy.ra,
                                                                 absSeqNum);

        if (trace) {
                SLog("max_abs_seq_num = ", max_abs_seq_num,
                     " fileOffsetCeiling = ", res.fileOffsetCeiling, "\n");
        }

        return res;
}

lookup_res topic_partition_log::range_for(uint64_t abs_seqnum, const uint32_t max_size, uint64_t max_abs_seq_num) {
        static constexpr bool trace{false};

        if (trace) {
                puts("\n\n\n\n\n\n");
                SLog(ansifmt::color_brown, ansifmt::inverse, ansifmt::bold, "range_for() ", ptr_repr(this), " abs_seqnum = ", abs_seqnum,
                     ", max_size = ", max_size,
                     ", max_abs_seq_num = ", max_abs_seq_num,
                     ", lastAssignedSeqNum = ", lastAssignedSeqNum,
                     ", firstAvailableSeqNum = ", firstAvailableSeqNum,
                     ", cur.baseSeqNum = ", cur.baseSeqNum, // can be std::numeric_limits<uint64_t>::max() if there's no current
                     ansifmt::reset, "\n");
        }

        if (abs_seqnum == 0) {
                if (trace) {
                        SLog("Asked to fetch starting from first available sequence number ", firstAvailableSeqNum, "\n");
                }

                if (firstAvailableSeqNum) {
                        abs_seqnum = firstAvailableSeqNum;
                } else {
                        // No content at all
                        if (trace) {
                                SLog("No content and abs_seqnum == 0: EOF\n");
                        }

                        return {lookup_res::Fault::AtEOF};
                }
        }

        // if (max_abs_seq_num != std::numeric_limits<uint64_t>::max())
        // then we are supposed to respect the highwater mark
        //
        // callees should special-case Fault::PastMax
        if (abs_seqnum > max_abs_seq_num) {
                if (trace) {
                        SLog(abs_seqnum, " is past max_abs_seq_num = ", max_abs_seq_num, "\n");
                }

                return {lookup_res::Fault::PastMax};
        } else if (abs_seqnum == lastAssignedSeqNum + 1) {
                if (trace) {
                        SLog("Asked for next(AtEOF)\n");
                }

                return {lookup_res::Fault::AtEOF};
        } else if (abs_seqnum > lastAssignedSeqNum) {
                if (trace) {
                        SLog("PAST abs_seqnum(", abs_seqnum, ") > lastAssignedSeqNum(", lastAssignedSeqNum, ")\n");
                }

                return {lookup_res::Fault::BoundaryCheck};
        } else if (abs_seqnum < firstAvailableSeqNum) {
                if (trace) {
                        SLog("< firstAvailableSeqNum\n");
                }

                return {lookup_res::Fault::BoundaryCheck};
        } else if (abs_seqnum >= cur.baseSeqNum) {
                // Great, definitely in the current ACTIVE segment
                if (trace) {
                        SLog("abs_seqnum(", abs_seqnum, ") >= cur.baseSeqNum(", cur.baseSeqNum, "). Will get from current\n");
                }

                return read_cur(abs_seqnum, max_size, max_abs_seq_num);
        }

        // not found in the active segment, fallback to the readlonly segments
        return range_for_immutable_segments(abs_seqnum, max_size, max_abs_seq_num);
}

lookup_res topic_partition_log::no_immutable_segment(const bool first_bundle_is_sparse) {
        // No segment, either one of the immutalbe segments or the current segment, that holds
        // a message with seqNum >= abs_seqnum
        //
        // so just point to the first(oldest) segment
        static constexpr const bool trace{false};
        const auto                  prevSegments = roSegments;

        if (!prevSegments->empty()) {
                const auto f = prevSegments->front();

                if (trace) {
                        SLog("Will use first R/O segment\n");
                }

                return {f->fdh, f->fileSize, f->baseSeqNum, 0, first_bundle_is_sparse};
        } else {
                if (trace) {
                        SLog("Will use current segment\n");
                }

                return {cur.fdh, cur.fileSize, cur.baseSeqNum, 0, first_bundle_is_sparse};
        }
}

lookup_res   topic_partition_log::from_immutable_segment(const topic_partition_log *const tpl,
                                                       ro_segment *const                f,
                                                       const uint64_t                   abs_seqnum,
                                                       const uint32_t                   max_size,
                                                       const uint64_t                   max_abs_seq_num) {
#pragma mark snap to the offset derived from the first message set in the index with sequence number <= abs_seqnum
        static constexpr const bool trace{false};

        if (unlikely(false == f->prepare_access(tpl->partition))) {
                lookup_res res;

                res.fault                  = lookup_res::Fault::SystemFault;
                res.fileOffsetCeiling      = 0;
                res.fdh                    = nullptr;
                res.absBaseSeqNum          = 0;
                res.fileOffset             = 0;
                res.first_bundle_is_sparse = false;

                return res;
        }

        const auto                  skiplist_size = static_cast<int32_t>(f->index.fileSize / sizeof(index_record));
        TANK_EXPECT(skiplist_size);
        const auto skiplist_data = reinterpret_cast<const index_record *>(f->index.data);
        int32_t    top           = skiplist_size - 1;
        const auto relSeqNum     = abs_seqnum - f->baseSeqNum;

        for (int32_t btm = 0; btm <= top;) {
                const auto mid = btm + (top - btm) / 2;
                const auto it  = skiplist_data[mid].relSeqNum;

                if (relSeqNum == it) {
                        top = mid;
                        break;
                } else if (relSeqNum < it) {
                        top = mid - 1;
                } else {
                        btm = mid + 1;
                }
        }

        const auto            sl_index    = top;
        const auto &          skiplist_it = skiplist_data[top >= 0 ? top : 0];
        ro_segment_lookup_res res{
            .record = skiplist_it,
            .span   = static_cast<uint32_t>(f->fileSize - skiplist_it.absPhysical)};
        uint32_t                                                  offsetCeil;
        TANKUtil::read_ahead<TANKUtil::read_ahead_default_stride> ra(f->fdh->fd);
        bool                                                      first_bundle_is_sparse;

        if (trace) {
                SLog(ansifmt::bold, "Snapped down to relSeqNum = ", res.record.relSeqNum,
                     ", absSeqNum = ", res.record.relSeqNum + f->baseSeqNum,
                     ", absPhysical = ", res.record.absPhysical,
                     ", span = ", res.span,
                     " (i.e bytes from absPhysical until the end of the file)", ansifmt::reset, "\n");
        }

        TANK_EXPECT(f->fdh.use_count());
        TANK_EXPECT(f->fdh->fd != -1);
        TANK_EXPECT(abs_seqnum >= f->baseSeqNum && abs_seqnum <= f->lastAvailSeqNum);

        if (trace) {
                SLog("Found in RO segment (f.baseSeqNum = ", f->baseSeqNum, ", f.lastAvailSeqNum = ", f->lastAvailSeqNum, ")\n");
        }

#pragma mark determine file range start
        {
                // see comments in other call sites of determine_consume_file_range_start() for why we need to
                // invoke it before we invoke determine_consume_file_range_end()
                // TODO: we can trivially cache this (see read_cur() impl.)
                // although it doesn't matter as much; most consumers will access cur
                const auto rs = determine_consume_file_range_start(f->baseSeqNum + res.record.relSeqNum,
                                                                   max_abs_seq_num,
                                                                   f->fdh->fd,
                                                                   f->fileSize,
                                                                   res.record.absPhysical,
                                                                   ra,
                                                                   abs_seqnum);

                res.record.relSeqNum   = rs.abs_seqnum - f->baseSeqNum;
                res.record.absPhysical = rs.file_offset;
                first_bundle_is_sparse = rs.first_bundle_is_sparse;

		if (trace) {
			SLog("Determined start {absSeqNum = ", rs.abs_seqnum, ", offset = ", rs.file_offset, "\n");
		}
        }

        if (max_abs_seq_num >= f->lastAvailSeqNum) {
                const auto rem = f->fileSize - res.record.absPhysical;

                if (rem <= max_size) {
                        // fast-path
                        if (trace) {
                                SLog(ansifmt::color_green, "fast-path", ansifmt::reset, " rem(", rem, ") <= max_size(", max_size, ")\n");
                        }

                        offsetCeil = f->fileSize;
                        goto l100;
                }
        }

#pragma mark determine file range end
        if (res.record.absPhysical + max_size >= f->fileSize && max_abs_seq_num >= f->lastAvailSeqNum) {
                if (trace) {
                        SLog("Requested upto ", res.record.absPhysical + max_size,
                             " >= ", f->fileSize,
                             ", max_abs_seq_num(", max_abs_seq_num,
                             ") >= f->lastAvailSeqNum(", f->lastAvailSeqNum,
                             "): range ends at end of file\n");
                }

                offsetCeil = f->fileSize;
                goto l100;
        }

#pragma mark Fast-Path
        {
                const auto file_offset_ceiling = std::min<size_t>(f->fileSize, res.record.absPhysical + max_size);
                const auto abs_seqnum_ceiling  = max_abs_seq_num;

                top = skiplist_size - 1;
                for (int32_t btm = sl_index; btm <= top;) {
                        const auto  mid            = btm + (top - btm) / 2;
                        const auto &it             = skiplist_data[mid];
                        const auto  msg_abs_seqnum = f->baseSeqNum + it.relSeqNum;

                        if (abs_seqnum_ceiling < msg_abs_seqnum || file_offset_ceiling < it.absPhysical) {
                                top = mid - 1;
                        } else {
                                btm = mid + 1;
                        }
                }

                TANK_EXPECT(top >= 0);
                TANK_EXPECT(top < skiplist_size);

                offsetCeil = skiplist_data[top].absPhysical;
                if (offsetCeil > res.record.absPhysical) {
                        goto l100;
                }
        }

#pragma mark Slow-Path
        {
                // Scan forward for the last message with (abs_seqnum < max_abs_seq_num)
                if (trace) {
                        SLog("Will scan forward for the last message with (", abs_seqnum, " < ", max_abs_seq_num, ")\n");
                }

                const auto lookup_file_offset = res.record.absPhysical;
                const auto lookup_seq_num     = f->baseSeqNum + res.record.relSeqNum;

                // TODO:
                // Unlike read_cur(), we can safely cache the result of determine_consume_file_range_end()
                offsetCeil = determine_consume_file_range_end(lookup_seq_num,
                                                              max_size,
                                                              max_abs_seq_num,
                                                              f->fdh->fd,
                                                              f->fileSize,
                                                              lookup_file_offset,
                                                              ra,
                                                              abs_seqnum);
        }

l100:
        if (trace) {
                SLog(ansifmt::color_brown, "Returning offsetCeil = ", offsetCeil,
                     ", f.baseSeqNum + ", res.record.relSeqNum,
                     " abs_seqnum = ", f->baseSeqNum + res.record.relSeqNum,
                     ", absPhysical = ", res.record.absPhysical, ansifmt::reset, " (span = ", size_repr(offsetCeil - res.record.absPhysical), ")\n");
        }

        return {f->fdh, offsetCeil,
                f->baseSeqNum + res.record.relSeqNum,
                res.record.absPhysical, first_bundle_is_sparse};
}

// Consider all RO segments. Look for the segment which includes abs_seqnum
// i.e where abs_seqnum is in the range [first_abs_seq_num, last_abs_seq_num]
//
// It dels with 'gaps' and other edge cases
//
// We don't really need to optimize this function as much as we need to care for read_cur()
// Most clients are likely going to be consuming from the current segment / tailing it, so to speak
lookup_res topic_partition_log::range_for_immutable_segments(uint64_t abs_seqnum, const uint32_t max_size, uint64_t max_abs_seq_num) {
        static constexpr bool trace{false};
        auto                  prevSegments = roSegments.get();
        const auto            size         = static_cast<int32_t>(prevSegments->size());
        const auto            data         = prevSegments->data();
        auto                  top          = size - 1;

        for (int32_t btm = 0; btm <= top;) {
                const auto mid = btm + (top - btm) / 2;
                const auto it  = data[mid]->baseSeqNum;

                if (abs_seqnum == it) {
                        top = mid;
                        break;
                } else if (abs_seqnum < it) {
                        top = mid - 1;
                } else {
                        btm = mid + 1;
                }
        }

        if (trace) {
                SLog("Looking for abs_seqnum = ", abs_seqnum,
                     ", max_size = ", max_size,
                     ", max_abs_seq_num = ", max_abs_seq_num,
                     " among ", prevSegments->size(), " RO segments\n");
        }

        if (top < 0) {
                // XXX: why first_bundle_is_sparse set to true?
                return no_immutable_segment(false);
        }

        TANK_EXPECT(top < size);

        // Solves:  https://github.com/phaistos-networks/TANK/issues/2
        // for e.g (1,26), (28, 50)
        // when you request 27 which may no longer exist because of compaction/cleanup, we need
        // to properly advance to the _next_ segment, and adjust abs_seqnum
        // accordingly
        //
        // TODO: we should probably come up with a binary search alternative to this linear search scan
        auto f = data[top];

        while (abs_seqnum > f->lastAvailSeqNum && ++top < size) {
                f          = data[top];
                abs_seqnum = f->baseSeqNum;
        }

        if (trace) {
                SLog(ansifmt::bold, "Found abs_seqnum ", abs_seqnum,
                     " in immutable segment with base seqnum ", f->baseSeqNum,
                     ", lastAvailSeqNum = ", f->lastAvailSeqNum,
                     ", fileSize = ", f->fileSize /* can be 0 if it hasn't been opened yet */, ansifmt::reset, "\n");
        }

        return from_immutable_segment(this, f, abs_seqnum, max_size, max_abs_seq_num);
}

// trampoline to topic_partition_log::range_for()
lookup_res topic_partition::read_from_local([[maybe_unused]] const bool fetch_only_committed, const uint64_t abs_seq_num, const uint32_t fetch_size) {
        static constexpr bool trace{false};
        const uint64_t        max_abs_seq_num = fetch_only_committed ? hwmark() : std::numeric_limits<uint64_t>::max();
        auto                  log             = _log.get();

        if (trace) {
                SLog("Fetching log segment for partition ", idx,
                     ", abs.sequence number ", abs_seq_num,
                     ", hwmark = ", hwmark(),
                     ", fetchSize ", fetch_size,
                     ", max_abs_seq_num = ", max_abs_seq_num,
                     " (fetch_only_committed = ", fetch_only_committed,
                     "), lastAssignedSeqNum = ", log->lastAssignedSeqNum, "\n");
        }

        return log->range_for(abs_seq_num, fetch_size, max_abs_seq_num);
}

#include "service_common.h"

#if 0 // no longer used
std::pair<uint32_t, uint32_t> ro_segment::snap_ceil(const uint64_t abs_seqnum) const {
        if (haveWideEntries) {
                IMPLEMENT_ME();
        }

        const auto        rel_seqnum = static_cast<uint32_t>(abs_seqnum - baseSeqNum);
        const auto *const all       = reinterpret_cast<const index_record *>(index.data);
        const auto *const end       = all + index.fileSize / sizeof(index_record);
        const auto        it        = std::lower_bound(all, end, rel_seqnum, [](const auto &a, const auto num) noexcept {
                return a.relSeqNum < num;
        });

        if (it == end) {
                // no index record where (record.rel_seqnum >= rel_seqnum)
                // that is, the last index record.rel_seqnum < rel_seqnum
                return {std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max()};
        } else {
                return {it->relSeqNum, it->absPhysical};
	}
}
#endif

// attempt sto determine the (bundle, offset) in the ro segment
// where (bundle.msgset.firstmsg.seqnum <= abs_seqnum)
// and offset is the offset in the file where that bundle is serialized
//
// it looks for the last bundle that may include the message with sequence number abs_seqnum
//
// It "snaps" to the last messages set tracked in the index where its first msg seqnunce number <= abs_seqnum
std::pair<uint32_t, uint32_t> ro_segment::snap_floor(const uint64_t abs_seqnum) const  TANK_NOEXCEPT_IF_NORUNTIME_CHECKS{
        static constexpr bool trace{false};

        if (haveWideEntries) {
                IMPLEMENT_ME();
        }

        const auto rel_seqnum = static_cast<uint32_t>(abs_seqnum - baseSeqNum);

        if (trace) {
                SLog("In snap_floor(): will determine to determine the (bundle, offset) in the RO segment where (bundle.msgset.firstmsg.seq_num <= ", abs_seqnum, ") and offset is the offset in the file where the bundle is serialized\n");
                SLog("abs_seqnum = ", abs_seqnum, ", rel_seqnum = ", rel_seqnum, "\n");
        }

        const auto *const all = reinterpret_cast<const index_record *>(index.data);
        const auto *const end = all + index.fileSize / sizeof(index_record);
        const auto        it  = std::upper_bound_or_match(all, end, rel_seqnum, [](const auto &a, const auto num) noexcept {
                return TrivialCmp(num, a.relSeqNum);
        });

        if (it == end) {
                // no index record where (record.rel_seqnum <= rel_seqnum)
                // that is, the first index record.rel_seqnum > rel_seqnum
                if (trace) {
                        SLog("Not found in SL (", rel_seqnum, ")\n");
                }

                return {0, 0};
        }

        if (trace) {
                SLog("Found rel_seq_num(", rel_seqnum, ") in skip-list snapped to: (relSeqNum = ", it->relSeqNum, ", absPhysical = ", it->absPhysical, "}\n");

                if (false) {
                        for (unsigned i = 0; i < index.fileSize / sizeof(index_record); ++i) {
                                const auto &it = all[i];

                                SLog(">> ", i, " ", it.relSeqNum, " ", it.absPhysical, "\n");
                        }
                }
        }

        return {it->relSeqNum, it->absPhysical};
}

ro_segment_lookup_res ro_segment::translate_floor(const uint64_t absSeqNum, const uint32_t max) const TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        const auto res = snap_floor(absSeqNum);

        return {{res.first, res.second}, fileSize - res.second};
}

// Searches forward starting from `file_offset`, in order to determine how far ahead it should read based on `max_abs_seq_num` and `file_size`
// in order to reduce the size of the chunk to be streamed.
//
// TODO(markp): read in 4k chunks/time or something more appropriate, reading 64bytes/time is just too expensive(need to trampoline to the kernel)
uint32_t search_before_offset(uint64_t       base_seqnum,
                              const uint32_t max_size,
                              const uint64_t max_abs_seq_num,
                              int            fd,
                              const uint32_t file_size,
                              uint32_t       file_offset) {
        static constexpr bool trace{false};
        uint8_t               buf[64];
        uint64_t              last_msg_seqnum;
        int                   r;
        const auto            limit = max_size != std::numeric_limits<uint32_t>::max()
                               ? std::min<uint32_t>(file_size, std::max<uint32_t>(file_offset + max_size, 8192))
                               : file_size;

        if (trace) {
                SLog(ansifmt::color_brown, "Searching forward starting from file_offset(", file_offset, ") in order to determine how far ahead we should read based on max_abs_seq_num(", max_abs_seq_num, ") and file_size(", file_size, ") in order to reduce the size of the chunk to be streamed", ansifmt::reset, "\n");
                SLog("Searching for max_abs_seq_num =  ",
                     max_abs_seq_num,
                     ", limit = ", limit,
                     "(max_size = ", max_size,
                     ", base_seqnum = ", base_seqnum,
                     ", file_offset = ", file_offset,
                     ", file_size = ", file_size, ")\n");
        }

	// we used snap_floor() to snap to a message set
	// so we can safely read assuming we are at the beginning of a message set
        while (file_offset < limit) {
                // read bundle header
                // we may wind up reading a partial header, and that's OK
                for (;;) {
                        r = pread64(fd, buf, sizeof(buf), file_offset);

                        if (-1 == r) {
                                if (EINTR == errno) {
                                        continue;
                                } else {
                                        throw Switch::system_error("pread() failed:", strerror(errno));
                                }
                        } else {
                                break;
                        }
                }

                if (unlikely(r < 4)) {
                        // we didn't get to read enough
                        // point to the end of file
                        if (trace) {
                                SLog("ODD, read just ", r, " bytes, expected at least 4\n");
                        }

			break;
                }

                const auto *   p                       = buf;
                const auto     bundle_len              = Compression::decode_varuint32(p);
                const auto     encoded_bundle_len_len  = std::distance(buf, const_cast<uint8_t *>(p)); // how many bytes used to varint encode the bundle length
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
                        SLog("Now at abs = ", base_seqnum, "(msgset_size = ", msgset_size, ") VS ", max_abs_seq_num, " at ", file_offset, "\n");
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
                        SLog(ansifmt::bold,"Skipping past this bundle, because base_seqnum(", base_seqnum, " <= ", max_abs_seq_num, "), to advance by ", bundle_len + encoded_bundle_len_len, ansifmt::reset, "\n");
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
                SLog("Returning file_offset = ", file_offset, "\n");
        }


#if 0 // actually, disregard all that
	// This is an important fix
	// 1. Assume we are looking for sequence number 12921505518
	// 2. We found a RO Segment with base sequence number 12911987799. Relative seqnum of 12921505518 in it is 9517719
	// 3. We snapped 9517719 via the skip-list to (rel_seqnum = 9517687, file_offset = 1073738775, abs_seq_num = 12921505486) in that RO Segment. span (i.e file_size - file_offset) = 4380
	// 4. In this function, we search starting at offset 1073738775 in order to determine the ceiling
	//
	// To accomplish this, we read from [1073738775, 1073743155). 1073743155 is the file size of the RO segmnet
	// The problem is, there is no other message set past the message set at 1073738775
	// so our (base_seqnum > max_abs_seq_num)
#endif




        return file_offset;
}

// We are operating on index boundaries, so our res.file_offset is aligned on an index boundary, which means
// we may stream (0, partition_config::indexInterval] excess bytes.
// This is probably fine, but we may as well
// scan ahead from that file_offset until we find an more appropriate file offset to begin streaming for, and adjust
// res.file_offset and res.base_seqnum accordingly if we can.
//
// If we don't adjust_range_start(), we'll avoid the scanning I/O cost, which should be minimal anyway, but we can potentially send
// more data at the expense of network I/O and transfer costs
//
// it returns true if it parsed the first bundle to stream, and that bundle is a sparse bundle (which means
// it encodes the sequence number of its first message in its header)
//
// See also search_before_offset()
bool adjust_range_start(lookup_res &                                                               res,
                        const uint64_t                                                             abs_seq_num,
                        robin_hood::unordered_map<uint64_t, adjust_range_start_cache_value> *const cache) {
        static constexpr bool trace{false};
        uint64_t              base_seqnum = res.absBaseSeqNum;

        TANK_EXPECT(res.fault == lookup_res::Fault::NoFault);

        if (trace) {
                SLog(ansifmt::color_green, "We are working on index boundaries, so our res.file_offset is aligned on an index boundary, which means we may stream (0, partition_config::indexInterval) excess bytes\n");
                SLog("We will scan AHEAD from that file offset until we find a more appropriate file offset to begin streaming for, and adjust res.file_offset and res.base_seqnum if possible", ansifmt::reset, "\n");
        }

        if (trace == false) {
                // explicitly allow so that we can verify it does the right thing when tracing
                if (base_seqnum == abs_seq_num || abs_seq_num <= 1) {
                        // No need for any adjustments
                        if (trace) {
                                SLog("No need for any adjustments\n");
                        }

                        return false;
                }
        }

        std::pair<std::remove_reference<decltype(*cache)>::type::iterator, bool> cache_res;

        if (cache) {
                // we only use this for the current segment
                // this should help with a thundering herd of consume requests
                cache_res = cache->emplace(abs_seq_num, adjust_range_start_cache_value{});

                if (!cache_res.second) {
                        // HIT
                        const auto &v = cache_res.first->second;

                        res.fileOffset    = v.file_offset;
                        res.absBaseSeqNum = v.seq_num;
                        return v.first_bundle_is_sparse;
                }
        }

        int        fd = res.fdh->fd;
        uint8_t    tiny_buf[sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)];
        const auto base_offset         = res.fileOffset;
        auto       o                   = base_offset;
        const auto file_offset_ceiling = res.fileOffsetCeiling;
        uint64_t   before              = trace ? Timings::Microseconds::Tick() : 0, last_msg_seqnum;
        bool       first_bundle_is_sparse{false};
        int        r;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_brown, "About to adjust range file_offset ", base_offset,
                     ", base_seqnum(", base_seqnum, "), abs_seq_num(", abs_seq_num,
                     "), file_offset_ceiling = ", file_offset_ceiling, ansifmt::reset, "\n");
        }

        while (o < file_offset_ceiling) {
                for (;;) {
                        r = pread64(fd, tiny_buf, sizeof(tiny_buf), o);

                        if (unlikely(-1 == r)) {
                                if (EINTR == errno) {
                                        continue;
                                } else {
                                        throw Switch::system_error("pread64() failed:", strerror(errno));
                                }
                        } else {
                                break;
                        }
                }

                if (unlikely(r < 32)) {
                        // TODO: what do we do?
                }

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

                        if (trace) {
                                SLog("bundleheader_sparsebit_set, first_msg_seqnum = ", first_msg_seqnum, ", last_msg_seqnum, ", last_msg_seqnum, "\n");
                        }

                        first_bundle_is_sparse = true;
                } else {
                        first_bundle_is_sparse = false;
                }

                const auto next_bundle_base_seqnum = bundleheader_sparsebit_set
                                                         ? last_msg_seqnum + 1
                                                         : base_seqnum + msgset_size;

                if (trace) {
                        SLog("Now at bundle(base_seqnum = ", base_seqnum,
                             "), msgset_size(", msgset_size,
                             "), bundleheader_flags(", bundleheader_flags,
                             "), next_bundle_base_seqnum = ",
                             next_bundle_base_seqnum, "\n");
                }

                if (abs_seq_num >= next_bundle_base_seqnum) {
                        // Our target is in later bundle
                        o += bundle_len + encoded_bundle_len_len;
                        base_seqnum = next_bundle_base_seqnum;

                        if (trace) {
                                SLog("Target abs_seqnum(", abs_seq_num,
                                     ") >= next_bundle_base_seqnum(", next_bundle_base_seqnum,
                                     ") in later bundle, advanced offset by ", bundle_len + encoded_bundle_len_len,
                                     " to = ", o, "\n");
                        }
                } else {
                        // Our target is in this bundle
                        if (trace) {
                                SLog("Target in this bundle(offset ", o, ")\n");
                        }

                        res.fileOffset    = o;
                        res.absBaseSeqNum = base_seqnum;
                        break;
                }
        }

        if (trace) {
                SLog(ansifmt::inverse, ansifmt::color_blue, "After adjustement, file_offset ", res.fileOffset,
                     ", fileOffsetCeiling = ", res.fileOffsetCeiling,
                     ", absBaseSeqNum(", res.absBaseSeqNum,
                     "), took  ", duration_repr(Timings::Microseconds::Since(before)), ansifmt::reset, "\n");
        }

        if (cache) {
                if (cache->size() > 8192) {
                        // conservative
                        cache->clear();
                }

                cache_res.first->second = adjust_range_start_cache_value{.first_bundle_is_sparse = first_bundle_is_sparse,
                                                                         .file_offset            = res.fileOffset,
                                                                         .seq_num                = res.absBaseSeqNum};
        }

        return first_bundle_is_sparse;
}

// TODO: https://github.com/phaistos-networks/TANK/issues/63
lookup_res topic_partition_log::read_cur(const uint64_t absSeqNum, const uint32_t maxSize, const uint64_t max_abs_seq_num) {
        // lock is expected to be locked
        static constexpr bool trace{false};
        TANK_EXPECT(absSeqNum >= cur.baseSeqNum);

        if (trace) {
                SLog(ansifmt::color_brown, "absSeqNum = ", absSeqNum, ", maxSize = ", maxSize, ", max_abs_seq_num = ", max_abs_seq_num, ansifmt::reset, "\n");
        }

        if (cur.index.haveWideEntries) {
                // need to use the appropriate skipList64 and a different index encoding format
                IMPLEMENT_ME();
        }

        lookup_res  res;
        bool        inSkiplist;
        const auto  relSeqNum = static_cast<uint32_t>(absSeqNum - cur.baseSeqNum);
        const auto &skipList  = cur.index.skipList;
        const auto  end       = skipList.end();
        const auto  it        = std::upper_bound_or_match(skipList.begin(), end, relSeqNum, [](const auto &a, const auto seqNum) noexcept {
                return TrivialCmp(seqNum, a.first);
        });

        res.fdh = cur.fdh;

        if (it != end) {
                // Found offset of the message with the highest sequence number that is (<= relSeqNum)
                if (trace) {
                        SLog("Found ", absSeqNum, "(", relSeqNum, ") in SL snapped to ", cur.baseSeqNum + it->first, " ", it->second, "\n");
                }

                res.absBaseSeqNum = cur.baseSeqNum + it->first;
                res.fileOffset    = it->second;

                inSkiplist = true;
        } else {
                // Unable to look it up in the memory resident skiplist; likely not recent, or
                // we just flushed it earlier in append_bundle()
                // Access the on-disk skiplist. We can use the finite-size cache for now
                auto &     cache     = cur.index.ondisk.cache;
                const auto cache_res = cache.emplace(relSeqNum, index_record{});

                if (!cache_res.second) {
                        // HIT
                        const auto &_v = cache_res.first->second;

                        if (trace) {
                                SLog("Cache Hit\n");
                        }

                        res.absBaseSeqNum = cur.baseSeqNum + _v.relSeqNum;
                        res.fileOffset    = _v.absPhysical;
                } else {
                        TANK_EXPECT(cur.index.ondisk.span); // we checked if it's in this current segment

                        if (trace) {
                                SLog("Considering ondisk index (cur.fileSize = ", cur.fileSize, ")\n");
                        }

                        const auto        size = cur.index.ondisk.span;
                        const auto *const all  = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                        const auto *const e    = all + size / sizeof(index_record);
                        const auto        i    = std::upper_bound_or_match(all, e, relSeqNum, [](const auto &a, const auto seqNum) noexcept {
                                return TrivialCmp(seqNum, a.relSeqNum);
                        });

                        if (i != e) {
                                // Found offset of the message with the highest sequence number that is (<= relSeqNum)
                                if (trace) {
                                        SLog("In ondisk index (relSeqNum:", i->relSeqNum, ", absPhysical:", i->absPhysical, ")\n");
                                }

                                res.absBaseSeqNum = cur.baseSeqNum + i->relSeqNum;
                                res.fileOffset    = i->absPhysical;

                                cache_res.first->second = *i;
                        } else {
                                // We 'll start from the beginning because
                                // our skiplist's first message sequence number is > relSeqNum
                                res.absBaseSeqNum = cur.baseSeqNum;
                                res.fileOffset    = 0;

                                cache_res.first->second = index_record{0, 0};
                        }

                        if (unlikely(cache.size() > 1024)) {
                                // keep it sane
                                cache.clear();
                        }
                }

                inSkiplist = false;
        }

        if (max_abs_seq_num != UINT64_MAX) {
                // Do the same, but this time looking for the upper/end of the range of messages we are interested in
                index_record   ref;
                const uint32_t key = static_cast<uint32_t>(max_abs_seq_num - cur.baseSeqNum);

                if (inSkiplist) {
                        const auto it = std::upper_bound_or_match(skipList.begin(), skipList.end(), key, [](const auto &a, const auto seqNum) noexcept {
                                return TrivialCmp(seqNum, a.first);
                        });

                        ref.relSeqNum   = it->first;
                        ref.absPhysical = it->second;

                        if (trace) {
                                SLog("Found ", key, " in skiplist at relSeqNum = ", ref.relSeqNum, ", absPhysical = ", ref.absPhysical, "\n");
                        }
                } else {
                        auto &     cache     = cur.index.ondisk.cache;
                        const auto cache_res = cache.emplace(key, index_record{});

                        if (!cache_res.second) {
                                // HIT
                                ref = cache_res.first->second;
                        } else {
                                const auto        size = cur.index.ondisk.span;
                                const auto *const all  = reinterpret_cast<const index_record *>(cur.index.ondisk.data);
                                const auto *const e    = all + size / sizeof(index_record);
                                const auto        it   = std::upper_bound_or_match(all, e, key, [](const auto &a, const uint32_t seqNum) noexcept {
                                        return TrivialCmp(seqNum, a.relSeqNum);
                                });

                                ref                     = *it;
                                cache_res.first->second = ref;

                                if (unlikely(cache.size() > 1024)) {
                                        cache.clear();
                                }
                        }
                }

#if 0
		res.fileOffsetCeiling = ref.absPhysical; 	// XXX: we actually need to set this to (it + 1).absPhysical so that we may not skip a bundle that includes
								// both the highwater mark but also messages with seqnum < highwater mark
#else
                // Yes, incur some tiny I/O overhead so that we 'll properly cut-off the content
                // i.e search in the range [ref.absPhysical, ref.absPhysical, min(ref.absPhysical + maxSize, cur.fileSize))
                // for an appropriate cut-off offset
                //
                // XXX: shouldn't we be looking for (res.fileOffset + maxSize) ?
                //WAS: res.fileOffsetCeiling = search_before_offset(cur.baseSeqNum, maxSize, max_abs_seq_num, cur.fdh->fd, cur.fileSize, ref.absPhysical);

                if (trace) {
                        SLog("Got ref.relSeqNum = ", ref.relSeqNum, ", ref.absPhysical = ", ref.absPhysical, ", cur.fileSize = ", cur.fileSize, ", cur.baseSeqNum = ", cur.baseSeqNum, "\n");
                }

                res.fileOffsetCeiling = search_before_offset(cur.baseSeqNum + ref.relSeqNum,
                                                             maxSize,
                                                             max_abs_seq_num,
                                                             cur.fdh->fd, cur.fileSize,
                                                             ref.absPhysical);
#endif

                if (trace) {
                        SLog("max_abs_seq_num = ", max_abs_seq_num, " fileOffsetCeiling = ", res.fileOffsetCeiling, "\n");
                }
        } else {
                // TODO: XXX: we should respect maxSize here
                // If we matches the base int he skiplist, we should at least walk the skiplist in order to get to
                // a (rel_seq_num, offset) where (offset - ref.absPhysical) <= maxSize
                res.fileOffsetCeiling = cur.fileSize;

                if (trace) {
                        SLog("res.fileOffsetCeiling = ", res.fileOffsetCeiling, "\n");
                }
        }

        return res;
}

lookup_res topic_partition_log::range_for(uint64_t abs_seqnum, const uint32_t max_size, uint64_t max_abs_seq_num) {
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::color_brown, ansifmt::inverse, ansifmt::bold, "range_for() abs_seqnum = ", abs_seqnum,
                     ", max_size = ", max_size,
                     ", max_abs_seq_num = ", max_abs_seq_num,
                     ", lastAssignedSeqNum = ", lastAssignedSeqNum,
                     ", firstAvailableSeqNum = ", firstAvailableSeqNum,
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

// Consider all RO segments. Look for the segment which includes abs_seqnum
// i.e where abs_seqnum is in the range [first_abs_seq_num, last_abs_seq_num] 
// 
// It dels with 'gaps' and other edge cases
lookup_res topic_partition_log::range_for_immutable_segments(uint64_t abs_seqnum, const uint32_t max_size, uint64_t max_abs_seq_num) {
        static constexpr bool trace{false};
        auto                  prevSegments = roSegments;
        const auto            end          = prevSegments->end();
        auto                  it           = std::upper_bound_or_match(prevSegments->begin(), end, abs_seqnum, [](const auto s, const auto abs_seqnum) noexcept {
                // we need that <=> operator
		// TODO: use C++20 spaceship operator
                return TrivialCmp(abs_seqnum, s->baseSeqNum);
        });

#if 0
	for (const auto it : *prevSegments) {
		if (abs_seqnum >= it->baseSeqNum) {
			SLog(ansifmt::bold, ansifmt::color_red, "MATCHED baseSeqNum = ", it->baseSeqNum, ", lastAssignedSeqNum = ", it->lastAvailSeqNum, " IN:", abs_seqnum <= it->lastAvailSeqNum, ansifmt::reset, "\n");
		}
	}
#endif


        if (trace) {
                SLog("Looking for abs_seqnum = ", abs_seqnum,
                     ", max_size = ", max_size,
                     ", max_abs_seq_num = ", max_abs_seq_num,
                     " among ", prevSegments->size(), " RO segments\n");
        }

        if (it != end) {
                // Solves:  https://github.com/phaistos-networks/TANK/issues/2
                // for e.g (1,26), (28, 50)
                // when you request 27 which may no longer exist because of compaction/cleanup, we need
                // to properly advance to the _next_ segment, and adjust abs_seqnum
                // accordingly
                //
                // TODO: we should probably come up with a binary search alternative to this linear search scan
                auto f = *it;

                while (abs_seqnum > f->lastAvailSeqNum && ++it != end) {
                        if (trace) {
                                SLog("Adjusting ", abs_seqnum, " to ", (*it)->baseSeqNum, "\n");
                        }

                        f          = *it;
                        abs_seqnum = f->baseSeqNum;
                }

                if (trace) {
                        SLog(ansifmt::bold, "Found abs_seqnum ", abs_seqnum, " in immutable segment with base seqnum ", f->baseSeqNum, ", lastAvailSeqNum = ", f->lastAvailSeqNum, ansifmt::reset, "\n");
                }

                const auto res = f->translate_floor(abs_seqnum, UINT32_MAX);
                uint32_t   offsetCeil;

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

                        if (false) {
                                for (const auto &it : *prevSegments) {
                                        SLog(">> (", it->baseSeqNum, ", ", it->lastAvailSeqNum, ")\n");
                                }
                        }
                }

                if (max_abs_seq_num != std::numeric_limits<uint64_t>::max()) {
                        // Scan forward for the last message with (abs_seqnum < max_abs_seq_num)
                        if (trace) {
                                SLog("Will scan forward for the last message with (", abs_seqnum, " < ", max_abs_seq_num, ")\n");
                        }

                        const auto r = f->snap_floor(max_abs_seq_num);

                        if (trace) {
                                SLog("snap_floor(", max_abs_seq_num, ") => ", r, "\n");
                        }

#if 0
			offsetCeil = r.second;  // XXX: we actually need to set this to (it + 1).absPhysical
#else
                        // Yes, incur some tiny I/O overhead so that we 'll properly cut-off the content
                        // WAS: offsetCeil = search_before_offset(f->baseSeqNum, max_size, max_abs_seq_num, f->fdh->fd, f->fileSize, r.second);

                        if (trace) {
                                SLog("Will now search_before_offset()\n");
                        }

                        offsetCeil = search_before_offset(f->baseSeqNum + r.first,
                                                          max_size,
                                                          max_abs_seq_num,
                                                          f->fdh->fd,
                                                          f->fileSize,
                                                          r.second);
#endif
                } else {
                        offsetCeil = f->fileSize;
                }

                if (trace) {
                        SLog("Returning offsetCeil = ", offsetCeil,
                             ", f.baseSeqNum + ", res.record.relSeqNum,
                             " = ", f->baseSeqNum + res.record.relSeqNum,
                             ", absPhysical = ", res.record.absPhysical, "\n");
                }

                return {f->fdh, offsetCeil,
                        f->baseSeqNum + res.record.relSeqNum,
                        res.record.absPhysical};
        } else if (trace) {
                SLog("*NOT found in SL*\n");
        }

        if (trace) {
                SLog("No segment suitable for abs_seqnum\n");
        }

        // No segment, either one of the immutalbe segments or the current segment, that holds
        // a message with seqNum >= abs_seqnum
        //
        // so just point to the first(oldest) segment
        if (!prevSegments->empty()) {
                const auto f = prevSegments->front();

                if (trace) {
                        SLog("Will use first R/O segment\n");
                }

                return {f->fdh, f->fileSize, f->baseSeqNum, 0};
        } else {
                if (trace) {
                        SLog("Will use current segment\n");
                }

                return {cur.fdh, cur.fileSize, cur.baseSeqNum, 0};
        }
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

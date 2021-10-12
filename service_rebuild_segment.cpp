#include <switch.h>
#include <ansifmt.h>
#include <compress.h>
#include "common.h"
#include <date.h>
#include <text.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <vector>

int rebuild_segment(const char *path) {
        struct msg final {
                uint64_t   seq_num;
                uint64_t   ts;
                str_view8  key;
                str_view32 content;
        };
        auto             s = str_view32::make_with_cstr(path);
        uint64_t         base_seqnum;
        std::vector<msg> bundle_msgs;

        if (const auto p = s.SearchR('/')) {
                s.advance_to(p + 1);
        }

        if (not s.StripSuffix(_S(".ilog"))) { // XXX: should also accept .log files; just need to account for it in base_seqnum parse
                Print("Expected a .log file\n");
                return 1;
        }

        if (const auto [first, ts] = s.divided('-'); first and ts and first.all_of_digits()) {
                base_seqnum = first.as_uint64();
        } else {
                Print("Unexpected file:", s, "\n");
                return 1;
        }

        int fd = open(path, O_RDONLY | O_LARGEFILE);

        if (-1 == fd) {
                Print("Failed to access ", path, ":", strerror(errno), "\n");
                return 1;
        }

        const auto file_size = lseek(fd, 0, SEEK_END);

        if (file_size <= 0) {
                Print("Unexpected file size:", file_size, "\n");
                close(fd);
                return 1;
        }

        Print("With base_seqnum ", base_seqnum, "\n");

        // TODO: use iovecs and multiple buffers(see compactions) for performance, so that we won't need to use 2 buffers because we need to encode the bundle length as varint
        auto        file_data  = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        uint32_t    next       = 0u;
        uint64_t    msg_seqnum = 0u; // for this application, it is relative to base_seqnum
        IOBuffer    out;
        IOBuffer    file_out;
        IOBuffer    buf;
        IOBuffer    c_buf;
        str_view8   key;
        std::size_t out_bytes      = 0u;
        bool        next_as_sparse = false;

        close(fd);

        if (file_data == MAP_FAILED) {
                Print("Failed to mmap():", strerror(errno), "\n");
                return 1;
        }

        madvise(file_data, file_size, MADV_SEQUENTIAL | MADV_DONTDUMP);
        file_out.reserve(std::min<std::size_t>(file_size, 512 * 1024 * 1024));

        for (const auto *p = static_cast<const uint8_t *>(file_data), *const e = p + file_size; p < e;) {
                // next bundle
                [[maybe_unused]] const auto bundle_base      = p;
                const auto                  bundle_len       = Compression::decode_varuint32(p);
                const auto                  next_bundle      = p + bundle_len;
                const auto                  bundle_hdr_flags = *(p++);
                const auto                  codec            = bundle_hdr_flags & 3u;
                const bool                  sparse_bundle    = bundle_hdr_flags & (1u << 6);
                const uint32_t              bundle_msgs_cnt  = ((bundle_hdr_flags >> 2) & 0xf) ?: Compression::decode_varuint32(p);
                const auto                  bogus            = bundle_msgs_cnt > bundle_len;
                uint64_t                    last_msg_seqnum;  // relative to base_seqnum
                uint64_t                    first_msg_seqnum; // relative to base_seqnum

                if (bogus) {
                        Print(ansifmt::bold, ansifmt::color_red, "Bogus ", bundle_msgs_cnt,
                              " for bundle of size ", bundle_len,
                              " (sparse:", sparse_bundle, "), for bundle first _relative_ message seqnum [", base_seqnum + next, ", ", base_seqnum + next + bundle_msgs_cnt, ") relative [", next, ", ", next + bundle_msgs_cnt, ")", ansifmt::reset, "\n");
                }

                if (sparse_bundle) {
                        first_msg_seqnum = decode_pod<uint64_t>(p); // this is _relative_ to the base sequence number of the segment, i.e base_seqnum

                        if (bundle_msgs_cnt != 1u) {
                                last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1u;
                        } else {
                                last_msg_seqnum = first_msg_seqnum;
                        }

                        next = last_msg_seqnum + 1u;
                } else {
                        first_msg_seqnum = next;
                        next += bundle_msgs_cnt;
                }

                // next now is the sequence number of the next bundle's first message

                const uint8_t *msgset_content     = p;
                std::size_t    msgset_content_len = std::distance(p, next_bundle);

                // advance to the next bundle
                p = next_bundle;

                if (bogus) {
                        // we are going to _skip_ this here bundle
                        // by skipping this, we are going to need to use a sparse bundle for the next bundle
                        // we could even disregard the range of messages in the bogus bundle but because other applications may be trackign that sequence number, that's
                        // could cause problens. We are effectively going to compact this here segment
                        //
                        // XXX: if this is the last bundle in the segment, that's going to cause problems. Maybe we need a bogus bundle anyway?
                        msg_seqnum     = next;
                        next_as_sparse = true;
                        Print("Will force sparse for next bundle, starting from (abs) ", next + base_seqnum, " (rel) ", next, "\n");
                        continue;
                }

#pragma mark collect bundle messages from the bundle's content
                uint64_t msg_ts    = 0u;
                uint32_t msg_index = 0u;

                if (codec) {
                        buf.clear();
                        if (not Compression::UnCompress(Compression::Algo::SNAPPY, msgset_content, msgset_content_len, &buf)) [[unlikely]] {
                                IMPLEMENT_ME();
                        }

                        msgset_content     = reinterpret_cast<const uint8_t *>(buf.data());
                        msgset_content_len = buf.size();
                }

                bundle_msgs.clear();
                for (const auto *p = msgset_content, *const e = p + msgset_content_len; p < e; ++msg_index, ++msg_seqnum) {
                        // for each msg in the bundle
                        const auto flags = *(p++);

                        if (sparse_bundle) {
                                if (0 == msg_index) {
                                        msg_seqnum = first_msg_seqnum;
                                } else if (msg_index == bundle_msgs_cnt - 1u) {
                                        msg_seqnum = last_msg_seqnum;
                                } else if (flags & unsigned(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                        //incremented in for(), i.e in previous loop iteration
                                } else {
                                        msg_seqnum += Compression::decode_varuint32(p);
                                }
                        }

                        if (0 == (flags & unsigned(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                msg_ts = decode_pod<uint64_t>(p);
                        }

                        if (flags & unsigned(TankFlags::BundleMsgFlags::HaveKey)) {
                                key.set(reinterpret_cast<const char *>(p + 1u), *p);
                                p += key.size() + sizeof(uint8_t);
                        } else {
                                key.reset();
                        }

                        const auto msg_len = Compression::decode_varuint32(p);

                        bundle_msgs.emplace_back(msg{
                            .seq_num = msg_seqnum,
                            .ts      = msg_ts,
                            .key     = key,
                            .content = str_view32(reinterpret_cast<const char *>(p), msg_len),
                        });

                        p += msg_len;

                        [[maybe_unused]] const auto abs_seqnum = msg_seqnum + base_seqnum;
                }

#pragma mark build new bundle
                // see comments about use of iovecs
                out.clear();

                auto         build_sparse         = sparse_bundle | next_as_sparse;
                uint64_t     expected             = first_msg_seqnum;
                const auto   out_bundle_msgs_size = bundle_msgs.size();
                uint8_t      out_bundle_flags     = 0u;
                const size_t new_bundle_offset    = 0u; // new bundle headers offset (excluding bundle length)
                const auto   all                  = bundle_msgs.data();
                uint64_t     last_ts              = 0u;

                for (const auto &it : bundle_msgs) {
                        build_sparse |= (it.seq_num != expected);
                        expected = it.seq_num + 1;
                }

                next_as_sparse = false;

                if (build_sparse) {
                        SLog("Will build sparse bundle, bundle first message sequence number (abs) ", bundle_msgs.front().seq_num + base_seqnum, " (rel) ", bundle_msgs.front().seq_num, "\n");
                }

                // sanity check
                if (expected != next) [[unlikely]] {
                        SLog("expected last: ", next, " is ", expected, " first expected:", first_msg_seqnum, " is ", bundle_msgs.front().seq_num, "\n");
                        std::abort();
                }

#pragma mark build bundle headers
                if (build_sparse) {
                        out_bundle_flags |= 1u << 6;

                        if (out_bundle_msgs_size < 16u) {
                                out_bundle_flags |= (out_bundle_msgs_size << 2u);
                                out.pack(out_bundle_flags);
                        } else {
                                out.pack(out_bundle_flags)
                                    .encode_varuint32(out_bundle_msgs_size);
                        }

                        // we store _absolute_ seq. numbers in the segment (the index stores _relative_ seqnums)
                        const auto first = all[0].seq_num + base_seqnum;
                        const auto last  = all[out_bundle_msgs_size - 1u].seq_num + base_seqnum;

                        out.pack(first);
                        if (out_bundle_msgs_size != 1u) {
                                out.encode_varuint32(last - first - 1u);
                        }
                } else {
                        out_bundle_flags = 0u;

                        if (out_bundle_msgs_size < 16u) {
                                out_bundle_flags |= (out_bundle_msgs_size << 2u);
                                out.pack(out_bundle_flags);
                        } else {
                                out.pack(out_bundle_flags)
                                    .encode_varuint32(out_bundle_msgs_size);
                        }
                }

#pragma mark build bundle content(bundle messages)
                const auto bundle_content_offset = out.size();

                for (uint32_t k = 0u; k < out_bundle_msgs_size; ++k) {
                        const auto &it        = all[k];
                        uint8_t     msg_flags = it.key ? unsigned(TankFlags::BundleMsgFlags::HaveKey) : 0u;
                        bool        encode_ts;
                        bool        encode_sparse_delta;

                        if (build_sparse and k and k != out_bundle_msgs_size - 1u) {
                                if (it.seq_num == all[k - 1].seq_num + 1u) {
                                        msg_flags |= unsigned(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne);
                                        encode_sparse_delta = false;
                                } else {
                                        encode_sparse_delta = true;
                                }
                        }

                        if (it.ts != last_ts and k) {
                                msg_flags |= unsigned(TankFlags::BundleMsgFlags::UseLastSpecifiedTS);
                                encode_ts = false;
                        } else {
                                last_ts   = it.ts;
                                encode_ts = true;
                        }

                        out.pack(msg_flags);

                        if (encode_sparse_delta) {
                                out.encode_varuint32(it.seq_num - bundle_msgs[k - 1u].seq_num - 1u);
                        }

                        if (encode_ts) {
                                out.pack(it.ts);
                        }

                        if (it.key) {
                                out.pack(static_cast<uint8_t>(it.key.size()))
                                    .serialize(it.key.data(), it.key.size());
                        }

                        out.encode_varuint32(it.content.size());
                        out.serialize(it.content.data(), it.content.size());
                }

                const auto bundle_content_size = out.size() - bundle_content_offset;

                if (bundle_content_size > 1024u) { // arbitrary
                        c_buf.clear();

                        if (not Compression::Compress(Compression::Algo::SNAPPY, out.data() + bundle_content_offset, bundle_content_size, &c_buf)) {
                                IMPLEMENT_ME();
                        }

                        const auto span = c_buf.size();

                        if (span < bundle_content_size) {
                                // replace with compressed content
                                out.resize(bundle_content_offset);
                                out.serialize(c_buf.data(), span);

                                *reinterpret_cast<uint8_t *>(out.data() + new_bundle_offset) |= 1u; // codec
                        }
                }

                file_out.encode_varuint32(out.size() - new_bundle_offset);
                file_out.serialize(out.data() + new_bundle_offset, out.size() - new_bundle_offset);
        }
        munmap(file_data, file_size);

        out_bytes += file_out.size();

        Print(size_repr(out_bytes), " from ", size_repr(file_size), "\n");

        // once saved, start a TANK instance, and  discover_partitions for the topic
        // it will rebuild the index
	if (auto fd = open("/tmp/segment.ilog", O_WRONLY|O_CREAT, 0775); -1 == fd) {
		perror("open()");
		std::abort();
	} else if (write(fd, file_out.data(), file_out.size()) != file_out.size()) {
		perror("write()");
		std::abort();
	} else {
		close(fd);
	}

        return 0;
}

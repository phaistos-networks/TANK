// See also http://en.cppreference.com/w/cpp/algorithm/iota
#pragma once
#include "switch_hash.h"

// range is (start():inclusive ... stop():exclusive)
// i.e [left, right)
template <typename VT = uint32_t, typename LT = uint32_t>
struct range_base final {
        using value_type  = VT;
        using length_type = LT;

        struct iterator {
                VT i;

                constexpr iterator(const VT index)
                    : i{index} {
                }

                constexpr VT inline operator*() const {
                        return i;
                }

                constexpr void operator++() noexcept {
                        ++i;
                }

                constexpr bool operator!=(const iterator &o) const noexcept {
                        return i != o.i;
                }
        };

        VT offset;
        LT len;

        constexpr iterator begin() const noexcept {
                return iterator(offset);
        }

        constexpr iterator end() const noexcept {
                return iterator(stop());
        }

        constexpr range_base()
            : offset{0}, len{0} {
        }

        constexpr range_base(const range_base &o)
            : offset(o.offset), len(o.len) {
        }

        constexpr range_base(const VT _o, const LT _l)
            : offset(_o), len(_l) {
        }

        //e.g range32_t{10,15}
        constexpr range_base(const std::pair<VT, VT> p)
            : offset{p.first}, len{p.second - p.first} {
        }

        constexpr range_base(const LT l)
            : offset{0}, len{l} {
        }

        constexpr operator bool() const noexcept {
                return len;
        }

        constexpr auto size() const noexcept {
                return len;
        }

        constexpr auto empty() const noexcept {
                return 0 == len;
        }

        constexpr bool SpansAll() const noexcept {
                return std::numeric_limits<VT>::min() == offset && std::numeric_limits<VT>::max() == stop();
        }

        void SetSpansAll() noexcept {
                offset = std::numeric_limits<VT>::min();
                len    = std::numeric_limits<VT>::max() - offset;
        }

        constexpr void Set(const VT _o, const LT _l) noexcept {
                offset = _o;
                len    = _l;
        }

        constexpr void set(const VT _o, const LT _l) noexcept {
                offset = _o;
                len    = _l;
        }


        constexpr void setStartEnd(const VT lo, const VT hi) noexcept {
                offset = lo;
                len    = hi - lo;
        }

        constexpr auto &operator=(const range_base &o) noexcept {
                offset = o.offset;
                len    = o.len;
                return *this;
        }

        constexpr void SetEnd(const VT e) noexcept {
                len = e - offset;
        }

        // Matching SetEnd(); adjusts offset of a valid range
        constexpr void reset_offset(const VT start) noexcept {
                len    = stop() - start;
                offset = start;
        }

        constexpr VT mid() const noexcept // (left + right) / 2
        {
                return offset + (len >> 1);
        }

        constexpr VT stop() const noexcept {
                return offset + len;
        }

        constexpr VT start() const noexcept {
                return offset;
        }

        // TODO: optimize
        // Very handy for iterating a subset e.g
        // for (auto i : range32(offset, perPage).ClippedTo(total) { .. }
        constexpr range_base<VT, LT> ClippedTo(const VT lim) const {
                range_base<VT, LT> res;

                res.offset = Min(offset, lim);
                res.len    = Min(stop(), lim) - res.offset;

                return res;
        }

        constexpr bool Contains(const VT o) const noexcept {
                // https://twitter.com/EricLengyel/status/546120250450653184
                // Single comparison impl. Works fine except shouldn't work for 64bit scalars

                return sizeof(VT) == 8
                           ? o >= offset && o < stop()
                           : uint32_t(o - offset) < len; // o in [offset, offset+len)
        }

        constexpr bool operator<(const range_base &o) const noexcept {
                return offset < o.offset || (offset == o.offset && len < o.len);
        }

        constexpr bool operator<=(const range_base &o) const noexcept {
                return offset < o.offset || (offset == o.offset && len <= o.len);
        }

        constexpr bool operator>(const range_base &o) const noexcept {
                return offset > o.offset || (offset == o.offset && len > o.len);
        }

        constexpr bool operator>=(const range_base &o) const noexcept {
                return offset > o.offset || (offset == o.offset && len >= o.len);
        }

        template <typename T>
        constexpr bool operator==(const T &o) const noexcept {
                return offset == o.offset && len == o.len;
        }

        template <typename T>
        constexpr bool operator!=(const T &o) const noexcept {
                return offset != o.offset || len != o.len;
        }

        range_base Intersection(const range_base &o) const noexcept {
                // A range containing the indices that exist in both ranges

                if (stop() <= o.offset || o.stop() <= offset)
                        return range_base(0, 0);
                else {
                        const auto _o = Max(offset, o.offset);

                        return range_base(_o, Min(stop(), o.stop()) - _o);
                }
        }

        bool intersects(const range_base &o) const noexcept {
                return Intersection(o);
        }

        void ClipOffsetTo(const VT o) noexcept {
                if (offset < o) {
                        if (o >= stop()) {
                                offset = o;
                                len    = 0;
                        } else {
                                const auto d = o - offset;

                                offset = o;
                                len -= d;
                        }
                }
        }

        void ClipEndTo(const VT e) noexcept {
                const auto end = stop();

                if (e < end) {
                        if (e < offset)
                                len = 0;
                        else
                                len -= end - e;
                }
        }

        constexpr bool Overlaps(const range_base &o) const noexcept {
                // range is (start() inclusive, stop() non inclusive)
                // e.g [start, end)
                //
                // alternative formula: a0 <= b1 && b0 <= a1
                // https://fgiesen.wordpress.com/2011/10/16/checking-for-interval-overlap/
                return !(stop() <= o.offset || o.stop() <= offset);
        }

        constexpr bool Contains(const range_base &o) const noexcept {
                return offset <= o.offset && stop() >= o.stop();
        }

        constexpr auto Union(const range_base &o) const noexcept {
                const auto _o = Min(offset, o.offset);

                return range_base(_o, Max(stop(), o.stop()) - _o);
        }

        // http://en.wikipedia.org/wiki/Disjoint_union
        // Make sure they overlap
        uint8_t DisjointUnion(const range_base &o, range_base *out) const noexcept {
                const range_base *const b = out;

                if (offset < o.offset) {
                        out->offset = offset;
                        out->len    = o.offset - offset;
                        ++out;
                } else if (o.offset < offset) {
                        out->offset = o.offset;
                        out->len    = offset - o.offset;
                        ++out;
                }

                const auto thisEnd = stop(), thatEnd = o.stop();

                if (thisEnd < thatEnd) {
                        out->offset = thisEnd;
                        out->len    = thatEnd - thisEnd;
                        ++out;

                } else if (thatEnd < thisEnd) {
                        out->offset = thatEnd;
                        out->len    = thisEnd - thatEnd;
                        ++out;
                }

                return out - b;
        }

        void TrimLeft(const LT span) noexcept {
                offset += span;
                len -= span;
        }

        [[deprecated("use reset() please")]] void Unset() {
                offset = 0;
                len    = 0;
        }

        constexpr void reset() noexcept {
                offset = 0;
                len    = 0;
        }
};

// e.g InBetweenRange(tm.tm_hour, 1, 5)
template <typename VT>
static constexpr bool IsBetweenRange(const VT v, const VT s, const VT e) noexcept {
        return sizeof(VT) == 8
                   ? v >= s && v < e
                   : uint32_t(v - s) < (e - s); // o in [offset, offset+len)
};

template <typename VT>
static constexpr bool IsBetweenRangeInclusive(const VT v, const VT s, const VT e) noexcept {
        return sizeof(VT) == 8
                   ? v >= s && v <= e
                   : uint32_t(v - s) <= (e - s); // o in [offset, offset+len]
};

template <typename VT = uint32_t, typename LT = uint32_t>
static inline uint32_t HashFor(const range_base<VT, LT> &r) {
        return SwitchHash(&r, sizeof(r));
}
#define RANGE32_FMT "(%u,%u:%u)"
#define RANGE32_TO_LABEL(r) (uint32_t)((r).offset), (uint32_t)((r).stop()), (uint32_t)((r).len)

using range8_t   = range_base<uint8_t, uint8_t>;
using range16_t  = range_base<uint16_t, uint16_t>;
using range32_t  = range_base<uint32_t, uint32_t>;
using range64_t  = range_base<uint64_t, uint64_t>;
using rangestr_t = range_base<const char *, uint32_t>; // Please ust strwlen instead

// great for iteration e.g
// {
// 	struct foo values[128];
// 	uint8_t cnt=5;
//
// 	for (const auto v : Switch::make_range(values, cnt)) { .. }
// }
template <typename VT, typename LT>
static constexpr auto MakeRange(const VT s, const LT l) noexcept -> range_base<VT, LT> {
        return {s, l};
}

namespace Switch {
        template <typename VT, typename LT>
        static constexpr auto make_range(const VT s, const LT l) noexcept {
                return range_base<VT, LT>(s, l);
        }
} // namespace Switch

namespace Switch {
        template <typename VT, typename LT>
        static inline auto clip_range(const range_base<VT, LT> a, const range_base<VT, LT> b, range_base<VT, LT> *out) {
                uint8_t    n     = 0;
                const auto b_end = b.offset + b.len;
                const auto a_end = a.offset + a.len;

                if (b_end > a_end) {
                        const auto o = std::max(a_end, b.offset);

                        out[n++] = range32_t(o, b_end - o);
                }

                if (b.offset < a.offset) {
                        const auto e = std::min(a.offset, b_end);

                        out[n++] = range32_t(b.offset, e - b.offset);
                }

                return n;
        }

} // namespace Switch

namespace std {
        template <typename VT, typename LT>
        struct hash<range_base<VT, LT>> {
                inline uint32_t operator()(const range_base<VT, LT> &r) const noexcept {
			return r.offset;
                }
        };
} // namespace std

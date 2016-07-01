#pragma once
#include "switch_numops.h"
#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <utility>

// http://gcc.gnu.org/onlinedocs/gcc/Other-Builtins.html
// returns true if this is a static const, most likely allocated in RODATA segment
// This is _very_ useful, for we can check if that's the case and just don't alloc()/copy/free() data instead just point to them
extern char etext, edata, end;

template <typename T>
[[gnu::always_inline]] inline static bool IsConstant(T *const expr)
{
        if (__builtin_constant_p(expr))
                return true;
        else
        {
                // This is only valid in either a macro or an inline function.
                // However, if you use it in an inlined function and pass an argument of the function as the argument to the built-in,
                // GCC never returns when you call the inline function with a string constant or compount literal, and does not return 1 when you
                // pass a constant number value to the inline function unless you specify the -o opetion
                //
                // So what really need to do is perhaps check if this is in RODATA segment, but how to do that?
                // Turns out, we can
                // Ref: http://stackoverflow.com/questions/4308996/finding-the-address-range-of-the-data-segment
                // Ref: http://manpages.ubuntu.com/manpages/jaunty/man3/end.3.html
                const uintptr_t addr = (uintptr_t)expr;

                return addr >= (uintptr_t)&etext && addr < (uintptr_t)&edata;
        }
}

template <typename LT, typename CT = char>
struct strwithlen
{
        const CT *p;
        LT len;

        using iterator = const CT *;
        using value_type = CT;

        strwithlen<LT> Div(const CT c)
        {
                if (const auto *const p = Search(c))
                {
                        const auto res = SuffixFrom(p + 1);

                        SetEnd(p);
                        return res;
                }
                else
                        return {};
        }

        std::pair<strwithlen, strwithlen> Divided(const CT c) const
        {
                if (const auto *const p = Search(c))
                        return {PrefixUpto(p), SuffixFrom(p + 1)};
                else
                        return {{this->p, this->len}, {}};
        }

        // warning: with copy assignment operator not allowed in union
        // UODATE: Well, we don't care now, C++11 allows for non PODs in unions as long as you initialize them
        strwithlen &operator=(const strwithlen &o)
        {
                p = o.p;
                len = o.len;
                return *this;
        }

        strwithlen &operator=(const CT *const s)
        {
                p = s;
                len = strlen(s);
                return *this;
        }

        [[gnu::always_inline]] inline bool IsConstant()
        {
                return p == nullptr || ::IsConstant(p);
        }

        inline uint8_t constexpr Length() const
        {
                return len;
        }

        strwithlen SubstringFrom(const CT *const s) const
        {
                return strwithlen(s, (p + len) - s);
        }

        strwithlen SubstringFrom(const LT o) const // Equivalent to SuffixFrom()
        {
                return strwithlen(p + o, len - o);
        }

        [[gnu::always_inline]] inline constexpr CT LastChar() const
        {
                return p[len - 1];
        }

        typename std::enable_if<std::is_same<char, CT>::value, LT>::type AsEscaped(CT *const out, const uint32_t available) const
        {

                return AsEscapedImpl((char *)p, len, (char *)out, available);
        }

        static inline uint8_t ToDec(const char c)
        {
                if (c >= 'a' && c <= 'f')
                        return c - 'a' + 10;
                else if (c >= 'A' && c <= 'F')
                        return c - 'A' + 10;
                else if (isdigit(c))
                        return c - '0';
                else
                        return UINT8_MAX;
        }

        LT CountOf(const CT c) const
        {
                LT res{0};

                for (const CT *it = p, *const e = it + len; it != e; ++it)
                {
                        if (*it == c)
                                ++res;
                }
                return res;
        }

        LT CountOf(const strwithlen needle) const
        {
                strwithlen in(p, len);
                LT cnt{0};

                while (const CT *const p = in.Search(needle))
                        in.AdvanceTo(p + needle.len);

                return cnt;
        }

        [[gnu::always_inline]] inline void FreeIfNotConstant()
        {
                if (IsConstant() == false)
                        free(const_cast<char *>(p));
                p = nullptr;
        }

        inline const CT *Search(const CT c) const
        {
                if (sizeof(CT) != sizeof(char))
                {
                        for (const CT *it = p, *const e = p + len; it != e; ++it)
                        {
                                if (*it == c)
                                        return it;
                        }
                        return nullptr;
                }
                else
                        return (char *)memchr((void *)p, c, len);
        }

        inline const typename std::enable_if<std::is_same<char, CT>::value, CT *>::type SearchR(const char c) const
        {
                return (CT *)memrchr((void *)p, c, len);
        }

        inline typename std::enable_if<std::is_same<char, CT>::value, CT *>::type SearchRWithLimit(const CT c, const LT limit) const
        {
                const auto l = std::min<LT>(len, limit);

                return (CT *)memrchr(p + len - l, c, l);
        }

        inline const CT *Search(const strwithlen needle) const
        {
                if (sizeof(CT) == sizeof(char))
                {
                        return (char *)memmem(p, len, needle.p, needle.len);
                }
                else
                {
                        const CT *const res = std::search(p, p + len, needle.p, needle.p + needle.len);

                        return res == p + len ? nullptr : res;
                }
        }

        inline const CT *Search(const CT *const needle, const LT needleLen) const
        {
                return Search(strwithlen(needle, needleLen));
        }

        // We can now deal with this just fine, C++11 makes it possible
        constexpr strwithlen()
            : p{nullptr}, len{0}
        {
        }

        strwithlen(const char *const s)
            : p{s}, len(strlen(s))
        {
        }

        strwithlen(const char *const s, const char *const e)
            : p{s}, len{LT(e - s)}
        {
        }

        int constexpr Cmp(const CT *const s, const LT l) const
        {
                return l == len ? memcmp(p, s, l)
                                : len < l ? ({const auto r = memcmp(p, s, len); r == 0 ? -1 : r; })
                                          : ({const auto r = memcmp(p, s, l); r == 0 ? 1 : r; });
        }

        inline int constexpr Cmp(const strwithlen *const o) const
        {
                return Cmp(o->p, o->len);
        }

        inline int constexpr Cmp(const strwithlen &o) const
        {
                return Cmp(o.p, o.len);
        }

        inline constexpr bool operator<(const strwithlen &o) const
        {
                return Cmp(&o) < 0;
        }

        inline constexpr bool operator>(const strwithlen &o) const
        {
                return Cmp(&o) > 0;
        }

        [[gnu::always_inline]] inline static uint32_t MaxLength()
        {
                static const uint64_t lens[] = {0, UINT8_MAX, UINT16_MAX, 0, UINT32_MAX, 0, 0, 0, UINT64_MAX};
                return lens[sizeof(LT)];
        }

        // using const uint32_t l not const LT l to silence compiler warnings
        strwithlen(const CT *const s, const uint32_t l)
            : p(s)
        {
                assert(l <= MaxLength());

                len = l;
        }

        [[gnu::always_inline]] inline constexpr operator bool() const
        {
                return len;
        }

        inline bool constexpr operator==(const strwithlen<LT> &o) const
        {
                return len == o.len && memcmp(p, o.p, len) == 0;
        }

        inline bool constexpr operator!=(const strwithlen<LT> &o) const
        {
                return len != o.len || memcmp(p, o.p, len);
        }

        // See range_base::Contains()
        inline bool constexpr Contains(const CT *const ptr) const
        {
                return sizeof(CT) == 8
                           ? ptr >= p && ptr < (p + len)
                           : uint32_t(ptr - p) < len;
        }

        inline bool constexpr Contains(const char *const op, const LT olen) const
        {
                return op >= p && op + olen <= p + len;
        }

        strwithlen Substr(const LT o, const LT l) const
        {
                assert(o + l <= len);

                return {p + o, l};
        }

        strwithlen Inset(const LT l, const LT r) const
        {
                const auto n = l + r;

                assert(n <= len);

                assert(n <= len);
                return {p + l, len - n};
        }

        template <typename T>
        inline bool constexpr Contains(const T &s) const
        {
                return Contains(s.p, s.len);
        }

        template <typename T>
        inline bool constexpr Contains(const T *const s) const
        {
                return Contains(*s);
        }

        inline bool Intersects(const CT *const op, const LT olen) const
        {
                const auto *const e = p + len, *const eo = op + olen;

                return e >= op && p <= eo;
        }

        template <typename T>
        inline bool Intersects(const T &s) const
        {
                return Intersects(s.p, s.len);
        }

        template <typename T>
        inline bool Intersects(const T *const s) const
        {
                return Intersects(*s);
        }

        void Unset()
        {
                p = nullptr;
                len = 0;
        }

        // Not using const LT l, so that compiler won't have to complain about missing casts
        void Set(const CT *const ptr, const uint32_t l)
        {
                len = l;
                p = ptr;
        }

        typename std::enable_if<std::is_same<char, CT>::value>::type Set(const char *const ptr)
        {
                p = ptr;
                SetLengthExpl(strlen((char *)p));
        }

        inline bool IsDigits() const
        {
                const CT *it = p, *const e = it + len;

                while (likely(it != e))
                {
                        if (!isdigit(*it))
                                return false;
                        ++it;
                }

                return len;
        }

        bool operator==(const CT *ptr) const
        {
                const auto *it = p;
                const auto *const end = p + len;

                while (it != end)
                {
                        if (*it != *ptr)
                                return false;
                        ++it;
                        ++ptr;
                }

                return *ptr == '\0';
        }

        double AsDouble() const
        {
                const auto *it = p, *const e = p + len;
                double sign;

                if (it == e)
                        return 0; // strtod() returns 0 for empty input
                else if (*it == '-')
                {
                        ++it;
                        sign = -1;
                }
                else if (unlikely(*it == '+'))
                {
                        ++it;
                        sign = 1;
                }
                else
                        sign = 1;

                double v{0};

                do
                {
                        if (*it == '.' || *it == ',') // support both radix characters
                        {
                                double exp{0};
                                // We could just use: double pow10{10.0} and in each iteration
                                // pow10*=10.0
                                // and then just use v += exp * (1.0l / pow10)
                                // but because we usualyl expect a digit digits for the exponents, we 'll just use a switch to avoid
                                // the multiplication
                                // UPDATE: nevermind, we 'll do that later
                                double pow10{1.0}; // faster than pow(exp, totalExpDigits)
                                // we could use case(totalExpDigits) in order to avoid this, or just use
                                //constexpr uint64_t scale[] = {pow(10, 1), pow(10, 2), pow(10, 3), pow(10, 4), pow(10, 5), pow(10, 6), pow(10, 7), ...

                                for (++it; it != e; ++it)
                                {
                                        if (likely(isdigit(*it)))
                                        {
                                                exp = exp * 10 + (*it - '0');
                                                pow10 *= 10.0;
                                        }
                                        else
                                        {
                                                // We could have handled exponent (e|E)
                                                // See: http://www.leapsecond.com/tools/fast_atof.c
                                                // but it's not really worth it; we never use it
                                                return NAN;
                                        }
                                }

                                return (v + (exp * (1.0L / pow10))) * sign;
                        }
                        else if (likely(isdigit(*it)))
                                v = v * 10 + (*(it++) - '0');
                        else
                                return NAN;

                } while (it != e);

                return v * sign;
        }

        typename std::enable_if<std::is_same<char, CT>::value, uint32_t>::type AsUint32() const
        {
                static constexpr uint32_t pow10[10] =
                    {
                        1000000000ul,
                        100000000ul,
                        10000000ul,
                        1000000ul,
                        100000ul,
                        10000ul,
                        1000ul,
                        100ul,
                        10ul,
                        1ul,
                    };

                // this test() and the test for d >= 10 really impact performance
                // so just don't do it
                if (unlikely(len > 10))
                {
                        // throw something?
                        return 0;
                }

                uint32_t res{0}, k{0};

                for (uint32_t i = sizeof_array(pow10) - len; k != len; ++i)
                {
                        const auto d = unsigned(p[k++]) - '0';

#if 0
                        if (unlikely(d >= 10))
                        {
                                // throw something?
                                return 0;
                        }
#endif

                        res += pow10[i] * d;
                }

                return res;
        }

        int32_t AsInt32() const
        {
                const auto *it = p, *const e = it + len;

                if (it != e)
                {
                        int32_t v{0};

                        if (*it == '-')
                        {
                                for (++it; it != e && isdigit(*it); ++it)
                                        v = v * 10 + (*it - '0');

                                return -v;
                        }
                        else
                        {
                                for (; it != e && isdigit(*it); ++it)
                                        v = v * 10 + (*it - '0');
                                return v;
                        }
                }

                return 0;
        }

        // See AsUint32() comments
        typename std::enable_if<std::is_same<char, CT>::value, uint64_t>::type AsUint64() const
        {
                static constexpr uint64_t pow10[20] __attribute__((__aligned__(64))) = // 20 because 20 digits are enough for a 64bit number
                    {
                        10000000000000000000ul,
                        1000000000000000000ul,
                        100000000000000000ul,
                        10000000000000000ul,
                        1000000000000000ul,
                        100000000000000ul,
                        10000000000000ul,
                        1000000000000ul,
                        100000000000ul,
                        10000000000ul,
                        1000000000ul,
                        100000000ul,
                        10000000ul,
                        1000000ul,
                        100000ul,
                        10000ul,
                        1000ul,
                        100ul,
                        10ul,
                        1ul,
                    };

                if (unlikely(len > 20))
                {
                        // throw something?
                        return 0;
                }

                uint64_t res{0};
                uint32_t k{0};

                for (uint32_t i = sizeof_array(pow10) - len; k != len; ++i)
                {
                        const auto d = unsigned(p[k++]) - '0';

                        res += pow10[i] * d;
                }

                return res;
        }

        [[gnu::always_inline]] inline bool Eq(const CT *const ptr) const
        {
                return operator==(ptr);
        }

        [[gnu::always_inline]] inline bool Eq(const CT *const v, const LT l) const
        {
                return l == len && memcmp(v, p, l) == 0;
        }

        [[gnu::always_inline]] inline typename std::enable_if<std::is_same<char, CT>::value, bool>::type EqNoCase(const CT *const v, const LT l) const
        {
                return l == len ? !strncasecmp((char *)v, (char *)p, l) : false;
        }

        typename std::enable_if<std::is_same<char, CT>::value, bool>::type EqNoCase(const CT *v) const
        {
                const auto *it = p, *const e = p + len;

                while (it != e && toupper(*v) == toupper(*it))
                {
                        ++it;
                        ++v;
                }

                return it == e && *v == '\0';
        }

        [[gnu::always_inline]] inline bool EqNoCase(const strwithlen &o) const
        {
                return EqNoCase(o.p, o.len);
        }

        [[gnu::always_inline]] inline bool IsEqual(const CT *const ptr, const LT l) const
        {
                return l == len && memcmp(p, ptr, l) == 0;
        }

        inline bool EndsWith(const CT *const v, const LT l) const
        {
                return l <= len && memcmp(p + len - l, v, l) == 0;
        }

        // useful for hostnames, e.g
        // delimEndsWith(_S(".google.com")) will return true for both "google.com", "www.google.com"
        // the first character is considered the delimeter
        bool delimEndsWith(const CT *const v, const LT lt) const
        {
                if (!lt)
                        return true;

                return EndsWith(v + 1, lt - 1) && (len == lt - 1 || p[len - lt] == v[0]);
        }

        inline bool EndsWithButNoExactMatch(const CT *const v, const LT l) const
        {
                return l < len && memcmp(p + len - l, v, l) == 0;
        }

        inline bool EndsWith(const CT *const v) const
        {
                return EndsWith(v, strlen(v));
        }

        inline bool EndsWithNoCase(const CT *const v) const
        {
                return EndsWithNoCase(v, strlen(v));
        }

        inline typename std::enable_if<std::is_same<char, CT>::value, bool>::type EndsWithNoCase(const CT *const v, const LT l) const
        {
                return l <= len && strncasecmp((char *)p + len - l, (char *)v, l) == 0;
        }

        inline typename std::enable_if<std::is_same<char, CT>::value, bool>::type BeginsWith(const CT *const v, const LT l) const
        {
                return l <= len && memcmp(p, v, l) == 0;
        }

        inline bool EndsWith(const CT c) const
        {
                return len && p[len - 1] == c;
        }

        inline bool HasPrefix(const CT *const v, const LT l) const
        {
                return BeginsWith(v, l);
        }

        inline bool BeginsWith(const CT c) const
        {
                return likely(len) ? *p == c : false;
        }

        inline bool HasPrefix(const CT c) const
        {
                return BeginsWith(c);
        }

        strwithlen Prefix(const LT l) const
        {
                return strwithlen(p, Min(len, l));
        }

        strwithlen Suffix(const LT l) const
        {
                return strwithlen(End() - l, l);
        }

        strwithlen SuffixFrom(const CT *const offset) const
        {
                return strwithlen(offset, End() - offset);
        }

        strwithlen FirstDigitsSeq() const
        {
                // handy utility function
                for (const char *it = p, *const e = End();; ++it)
                {
                        if (isdigit(*it))
                        {
                                const char *const b = it;

                                for (++it; it != e && isdigit(*it); ++it)
                                        continue;

                                return {b, it};
                        }
                }
                return {};
        }

        strwithlen SuffixFrom(const LT o) const
        {
                return SuffixFrom(p + o);
        }

        strwithlen PrefixUpto(const CT *const o) const
        {
                return strwithlen(p, o - p);
        }

        // e.g if (name.Extension().Eq(_S("png"))) { .. }
        strwithlen Extension(const CT c = '.', const LT maxLength = 16) const
        {
                if (const auto *const it = SearchRWithLimit(c, maxLength))
                        return SuffixFrom(it + 1);
                else
                        return {};
        }

        inline typename std::enable_if<std::is_same<char, CT>::value, bool>::type BeginsWithNoCase(const CT *const v, const LT l) const
        {
                return l <= len && strncasecmp((char *)p, (char *)v, l) == 0;
        }

        bool BeginsWith(const CT *ptr) const
        {
                const auto *it = p;
                const auto *const end = p + len;

                while (it != end)
                {
                        if (*it != *ptr)
                                return false;

                        if (++it == end)
                                return true;
                        ++ptr;
                }

                return *ptr == '\0';
        }

        bool HasPrefix(const CT *ptr) const
        {
                return BeginsWith(ptr);
        }

        template <typename T>
        inline bool BeginsWith(const T &s) const
        {
                return BeginsWith(s.p, s.len);
        }

        template <typename T>
        inline bool BeginsWith(const T *const s) const
        {
                return BeginsWith(s);
        }

        template <typename T>
        inline bool EndsWith(const T *const s) const
        {
                return EndsWith(*s);
        }

        [[gnu::always_inline]] inline const CT *End() const
        {
                return p + len;
        }

        void Extend(const LT l)
        {
                len += l;
        }

        inline void SetLengthExpl(const LT l)
        {
                assert(l <= MaxLength());
                len = l;
        }

        void InitWithCopy(const void *const s, const LT l)
        {
                SetLengthExpl(l);

                if (len)
                {
                        auto *const ptr = (CT *)malloc(len * sizeof(CT));

                        assert(ptr != nullptr);
                        memcpy(ptr, s, len * sizeof(CT));
                        p = ptr;
                }
                else
                        p = nullptr;
        }

        typename std::enable_if<std::is_same<char, CT>::value, CT *>::type ToCString() const
        {
                auto *const r = (CT *)malloc((len * sizeof(CT)) + 1);

                assert(r != nullptr);
                memcpy(r, p, len * sizeof(CT));
                r[len] = '\0';
                return r;
        }

        typename std::enable_if<std::is_same<char, CT>::value, CT *>::type ToCString(CT *const out) const
        {
                memcpy(out, p, len * sizeof(CT));
                out[len] = '\0';
                return out;
        }

        typename std::enable_if<std::is_same<char, CT>::value, CT *>::type ToCString(CT *const out, const uint32_t outSize) const
        {
                assert(len + 1 <= outSize);
                memcpy(out, p, len * sizeof(CT));
                out[len] = '\0';
                return out;
        }

        CT *Copy() const
        {
                auto *const r = (CT *)malloc(len * sizeof(CT));

                assert(r != nullptr);
                memcpy(r, p, len * sizeof(CT));
                return r;
        }

        [[gnu::always_inline]] inline const CT *At(const LT o) const
        {
                return p + o;
        }

        CT *CopyTo(CT *const to) const
        {
                memcpy(to, p, len * sizeof(CT));
                return to + len;
        }

        CT *asLowercase(CT *const out) const
        {
                for (LT i{0}; i != len; ++i)
                        out[i] = tolower(p[i]);

                return out;
        }

        void InitWithCopy(const strwithlen &o)
        {
                InitWithCopy(o.p, o.len);
        }

        void InitWithCopy(const strwithlen *const o)
        {
                InitWithCopy(o->p, o->len);
        }

        inline void AdjustRight(const LT v)
        {
                len -= v;
        }

        inline void AdjustLeft(const LT v)
        {
                p += v;
                len -= v;
        }

        LT CommonPrefixLen(const strwithlen o) const
        {
                const auto *it = p;

                for (const auto *oit = o.p, *const oend = oit + o.len, *const end = p + len;
                     it != end && oit != oend && *it == *oit;
                     ++it, ++oit)
                {
                        continue;
                }

                return it - p;
        }

        // This really only makes sense in very specific situations
        // for usually p[len] is not accessible
        inline auto isNullTerminated() const
        {
                return !p[len];
        }

        inline strwithlen CommonPrefix(const strwithlen o) const
        {
                return strwithlen(p, CommonPrefixLen(o));
        }

        LT CommonSuffixLen(const strwithlen o) const
        {
                const auto *const e = End(), *const oend = o.End(), *const op = o.p;
                const auto *it = e;

                for (const auto *oit = oend;
                     oit != op && it != p && oit[-1] == it[-1];
                     --it, --oit)
                {
                        continue;
                }

                return e - it;
        }

        // Excluding common prefix and suffixes of @o
        // e.g "STARWARSCRAFT".IntersectionOf("STARCRAFT") = "WARS"
        inline strwithlen IntersectionOf(const strwithlen o) const
        {
                const auto *const from = p + CommonPrefixLen(o);
                const auto *const upto = End() - CommonSuffixLen(o);

                return strwithlen(from, upto - from);
        }

        inline strwithlen CommonSuffix(const strwithlen o) const
        {
                return SuffixFrom(len - CommonSuffixLen(o));
        }

        inline void StripPrefix(const LT v)
        {
                AdjustLeft(v);
        }

        strwithlen AsTrimmedBy(const LT l) const
        {
                strwithlen res{p, len};

                res.StripSuffix(l);
                return res;
        }

        bool StripPrefix(const CT *const s, const LT l)
        {
                if (BeginsWith(s, l))
                {
                        StripPrefix(l);
                        return true;
                }
                else
                        return false;
        }

        bool StripSuffix(const CT *const s, const LT l)
        {
                if (EndsWith(s, l))
                {
                        StripSuffix(l);
                        return true;
                }
                else
                        return false;
        }

        inline void StripTrailingCharacter(const CT c)
        {
                while (len && p[len - 1] == c)
                        --len;
        }

        inline void StripInitialCharacter(const CT c)
        {
                while (len && *p == c)
                {
                        ++p;
                        --len;
                }
        }

        inline void StripSuffix(const LT v)
        {
                AdjustRight(v);
        }

        bool InRange(const CT *const s) const
        {
                return s >= p && s < p + len;
        }

        inline void AdvanceTo(const CT *const to)
        {
                len -= to - p;
                p = to;
        }

        void SetEnd(const CT *const e)
        {
                len = e - p;
        }

        void SetEndTo(const CT c)
        {
                if (const auto *const res = Search(c))
                        SetEnd(res);
        }

        const CT *NextWS() const
        {
                for (const auto *it = p, *const end = End(); it != end; ++it)
                {
                        if (isspace(*it))
                                return it;
                }
                return nullptr;
        }

        inline LT OffsetAt(const CT *const it) const
        {
                return it - p;
        }

        inline strwithlen Replica() const
        {
                return {p, len};
        }

        strwithlen &TrimWS()
        {
                while (len && isspace(*p))
                {
                        ++p;
                        --len;
                }
                while (len && isspace(p[len - 1]))
                        --len;
                return *this;
        }

        bool IsBlank() const
        {
                for (const auto *it = p, *const end = p + len; it != end; ++it)
                {
                        if (!isspace(*it))
                                return false;
                }
                return true;
        }

        const CT *begin() const
        {
                return p;
        }

        const CT *end() const
        {
                return p + len;
        }

        const CT *data() const
        {
                return p;
        }

        struct _segments
        {
                const strwithlen s;
                const CT sep;

                struct iterator
                {
                        strwithlen cur;
                        const CT sep;
                        const CT *next;

                        void Next()
                        {
                                const auto *const e = cur.End();

                                while (next != e && *next != sep)
                                        ++next;
                        }

                        iterator(const strwithlen input, const CT c)
                            : cur(input), sep(c), next(cur.p)
                        {
                                Next();
                        }

                        bool operator!=(const iterator &o) const
                        {
                                return cur.p != o.cur.p;
                        }

                        iterator &operator++()
                        {
                                cur.AdvanceTo(next);
                                if (cur)
                                {
                                        cur.AdjustLeft(1);
                                        ++next;
                                        Next();
                                }
                                return *this;
                        }

                        inline strwithlen operator*() const
                        {
                                return strwithlen(cur.p, next - cur.p);
                        }
                };

                _segments(const strwithlen in, const CT c)
                    : s(in), sep(c)
                {
                }

                iterator begin() const
                {
                        return iterator(s, sep);
                }

                iterator end() const
                {
                        return iterator({s.End(), uint32_t(0)}, sep);
                }
        };

        template <typename F>
        struct _segmentsF
        {
                const strwithlen s;
                F &l;

                struct iterator
                {
                        strwithlen cur;
                        F &l;
                        const CT *next;

                        void Next()
                        {
                                const auto *const e = cur.end();

                                while (next != e && !l(*next))
                                        ++next;
                        }

                        iterator(const strwithlen input, F &lambda)
                            : cur(input), l{lambda}, next(cur.p)
                        {
                                Next();
                        }

                        bool operator!=(const iterator &o) const
                        {
                                return cur.p != o.cur.p;
                        }

                        iterator &operator++()
                        {
                                cur.AdvanceTo(next);
                                if (cur)
                                {
                                        cur.AdjustLeft(1);
                                        ++next;
                                        Next();
                                }
                                return *this;
                        }

                        inline strwithlen operator*() const
                        {
                                return strwithlen(cur.p, next - cur.p);
                        }
                };

                _segmentsF(const strwithlen in, F &lambda)
                    : s(in), l(lambda)
                {
                }

                iterator begin() const
                {
                        return iterator(s, l);
                }

                iterator end() const
                {
                        return iterator({s.End(), uint32_t(0)}, l);
                }
        };

        // e.g for (const auto it : strwlen32_t("com.markpadakis.apps").Segments('.') { .. }
        auto Segments(const CT separator) const
        {
                return _segments(*this, separator);
        }

        auto Split(const CT separator) const
        {
                return _segments(*this, separator);
        }

        template <typename F>
        auto splitL(F &&l) const
        {
                return _segmentsF<F>(*this, l);
        }

        uint32_t SplitInto(const CT separator, strwithlen *const out, const size_t capacity) const
        {
                if (!len)
                        return 0;

                uint32_t n{0};
                auto it = p;

                out->p = it;
                for (const auto *const e = end(); it != e;)
                {
                        if (*it == separator)
                        {
                                out[n++].SetEnd(it);
                                if (n == capacity)
                                        return UINT32_MAX;
                                out[n].p = ++it;
                        }
                        else
                                ++it;
                }
                out[n++].SetEnd(it);

                return n;
        }

        // TODO:
        // __keyvalues KeyValues(const char sep = ',', const bool ignoreBlanks = true, const bool kvSep = '=') const
};

typedef strwithlen<uint64_t> strwithlen64_t, strwlen64_t;
typedef strwithlen<uint32_t> strwithlen32_t, strwlen32_t;
typedef strwithlen<uint16_t> strwithlen16_t, strwlen16_t;
typedef strwithlen<uint8_t> strwithlen8_t, strwlen8_t;

[[gnu::always_inline]] inline static auto S32(const char *const p, const uint32_t len)
{
        return strwlen32_t(p, len);
}

[[gnu::always_inline]] inline static auto S8(const char *const p, const uint32_t len)
{
        return strwlen8_t(p, len);
}

[[gnu::always_inline]] inline static auto S16(const char *const p, const uint32_t len)
{
        return strwlen16_t(p, len);
}

#define _S32(s) strwlen32_t(s, STRLEN(s))
#define _S16(s) strwlen16_t(s, STRLEN(s))
#define _S8(s) strwlen8_t(s, STRLEN(s))

#ifdef LEAN_SWITCH
namespace std
{
        template <typename LT, typename CT>
        struct hash<strwithlen<LT, CT>>
        {
                using argument_type = strwithlen<LT, CT>;
                using result_type = std::size_t;

                result_type operator()(const argument_type &e) const
                {
                        size_t h{2166136261U};

                        for (uint32_t i{0}; i != e.len; ++i)
                                h = (h * 16777619) ^ e.p[i];

                        return h;
                }
        };
}
#endif

template <typename T>
[[gnu::always_inline]] inline static constexpr int32_t TrivialCmp(const T &a, const T &b)
{
        // Ref: http://stackoverflow.com/questions/10996418/efficient-integer-compare-function
        return (a > b) - (a < b);
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const uint8_t a, const uint8_t b)
{
        return a - b;
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const int8_t a, const int8_t b)
{
        return a - b;
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const uint16_t a, const uint16_t b)
{
        return a - b;
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const int16_t a, const int16_t b)
{
        return a - b;
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const uint32_t a, const uint32_t b)
{
        return (a > b) - (a < b);
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const int32_t a, const int32_t b)
{
        return a - b;
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const uint64_t a, const uint64_t b)
{
        return (a > b) - (a < b);
}

[[gnu::always_inline]] inline int32_t constexpr TrivialCmp(const int64_t a, const int64_t b)
{
        return (a > b) - (a < b);
}

#pragma once

struct size_repr
{
        const uint64_t v;

        size_repr(const uint64_t value)
            : v{value}
        {
        }

        strwlen8_t Get(char *const p) const
        {
                static const std::pair<const uint64_t, const char *> bases[] =
                    {
                        {1024ULL * 1024ULL * 1024ULL * 1024ULL, "tb"},
                        {1024UL * 1024UL * 1024UL, "gb"},
                        {1024 * 1024, "mb"},
                        {1024, "kb"},
                    };

                for (const auto &it : bases)
                {
                        const auto r = double(v) / it.first;

                        if (r >= 1)
                        {
                                const auto repr = it.second;
                                uint8_t len = sprintf(p, "%.2lf", r);

                                while (len && p[len - 1] == '0')
                                        --len;
                                if (len && p[len - 1] == '.')
                                        --len;

                                p[len] = *repr;
                                p[len + 1] = repr[1];
                                return strwlen8_t(p, len + 2);
                        }
                }

                return strwlen8_t(p, sprintf(p, "%ub", uint32_t(v)));
        }
};

static inline void PrintImpl(Buffer &out, const size_repr &s)
{
        out.reserve(32);

        out.advance_size(s.Get(out.end()).len);
}

struct dotnotation_repr
{
        const uint64_t value;
        const char sep;

        dotnotation_repr(const uint64_t v, const char separator = ',')
            : value{v}, sep{separator}
        {
        }

        strwlen8_t Get(char *const out) const
        {
                uint16_t t[8];
                uint8_t n{0};
                auto r = value;
                char *o;

                do
                {

                        t[n++] = r % 1000;
                        r /= 1000;
                } while (r);

                for (o = out + sprintf(out, "%u", t[--n]); n;)
                {
                        *o++ = sep;
                        o += sprintf(o, "%03u", t[--n]);
                }

                return {out, uint8_t(o - out)};
        }
};

static inline void PrintImpl(Buffer &out, const dotnotation_repr &r)
{
        out.reserve(32);
        out.advance_size(r.Get(out.end()).len);
}


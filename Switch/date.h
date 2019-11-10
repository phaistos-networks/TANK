#pragma once

namespace Date
{
        struct ts_repr
        {
                const time_t t;

                ts_repr(const time_t v)
                    : t{v}
                {
                }

                strwlen8_t Get(char *const out) const
                {
                        struct tm tm;

                        localtime_r(&t, &tm);
                        return strwlen8_t(out, sprintf(out, "%02u.%02u.%02u %02u:%02u:%02u", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec));
                }
        };
}

static inline void PrintImpl(Buffer &out, const Date::ts_repr &r)
{
        struct tm tm;

        localtime_r(&r.t, &tm);
        out.AppendFmt("%02u.%02u.%02u %02u:%02u:%02u", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}

#pragma once

namespace Date {
        inline time32_t day_first_second(const time32_t ts) {
                struct tm tm;

                localtime_r(&ts, &tm);
                tm.tm_hour  = 0;
                tm.tm_min   = 0;
                tm.tm_sec   = 0;
                tm.tm_isdst = -1;
                return mktime(&tm);
        }

        inline time32_t day_last_second(const time32_t ts) {
                struct tm tm;

                localtime_r(&ts, &tm);
                tm.tm_hour  = 23;
                tm.tm_min   = 59;
                tm.tm_sec   = 59;
                tm.tm_isdst = -1;
                return mktime(&tm);
        }

        inline int HoursByDelta(const int32_t hours, const int32_t delta) {
                return (24 + (hours + (delta % 24))) % 24;
        }

        inline int HoursAgo(const int32_t hours, const uint32_t delta) {
                return HoursByDelta(hours, -((int32_t)delta));
        }

        inline int HoursLater(const uint32_t hours, const uint32_t delta) {
                return (hours + (delta % 24)) % 24;
        }

        inline time32_t DaysAgo(time_t now, const uint32_t days) {
                struct tm tm, then;

                localtime_r(&now, &tm);
                now -= 86400 * days;

                localtime_r(&now, &then);
                if (then.tm_hour == HoursLater(tm.tm_hour, 1))
                        now -= 3600;
                else if (then.tm_hour == HoursAgo(tm.tm_hour, 1))
                        now += 3600;

                return now;
        }

        struct ts_repr {
                const time_t t;

                ts_repr(const time_t v)
                    : t{v} {
                }

                strwlen8_t Get(char *const out) const {
                        struct tm tm;

                        localtime_r(&t, &tm);
                        return strwlen8_t(out, sprintf(out, "%02u.%02u.%02u %02u:%02u:%02u", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec));
                }
        };
} // namespace Date

static inline void PrintImpl(Buffer &out, const Date::ts_repr &r) {
        struct tm tm;

        localtime_r(&r.t, &tm);
        out.AppendFmt("%02u.%02u.%02u %02u:%02u:%02u", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}

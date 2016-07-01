#pragma once
#include <time.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/syscall.h>

namespace Timings
{
        namespace SystemClock
        {
                static uint64_t Tick()
                {
                        struct timespec res;

                        clock_gettime(CLOCK_MONOTONIC, &res);
                        return (res.tv_sec * 1000000000ULL) + res.tv_nsec;
                }

                static inline uint64_t Time();

                [[gnu::always_inline]] inline static uint64_t CurTimeInNS()
                {
                        return Tick();
                }

                [[gnu::always_inline]] inline static uint64_t CurTimeInMillis()
                {
                        return Time();
                }
        }

        template <uint64_t asNanoseconds>
        struct Unit
        {
                static inline void Set(const uint64_t n, timespec *const ts)
                {
                        ts->tv_sec = ToSeconds(n);
                        ts->tv_nsec = (n * asNanoseconds) - (ts->tv_sec * 1000000000ULL);
                }

                static inline uint64_t Tick()
                {
                        return SystemClock::Tick() / asNanoseconds;
                }

                static inline uint64_t Since(const uint64_t t)
                {
                        return Tick() - t;
                }

                static inline uint64_t ToSeconds(const uint64_t n)
                {
                        return n * asNanoseconds / 1000000000ULL;
                }

                static inline timespec ToTimespec(const uint64_t n)
                {
                        timespec res;
                        const auto asN(n * asNanoseconds);

                        res.tv_sec = asN / 1000000000ULL;
                        res.tv_nsec = asN - res.tv_sec * 1000000000ULL;

                        return res;
                }

                static constexpr inline uint64_t ToMinutes(const uint64_t n)
                {
                        return ToSeconds(n) / 60;
                }

                static constexpr inline uint64_t ToHours(const uint64_t n)
                {
                        return ToMinutes(n) / 60;
                }

                static constexpr inline uint64_t ToDays(const uint64_t n)
                {
                        return ToHours(n) / 24;
                }

                static constexpr inline uint64_t ToMicros(const uint64_t n)
                {
                        return n * asNanoseconds / 1000;
                }

                static constexpr inline uint64_t ToMillis(const uint64_t n)
                {
                        return n * asNanoseconds / 1000000;
                }

                static constexpr inline uint64_t ToNanos(const uint64_t n)
                {
                        return n * asNanoseconds;
                }

                static void Sleep(const uint64_t n)
                {
                        timespec req, rem;

                        req.tv_sec = ToSeconds(n);
                        req.tv_nsec = (n * asNanoseconds) - (req.tv_sec * 1000000000);

                        while (unlikely(nanosleep(&req, &rem) == -1 && errno == EINTR))
                                req = rem;
                }

                // Need all those signatures to avoid ambiguity
                static inline void Sleep(const uint32_t n)
                {
                        Sleep((uint64_t)n);
                }

                static inline void Sleep(const int32_t n)
                {
                        Sleep((uint64_t)n);
                }

                static inline void Sleep(const uint16_t n)
                {
                        Sleep((uint64_t)n);
                }

                static inline void Sleep(const uint8_t n)
                {
                        Sleep((uint64_t)n);
                }

                static void SleepInterruptible(const uint64_t n)
                {
                        struct timespec req, rem;

                        req.tv_sec = ToSeconds(n);
                        req.tv_nsec = (n * asNanoseconds) - (req.tv_sec * 1000000000);

                        nanosleep(&req, &rem);
                }

                static uint64_t SysTime()
                {
                        struct timeval tv;

                        if (unlikely(gettimeofday(&tv, nullptr) == -1))
                        {
				abort();
                        }
                        else if (unlikely(tv.tv_sec < 1451982426u))
                        {
				abort();
			}
                        else
                                return ((tv.tv_sec * 1000000000ULL) + (tv.tv_usec * 1000ULL)) / asNanoseconds;
                }
        };

        struct Seconds
            : public Unit<1000000000ULL>
        {
        };

        struct Milliseconds
            : public Unit<1000000UL>
        {
        };

        struct Microseconds
            : public Unit<1000>
        {
        };

        struct Nanoseconds
            : public Unit<1>
        {
        };

        struct Minutes
            : public Unit<1000000000ULL * 60>
        {
        };

        struct Hours
            : public Unit<1000000000ULL * 60 * 60>
        {
        };

        struct Days
            : public Unit<1000000000ULL * 60 * 60 * 24>
        {
        };

        struct Weeks
            : public Unit<1000000000ULL * 60 * 60 * 24 * 7>
        {
        };

        uint64_t SystemClock::Time()
        {
                return Timings::Nanoseconds::ToMillis(Tick());
        }
}

#pragma once
#include "portability.h"
#include <assert.h>
#include <ctype.h>
#include <stdint.h>
#include <cstdio>
#include <string.h>
#include <type_traits>
#include <unistd.h>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <limits.h>
#include <utility>
#include <limits>
#include <memory>
#include <functional>
#include <algorithm>
#include <cmath>

#define require(x) assert(x)
#define Drequire(x) assert(x)
#define EXPECT(x) assert(x)
#define DEXPECT(x) assert(x)


[[gnu::noreturn]] static inline void Unreachable()
{
        __builtin_unreachable();
}


static inline size_t goodMallocSize(const size_t n) noexcept
{
        return n;
}

// Ref: http://cnicholson.net/2011/01/stupid-c-tricks-a-better-sizeof_array/
// our sizeof_array macro doesn no parameter type-checking; you can pass anything to it, and
// as long as x[0] can be evaluated, you 'll get a successful compilation.
namespace detail
{
        template <typename T, size_t N>
        char (&SIZEOF_ARRAY_REQUIRES_ARRAY_ARGUMENT(T (&)[N]))[N]; // XXX: why does this work and what does it do?
}
#define sizeof_array(x) sizeof(detail::SIZEOF_ARRAY_REQUIRES_ARRAY_ARGUMENT(x))




#define STRLEN(p) (uint32_t)(sizeof(p) - 1)
#define STRWITHLEN(p) (p), (uint32_t)(sizeof(p) - 1)
#define LENWITHSTR(p) (uint32_t)(sizeof(p) - 1), (p)
#define STRWLEN(p) STRWITHLEN(p)
#define LENWSTR(p) LENWITHSTR(p)
#define _S(p) STRWITHLEN(p)



#include "switch_common.h"
#include "switch_ranges.h"
#include "buffer.h"
#include "switch_exceptions.h"
#include "switch_numops.h"
#include "switch_ranges.h"
#include "timings.h"

// Src: folly
template <class Lambda>
class AtScopeExit
{
      private:
        Lambda &l;

      public:
        AtScopeExit(Lambda &action)
            : l(action)
        {
        }

        ~AtScopeExit(void)
        {
                l();
        }
};

template<typename T>
static inline T Min(const T a, const T b)
{
	return std::min(a, b);
}

template<typename T>
static inline T Max(const T a, const T b)
{
	return std::max(a, b);
}

[[gnu::always_inline]] inline void assume(bool cond)
{
#if defined(__clang__)
        __builtin_assume(cond);
#elif defined(__GNUC__)
        if (!cond)
                __builtin_unreachable();
#elif defined(_MSC_VER)
        __assume(cond);
#endif
}

[[ noreturn, gnu::always_inline ]] inline void assume_unreachable()
{
        assume(false);
#if defined(__GNUC__)
        __builtin_unreachable();
#elif defined(_MSC_VER)
        __assume(0);
#else
        std::abort();
#endif
}

#define TOKEN_PASTE(x, y) x##y
#define TOKEN_PASTE2(x, y) TOKEN_PASTE(x, y)

#define Auto_INTERNAL1(lname, aname, ...) \
        auto lname = [&]() {              \
                __VA_ARGS__;              \
        };                                \
        AtScopeExit<decltype(lname)> aname(lname);
#define Auto_INTERNAL2(ctr, ...) Auto_INTERNAL1(TOKEN_PASTE(Auto_func_, ctr), TOKEN_PASTE(Auto_Instance_, ctr), __VA_ARGS__)
#define Defer(...) Auto_INTERNAL2(__COUNTER__, __VA_ARGS__)

#define IMPLEMENT_ME()                                                                         \
        do                                                                                     \
        {                                                                                      \
                Print(ansifmt::bold, ansifmt::color_red, "Implementation Missing", ansifmt::reset, " at ", __FILE__, ":", __LINE__, ": Will Exit\n"); \
                std::abort();                                                                  \
        } while (0)

#define IMPLEMENT_ME_NOEXIT() Print(ansifmt::bold, ansifmt::color_red, "WARNING: Implementation Missing", ansifmt::reset, " at ", __FILE__, ":", __LINE__, "\n")








template <typename L>
struct scope_guard
{
        L l_;
        bool invoked_{false};

        scope_guard(L &&l)
            : l_(std::move(l))
        {
        }

        ~scope_guard()
        {
                invoke();
        }

        void invoke()
        {
                if (!invoked_)
                {
                        l_();
                        invoked_ = false;
                }
        }

        void cancel()
        {
                invoked_ = true;
        }
};

template <typename T>
inline auto make_scope_guard(T &&l)
{
        return scope_guard<T>(std::forward<T>(l));
}

#define DEFER(...) auto TOKEN_PASTE2(__deferred, __COUNTER__) = make_scope_guard([&] { __VA_ARGS__ ;});

#include "switch_exceptions.h"

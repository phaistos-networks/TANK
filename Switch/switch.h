#pragma once
#include <assert.h>
#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <type_traits>
#include <unistd.h>

#define require(x) assert(x)
#define Drequire(x) assert(x)
#define expect(x) assert(x)
#define Dexpect(x) assert(x)

#if __GNUC__ >= 3
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

[[gnu::noreturn]] static inline void Unreachable()
{
        __builtin_unreachable();
}

#ifdef __clang__
static inline void assume(const bool x)
{
        __builtin_assume(x);
}
#else
#define assume(x) \
        if (x)    \
                ; \
        else      \
        Unreachable() // XXX: use unlikely() ?
#endif

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


#define TOKEN_PASTE(x, y) x##y
#define TOKEN_PASTE2(x, y) TOKEN_PASTE(x, y)

#define Auto_INTERNAL1(lname, aname, ...) \
        auto lname = [&]() {              \
                __VA_ARGS__;              \
        };                                \
        AtScopeExit<decltype(lname)> aname(lname);
#define Auto_INTERNAL2(ctr, ...) Auto_INTERNAL1(TOKEN_PASTE(Auto_func_, ctr), TOKEN_PASTE(Auto_Instance_, ctr), __VA_ARGS__)
#define Defer(...) Auto_INTERNAL2(__COUNTER__, __VA_ARGS__)

#include "switch_exceptions.h"

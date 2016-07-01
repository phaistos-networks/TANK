#pragma once
#include "switch_print.h"

namespace Switch
{
        struct exception
            : public std::exception
        {
                Buffer b;

                [[gnu::noinline]] explicit exception(const strwithlen32_t &s)
                {
                        b.Append(s.p, s.len);
                }

                template <typename... T>
                [[gnu::noinline]] exception(const T &... args)
                {
                        PrintImpl(b, args...);
                }

                exception(const exception &o)
			: b(o.b)
                {
                }

                exception(exception &&o)
			: b(std::move(o.b))
                {

                }

                exception() = delete;

                const char *what() const noexcept override
                {
                        return b.data();
                }
        };

        struct recoverable_error
            : public std::exception
        {
                Buffer b;

                [[gnu::noinline]] explicit recoverable_error(const strwithlen32_t &s)
                {
                        b.Append(s.p, s.len);
                }

                template <typename... T>
                [[gnu::noinline]] recoverable_error(const T &... args)
                {
                        PrintImpl(b, args...);
                }

                recoverable_error(const recoverable_error &o)
			: b(o.b)
                {

                }

                recoverable_error(recoverable_error &&o)
			: b(std::move(o.b))
                {

                }

                recoverable_error() = delete;

                const char *what() const noexcept override
                {
                        return b.data();
                }
        };

        using runtime_error = recoverable_error;
        using range_error = recoverable_error;
        using overflow_error = recoverable_error;
        using underflow_error = recoverable_error;
        using system_error = recoverable_error;
        using invalid_argument = recoverable_error;
        using length_error = recoverable_error;
        using out_of_range = recoverable_error;
        using data_error = recoverable_error;
}

#define SLog(...) ::Print(srcline_repr(), __VA_ARGS__)

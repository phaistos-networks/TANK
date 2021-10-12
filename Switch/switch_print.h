#pragma once
#include "buffer.h"
#ifndef __linux__
#include <pthread.h>
#endif

struct ptr_repr {
        const void *const ptr;

        ptr_repr(const void *const p)
            : ptr(p) {
        }

        inline strwlen8_t Get(char *const out) const {
                return strwlen8_t(out, sprintf(out, "%p", ptr));
        }
};

static inline void PrintImpl(Buffer &out, const ptr_repr &repr) {
        out.AppendFmt("%p", repr.ptr);
}

static inline void PrintImpl(void) {
}

static inline void PrintImpl(Buffer &out) {
}

static inline void PrintImpl(Buffer &out, const bool &v) {
        if (v)
                out.Append(_S("true"));
        else
                out.Append(_S("false"));
}

static inline void PrintImpl(Buffer &out, const double &v) {
        out.AppendFmt("%lf", v);
}

static inline void PrintImpl(Buffer &out, const char v) {
        out.AppendFmt("%c", v);
}

static inline void PrintImpl(Buffer &out, const void *const ptr) {
        out.AppendFmt("%p", ptr);
}

static inline void PrintImpl(Buffer &out, void *const ptr) {
        out.AppendFmt("%p", ptr);
}

static inline void PrintImpl(Buffer &out, const float &v) {
        out.AppendFmt("%f", v);
}

static inline void PrintImpl(Buffer &out, const int &v) {
        out.AppendFmt("%d", v);
}

static inline void PrintImpl(Buffer &out, const char *p) {
        if (likely(p))
                out.Append(p, strlen(p));
        else
                out.Append(_S("(nullptr)"));
}

static inline void PrintImpl(Buffer &out, char *p) {
        if (likely(p))
                out.Append(p, strlen(p));
        else
                out.Append(_S("(nullptr)"));
}

static inline void PrintImpl(Buffer &out, const uint32_t &v) {
        out.AppendFmt("%" PRIu32, v);
}

static inline void PrintImpl(Buffer &out, const uint8_t &v) {
        out.AppendFmt("%" PRIu32, v);
}

static inline void PrintImpl(Buffer &out, const uint16_t &v) {
        out.AppendFmt("%" PRIu32, v);
}

static inline void PrintImpl(Buffer &out, const int16_t &v) {
        out.AppendFmt("%" PRId32, v);
}

static inline void PrintImpl(Buffer &out, const int8_t &v) {
        out.AppendFmt("%" PRId32, v);
}

static inline void PrintImpl(Buffer &out, const uint64_t &v) {
        out.AppendFmt("%" PRIu64, v);
}

static inline void PrintImpl(Buffer &out, const int64_t &v) {
        out.AppendFmt("%" PRId64, v);
}

static inline void PrintImpl(Buffer &out, const Buffer &o) {
        out.Append(o);
}

static inline void PrintImpl(Buffer &out, const strwlen8_t &o) {
        out.Append(o);
}

static inline void PrintImpl(Buffer &out, const strwlen16_t &o) {
        out.Append(o);
}

static inline void PrintImpl(Buffer &out, const strwlen32_t &o) {
        out.Append(o);
}

template <typename T, typename... Args>
static void PrintImpl(Buffer &b, const T &v, const Args &... args) {
        PrintImpl(b, v);
        PrintImpl(b, args...);
}

#ifndef __linux__
static pthread_key_t bufKey;

[[gnu::constructor]] static void _init() {
        pthread_key_create(&bufKey, nullptr);
}

[[gnu::destructor]] static void _tear_down() {
        pthread_key_delete(bufKey);
}
#endif

static inline Buffer &thread_local_buf() {
#ifdef __linux__
        static thread_local Buffer b;

        return b;
#else
        auto p = (Buffer *)pthread_getspecific(bufKey);

        if (!p) {
                p = new Buffer();
                pthread_setspecific(bufKey, p);
        }

        return *p;
#endif
}

template <typename T, typename... Args>
static void Print(const T &v, const Args &... args) {
        auto &b = thread_local_buf();

        b.clear();
        PrintImpl(b, v);
        PrintImpl(b, args...);
        const auto r = write(STDOUT_FILENO, b.data(), b.size());

        (void)r; // (void)write triggers warning if  -Wunused-result is set
                 // and write() is declared like so
}

template <typename T, typename... Args>
static void ToBuffer(Buffer &out, const T &v, const Args &... args) {
        PrintImpl(out, v);
        PrintImpl(out, args...);
}

template <typename A, typename B>
static void PrintImpl(Buffer &b, const std::pair<A, B> &pair) {
        b.Append('<');
        PrintImpl(b, pair.first);
        b.Append(_S(", "));
        PrintImpl(b, pair.second);
        b.Append('>');
}

template <typename T>
static void PrintImpl(Buffer &b, const T &v) {
        if (std::is_enum<T>::value) {
                // We can now use 'naked' enums and they will be printed properly
                PrintImpl(b, (typename std::underlying_type<T>::type)v);
        } else {
                // catch-all for when we have no PrintImpl() specialization
                fprintf(stderr, "Specialization for type not defined\n");
                std::abort();
        }
}

// Handy alternative to snprintf()
// See also:
// Buffer::append<>
// Buffer::build<>
// RPCString::build<>
template <typename... Args>
static size_t Snprint(char *out, const size_t outLen, Args &&... args) {
        auto &b = thread_local_buf();

        b.clear();
        ToBuffer(b, std::forward<Args>(args)...);

        const auto l = b.size();

        b.AsS32().ToCString(out, outLen);
        return l;
}

static inline void PrintImpl(Buffer &out, const unsigned long long &v) {
        out.AppendFmt("%llu", v);
}

struct align_to final {
	const size_t A;

	align_to(const size_t v) 
		: A{v}  {
	}
};

inline void PrintImpl(Buffer &out, const align_to &a) {
	if (const auto l = out.size(); l < a.A) {
		const auto extra = a.A - l;

		out.reserve(extra + 1);
		memset(out.data() + l, ' ', extra);
		out.resize(l + extra);
	}
}

struct left_aligned final {
        const size_t     A;
        const str_view32 content;
        const size_t     line_len;

        left_aligned(const size_t _alignment, const str_view32 _content, const size_t ll = std::numeric_limits<size_t>::max())
            : A{_alignment}
            , content{_content}
            , line_len{ll} {
        }
};

inline void PrintImpl(Buffer &out, const left_aligned &a) {
        const auto *p = a.content.data(), *const e = p + a.content.size();

        while (p < e && isblank(*p)) {
                ++p;
        }

        if (p == e) {
                return;
        }

        const auto threshold = a.line_len;
        const auto alignment = a.A;

        for (;;) {
                const auto base = p;
                auto       bp   = p + 1;
                const auto eol  = std::min(e, p + threshold);

                out.reserve(out.size() + alignment);
                memset(out.data() + out.size(), ' ', alignment);
                out.resize(out.size() + alignment);

                for (;;) {
                        if (p == e) {
                                bp = p;
                                break;
                        } else if (p >= eol) {
                                break;
			} else if (*p == '\n') {
				bp = p;
				break;
                        } else {
                                const auto c = *p;

                                if (c == '.' || c == ',' || c == ' ' || c == '\t' || c == '\n' || c == '(' || c == '+' || c == '-' || c == '/' || c == '\n') {
                                        bp = p;
                                }

                                ++p;
                        }
                }

                out.append(str_view32(base, std::distance(base, bp)));

                while (bp < e && (*bp == ' ' || *bp == '\t' || *bp == '\t')) {
                        ++bp;
                }

		if (bp < e && *bp == '\n') {
			++bp;
		}

                p = bp;
                if (p == e) {
                        break;
                } else {
                        out.append('\n');
                }
        }
}


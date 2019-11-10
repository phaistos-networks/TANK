#include "text.h"

static constexpr inline uint8_t idx_of(const char c) noexcept
{
        switch (c)
        {
                case '0' ... '9':
                        return c - '0';

                case 'a' ... 'z':
                        return c - 'a' + 10;

                case 'A' ... 'Z':
                        return c - 'A' + 10 + 26;

                case '_':
                        return 26 + 10 + 26 + 0;

                case '!':
                        return 26 + 10 + 26 + 1;

                case '#':
                        return 26 + 10 + 26 + 2;

                case '$':
                        return 26 + 10 + 26 + 3;

                case '%':
                        return 26 + 10 + 26 + 4;

                case '&':
                        return 26 + 10 + 26 + 5;

                case '*':
                        return 26 + 10 + 26 + 6;

                case '+':
                        return 26 + 10 + 26 + 7;

                case '-':
                        return 26 + 10 + 26 + 8;

                case '.':
                        return 26 + 10 + 26 + 9;

                case '^':
                        return 26 + 10 + 26 + 10;

                case '|':
                        return 26 + 10 + 26 + 11;

                case '~':
                        return 26 + 10 + 26 + 12;

                case '\'':
                        return 26 + 10 + 26 + 13;

                case '`':
                        return 26 + 10 + 26 + 14;

                default:
                        return 0xff;
        }
}

static const char numChars[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_!#$%&*+-.^|~'`";

uint8_t Text::ToBase(uint64_t input, const uint32_t toBase, char *out)
{
        char bufr[128], *c = out;
        uint32_t i{0};

        do
        {
                bufr[i++] = numChars[input % toBase];
                input /= toBase;
        } while (input);

        do
        {
                *out++ = bufr[--i];
        } while (i);

        *out = '\0';
        return out - c;
}

uint64_t Text::FromBase(const char *const input, const uint32_t len, const uint8_t base)
{
        uint64_t res{0};

        for (uint32_t i{0}; i != len; ++i)
        {
                if (const auto idx = idx_of(input[i]); idx == 0xff)
                        return 0;
                else
                        res = res * base + idx;
        }
        return res;
}

size_t as_escaped_repr_length(const char *p, const size_t len)
{
        size_t n{0};

        for (uint32_t i{0}; i != len; ++i)
        {
                switch (p[i])
                {
                        case '\"':
                        case '\'':
                        case '\\':
                        case '\n':
                        case '\r':
                        case '\0':
                        case '\032':
                                n += 2;
                                break;

                        default:
                                ++n;
                                break;
                }
        }

        return n;
}

uint32_t escape_impl(const char *const p, const uint32_t len, char *out, const uint32_t available)
{
        const char *const base = out, *const end = p + len;
        const char *ckpt = p;
        char chr;

        for (const char *it = p; it != end;)
        {
                const char c = *it;

                if (c == '\"' || c == '\\' || c == '\'')
                        chr = c;
#if 0 // Stupid mySQL won't handle \t and \v
		else if (c == '\t')
			chr = 't';
		else if (c == '\v')
			chr = 'v';
#endif
                else if (c == '\n')
                        chr = 'n';
                else if (c == '\r')
                        chr = 'r';
                else if (c == '\0')
                        chr = '0';
                else if (c == '\032') // Issues on Win32 (ref: mysql's escape_string_for_mysql() implementation)
                        chr = 'Z';
                else
                {
                        ++it;
                        continue;
                }

                const uint32_t _len = it - ckpt;

                memcpy(out, ckpt, _len);
                out += _len;
                out[0] = '\\';
                out[1] = chr;
                out += 2;
                ckpt = ++it;
        }

        const uint32_t _len = end - ckpt;

        memcpy(out, ckpt, _len);
        out += _len;

        const auto actual = out - base;

        EXPECT(actual <= available);
        return actual;
}

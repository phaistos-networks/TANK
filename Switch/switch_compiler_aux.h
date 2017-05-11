#pragma once

#define containerof(type, member_name, ptr) (type *)((char *)(ptr)-offsetof(type, member_name))

#define ctou16(_cp_) (*(unsigned short *)(_cp_))
#define ctou32(_cp_) (*(unsigned       *)(_cp_))
#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 8
#define bswap16(x) __builtin_bswap16(x)
#else
static inline unsigned short bswap16(unsigned short x) { return __builtin_bswap32(x << 16); }
#endif
#define bswap32(x) __builtin_bswap32(x)
#define bswap64(x) __builtin_bswap64(x)

#define varbyte_put32(_op_, _x_)                                               \
        {                                                                  \
                if (likely((_x_) < (1 << 7)))                              \
                {                                                          \
                        *_op_++ = _x_;                                     \
                }                                                          \
                else if (likely((_x_) < (1 << 14)))                        \
                {                                                          \
                        ctou16(_op_) = uint16_t(bswap16((_x_) | 0x8000u)); \
                        _op_ += 2;                                         \
                }                                                          \
                else if (likely((_x_) < (1 << 21)))                        \
                {                                                          \
                        *_op_++ = _x_ >> 16 | 0xc0u;                       \
                        ctou16(_op_) = uint16_t(_x_);                      \
                        _op_ += 2;                                         \
                }                                                          \
                else if (likely((_x_) < (1 << 28)))                        \
                {                                                          \
                        ctou32(_op_) = bswap32((_x_) | 0xe0000000u);       \
                        _op_ += 4;                                         \
                }                                                          \
                else                                                       \
                {                                                          \
                        *_op_++ = (unsigned long long)(_x_) >> 32 | 0xf0u; \
                        ctou32(_op_) = _x_;                                \
                        _op_ += 4;                                         \
                }                                                          \
        }

#define varbyte_get32(_ip_, _x_)                                                         \
        do                                                                           \
        {                                                                            \
                _x_ = (unsigned)(*_ip_++);                                           \
                if (!(_x_ & 0x80u))                                                  \
                {                                                                    \
                }                                                                    \
                else if (!(_x_ & 0x40u))                                             \
                {                                                                    \
                        _x_ = bswap16(ctou16(_ip_ - 1) & 0xff3fu);                   \
                        _ip_++;                                                      \
                }                                                                    \
                else if (!(_x_ & 0x20u))                                             \
                {                                                                    \
                        _x_ = (_x_ & 0x1f) << 16 | ctou16(_ip_);                     \
                        _ip_ += 2;                                                   \
                }                                                                    \
                else if (!(_x_ & 0x10u))                                             \
                {                                                                    \
                        _x_ = bswap32(ctou32(_ip_ - 1) & 0xffffff0fu);               \
                        _ip_ += 3;                                                   \
                }                                                                    \
                else                                                                 \
                {                                                                    \
                        _x_ = (unsigned long long)((_x_)&0x07) << 32 | ctou32(_ip_); \
                        _ip_ += 4;                                                   \
                }                                                                    \
        } while (0)

#define varbyte_get64(_ip_, _x_)                                                                                    \
        do                                                                                                      \
        {                                                                                                       \
                _x_ = *_ip_++;                                                                                  \
                if (!(_x_ & 0x80))                                                                              \
                {                                                                                               \
                }                                                                                               \
                else if (!(_x_ & 0x40))                                                                         \
                {                                                                                               \
                        _x_ = bswap16(ctou16(_ip_++ - 1) & 0xff3f);                                             \
                }                                                                                               \
                else if (!(_x_ & 0x20))                                                                         \
                {                                                                                               \
                        _x_ = (_x_ & 0x1f) << 16 | ctou16(_ip_);                                                \
                        _ip_ += 2;                                                                              \
                }                                                                                               \
                else if (!(_x_ & 0x10))                                                                         \
                {                                                                                               \
                        _x_ = bswap32(ctou32(_ip_ - 1) & 0xffffff0f);                                           \
                        _ip_ += 3;                                                                              \
                }                                                                                               \
                else if (!(_x_ & 0x08))                                                                         \
                {                                                                                               \
                        _x_ = (_x_ & 0x07) << 32 | ctou32(_ip_);                                                \
                        _ip_ += 4;                                                                              \
                }                                                                                               \
                else if (!(_x_ & 0x04))                                                                         \
                {                                                                                               \
                        _x_ = (unsigned long long)(bswap16(ctou16(_ip_ - 1)) & 0x7ff) << 32 | ctou32(_ip_ + 1); \
                        _ip_ += 5;                                                                              \
                }                                                                                               \
                else if (!(_x_ & 0x02))                                                                         \
                {                                                                                               \
                        _x_ = (_x_ & 0x03) << 48 | (unsigned long long)ctou16(_ip_) << 32 | ctou32(_ip_ + 2);   \
                        _ip_ += 6;                                                                              \
                }                                                                                               \
                else if (!(_x_ & 0x01))                                                                         \
                {                                                                                               \
                        _x_ = bswap64(ctou64(_ip_ - 1)) & 0x01ffffffffffffffull;                                \
                        _ip_ += 7;                                                                              \
                }                                                                                               \
                else                                                                                            \
                {                                                                                               \
                        _x_ = ctou64(_ip_);                                                                     \
                        _ip_ += 8;                                                                              \
                }                                                                                               \
        } while (0)

#define varbyte_put64(_op_, _x_)                                                 \
        {                                                                    \
                if (likely(_x_ < (1 << 7)))                                  \
                {                                                            \
                        *_op_++ = _x_;                                       \
                }                                                            \
                else if (likely(_x_ < (1 << 14)))                            \
                {                                                            \
                        ctou16(_op_) = bswap16(_x_ | 0x8000);                \
                        _op_ += 2;                                           \
                }                                                            \
                else if (likely(_x_ < (1 << 21)))                            \
                {                                                            \
                        *_op_++ = _x_ >> 16 | 0xc0;                          \
                        ctou16(_op_) = _x_;                                  \
                        _op_ += 2;                                           \
                }                                                            \
                else if (likely(_x_ < (1 << 28)))                            \
                {                                                            \
                        ctou32(_op_) = bswap32(_x_ | 0xe0000000);            \
                        _op_ += 4;                                           \
                }                                                            \
                else if (_x_ < 1ull << 35)                                   \
                {                                                            \
                        *_op_++ = _x_ >> 32 | 0xf0;                          \
                        ctou32(_op_) = _x_;                                  \
                        _op_ += 4;                                           \
                }                                                            \
                else if (_x_ < 1ull << 42)                                   \
                {                                                            \
                        ctou16(_op_) = bswap16(_x_ >> 32 | 0xf800);          \
                        _op_ += 2;                                           \
                        ctou32(_op_) = _x_;                                  \
                        _op_ += 4;                                           \
                }                                                            \
                else if (_x_ < 1ull << 49)                                   \
                {                                                            \
                        *_op_++ = _x_ >> 48 | 0xfc;                          \
                        ctou16(_op_) = _x_ >> 32;                            \
                        _op_ += 2;                                           \
                        ctou32(_op_) = _x_;                                  \
                        _op_ += 4;                                           \
                }                                                            \
                else if (_x_ < 1ull << 56)                                   \
                {                                                            \
                        ctou64(_op_) = bswap64(_x_ | 0xfe00000000000000ull); \
                        _op_ += 8;                                           \
                }                                                            \
                else                                                         \
                {                                                            \
                        *_op_++ = 0xff;                                      \
                        ctou64(_op_) = _x_;                                  \
                        _op_ += 8;                                           \
                }                                                            \
        }

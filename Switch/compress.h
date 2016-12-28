#pragma once
#include "switch.h"
#include "ext_snappy/snappy.h"

namespace Compression
{

        enum class Algo : int8_t
        {
                UNKNOWN = -1,
		SNAPPY
        };

        inline bool Compress(const Algo algorithm, const void *data, const uint32_t dataLen, Buffer *dest)
        {
                switch (algorithm)
                {
                        case Algo::SNAPPY:
                        {
                                size_t outLen = 0;

                                dest->reserve(snappy::MaxCompressedLength(dataLen + 2));
                                snappy::RawCompress((char *)data, dataLen, dest->At(dest->size()), &outLen);
                                dest->advance_size(outLen);

                                return true;
                        }
                        break;

                        default:
                                return false;
                }
        }

        inline bool UnCompress(const Algo algorithm, const void *const source, const uint32_t sourceLen, Buffer *const dest)
        {
                switch (algorithm)
                {
                        case Algo::SNAPPY:
                        {
                                size_t outLen;

                                if (unlikely(!snappy::GetUncompressedLength((char *)source, sourceLen, &outLen)))
                                        return false;
                                else
                                {
                                        dest->reserve(outLen + 8);

                                        if (unlikely(!snappy::RawUncompress((char *)source, sourceLen, dest->At(dest->size()))))
                                                return false;
					else
                                        {
                                                dest->advance_size(outLen);
                                                return true;
                                        }
                                }
                        }
                        break;

                        default:
                                return false;
                }
        }

        inline uint8_t *PackUInt32(const uint32_t n, uint8_t *out)
        {
#define AS_FLIPPED(_v_) (_v_) | 128
// This would have worked if it wasn't for the edge cases of e.g (1<<7), etc
//#define AS_FLIPPED_F(v) (((v)&127) + 128)
#define AS_FLIPPED_F(_v_) (_v_) | 128

                if (n < (1 << 7))
                {
                        *(out++) = n;
                }
                else if (n < (1 << 14))
                {
                        *(out++) = AS_FLIPPED(n);
                        *(out++) = n >> 7;
                }
                else if (n < (1 << 21))
                {
                        *(out++) = AS_FLIPPED(n);
                        *(out++) = AS_FLIPPED_F(n >> 7);
                        *(out++) = n >> 14;
                }
                else if (n < (1 << 28))
                {
                        *(out++) = AS_FLIPPED(n);
                        *(out++) = AS_FLIPPED_F(n >> 7);
                        *(out++) = AS_FLIPPED_F(n >> 14);
                        *(out++) = n >> 21;
                }
                else
                {
                        *(out++) = AS_FLIPPED(n);
                        *(out++) = AS_FLIPPED_F(n >> 7);
                        *(out++) = AS_FLIPPED_F(n >> 14);
                        *(out++) = AS_FLIPPED_F(n >> 21);
                        *(out++) = n >> 28;
                }

                return out;
#undef AS_FLIPPED
#undef AS_FLIPPED_V
        }

        inline uint8_t UnpackUInt32Check(const uint8_t *p, const uint8_t *const e)
        {
                for (uint8_t i{0}; i != 5; ++i)
                {
                        if (unlikely(p >= e))
                                break;
                        else if (*p < 128)
                                return i + 1;
                        else
                                ++p;
                }

                return 0;
        }

        inline uint32_t UnpackUInt32(const uint8_t *&buf)
        {
#define FLIPPED(v) ((v) & ~128)
                if (buf[0] > 127)
                {
                        if (buf[1] > 127)
                        {
                                if (buf[2] > 127)
                                {
                                        if (buf[3] > 127)
                                        {
                                                const uint32_t r = FLIPPED(buf[0]) | (FLIPPED(buf[1]) << 7) | (FLIPPED(buf[2]) << 14) | (FLIPPED(buf[3]) << 21) | (buf[4] << 28);

                                                buf += 5;
                                                return r;
                                        }
                                        else
                                        {
                                                const uint32_t r = FLIPPED(buf[0]) | (FLIPPED(buf[1]) << 7) | (FLIPPED(buf[2]) << 14) | (buf[3] << 21);

                                                buf += 4;
                                                return r;
                                        }
                                }
                                else
                                {
                                        const uint32_t r = FLIPPED(buf[0]) | (FLIPPED(buf[1]) << 7) | (buf[2] << 14);

                                        buf += 3;
                                        return r;
                                }
                        }
                        else
                        {
                                const uint32_t r = FLIPPED(buf[0]) | (buf[1] << 7);

                                buf += 2;
                                return r;
                        }
                }
                else
                        return *buf++;
#undef FLIPPED
        }
}

void IOBuffer::SerializeVarUInt32(const uint32_t n)
{
        reserve(8);

        uint8_t *e = (uint8_t *)(buffer + length_);
        length_ += Compression::PackUInt32(n, e) - e;
}

uint32_t IOBuffer::UnserializeVarUInt32(void)
{
        const uint8_t *e = (uint8_t *)(buffer + position), *const b = e;
        const uint32_t r = Compression::UnpackUInt32(e);

        position += e - b;
        return r;
}

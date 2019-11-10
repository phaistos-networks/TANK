#pragma once
#include <switch.h>
#include <sys/mman.h>
#include <fcntl.h>

namespace SwitchBitOps
{
	// http://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
	// Fast way to test if a zero byte is in a word
	[[gnu::always_inline]] inline constexpr bool U32HasZero(const uint32_t v)
	{
		return (v - 0x01010101UL) & ~v & 0x80808080UL;
	}

	// To check for 4 different bytes, xor the word with those bytes and then check for zero bytes:
	// v = (((unsigned char)c * 0x1010101U) ^ delimiter)
	// where delimter is the 4byte value to look for (u32)
	// and c is the character to check
	//
	// e.g  to check if a character is either of (?, /, -, :)
	// IsDelimter(uint8_t(c), '?'<<24|'/'<<16, '-'<<8, ':');
	//
	// Those are used by HAProxy / pattern.c
	[[gnu::always_inline]] inline static bool IsDelimter(const uint8_t c, uint32_t mask)
	{
		mask ^= (c * 0x01010101); // propagate the char to all 4 bytes
		return (mask - 0x01010101) & ~mask & 0x80808080U;
	}
		



	template<typename T>
	[[gnu::always_inline]] inline bool IsSet(const T n, const uint8_t index)
	{
		return n&(((T)1)<<index);
	}	

	template<typename T>  
	[[gnu::always_inline]] inline static constexpr T MSB(const T n, const uint8_t span)
	{
		return n >> ((sizeof(T) * 8) - span);
	}

	template<typename T>
	[[gnu::always_inline]] inline static constexpr T LSBMask(const uint8_t span)
	{
		return ((T)-1) >> ((sizeof(T) * 8) - span);
	}

	template<typename T>
	[[gnu::always_inline]] inline static constexpr T LSB(const T n, const uint8_t span)
	{
		return n & LSBMask<T>(span);
	}

	template<typename T>
	[[gnu::always_inline]] inline static constexpr uint8_t LeadingZeros(const T v)
	{
		return __builtin_clz(v);
	}

	// Need specialized for (u16, u8) because __builtin_clz() operates on uints
	[[gnu::always_inline]] inline static constexpr uint8_t LeadingZeros(const uint16_t v) 
	{
		return __builtin_clz(v) - 16;
	}

	[[gnu::always_inline]] inline static constexpr uint8_t LeadingZeros(const uint8_t v) 
	{
		return __builtin_clz(v) - 24;
	}
	
	[[gnu::always_inline]] inline static constexpr uint8_t LeadingZeros(const uint64_t v) // if v == 0 return std::numeric_limits<T>::digits
	{
		return __builtin_clzll(v);
	}

	template<typename T>
	[[gnu::always_inline]] inline static constexpr uint8_t TrailingZeros(const T v)
	{
		return __builtin_ctz(v);
	}
	
	[[gnu::always_inline]] inline static constexpr uint8_t TrailingZeros(const uint64_t v)
	{
		return __builtin_ctzll(v); 
	}

	[[gnu::always_inline]] inline static uint8_t TrailingZeros(const int64_t v)
	{
		return __builtin_ctzll(*(uint64_t *)&v);
	}


#ifdef SWITCH_ARCH_64BIT
	[[gnu::always_inline]] inline static constexpr uint8_t TrailingZeros(const long long v)
	{
		return __builtin_ctzll(v);
	}
#endif


	template<typename T>
	[[gnu::always_inline]] inline static constexpr uint8_t PopCnt(const T v)
	{
		return __builtin_popcount(v);
	}

	[[gnu::always_inline]] inline static constexpr uint8_t PopCnt(const uint64_t v)
	{
		return __builtin_popcountll(v);
	}


	// Returns 1 + index of the least significant 1-bit of x, or if x == 0, returns 0
	template<typename T>
	[[gnu::always_inline]] inline static constexpr uint8_t LeastSignificantBitSet(const T v) 
	{
		return __builtin_ffs(v);
	}

	[[gnu::always_inline]] inline static constexpr uint8_t LeastSignificantBitSet(const uint64_t v)
	{
		return __builtin_ffsll(v);
	}



	// https://en.wikipedia.org/wiki/Hamming_distance
	// The number of positions where the corresponding bits differ.
	template<typename T>
	[[gnu::always_inline]] inline constexpr uint8_t HammingDistance(const T h1, const T h2)
	{
		return PopCnt(h1 ^ h2);
	}


	// A lean bitmap
	//
	// We could get log2(sizeof(T) << 8) and shift by that instead of
	// dividing but the compiler should be smart enough to figure that out anyway
	template<typename T>
		struct Bitmap
		{
			static_assert(std::numeric_limits<T>::is_integer, "T must be an integer");
			static_assert(!std::numeric_limits<T>::is_signed, "T must be an unsigned integer");

			private:
			T *const bitmap;

			public:
			static inline uint32_t FirstSet(const T *const bm, const uint32_t bmSize /* in Ts not in bits */)
			{
				for (uint32_t i{0}; i != bmSize; ++i)
				{
					if (const auto v = bm[i])
						return (63 - SwitchBitOps::LeadingZeros(v)) + (i * sizeof(T) << 3);
				}

				return UINT32_MAX;
			}

			static auto cardinality(const T *const bm, const uint32_t n /* in Ts, not in bits */)
			{
				size_t cnt{0};

				for (uint32_t i{0}; i != n; ++i)
				{
					if (const auto v = bm[i])
						cnt+=PopCnt(v);
				}

				return cnt;
			}

			static auto anySet(const T *const bm, const uint32_t n /* in Ts, not in bits */)
			{
				for (uint32_t i{0}; i != n; ++i)
				{
					if (bm[i])
						return true;
				}
				return false;
			}

			static inline void Set(T *const bm, const uint32_t index)
			{
				const auto i = index / (sizeof(T)<<3);
				const T mask = (T)1U << (index&((sizeof(T) * 8) - 1));

				bm[i]|=mask;
			}

			static inline void Toggle(T *const bm, const uint32_t index)
			{
				const auto i = index / (sizeof(T)<<3);
				const T mask = (T)1U << (index&((sizeof(T) * 8) - 1));

				bm[i]^=mask;
			}

			static inline bool SetIfUnset(T *const bm, const uint32_t index)
			{
				const auto i = index / (sizeof(T)<<3);
				const T mask = (T)1U << (index&((sizeof(T) * 8) - 1));
				auto &v = bm[i];

				if (v&mask)
					return false;
				else
				{
					v|=mask;
					return true;
				}
			}

			static inline void Unset(T *const bm, const uint32_t index)
			{
				const auto i = index / (sizeof(T)<<3);
				const T mask = (T)1U << (index&((sizeof(T) * 8) - 1));

				bm[i]&=~mask;
			}

			static inline bool IsSet(T *const bm, const uint32_t index)
			{
				const auto i = index / (sizeof(T)<<3);
				const T mask = (T)1U << (index&((sizeof(T) * 8) - 1));

				return bm[i]&mask;
			}



			public:
			Bitmap(const uint32_t capacity)
				: bitmap{ (T *)calloc((capacity / (sizeof(T) << 3)) + 1, sizeof(T)) }
			{

			}

			~Bitmap(void)
			{
				::free(bitmap);
			}

			int MLock(const uint32_t capacity)
			{
				return mlock(bitmap, ((capacity / (sizeof(T) << 3)) + 1) * sizeof(T));
			}
				
			int MUnlock(const uint32_t capacity)
			{
				return munlock(bitmap, ((capacity / (sizeof(T) << 3)) + 1) * sizeof(T));
			}

			inline void Set(const uint32_t index)
			{
				return Set(bitmap, index);
			}

			inline bool SetIfUnset(const uint32_t index)
			{
				return SetIfUnset(bitmap, index);
			}

			inline void Unset(const uint32_t index)
			{
				return Unset(bitmap, index);
			}

			inline bool IsSet(const uint32_t index) const
			{
				return IsSet(bitmap, index);
			}

			T *Data(void) const
			{
				return bitmap;
			}

			bool FromFile(const char *const path)
			{
				int fd = open(path, O_RDONLY|O_LARGEFILE);

				if (fd == -1)
					return false;

				const auto fileSize = lseek64(fd, 0, SEEK_END);

				if (pread64(fd, bitmap, fileSize, 0) != fileSize)
				{
					(void)close(fd);
					return false;
				}
				else
				{
					(void)close(fd);
					return true;
				}
			}

		};
};

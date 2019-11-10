#pragma once
#include <switch.h>
#include <switch_bitops.h>

// Port of Lucene's SparseFixedBitSet
// A bit-set that only stores u64 that have at least one bit which is set
// It works by diving the space into blocks of 4k bits, which is 64 longs. For each block, we have
// a list which stores the non-zero u64 for that block, and a u64, for which any bit set means that the i-th u64 of the block
// is not 0, and the offset in the array of longs it the number of bits on the right of the i-th bit.
//
// This is a pretty clever design.
// You should use it if you care for fast random acces as opposed to checking mostly monotonically incrementing IDs, and memory footprint
class SparseFixedBitSet
{
      private:
        static constexpr bool trace{false};

        struct bucket
        {
                uint64_t *values;
                uint8_t capacity;
        };

      private:
        const uint32_t length;
        uint64_t *const indices;
        bucket *bits;
        uint32_t nonZero{0};

      private:
        void insert_block(const uint32_t i4k, const uint32_t i64, const uint32_t i)
        {
                if (trace)
                        SLog("OK will set bit ", i64 & 63, " of indices[", i4k, "]\n");

                indices[i4k] |= uint64_t(1) << (i64 & 63);
                require(bits[i4k].capacity == 0);

                auto *const b = bits + i4k;

                b->capacity = 4;
                b->values = (uint64_t *)malloc(b->capacity * sizeof(uint64_t));
                b->values[0] = uint64_t(1) << (i & 63);
		b->values[1] = b->values[2] = b->values[3] = 0;

                if (trace)
                        SLog("Did set v->values[0] to ", b->values[0], "\n");

                ++nonZero;
        }

	inline std::size_t block_count() const noexcept
	{
		return (length + 4095) / 4096;
	}

        void insert_u64(const uint32_t i4k, const uint32_t i64, const uint32_t i, const uint64_t index)
        {
                const auto mask = uint64_t(1) << i64;

                if (trace)
                {
                        SLog("mask = ", mask, ", i4k = ", i4k, ", i64 = ", i64, ", i = ", i, ", index = ", index, "\n");
                        SLog("indices[", i4k, "] |= ", mask, "\n");
                }

                indices[i4k] |= mask;

                const auto o = SwitchBitOps::PopCnt(index & (mask - 1));

                if (trace)
                        SLog("o = ", o, "\n");

                auto *const ba = bits + i4k;

                if (trace)
                        SLog("capacity = ", ba->capacity, "\n");

                if (o >= ba->capacity)
                {
                        const auto n = ba->capacity;

                        ba->capacity = std::min<uint8_t>(64, ba->capacity + 4);
                        ba->values = (uint64_t *)realloc(ba->values, sizeof(uint64_t) * ba->capacity);
                        memset(ba->values + n, 0, (ba->capacity - n) * sizeof(uint64_t));
                }

                memmove(ba->values + o, ba->values + o + 1, (ba->capacity - o - 1) * sizeof(uint64_t));
                ba->values[o] = uint64_t(1) << (i & 63);

                if (trace)
                        SLog("Did set values[", o, "] to ", ba->values[o], "\n");

                ++nonZero;
        }

      public:
        // can hold bits between [0, length)
        SparseFixedBitSet(const std::size_t len)
            : length(len < 1 ? 1 : len), indices((uint64_t *)calloc(sizeof(uint64_t), block_count()))
        {
                EXPECT(indices);
                bits = (bucket *)malloc(sizeof(bucket) * block_count());
                if (!bits)
                        throw Switch::data_error("Failed to allocate memory");

		const auto bc = block_count();

                for (uint32_t i{0}; i != bc; ++i)
                        bits[i].capacity = 0;
        }

        ~SparseFixedBitSet()
        {
		const auto bc = block_count();

                std::free(indices);

                for (uint32_t i{0}; i != bc; ++i)
                {
                        if (bits[i].capacity)
                                std::free(bits[i].values);
                }
                std::free(bits);
        }

        std::size_t cardinality() const noexcept
        {
                std::size_t res{0};
		const auto bc = block_count();

                for (uint32_t i{0}; i != bc; ++i)
                {
                        const auto &b = bits[i];

                        for (uint32_t i{0}; i != b.capacity; ++i)
			{
				if (const auto v = b.values[i])
	                                res += SwitchBitOps::PopCnt(b.values[i]);
			}
                }
                return res;
        }

        std::size_t approximate_cardinality() const noexcept
        {
                // assuming the bits are unformly set, and use of the linear counting algo. to estimate the number of bits
                // that are set based on the number of u64 that are != 0
                const auto total = (length + 63) >> 6; // total u64 in the space
                const auto zero = total - nonZero;

                // guard against division by zero
                const auto estimate = std::round(total + log(double(total) / double(zero)));

                return std::min<std::size_t>(length, estimate);
        }

        inline bool test_consider_len(const uint32_t i) const noexcept
        {
                return i < length ? test(i) : false;
        }

        // just one branch, but multiple bitops
        bool test(const uint32_t i) const noexcept
        {
                const uint32_t i4k = i >> 12;
                const auto index = indices[i4k];      // i-th bit set for bits[index][i-th] that's != 0
                const auto i64 = (i & 4095) >> 6;     // bucket index in the 4K partition
                const auto mask = uint64_t(1) << i64;

                if (trace)
                        SLog(ansifmt::color_blue, "for i = ", i, ", i4k = ", i4k, ", index = ", index, ", i64 = ", i64, ansifmt::reset, "\n");

                if (!(index & mask))
                {
                        if (trace)
                                SLog("Bucket not set\n");
                        return false;
                }

                // count the number of bits that are set on the right of i64, and that gives us
                // the index of the u64 that stores the bits we are interested in
                const auto b = bits[i4k].values[SwitchBitOps::PopCnt(index & (mask - 1))];

                if (trace)
                        SLog("bitscount = ", SwitchBitOps::PopCnt(index & (mask - 1)), " ", b, " ", (bool)(!!(b & (uint64_t(1) << (i & 63)))), "\n");

                return b & (uint64_t(1) << (i & 63));
        }

        void set(const uint32_t i)
        {
                const uint32_t i4k = i >> 12;
                const auto index = indices[i4k]; // i-th bit set for bits[index][i-th] that's != 0
                const auto i64 = (i & 4095) >> 6;
                const auto mask = uint64_t(1) << i64;

                if (trace)
                        SLog(ansifmt::color_green, "set i4k = ", i4k, ", i = ", i, ", index = ", index, ", i64 = ", i64, ", mask = ", mask, ansifmt::reset, "\n");

                if (index & mask)
                {
                        // the sub 64-bits block already exists
                        // set a bit in the exisiting u64, the number of ones on the right of the u64 gives us the index of the long we need to update
                        if (trace)
                                SLog("Already exists, setting bit ", i & 63, "\n");

                        bits[i4k].values[SwitchBitOps::PopCnt(index & (mask - 1))] |= uint64_t(1) << (i & 63);
                }
                else if (0 == index)
                {
                        // found a block of 4k bits that has no bit that is set yet
                        if (trace)
                                SLog("No block\n");

                        insert_block(i4k, i64, i);
                }
                else
                {
                        // found a block of k bits that has some values, but the sub-block of 64bits we are interested in has no value yet
                        if (trace)
                                SLog("Yes block\n");

                        insert_u64(i4k, i64, i, index);
                }
        }

        bool try_set(const uint32_t i)
        {
                const uint32_t i4k = i >> 12;
                const auto index = indices[i4k]; // i-th bit set for bits[index][i-th] that's != 0
                const auto i64 = (i & 4095) >> 6;
                const auto mask = uint64_t(1) << i64;

                if (trace)
                        SLog(ansifmt::color_green, "set i4k = ", i4k, ", i = ", i, ", index = ", index, ", i64 = ", i64, ", mask = ", mask, ansifmt::reset, "\n");

                if (index & mask)
                {
                        // the sub 64-bits block already exists
                        // set a bit in the exisiting u64, the number of ones on the right of the u64 gives us the index of the long we need to update
                        if (trace)
                                SLog("Already exists, setting bit ", i & 63, "\n");

			auto &m = bits[i4k].values[SwitchBitOps::PopCnt(index & (mask - 1))];
			const auto saved{m};

                        m |= uint64_t(1) << (i & 63);
			return m != saved;
                }
                else if (0 == index)
                {
                        // found a block of 4k bits that has no bit that is set yet
                        if (trace)
                                SLog("No block\n");

                        insert_block(i4k, i64, i);
			return true;
                }
                else
                {
                        // found a block of k bits that has some values, but the sub-block of 64bits we are interested in has no value yet
                        if (trace)
                                SLog("Yes block\n");

                        insert_u64(i4k, i64, i, index);
			return true;
                }
        }
};

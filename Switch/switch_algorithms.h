#pragma once
#include <stdlib.h>

namespace SwitchAlgorithms
{
	static inline uint64_t Uniform(const uint64_t low, const uint64_t high)
	{
		return low + (rand()%(high - low));
	}

        inline uint32_t ComputeExponentialBackoffWithDeccorelatedJitter(const uint64_t cap, const uint64_t base, const uint64_t prevSleep)
        {
                return std::min<uint64_t>(cap, Uniform(base, prevSleep * 3));
        }
}

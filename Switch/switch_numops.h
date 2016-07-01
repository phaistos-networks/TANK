#pragma once
#include <algorithm>

template<typename T>
[[gnu::always_inline]] inline T Clamp(const T v, const T min, const T max)
{
	static_assert(std::is_scalar<T>::value, "Expected scalar");
	return std::min(std::max(v, min), max);
}

inline static int RoundToMultiple(const int v, const int alignment)
{
        const int mask = alignment - 1;

        return (v + mask) & ~mask;
}


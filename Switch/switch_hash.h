#pragma once

#ifndef __clang__
extern "C" {
#endif
unsigned long long XXH64(const void *input, unsigned int len, unsigned long long seed);
#ifndef __clang__
}
#endif

static inline uint64_t FNVHash64(const uint8_t *const p, const uint32_t len)
{
        uint64_t h{14695981039346656037ULL};

        for (uint32_t i = 0; i != len; ++i)
                h = (h * 1099511628211ULL) ^ p[i];

        return h;
}

static inline constexpr uint64_t BeginFNVHash64(void)
{
        return 14695981039346656037ULL;
}

static inline uint64_t FNVHash64(uint64_t h, const uint8_t *const p, const uint32_t len)
{
        for (uint32_t i = 0; i != len; ++i)
                h = (h * 1099511628211ULL) ^ p[i];
        return h;
}



#pragma once
#include <switch.h>

#ifdef LEAN_SWITCH
#define RFLog(...) Print(__VA_ARGS__)
#endif

namespace TankFlags
{
        enum class BundleMsgFlags : uint8_t
        {
                HaveKey = 1,
                UseLastSpecifiedTS = 2
        };
}

#pragma once
#include <switch.h>

#define TANK_VERSION (0 * 100) + 17

// All kind of if (trace) SLog() calls here, for checks and for debugging. Will be stripped out later

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

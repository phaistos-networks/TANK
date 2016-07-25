/*
 *	(C) Phaistos Networks, S.A
 *	http://phaistosnetworks.gr/
 *
 *	Licensed under Apache 2 License
 *
 */
#pragma once
#include <switch.h>

#define TANK_VERSION (0 * 100 + 32)

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

enum class TankAPIMsgType : uint8_t
{
        Produce = 0x1,
        Consume = 0x2,
        Ping = 0x3,
        RegReplica = 0x4
};

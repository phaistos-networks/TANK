/*
 *	(C) Phaistos Networks, S.A
 *	http://phaistosnetworks.gr/
 *
 *	Licensed under Apache 2 License
 *
 */
#pragma once
#include <switch.h>

#define TANK_VERSION (0 * 100 + 53)

// All kind of if (trace) SLog() calls here, for checks and for debugging. Will be stripped out later

#ifdef LEAN_SWITCH
#define RFLog(...) Print(__VA_ARGS__)
#endif

namespace TankFlags
{
        enum class BundleMsgFlags : uint8_t
        {
                HaveKey = 1,
                UseLastSpecifiedTS = 2,
		SeqNumPrevPlusOne = 4
        };
}

enum class TankAPIMsgType : uint8_t
{
        Produce = 0x1,
        Consume = 0x2,
        Ping = 0x3,
        RegReplica = 0x4,

	// Same as Produce, except that the seq.num of the first msg of the first bundle is going to
	// be explicitly encoded in the request.
	//
	// This going to be used by tank-cli mirroring functionality (and maybe for replication), where
	// for example older segments may have been deleted and we are expected to consume from a node and produce to another, except
	// that if the other is starting off with no data, it will logically assign (1) as the first message of the bundle, not
	// whatever the origin had when we consumed from it.
	//
	// For example, if the earliest available segment for a partition in origin has a baseSeqNum (1000) (i.e not 1), and we consume from it
	// in order to mirror to another node, and that node has no data for that partition, it will create a new segment with baseSeqNum(0), not 1.
	ProduceWithBaseSeqNum=0x5,
	DiscoverPartitions=0x6,

	CreateTopic
};

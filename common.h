#pragma once
#include <switch.h>
#include <ext/martinus/robin_hood.h>
#include <cassert>

#define TANK_RUNTIME_CHECKS 1

#ifdef TANK_RUNTIME_CHECKS
//#define TANK_EXPECT(...) EXPECT(__VA_ARGS__)
#define TANK_EXPECT(...) do { assert(__VA_ARGS__); } while (0)
#define TANK_NOEXCEPT_IF_NORUNTIME_CHECKS
#else
#define TANK_EXPECT(...) \
        do {             \
        } while (0)
#define TANK_NOEXCEPT_IF_NORUNTIME_CHECKS noexcept
#define TANK_THROW_SWITCH_EXCEPTIONS 1
#endif

#define MAKE_TANK_RELEASE(major, minor) ((major)*100 + (minor))
#define TANK_VERSION (MAKE_TANK_RELEASE(3, 5))

namespace TankFlags {
        enum class BundleMsgFlags : uint8_t {
                HaveKey            = 1,
                UseLastSpecifiedTS = 2,
                SeqNumPrevPlusOne  = 4
        };
}

namespace TANK_Limits {
        static constexpr const std::size_t max_topic_partitions{65530};
        static constexpr const std::size_t max_topic_name_len{64};
}

enum class TankAPIMsgType : uint8_t {
        Produce               = 0x1,
        Consume               = 0x2,
        Ping                  = 0x3,
        __obsolete_RegReplica = 0x4,

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
        ProduceWithSeqnum  = 0x5,
        DiscoverPartitions = 0x6,
        CreateTopic        = 0x7,
        ReloadConf         = 0x8,
        ConsumePeer        = 0x9,
        Status             = 10,
};

namespace TANKUtil {
        template <typename T>
        inline T minimum(const T &t) noexcept {
                return t;
        }
        template <typename T, typename... P>
        inline auto minimum(const T &t, const P &... p) noexcept {
                using res_type = std::common_type_t<T, P...>;

                return std::min(res_type(t), res_type(minimum(p...)));
        }

        // TODO: figure out a better name
        namespace produce_request_acks {
                // Require ack from all nodes in ISR
                inline uint8_t ISR() noexcept {
                        return 0;
                }

                // Require ack from (all nodes in ISR / 2 + 1)
                inline uint8_t ISR_quorum() noexcept {
                        return 255;
                }

                inline uint8_t value(const uint8_t v) {
                        TANK_EXPECT(v != ISR());
                        TANK_EXPECT(v != ISR_quorum());
                        return v;
                }
        } // namespace produce_request_acks

        // we need this to guard agains race condition which stem from our deferred _now updates
        inline constexpr uint32_t time32_delta(const time32_t start, const time32_t end) {
                return end >= start ? end - start : 0;
        }

	inline void safe_close(int fd) {
		TANK_EXPECT(fd > 2);
		close(fd);
	}

        // saniy check
        static_assert(time32_delta(0, 10) == 10);
        static_assert(time32_delta(11, 10) == 0);
} // namespace TANKUtil

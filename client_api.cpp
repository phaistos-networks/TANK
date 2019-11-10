#include "client_common.h"

bool TankClient::process_msg(connection *const c, const uint8_t msg, const uint8_t *const content, const size_t len) {
        static constexpr bool trace{false};

        if (trace) {
                SLog("About to process message of type ", msg, ", len = ", len, "\n");
        }

        switch (TankAPIMsgType(msg)) {
                case TankAPIMsgType::Produce:
                        return process_produce(c, content, len);

                case TankAPIMsgType::Consume:
#ifdef TANK_CLIENT_FAST_CONSUME
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                        throw Switch::runtime_error("Unexpected message");
#else
                        throw std::runtime_error("Unexpected Message");
#endif
#else
                        return process_consume(c, content, len);
#endif

                case TankAPIMsgType::DiscoverPartitions:
                        return process_discover_partitions(c, content, len);

                case TankAPIMsgType::ReloadConf:
                        return process_reload_partition_conf(c, content, len);

                case TankAPIMsgType::CreateTopic:
                        return process_create_topic(c, content, len);

                case TankAPIMsgType::Status:
                        return process_srv_status(c, content, len);

                case TankAPIMsgType::Ping:
                        if (trace) {
                                SLog("PING\n");
                        }

                        return true;

                default:
                        shutdown(c, __LINE__);
                        return false;
        }
}


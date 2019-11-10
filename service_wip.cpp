#include "service_common.h"

bool Service::generate_produce_response(produce_response *pr) {
        static constexpr bool trace{false};
        TANK_EXPECT(pr);
        auto c = pr->client_ctx.ch.get();

        if (trace) {
                SLog("Generating produce response for ", ptr_repr(pr), ", participants = ", pr->participants.size(), "\n");
        }

        if (!c || -1 == c->fd) {
                // connection's no longer around
                if (trace) {
                        SLog("Connection is NA\n");
                }

                return true;
        }

        auto q   = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto dvp = get_data_vector_payload();
        auto b   = (dvp->buf = get_buf());

        b->pack(static_cast<uint8_t>(TankAPIMsgType::Produce), static_cast<uint32_t>(0), static_cast<uint32_t>(pr->client_ctx.req_id));

        const auto encode_err = [b](const auto &it) {
                switch (it.res) {
                        case produce_response::participant::OpRes::OK:
                                b->pack(static_cast<uint8_t>(0));
                                break;

                        case produce_response::participant::OpRes::InvalidSeqNums:
                                b->pack(static_cast<uint8_t>(10));
                                break;

                        case produce_response::participant::OpRes::UnknownPartition:
                                b->pack(static_cast<uint8_t>(0x1));
                                break;

                        case produce_response::participant::OpRes::NoLeader:
                                b->pack(static_cast<uint8_t>(0xfd));
                                break;

                        case produce_response::participant::OpRes::OtherLeader: {
                                auto leader = it.p->cluster.leader.node;

                                TANK_EXPECT(leader);
                                b->pack(static_cast<uint8_t>(0xfc));
                                b->pack(leader->ep.addr4, leader->ep.port);
                        } break;

                        case produce_response::participant::OpRes::IO_Fault:
                                b->pack(static_cast<uint8_t>(0x2));
                                break;

                        case produce_response::participant::OpRes::Pending:
                                // unable to get acks.
                                b->pack(static_cast<uint8_t>(0x3));
                                break;
                                break;

                        case produce_response::participant::OpRes::InsufficientReplicas:
                                // unable to get acks.
                                b->pack(static_cast<uint8_t>(0x4));
                                break;

                        default:
                                IMPLEMENT_ME();
                }
        };

        for (const auto *p = pr->participants.data(), *const e = p + pr->participants.size(); p < e;) {
                const auto topic_name = p->topic_name;

                if (p->res == produce_response::participant::OpRes::UnknownTopic || p->res == produce_response::participant::OpRes::ReadOnly) {
                        if (trace) {
                                SLog("Unknown Topic\n");
                        }

                        switch (p->res) {
                                case produce_response::participant::OpRes::UnknownTopic:
                                        b->pack(static_cast<uint8_t>(0xff));
                                        break;

                                case produce_response::participant::OpRes::ReadOnly:
                                        b->pack(static_cast<uint8_t>(0xfe));
                                        break;

                                default:
                                        IMPLEMENT_ME();
                        }

                        do {
                                //
                        } while (++p < e && p->topic_name == topic_name);
                        continue;
                }

                // for the first partition
                // this is rather silly, but we can't break the protocol
                encode_err(*p);

                for (;;) {
                        if (++p == e || p->topic_name != topic_name) {
                                break;
                        } else {
                                encode_err(*p);
                        }
                }
        }

        *reinterpret_cast<uint32_t *>(b->data() + sizeof(uint8_t)) = b->size() - sizeof(uint8_t) - sizeof(uint32_t);

        if (trace) {
                SLog("Generated produce response, NeedOutAvail = ", c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)), "\n");
        }

        dvp->append(b->as_s32());
        q->push_back(dvp);
        return try_tx(c);
}

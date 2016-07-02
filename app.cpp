#include "tank_client.h"
#include <text.h>

int main(int argc, char *argv[])
{
	if (argc < 2)
	{
		Print("This is a demo app; it will connect to Tank broker at 127.0.0.1:1025 and will either produce(publish) or consume(fetch) some data from it\n");
		Print("Use ./app get to some some data. If you specify an argument, that will be the sequence number to begin consuming from\n");
		Print("Use ./app set to set some data. Use ./app set filePath to read lines from that file and set each as a distinct message\n");

		return 0;
	}
        TankClient client;
        const strwlen32_t req(argv[1]);

	client.set_default_leader(":1025");

        if (req.Eq(_S("get")))
        {
                client.consume(
                    {{{"bp_activity", 0}, {argc > 2 ? strwlen32_t(argv[2]).AsUint32() : UINT64_MAX, 10'000}}}, 10000, 128);
        }
        else if (req.Eq(_S("set")))
        {
                if (argc > 2 && argv[2][0] == '/')
                {
                        std::vector<TankClient::msg> msgs;
                        IOBuffer b;
                        const auto now = Timings::Milliseconds::SysTime();
                        int fd = open(argv[2], O_RDONLY | O_LARGEFILE);

                        Print("Reading ", argv[2], " ..\n");

                        if (fd == -1)
                        {
                                Print("Unable to read ", argv[2], "\n");
                                return 1;
                        }

                        const auto fileSize = lseek64(fd, 0, SEEK_END);

                        if (!fileSize)
                        {
                                close(fd);
                                Print("Empty file\n");
                                return 1;
                        }

                        auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                        assert(fileData != MAP_FAILED);
                        madvise(fileData, fileSize, MADV_SEQUENTIAL);

                        Print("Processing\n");
                        for (const auto &line : strwlen32_t((char *)fileData, fileSize).Split('\n'))
                        {
                                msgs.push_back({line, now});

                                if (msgs.size() == 128)
                                {
                                        client.produce(
                                            {{{"bp_activity", 0},
                                              msgs}

                                            });

                                        msgs.clear();
                                }
                        }

                        if (msgs.size())
                        {
                                client.produce(
                                    {{{"bp_activity", 0},
                                      msgs}

                                    });
                        }

                        munmap(fileData, fileSize);
                }
                else if (argc == 2)
                {
                        Print("Use ./app set msg1 msg2 msg2 .. to set messages\n");
                        return 0;
                }
                else
                {
                        std::vector<TankClient::msg> msgs;

                        for (uint32_t i{2}; i != argc; ++i)
                                msgs.push_back({argv[i], Timings::Milliseconds::Tick()});

                        const auto req = client.produce(
                            {{{"bp_activity", 0},
                              msgs}});

			Print("Publishing ", dotnotation_repr(msgs.size()), " msg(s) to bp_activity.0, client request id = ", req, "\n");
                }
        }

        try
	{
                for (;;)
                {
                        client.poll(1000);

                        for (const auto &it : client.consumed())
                        {
                                Print("Consumed from ", it.topic, ".", it.partition, ", next {", it.next.seqNum, ", ", it.next.minFetchSize, "}\n");

                                for (const auto mit : it.msgs)
                                {
                                        Print("[", mit->key, "]:", mit->content, "(", mit->seqNum, ")\n");
                                }
                        }

                        for (const auto &it : client.faults())
                        {
                                Print("Fault for ", it.clientReqId, ", type of fault:");

                                switch (it.type)
                                {
                                        case TankClient::fault::Type::BoundaryCheck:
						Print("BoundaryCheck\n");
                                                Print("firstAvailSeqNum = ", it.ctx.firstAvailSeqNum, ", hwMark = ", it.ctx.highWaterMark, "\n");
                                                break;

					case TankClient::fault::Type::UnknownTopic:
						Print("Unknown topic\n");
						break;

					case TankClient::fault::Type::UnknownPartition:
						Print("Unknown partition\n");
						break;


					case TankClient::fault::Type::Access:
						Print("Access\n");
						break;

					case TankClient::fault::Type::Network:
						Print("Network\n");
						break;


                                        default:
                                                break;
                                }
                        }

                        for (const auto &it : client.produce_acks())
                        {
                                Print("Produced OK ", it.clientReqId, "\n");
                        }
                }
        }
        catch (const std::exception &e)
        {
		Print("failed:", e.what(), "\n");
        }

        return 0;
}

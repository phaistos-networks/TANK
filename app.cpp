#include "tank_client.h"
#include <text.h>

int main(int argc, char *argv[])
{
	Buffer endpoint, topic;
	uint16_t partition{0};
	int r;
	
	topic.Append(_S("events"));
	endpoint.Append(_S("127.0.0.1:1025"));
	while ((r = getopt(argc, argv, "b:t:p:")) != -1)
        {
                switch (r)
                {
                        case 'b':
                                endpoint.clear();
                                endpoint.Append(optarg);
                                break;

                        case 't':
                                topic.clear();
                                topic.Append(optarg);
                                break;

                        case 'p':
                                partition = strwlen32_t(optarg).AsUint32();
                                break;

                        default:
                                return 1;
                }
        }

        argc-=optind;
	argv+=optind;


	if (argc < 1)
	{
		Print("This is a demo app; it will connect to Tank broker at ", endpoint, " and will either produce(publish) or consume(fetch) some data from it\n");
		Print("(selected topic '", topic, "', partition ", partition, ")\n");
		Print("Use ./app get to some some data. If you specify an argument, that will be the sequence number to begin consuming from.\n");
		Print("\tIf that argument is EOF it will 'tail' that partition. If it is 0, it will stream starting from the first available message\n");
		Print("Use ./app set to set some data. Use ./app set filePath to read lines from that file and set each as a distinct message\n");
		Print("Options:\n");
		Print("-b endpoint: standalone broker endpoint (default 127.0.0.1:1025)\n");
		Print("-t topic: selected topic name (default is \"events\")\n");
		Print("-p partition: selected partition (default 0)\n");
		Print("e.g ./app -b :1025 -t events -p 0 set   first second third fourth\n");
		Print("e.g ./app -b :1025 -t events get\n");
		return 0;
	}
        TankClient client;
        const strwlen32_t req(argv[0]);

	client.set_default_leader(endpoint.AsS32());

        if (req.Eq(_S("get")))
        {
                const uint64_t base = argc > 1 ? !memcmp(argv[1], _S("EOF")) ? UINT64_MAX : strwlen32_t(argv[1]).AsUint64() : 0;

                client.consume(
                    {{{topic.AsS8(), partition}, {base, 10'000}}}, 10000, 0);
        }
        else if (req.Eq(_S("set")))
        {
                if (argc > 1 && argv[1][0] == '/')
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
                                            {{{topic.AsS8(), partition},
                                              msgs}

                                            });

                                        msgs.clear();
                                }
                        }

                        if (msgs.size())
                        {
                                client.produce(
                                    {{{topic.AsS8(), partition},
                                      msgs}

                                    });
                        }

                        munmap(fileData, fileSize);
                }
                else if (argc == 1)
                {
                        Print("Use ./app set msg1 msg2 msg2 .. to set messages\n");
                        return 0;
                }
                else
                {
                        std::vector<TankClient::msg> msgs;

                        for (uint32_t i{1}; i != argc; ++i)
                                msgs.push_back({argv[i], Timings::Milliseconds::Tick()});

                        const auto req = client.produce(
                            {{{topic.AsS8(), partition},
                              msgs}});

			Print("Publishing ", dotnotation_repr(msgs.size()), " msg(s) to ", topic, ".", partition, ", client request id = ", req, "\n");
                }
        }
	else
	{
		Print("Unsupported command ", req, "\n");
		return 1;
	}

        try
	{
		bool noEvents{true};

		while (noEvents)
                {
                        client.poll(1000);

                        for (const auto &it : client.consumed())
                        {
                                Print("Consumed from ", it.topic, ".", it.partition, ", next {", it.next.seqNum, ", ", it.next.minFetchSize, "}\n");

                                for (const auto mit : it.msgs)
                                {
                                        Print("[", mit->key, "]:", mit->content, "(", mit->seqNum, ")\n");
                                }
				noEvents = false;
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
				noEvents = false;
                        }

                        for (const auto &it : client.produce_acks())
                        {
                                Print("Produced OK ", it.clientReqId, "\n");
				noEvents = false;
                        }
                }
        }
        catch (const std::exception &e)
        {
		Print("failed:", e.what(), "\n");
        }

        return 0;
}

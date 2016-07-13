#include "tank_client.h"
#include <fcntl.h>
#include <network.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <set>

int main(int argc, char *argv[])
{
	Buffer topic, endpoint;
	uint16_t partition{0};
	int r;
	TankClient tankClient;
	const char *const app = argv[0];

	if (argc == 1)
		goto help;

	while ((r = getopt(argc, argv, "+b:t:p:h")) != -1) 	// see GETOPT(3) for '+' initial character semantics
        {
                switch (r)
                {

			case 'b':
				endpoint.clear();
				endpoint.append(optarg);
				break;

                        case 't':
                                topic.clear();
                                topic.append(optarg);
				if (topic.length() > 255)
				{
					Print("Inalid topic name '", topic, "'\n");
					return 1;
				}
                                break;

                        case 'p':
                        {
                                const strwlen32_t s(optarg);

                                if (!s.IsDigits())
                                {
                                        Print("Invalid partition\n");
                                        return 1;
                                }

                                const auto v = s.AsInt32();

                                if (v < 0 || v > UINT16_MAX)
                                {
                                        Print("Invalid partition\n");
                                        return 1;
                                }

                                partition = v;
                        }
                        break;


			case 'h':
				help:
				Print(app, " [common options] command [command options] [command arguments]\n");
				Print("Common options include:\n");
				Print("-b broker endpoint: The endpoint of the Tank broker\n");
				Print("-t topic: The topic to produce to or consume from\n");
				Print("-p partition: The partition of the topic to produce to or consme from\n");
				Print("Commands available: consume, produce\n");
				return 0;

                        default:
				Print("Please use ", app, " -h for options\n");
                                return 1;
                }
        }

	if (!topic.length())
	{
		Print("Topic not specified. Use -t to specify topic\n");
		return 1;
	}

	if (!endpoint.length())
	{	
		Print("Broker endpoint not specified. Use -b to specify endpoint\n");
		return 1;
	}

        argc-=optind;
	argv+=optind;

        try
        {
                tankClient.set_default_leader(endpoint.AsS32());
        }
        catch (...)
        {
                Print("Invalid broker endpoint specified '", endpoint, "'\n");
                return 1;
        }

	if (!argc)
	{
		Print("Command not specified. Please use ", app, " -h for available commands\n");
		return 1;
	}

	const strwlen32_t cmd(argv[0]);
	const TankClient::topic_partition topicPartition(topic.AsS8(), partition);
        const auto consider_fault = [](const TankClient::fault &f) {
                switch (f.type)
                {
                        case TankClient::fault::Type::BoundaryCheck:
                                Print("Boundary Check fault. first available sequence number is ", f.ctx.firstAvailSeqNum, ", high watermark is ", f.ctx.highWaterMark, "\n");
				break;

                        case TankClient::fault::Type::UnknownTopic:
                                Print("Unknown topic '", f.topic, "' error\n");
				break;

                        case TankClient::fault::Type::UnknownPartition:
                                Print("Unknown partition of '", f.topic, "' error\n");
				break;

                        case TankClient::fault::Type::Access:
                                Print("Access error\n");
				break;

                        case TankClient::fault::Type::Network:
                                Print("Network error\n");
				break;
			
			default:
				break;
                }
        };

        if (cmd.Eq(_S("get")) || cmd.Eq(_S("consume")))
        {
                uint64_t next{0};
                enum class Fields : uint8_t
                {
                        SeqNum = 0,
                        Key,
                        Content,
                        TS
                };
                uint8_t displayFields{1u << uint8_t(Fields::Content)};
                static constexpr size_t defaultMinFetchSize{32 * 1024 * 1024};
                size_t minFetchSize{defaultMinFetchSize};
                uint32_t pendingResp{0};
		bool statsOnly{false};

		optind = 0;
                while ((r = getopt(argc, argv, "SF:h")) != -1)
                {
                        switch (r)
                        {
				case 'S':
					statsOnly = true;
					break;

                                case 'F':
                                        displayFields = 0;
                                        for (const auto it : strwlen32_t(optarg).Split(','))
                                        {
                                                if (it.Eq(_S("seqnum")))
                                                        displayFields |= 1u << uint8_t(Fields::SeqNum);
                                                else if (it.Eq(_S("key")))
                                                        displayFields |= 1u << uint8_t(Fields::Key);
                                                else if (it.Eq(_S("content")))
                                                        displayFields |= 1u << uint8_t(Fields::Content);
                                                else if (it.Eq(_S("ts")))
                                                        displayFields |= 1u << uint8_t(Fields::TS);
                                                else
                                                {
                                                        Print("Unknown field '", it, "'\n");
                                                        return 1;
                                                }
                                        }
                                        break;

				case 'h':
					Print("CONSUME [options] from\n");
					Print("Options include:\n");
					Print("-F display format: Specify a ',' separated list of message properties to be displayed. Properties include: \"seqnum\", \"key\", \"content\", \"ts\". By default, only the content is displayed\n");
					Print("-S: statistics only\n");
					Print("\"from\" specifies the first message we are interested in.\n");
					Print("If from is \"beginning\" or \"start\"," \
						" it will start consuming from the first available message in the selected topic. If it is \"eof\" or \"end\", it will " \
						"tail the topic for newly produced messages, otherwise it must be an absolute 64bit sequence number\n");
					return 0;

                                default:
                                        return 1;
                        }
                }

                argc -= optind;
                argv += optind;

		if (!argc)
		{
			Print("Expected sequence number to begin consuming from. Please see ", app, " consume -h\n");
			return 1;
		}
		else
                {
                        const strwlen32_t from(argv[0]);

                        if (from.EqNoCase(_S("beginning")) || from.Eq(_S("first")))
                                next = 0;
                        else if (from.EqNoCase(_S("end")) || from.EqNoCase(_S("eof")))
                                next = UINT64_MAX;
                        else if (!from.IsDigits())
                        {
                                Print("Expected either \"beginning\", \"end\" or a sequence number for -f option\n");
                                return 1;
                        }
                        else
                                next = from.AsUint64();
                }

                for (;;)
                {
                        if (!pendingResp)
                        {
                                pendingResp = tankClient.consume({{topicPartition, {next, minFetchSize}}}, 2e3, 0);

                                if (!pendingResp)
                                {
                                        Print("Unable to issue consume request. Will abort\n");
                                        return 1;
                                }
                        }

                        try
                        {
                                tankClient.poll(1e3);
                        }
                        catch (...)
                        {
                                continue;
                        }

                        for (const auto &it : tankClient.faults())
                        {
				consider_fault(it);
				return 1;
			}

			for (const auto &it : tankClient.consumed())
                        {
                                if (statsOnly)
                                {
					Print(it.msgs.len, " messages\n");
                                }
                                else
                                {
                                        for (const auto m : it.msgs)
                                                Print(m->content, "\n");
                                }

                                minFetchSize = Max<size_t>(it.next.minFetchSize, defaultMinFetchSize);
                                next = it.next.seqNum;
                                pendingResp = 0;
                        }
                }
        }
        else if (cmd.Eq(_S("set")) || cmd.Eq(_S("produce")) || cmd.Eq(_S("publish")))
        {
		char path[PATH_MAX];
		size_t bundleSize{1};

                optind = 0;
		path[0] = '\0';
                while ((r = getopt(argc, argv, "s:f:h")) != -1)
                {
                        switch (r)
                        {
				case 's':
					bundleSize = strwlen32_t(optarg).AsUint32();
					if (!bundleSize)
					{
						Print("Invalid bundle size specified\n");
						return 1;
					}
					break;

				case 'f':
					{
						const auto l = strlen(optarg);

						require(l < sizeof(path));
						memcpy(path, optarg, l);
						path[l] = '\0';
					}
					break;

                                case 'h':
                                        Print("PRODUCE options [message1 message2...]\n");
                                        Print("Options include:\n");
                                        Print("-s number: The bundle size; how many messages to be grouped into a bundle before producing that to the broker. Default is 1, which means each new message is published as a single bundle\n");
                                        Print("-f file: The messages are read from `file`, which is expected to contain the mesasges in every new line. The `file` can be \"-\" for stdin. If this option is provided, the messages list is ignored\n");
                                        return 1;

                                default:
                                        return 1;
                        }
                }

                argc -= optind;
                argv += optind;

                static constexpr size_t pollInterval{20};
                std::vector<TankClient::msg> msgs;
                size_t pendingAckAcnt{0};
                std::set<uint32_t> pendingResps;
                const auto poll = [&]() {
                        tankClient.poll(8e2);

                        for (const auto &it : tankClient.faults())
                        {
                                consider_fault(it);
                                return false;
                        }

                        for (const auto &it : tankClient.produce_acks())
                                pendingResps.erase(it.clientReqId);

                        return true;
                };
                const auto publish_msgs = [&]() {
                        const auto reqId = tankClient.produce(
                            {{topicPartition, msgs}});

                        if (!reqId)
                        {
                                Print("Failed to schedule messages to broker\n");
                                return false;
                        }
                        else
                                pendingResps.insert(reqId);

                        pendingAckAcnt += msgs.size();

                        if (pendingAckAcnt >= pollInterval)
                        {
                                while (tankClient.should_poll())
                                {
                                        if (!poll())
                                                return false;
                                }

                                pendingAckAcnt = 0;
                        }

                        return true;
                };

                if (path[0])
                {
                        static constexpr size_t bufSize{64 * 1024};
			int fd;
			auto *const buf = (char *)malloc(bufSize);
			range32_t range;
			strwlen32_t s;

                        Defer({
                                free(buf);
                                if (fd != STDIN_FILENO && fd != -1)
                                        close(fd);
                        });

                        if (!strcmp(path, "-"))
			{
				fd = STDIN_FILENO;
				require(fd != -1);
			}
			else
                        {
                                fd = open(path, O_RDONLY | O_LARGEFILE);

                                if (fd == -1)
                                {
                                        Print("Failed to open(", path, "): ", strerror(errno), "\n");
                                        return 1;
                                }
                        }


                        const auto consider_input = [&](const bool last) {

				while (range)
                                {
                                        s.Set(buf + range.offset, range.len);

                                        if (const auto *const p = s.Search('\n'))
                                        {
                                                const uint32_t n = p - s.p;
						auto data = (char *)malloc(n);

						memcpy(data, s.p, n);
                                                msgs.push_back({{data, n}, Timings::Milliseconds::SysTime(), {}});
                                                range.TrimLeft(n + 1);

						if (msgs.size() == bundleSize)
						{
							if (!publish_msgs())
								return false;

							for (auto &it : msgs)
								std::free(const_cast<char *>(it.content.p));
							msgs.clear();
						}
                                        }
                                        else
                                                break;
                                }

				if (last && msgs.size())
                                {
                                        if (!publish_msgs())
                                                return false;

                                        for (auto &it : msgs)
                                                std::free(const_cast<char *>(it.content.p));
                                        msgs.clear();
                                }

                                if (!range)
                                        range.Unset();

				return true;
                        };

                        range.Unset();
			for (;;)
			{
				const auto capacity = bufSize - range.End();
				auto r = read(fd, buf + range.offset, capacity);

				if (r == -1)
				{
					Print("Failed to read data:", strerror(errno), "\n");
					return 1;
				}
				else if (!r)
					break;
				else
				{
					range.len += r;
                                        if (!consider_input(false))
						return false;
                                }
                        }

                        if (!consider_input(true))
				return false;

                        while (tankClient.should_poll())
			{
				if (!poll())
					return 1;
			}
                }
		else if (!argc)
		{
			Print("No messages specified, and no input file was specified with -f. Please see ", app, " produce -h\n");
			return 1;
		}
		else
                {
                        for (uint32_t i{0}; i != argc; ++i)
                        {
                                msgs.push_back({strwlen32_t(argv[i]), Timings::Milliseconds::SysTime(), {}});

                                if (msgs.size() == bundleSize)
                                {
                                        if (!publish_msgs())
                                                return false;
                                        msgs.clear();
                                }
                        }

                        if (msgs.size())
                        {
                                if (!publish_msgs())
                                        return false;

                                msgs.clear();
                        }

                        while (tankClient.should_poll())
                        {
                                if (!poll())
                                        return false;
                        }
                }
        }
        else
	{
		Print("Command '", cmd, "' not supported. Please see ", app, " -h\n");
		return 1;
	}	

        return 0;
}

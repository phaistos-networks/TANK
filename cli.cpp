#include "tank_client.h"
#include <date.h>
#include <fcntl.h>
#include <network.h>
#include <set>
#include <sys/stat.h>
#include <sys/types.h>
#include <sysexits.h>
#include <text.h>
#include <unordered_map>
//#ifdef SWITCH_PHAISTOS
#include <text_table.h>
//#endif

static uint64_t parse_timestamp(strwlen32_t s) {
        strwlen32_t c;
        struct tm   tm;

        if (s.StripPrefix(_S("T-"))) {
                const char *p = s.data(), *const e = p + s.size();

                if (p == e || !isdigit(*p)) {
                        return 0;
                }

                uint32_t v{0};

                do {
                        v = v * 10 + (*(p++) - '0');
                } while (p < e && isdigit(*p));

                if (p < e) {
                        if (*p == 's' || *p == 'S') {
                                // seconds
                        } else if (*p == 'm' || *p == 'M') {
                                v *= 60;
                        } else if (*p == 'h' || *p == 'H') {
                                v *= 3600;
                        } else if (*p == 'd' || *p == 'D') {
                                v *= 86400;
                        }
                }

                return Timings::Seconds::ToMillis(time(nullptr) - v);
        }

        // for now, YYYYMMDDHH:MM:SS
        // Eventually, will support more date/timeformats

	if (s.StripPrefix(_S("today"))) {
		auto now = time(nullptr);

		localtime_r(&now, &tm);
	} else if (s.StripPrefix(_S("yesterday"))) {
		auto now = time(nullptr) - 86400;

		localtime_r(&now, &tm);
        } else {
                if (s.size() < "20181001"_len) {
                        return 0;
                }

                c = s.Prefix(4);
                if (!c.all_of_digits()) {
                        return 0;
                }
                s.StripPrefix(4);
                tm.tm_year = c.AsUint32() - 1900;

                s.StripPrefix(_S("."));
                c = s.Prefix(2);
                if (!c.all_of_digits()) {
                        return 0;
                }
                s.StripPrefix(2);
                tm.tm_mon = c.AsUint32() - 1;

                s.StripPrefix(_S("."));
                c = s.Prefix(2);
                if (!c.all_of_digits()) {
                        return 0;
                }
                s.StripPrefix(2);
                tm.tm_mday = c.AsUint32();
        }
	if (s && (s.front() == '@' || s.front() == ':')) {
		s.strip_prefix(1);
	}

        c = s.Prefix(2);
        s.StripPrefix(2);
        tm.tm_hour = c.AsUint32();

        if (s && s.StripPrefix(_S(":"))) {
                c = s.Prefix(2);
                s.StripPrefix(2);
                tm.tm_min = c.AsUint32();

                if (s && s.StripPrefix(_S(":"))) {
                        c = s.Prefix(2);
                        s.StripPrefix(2);
                        tm.tm_sec = c.AsUint32();
                } else {
                        tm.tm_sec = 0;
                }
        } else {
                tm.tm_min = 0;
		tm.tm_sec = 0;
        }

        tm.tm_isdst = -1;

	// SLog(Date::ts_repr(mktime(&tm)), "\n"); exit(0);

        if (const auto res = mktime(&tm); - 1 == res) {
                return 0;
        } else {
                return Timings::Seconds::ToMillis(res);
        }
}

static uint64_t lcrng(const uint64_t state) noexcept {
        return state * 6364136223846793005 + 1442695040888963407;
}

int main(int argc, char *argv[]) {
        Buffer            topic, endpoint;
        uint16_t          partition{0};
        bool              partition_specified{false};
        int               r;
        TankClient        tank_client;
        const char *const app = argv[0];
        bool              verbose{false}, retry{false};

        if (argc == 1) {
                goto help;
	}

        tank_client.set_retry_strategy(TankClient::RetryStrategy::RetryNever);
        while ((r = getopt(argc, argv, "+vb:t:p:hrS:R:")) != -1) // see GETOPT(3) for '+' initial character semantics
        {
                switch (r) {
                        case 'S':
                                tank_client.set_sock_sndbuf_size(strwlen32_t(optarg).AsUint32());
                                break;

                        case 'R':
                                tank_client.set_sock_rcvbuf_size(strwlen32_t(optarg).AsUint32());
                                break;

                        case 'r':
                                retry = true;
                                tank_client.set_retry_strategy(TankClient::RetryStrategy::RetryAlways);
                                break;

                        case 'v':
                                verbose = true;
                                break;

                        case 'b':
                                endpoint.clear();
                                endpoint.append(optarg);
                                break;

                        case 't': {
                                const strwlen32_t s(optarg);

                                topic.clear();

                                if (const auto p = s.Search('/')) {
                                        const auto str = s.SuffixFrom(p + 1);

                                        if (!str.IsDigits()) {
                                                Print("Invalid partition [", str, "]\n");
                                                return 1;
                                        }

                                        const auto v = str.AsInt32();

                                        if (v < 0 || v > UINT16_MAX) {
                                                Print("Invalid partition [", str, "]\n");
                                                return 1;
                                        }

                                        topic.append(s.PrefixUpto(p));
                                        partition = v;
					partition_specified = true;
                                } else {
                                        topic.append(s);
				}

                                if (!IsBetweenRangeInclusive<uint32_t>(topic.size(), 1, 240)) {
                                        Print("Inalid topic name '", topic, "'\n");
                                        return 1;
                                }
                        } break;

                        case 'p': {
                                const strwlen32_t s(optarg);

                                if (!s.IsDigits()) {
                                        Print("Invalid partition '", s, "'. Expected numeric id from 0 upto ", UINT16_MAX, "\n");
                                        return 1;
                                }

                                const auto v = s.AsInt32();

                                if (v < 0 || v > UINT16_MAX) {
                                        Print("Invalid partition\n");
                                        return 1;
                                }

				partition_specified = true;
                                partition = v;
                        } break;

                        case 'h':
                        help:
				Print("Usage: ", app, " <settings> [other common options] <command> [args]\n");
				Print("\nSettings:\n");
				Print(Buffer{}.append(align_to(5), "-b broker endpoint"_s32, align_to(32), "The endpoint of the TANK broker. If not specified, the default is localhost:11011"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "-t topic"_s32, align_to(32), "Selected topic"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "-p partition"_s32, align_to(32), "Selected partition. You can also use -t topic/partition to specify both the topic and the partition with -t"_s32), "\n");

				Print("\nOther common options:\n");
				Print(Buffer{}.append(align_to(5), "-S bytes"_s32, align_to(32), "Sets TANK Client's socket send buffer size"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "-R bytes"_s32, align_to(32), "Sets TANK Client's socket receive buffer size"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "-v"_s32, align_to(32), "Enables Verbose output"_s32), "\n");

				Print("\nCommands:\n");
				Print(Buffer{}.append(align_to(5), "consume"_s32, align_to(32), "Consumes content from partition"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "produce"_s32, align_to(32), "Produce content(events, messages) to partition"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "benchmark"_s32, align_to(32), "Benchmark TANK"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "discover_partitions"_s32, align_to(32), "Enumerates all defined topic's partitions"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "create_topic"_s32, align_to(32), "Creates a new topic"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "mirror"_s32, align_to(32), "Mirror partitions across TANK nodes"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "reload_config"_s32, align_to(32), "Reload per-topic configuration"_s32), "\n");
				Print(Buffer{}.append(align_to(5), "status"_s32, align_to(32), "Displays service status"_s32), "\n");
                                return 0;

                        default:
                                Print("Please use ", app, " -h for options\n");
                                return 1;
                }
        }

        argc -= optind;
        argv += optind;

        try {
                if (endpoint.size())
                        tank_client.set_default_leader(endpoint.AsS32());
                else {
                        // By default, access local instance
                        tank_client.set_default_leader(":11011"_s32);
                }
        } catch (const std::exception &e) {
                Print("The broker endpoint \"", endpoint, "\" endpoint specified is invalid.\n");
		Print("Examples of valid endpoints include:\n");
		Print(Buffer{}.append(align_to(5), "localhost:11011"_s32), "\n");
		Print(Buffer{}.append(align_to(5), ":11011"_s32), "\n");
		Print(Buffer{}.append(align_to(5), "127.0.0.1:11011"_s32), "\n");
		Print(Buffer{}.append(align_to(5), "127.0.0.1"_s32), "\n");
                return 1;
        }

        if (!argc) {
		Print("Command was not specified. Please run ", app, " for a list of all available commands.\n");
                return 1;
        }

        const strwlen32_t                 cmd(argv[0]);

        if (topic.empty()) {
                if (!cmd.Eq(_S("status"))) {
                        Print("Topic was not specified. Use ", ansifmt::bold, "-t", ansifmt::reset, " to specify the topic name\n");
                        return 1;
                }
        }

        const TankClient::topic_partition topicPartition(topic.AsS8(), partition);
        const auto                        consider_fault = [](const TankClient::fault &f) {
                switch (f.type) {
                        case TankClient::fault::Type::UnsupportedReq:
                                Print("Unable to process request. Service likely running in cluster-aware mode?\n");
                                break;

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
                                Print("Access Error\n");
                                break;

                        case TankClient::fault::Type::SystemFail:
                                Print("System Error\n");
                                break;

			case TankClient::fault::Type::InsufficientReplicas:
				Print("Insufficient Replicas\n");
				break;

                        case TankClient::fault::Type::InvalidReq:
                                Print("Invalid Request\n");
                                break;

                        case TankClient::fault::Type::Network:
                                Print("Network error\n");
                                break;

                        case TankClient::fault::Type::AlreadyExists:
                                Print("Already Exists\n");
                                break;

                        case TankClient::fault::Type::Timeout:
                                Print("Timeout\n");
                                break;

                        default:
                                break;
                }
        };

        if (cmd.Eq(_S("get")) || cmd.Eq(_S("consume"))) {
                uint64_t next{0};
                enum class Fields : uint8_t {
                        SeqNum = 0,
                        Key,
                        Content,
                        TS,
			TS_MS,
                        Size
                };
                uint8_t    displayFields{1u << uint8_t(Fields::Content)};
                size_t     defaultMinFetchSize{128 * 1024 * 1024};
                uint32_t   pendingResp{0};
                bool       statsOnly{false}, asKV{false};
                IOBuffer   buf;
                range64_t  time_range{0, UINT64_MAX};
                bool       drain_and_exit{false};
                uint64_t   endSeqNum{UINT64_MAX};
                str_view32 filter;
                uint64_t   msgs_limit = std::numeric_limits<uint64_t>::max();

                if (1 == argc) {
			goto help_get;
		}

                optind = 0;
                while ((r = getopt(argc, argv, "+SF:hBT:KdE:s:f:l:")) != -1) {
                        switch (r) {
				case 'l':
					msgs_limit = str_view32(optarg).as_uint64();
					break;

                                case 'f':
                                        filter.set(optarg);
                                        break;

                                case 'E':
                                        endSeqNum = strwlen32_t(optarg).AsUint64();
                                        break;

                                case 'd':
                                        drain_and_exit = true;
                                        break;

                                case 'K':
                                        asKV = true;
                                        break;

                                case 's':
                                        defaultMinFetchSize = strwlen32_t(optarg).AsUint64();
                                        if (!defaultMinFetchSize) {
                                                Print("Invalid fetch size value\n");
                                                return 1;
                                        }
                                        break;

                                case 'T': {
                                        str_view32  s(optarg), s2;
                                        const char *p = s.data(), *const e = p + s.size();
                                        char c = 0;

                                        if (p + "T-"_len < e) {
                                                p += "T-"_len;
                                        }

                                        while (p < e) {
                                                if (*p == '-' || *p == ',') {
                                                        s2 = s.SuffixFrom(p + 1);
                                                        s.set_end(p);
                                                        c = '-';
                                                        break;
                                                } else if (*p == '.' && p[1] == '.') {
                                                        const auto _p = p;

                                                        for (++p; p < e && *p == '.'; ++p) {
                                                                // as many dots as you want
                                                        }
                                                        s2 = s.SuffixFrom(p);
                                                        s.set_end(_p);
                                                        c = '-';
                                                        break;
                                                } else if (*p == '+') {
                                                        s2 = s.SuffixFrom(p + 1);
                                                        s.set_end(p);
                                                        c = '+';
                                                        break;
                                                } else {
                                                        ++p;
                                                }
                                        }

                                        time_range.offset = parse_timestamp(s);
                                        if (!time_range.offset) {
                                                Print("Failed to parse timestamp ", s, "\n");
                                                return 1;
                                        }

                                        if (c == '-') {
                                                if (const auto end = parse_timestamp(s2); !end) {
                                                        Print("Failed to parse timestamp ", s2, "\n");

                                                        return 1;
                                                } else {
                                                        time_range.SetEnd(end + 1);
                                                }

                                        } else if (c == '+') {
                                                const char *p = s2.data(), *const e = p + s2.size();

                                                if (p == e || !isdigit(*p)) {
                                                        Print("Unexpected offset\n");
                                                        return 1;
                                                }

                                                uint32_t v{0};

                                                do {
                                                        v = v * 10 + (*(p++) - '0');
                                                } while (p < e && isdigit(*p));

                                                if (p < e) {
                                                        if (*p == 's' || *p == 'S') {
                                                                // seconds
                                                        } else if (*p == 'm' || *p == 'M') {
                                                                v *= 60;
                                                        } else if (*p == 'h' || *p == 'H') {
                                                                v *= 3600;
                                                        } else if (*p == 'd' || *p == 'D') {
                                                                v *= 86400;
                                                        }
                                                }

                                                time_range.len = v * 1000;
                                        } else {
                                                time_range.len = UINT64_MAX - time_range.offset;
                                        }
                                } break;


                                case 'S':
                                        statsOnly = true;
                                        break;

                                case 'F':
                                        displayFields = 0;
                                        for (const auto it : strwlen32_t(optarg).Split(',')) {
                                                if (it.Eq(_S("seqnum"))) {
                                                        displayFields |= 1u << uint8_t(Fields::SeqNum);
                                                } else if (it.Eq(_S("key"))) {
                                                        displayFields |= 1u << uint8_t(Fields::Key);
                                                } else if (it.Eq(_S("content"))) {
                                                        displayFields |= 1u << uint8_t(Fields::Content);
                                                } else if (it.Eq(_S("ts"))) {
                                                        displayFields |= 1u << uint8_t(Fields::TS);
                                                } else if (it.Eq(_S("size"))) {
                                                        displayFields |= 1u << uint8_t(Fields::Size);
						} else if (it.Eq(_S("ts_ms"))) {
                                                        displayFields |= 1u << uint8_t(Fields::TS_MS);
                                                } else {
                                                        Print("Unknown field '", it, "'\n");
                                                        return 1;
                                                }
                                        }
                                        break;

                                case 'h':
					help_get:
					Print("Usage: ", app, " get [options] <start-spec>\n");
					Print(Buffer{}.append(left_aligned(5, "Consumes/retrieves messages from the specified TANK <topic>/<partition>.\n\nBy default, the retrieved messages content will be displayed in stdout, but you can optionally specify a different display format or use the -S option for display of statistics related to the events retrieved.\n\nIt will begin reading starting from <start-spec>\n<start-spec> can be \"EOF\" (so that it will begin reading from the end of the partition, effectively, \"tailing\" the partition), or a sequence number. If the -T option is used, <start-spec> can be ommitted(if it is specified togeher with -T, <start-spec> is ignored)."_s32, 76)), "\n");

					Print("\nOptions:\n\n"_s32);
					Print(Buffer{}.append(align_to(3), "-F <format>"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Specify a comma separated fields to be displayed\nOverrides the default display(only content) and accepts the following valid field names: 'seqnum', 'key', 'content', 'ts'"_s32, 76)), "\n\n");
					
					
					Print(Buffer{}.append(align_to(3), "-S"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Displays statistics about retrieved messages instead of the messages themselves"_s32, 76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-l <limit>"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Limit number of messages output"_s32,  76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-E <sequence number>"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "When specified, it will stop as soon as it processes a message with seqnumber number >= the specified sequence number"_s32,  76)), "\n\n");
						
					Print(Buffer{}.append(align_to(3), "-d"_s32, "\n"));
					Print(Buffer{}.append(left_aligned(5, "When specified, it will stop as soon as the partition has been drained. That is, as soon as a consume request returns no more messages"_s32, 76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-T <spec>"_s32, "\n"));
					Print(Buffer{}.append(left_aligned(5, "With this option, you can specify the beginning and optionally the end of the range of messages to consume by time, as opposed to by sequence number.\nThe <spec> supports three different notations:\n   <start>\n   <start>-<end>\n   <start>+<span>\n\nFor <start> and <end> you can either use T[-[N(DHMS)]] to denote the current wall time (optionally offsetting by a specified amount), or you can specify it using [YYYY][[.][MM][[.][DD][[@]HH[:[MM][:SS]]]]] notation.\n<span> can be represented as <countUNIT> where count unit is either (s, h, m, w) for seconds, hours, minutes, weeks.\n\nExamples:\n   -T 2018.09.15@20:30: The first message's timestamp shall be >= that specified time\n   -T 2018.09.15@20:30-2018.09.20@10:30: The first message's timestamp shall be>= the specified start time and the last message's timestamp shall be <= the specified end time\n   -T T-1h+2m: Th first message's timestamp shall be >= 1 hour ago and the last message's timestamp shall be <= 2 minutes past that specified start time\n\nThis tool will use binary search to quickly determine the sequence number of a message that is close to the message specified by the start time and then it will use linear search to advance to the appropriate message."_s32, 76)), "\n\n");
                                        return 0;

                                default:
                                        return 1;
                        }
                }

                argc -= optind;
                argv += optind;

                if (time_range.offset) {
                        // not required, we 'll determine it based on binary search triangulation
                        next = 0;
                } else if (!argc) {
                        Print("Expected sequence number to begin consuming from. Please see ", app, " consume -h\n");
                        return 1;
                } else {
                        const strwlen32_t from(argv[0]);

                        if (from.EqNoCase(_S("beginning")) || from.Eq(_S("first")))
                                next = 0;
                        else if (from.EqNoCase(_S("end")) || from.EqNoCase(_S("eof")))
                                next = UINT64_MAX;
                        else if (!from.IsDigits()) {
                                Print("Expected either \"beginning\", \"end\" or a sequence number for -f option\n");
                                return 1;
                        } else {
                                next = from.as_uint64();
                        }
                }

                size_t totalMsgs{0}, sumBytes{0};
                auto   minFetchSize = defaultMinFetchSize;

                if (const auto seek_ts = time_range.offset) {
                        // @robert's idea
                        // we 'll just binary search against the available messages space
                        // in order to determine an appropriate start offset.
                        // we 'll read a single message at the time in order to figure out the sequence number we need based
                        // on it's timestamp
                        static constexpr bool          trace{false};
                        const auto                     before = Timings::Microseconds::Tick();
                        const auto                     r      = tank_client.discover_partitions(topicPartition.first);
                        range_base<uint64_t, uint64_t> offsets_space;
                        bool                           have_offsets_space{false};
                        size_t                         iterations{0};

                        if (!r) {
                                Print("Failed to discover partitions of '", topicPartition.first, "'\n");
                                return 1;
                        }

                        if (trace) {
                                SLog("Determining space for '", topicPartition.first, "'\n");
                        }

                        while (tank_client.should_poll()) {
                                try {
                                        tank_client.poll(1e3);
                                } catch (const std::exception &e) {
                                        Print("Failed to discover partitions:", e.what(), "\n");
                                        return 1;
                                }

                                if (!tank_client.discovered_partitions().empty()) {
                                        const auto &it = tank_client.discovered_partitions().front();

                                        TANK_EXPECT(tank_client.discovered_partitions().size() == 1);
                                        if (unlikely(it.topic != topicPartition.first)) {
                                                Print("Unexpected topic [", it.topic, "] does not match [", topicPartition.first, "]\n");
                                                std::abort();
                                        }

                                        if (topicPartition.second >= it.watermarks.size()) {
                                                Print("Partition of '", topicPartition.first, "' is not defined\n");
                                                return 1;
                                        }

                                        offsets_space      = it.watermarks.offset[topicPartition.second];
                                        have_offsets_space = true;
                                        break;
                                }
                        }

                        if (!have_offsets_space) {
                                Print("Unable to determine messages space for '", topicPartition.first, "'\n");
                                return 1;
                        } else if (0 == offsets_space.size()) {
                                Print("Partition ", topicPartition.first, "/", topicPartition.second, " is empty\n");
                                return 0;
                        } else {
                                // Use binary search to determine the appropriate base_seq_num
                                const auto cut_off      = seek_ts - (5 * 60 * 100);
                                uint64_t   low          = offsets_space.offset;
                                uint64_t   high         = low + offsets_space.size() - 1;
                                uint64_t   base_seq_num = low;

                                if (trace) {
                                        SLog("OK have ", offsets_space, ", looking for the first for ", Date::ts_repr(seek_ts / 1000), "\n");
                                }

                                while (low <= high) {
                                        // we will need to avoid overflow here
                                        uint64_t msg_ts;
                                        auto     mid = low + ((high - low) / 2);

                                        // Figure out the timestamp of the first message with (seq_num >= mid)
                                        ++iterations;
                                        for (uint32_t req_id{0}, fetch_size{32 * 1024};;) {
                                                if (!req_id) {
                                                        if (trace) {
                                                                SLog("At ", mid, " ", fetch_size, "\n");
                                                        }

                                                        req_id = tank_client.consume_from(topicPartition, mid, fetch_size, 0, 0);
                                                        if (!req_id) {
                                                                tank_client.reset();
                                                                continue;
                                                        }
                                                }

                                                try {
                                                        tank_client.poll(100);
                                                } catch (const std::exception &e) {
                                                        Print("Failed to poll():", e.what(), "\n");
                                                        return 1;
                                                }

                                                if (!tank_client.faults().empty()) {
                                                        for (const auto &it : tank_client.faults()) {
                                                                if (it.type == TankClient::fault::Type::BoundaryCheck) {
                                                                        // this is weird, but sure, we 'll play along
                                                                        mid = it.adjust_seqnum_by_boundaries(mid);

                                                                        if (trace) {
                                                                                SLog("Adjusted to ", mid, "\n");
                                                                        }
                                                                } else {
                                                                        Print("Failed to access message\n");
                                                                        return 1;
                                                                }
                                                        }

                                                        req_id = 0;
                                                }

                                                if (tank_client.consumed().empty()) {
                                                        continue;
                                                }

                                                TANK_EXPECT(tank_client.consumed().size() == 1);

                                                const auto &it = tank_client.consumed().front();

                                                req_id = 0;
                                                if (!it.msgs.empty()) {
                                                        msg_ts = it.msgs.offset->ts;
                                                        break;
                                                } else {
                                                        fetch_size = std::max<uint32_t>(fetch_size, it.next.minFetchSize);
                                                        mid        = it.next.seqNum;
                                                }
                                        }

                                        if (msg_ts < seek_ts && msg_ts >= cut_off) {
                                                // optimization: save some iterations by
                                                // aborting early if we are not far from our target message
                                                if (trace) {
                                                        SLog("got cut_off at ", Date::ts_repr(msg_ts / 1000), ", stopping\n");
                                                }
                                                break;
                                        }

                                        if (seek_ts < msg_ts) {
                                                high = mid - 1;
                                        } else {
                                                base_seq_num = mid;
                                                low          = mid + 1;
                                        }

                                        if (trace) {
                                                SLog("For ", mid, " ", msg_ts, " ", Date::ts_repr(msg_ts / 1000), " seek ", Date::ts_repr(seek_ts / 1000), "\n");
                                        }
                                }

                                if (trace) {
                                        SLog("DONE, took ", duration_repr(Timings::Microseconds::Since(before)), " to determine base sequence number, ", dotnotation_repr(iterations), " iterations\n");
                                }

                                next = base_seq_num;
                        }
                }

                const auto b = Timings::Microseconds::Tick();

                for (const auto time_range_end = time_range.offset + time_range.size();;) {
                        if (!pendingResp) {
                                if (verbose) {
                                        Print("Requesting from ", next, "\n");
                                }

                                pendingResp = tank_client.consume({{topicPartition, {next, minFetchSize}}}, drain_and_exit ? 0 : 8e3, 0);

                                if (!pendingResp) {
                                        Print("Unable to issue consume request. Will abort\n");
                                        return 1;
                                }
                        }

                        try {
                                tank_client.poll(1e3);
                        } catch (const std::exception &e) {
                                tank_client.reset();
                                Timings::Seconds::Sleep(1);
                                pendingResp = 0;
                                continue;
                        }

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);

                                if (retry && it.type == TankClient::fault::Type::Network) {
                                        Timings::Milliseconds::Sleep(400);
                                } else {
                                        return 1;
                                }

                                pendingResp = 0;
                        }

                        if (tank_client.consumed().empty()) {
                                continue;
                        }

			if (verbose) {
				SLog("consumed ", tank_client.consumed().size(), "\n");
			}

                        pendingResp = 0;
                        for (const auto &it : tank_client.consumed()) {
				if (verbose) {
					SLog("Drained:", it.drained, "\n");
				}

                                if (drain_and_exit && it.drained) {
                                        // Drained if we got no message in the response, and if the size we specified
                                        // is <= next.minFetchSize. This is important because we could get no messages
                                        // because the message is so large the minFetchSize we provided for the request was too low
                                        goto out;
                                }

                                if (statsOnly) {
                                        Print(">> ", dotnotation_repr(it.msgs.size()), " messages\n");

                                        if (time_range.offset) {
                                                bool should_abort{false};

                                                if (verbose) {
                                                        for (const auto m : it.msgs) {
                                                                if (time_range.Contains(m->ts)) {
                                                                        if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                                continue;
                                                                        }

                                                                        Print(m->seqNum, ": ", size_repr(m->content.size()), "\n");
                                                                        sumBytes += m->content.len + m->key.len + sizeof(uint64_t);

                                                                        if (++totalMsgs == msgs_limit) {
										goto out;
									}
										
                                                                } else if (m->ts >= time_range_end) {
                                                                        should_abort = true;
                                                                }
                                                        }

                                                } else {
                                                        for (const auto m : it.msgs) {
                                                                if (time_range.Contains(m->ts)) {
                                                                        if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                                continue;
                                                                        }

                                                                        sumBytes += m->content.len + m->key.len + sizeof(uint64_t);

                                                                        if (++totalMsgs == msgs_limit) {
										goto out;
									}

                                                                } else if (m->ts >= time_range_end) {
                                                                        should_abort = true;
                                                                }
                                                        }
                                                }

                                                if (should_abort) {
                                                        goto out;
                                                }

                                        } else {
                                                totalMsgs += it.msgs.size();

                                                if (verbose) {
                                                        for (const auto m : it.msgs) {
                                                                if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                        continue;
                                                                }

                                                                Print(m->seqNum, ": ", size_repr(m->content.size()), "\n");
                                                                sumBytes += m->content.len + m->key.len + sizeof(uint64_t);

                                                                        if (++totalMsgs == msgs_limit) {
										goto out;
									}
                                                        }
                                                } else {
                                                        for (const auto m : it.msgs) {
                                                                if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                        continue;
                                                                }

                                                                sumBytes += m->content.len + m->key.len + sizeof(uint64_t);

								if (++totalMsgs == msgs_limit) {
									goto out;
								}
							}
                                                }
                                        }
                                } else {
                                        size_t sum{0};
                                        bool   should_abort{false};

                                        if (!time_range.offset) {
                                                for (const auto m : it.msgs) {
                                                        if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                continue;
                                                        }

                                                        sum += m->content.size();
                                                }

                                                sum += it.msgs.size() * 2;
                                        } else {
                                                for (const auto m : it.msgs) {
                                                        if (time_range.Contains(m->ts)) {
                                                                if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                        continue;
                                                                }

                                                                sum += m->content.size();
                                                                sum += 2;
                                                        }
                                                }
                                        }

                                        buf.clear();
                                        buf.reserve(sum);
                                        for (const auto m : it.msgs) {
                                                if (m->seqNum > endSeqNum || m->ts >= time_range_end) {
                                                        should_abort = true;
                                                        break;
                                                } else if (time_range.Contains(m->ts)) {
                                                        if (filter && !m->content.Search(filter.data(), filter.size())) {
                                                                continue;
                                                        }

                                                        if (asKV) {
                                                                buf.append(m->seqNum, " [", m->key, "] = [", m->content, "]");
                                                        } else if (displayFields) {
                                                                if (displayFields & (1u << uint8_t(Fields::TS))) {
                                                                        buf.append(Date::ts_repr(Timings::Milliseconds::ToSeconds(m->ts)), ':');
								}

                                                                if (displayFields & (1u << uint8_t(Fields::TS_MS))) {
                                                                        buf.append(m->ts, ':');
								}

                                                                if (displayFields & (1u << uint8_t(Fields::SeqNum))) {
                                                                        buf.append("seq=", m->seqNum, ':');
								}

                                                                if (displayFields & (1u << uint8_t(Fields::Size))) {
                                                                        buf.append("size=", m->content.size(), ':');
								}

								if (displayFields & (1u << unsigned(Fields::Key))) {
									buf.append('[', m->key, "]:"_s32);
								}

                                                                if (displayFields & (1u << uint8_t(Fields::Content))) {
                                                                        buf.append(m->content);
								}

                                                        } else {
                                                                buf.append(m->content);
                                                        }

                                                        buf.append('\n');

								if (++totalMsgs == msgs_limit) {
									break;
								}
                                                }
                                        }

                                        if (auto s = buf.as_s32()) {
                                                do {
                                                        const auto r = write(STDOUT_FILENO, s.data(), s.size());

                                                        if (r == -1) {
                                                                Print("(Failed to output data to stdout:", strerror(errno), ". Exiting\n");
                                                                return 1;
                                                        } else {
                                                                s.strip_prefix(r);
                                                        }

                                                } while (s);
                                        }

                                        if (should_abort || totalMsgs >= msgs_limit) {
                                                goto out;
                                        }
                                }

                                minFetchSize = Max<size_t>(it.next.minFetchSize, defaultMinFetchSize);
                                next         = it.next.seqNum;
                        }
                }

        out:
                if (statsOnly) {
                        Print(dotnotation_repr(totalMsgs), " messages consumed in ", duration_repr(Timings::Microseconds::Since(b)), ", ", size_repr(sumBytes), " consumed\n");
                }

	} else if (cmd.Eq(_S("status"))) {
                optind = 0;
                while ((r = getopt(argc, argv, "h")) != -1) {
                        switch (r) {
                                case 'h':
                                        Print("Usage ", app, " status\n");
                                        Print(Buffer{}.append(left_aligned(5, "Outputs service status"_s32, 76)), "\n");
					return 0;

                                default:
                                        return 1;
                        }
                }
                argc -= optind;
                argv += optind;

		const auto req_id = tank_client.service_status();

		if (!req_id) {
			Print("Unable to schedule service status request\n");
			return 1;
		}

                while (tank_client.should_poll()) {
                        tank_client.poll(1000);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                        }

                        for (const auto &it : tank_client.statuses()) {
                                Print(Buffer{}.append("Topics"_s32, align_to(12), dotnotation_repr(it.counts.topics)), "\n");
                                Print(Buffer{}.append("Partitions"_s32, align_to(12), dotnotation_repr(it.counts.partitions)), "\n");

                                if (it.cluster_name.len) {
                                        Print(Buffer{}.append("Nodes"_s32, align_to(12), dotnotation_repr(it.counts.nodes)), "\n");
                                        Print(Buffer{}.append("Cluster"_s32, align_to(12), str_view32(it.cluster_name.data, it.cluster_name.len)), "\n");
                                }
                        }
                }

                return 0;
        } else if (cmd.Eq(_S("create_topic"))) {
                Buffer config;

		if (1 == argc) {
			goto help_create_topic;
		}

                optind = 0;
                while ((r = getopt(argc, argv, "+ho:")) != -1) {
                        switch (r) {
                                case 'h':
					help_create_topic:
					Print("Usage ", app, " create_topic [total partitions]\n");
					Print(Buffer{}.append(left_aligned(5, "Creates a new topic, with `total partitions` topics in the range of [0, total partitions)\n\nIf the TANK endpooint is operating in cluster aware mode, no more than 64 parititions will be created.\nThis is an API limitation which will be lifted in forthcoming releases."_s32, 76), "\n"));
                                        return 0;

                                case 'o':
                                        for (const auto &it : strwlen32_t(optarg).Split(',')) {
                                                strwlen32_t k, v;

                                                std::tie(k, v) = it.Divided('=');
                                                if (!k || !v) {
                                                        Print("Invalid configuration syntax for [", it, "]\n");
                                                        return 1;
                                                }
                                                config.append(k, "=", v, "\n");
                                        }
                                        break;

                                default:
                                        return 1;
                        }
                }
                argc -= optind;
                argv += optind;

                if (argc == 0) {
                        Print("Expected total partitions for new topic '", topicPartition.first, "'\n");
                        return 1;
                }

                const strwlen32_t s(argv[0]);

                if (!s.IsDigits() || s.AsUint32() > UINT16_MAX || !s.AsUint32()) {
                        Print("Invalid total partitions specified\n");
                        return 1;
                }

                const auto reqId = tank_client.create_topic(topicPartition.first, s.AsUint32(), config.AsS32());

                if (!reqId) {
                        Print("Unable to schedule create topic request\n");
                        return 1;
                }

                while (tank_client.should_poll()) {
                        tank_client.poll(1e3);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                        }

                        for (const auto &it : tank_client.created_topics()) {
                                TANK_EXPECT(it.clientReqId == reqId);
                                Print("Created topic ", ansifmt::bold, it.topic, ansifmt::reset, "\n");
                        }
                }

                return 0;
        } else if (cmd.Eq(_S("mirror"))) {
                // See: https://eng.uber.com/umirrormaker/
                TankClient dest;
                uint32_t   reqId1, reqId2;
                // XXX: arbitrary defaults
                size_t bundleMsgsSetCntThreshold{128};
                size_t bundleMsgsSetSizeThreshold{4 * 1024 * 1024};
                size_t sleepTime{0};

                // TODO: throttling options
                optind = 0;
                while ((r = getopt(argc, argv, "+hC:S:z:")) != -1) {
                        switch (r) {
                                case 'z':
                                        sleepTime = strwlen32_t(optarg).AsUint64();
                                        break;

                                case 'h':
					Print("Usage: ", app, " mirror [options] endpoint\n");
					Print(Buffer{}.append(left_aligned(5, "Mirrors either 1 or all selected topic's partitions from the selected broker to <endpoint>.\nThe topic and partitions to be mirrored must be defined in the destination endpoint before attempting to mirror them from the source.\nIf you specify the partition explicitly using -p or <topic>/<partition> notation, then only that partition will be mirrored, otherwise all partitions will be mirrored."_s32, 86)), "\n");
					Print("\nOptions:\n\n"_s32);
					Print(Buffer{}.append(align_to(3), "-C <count>"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Specify the size of bundles in messages. A bundle will not contain more than <count> messages. Default is 1"_s32, 76)), "\n\n");
					Print(Buffer{}.append(align_to(3), "-S <size>"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Specify the size of bundles in bytes. Produces bundles will not be larger than <size> bytes."_s32, 76)), "\n\n");
                                        return 0;

                                case 'C':
                                        bundleMsgsSetCntThreshold = strwlen32_t(optarg).AsUint32();
                                        if (!IsBetweenRange<size_t>(bundleMsgsSetCntThreshold, 1, 65536)) {
                                                Print("Invalid value ", optarg, "\n");
                                                return 1;
                                        }
                                        break;

                                case 'S':
                                        bundleMsgsSetSizeThreshold = strwlen32_t(optarg).AsUint32();
                                        if (!IsBetweenRange<size_t>(bundleMsgsSetSizeThreshold, 64, 8 * 1024 * 1024)) {
                                                Print("Invalid value ", optarg, "\n");
                                                return 1;
                                        }
                                        break;

                                default:
                                        return 1;
                        }
                }

                argc -= optind;
                argv += optind;

                if (!argc) {
                        Print("Mirror destination endpoint not specified\n");
                        return 1;
                }

                try {
                        dest.set_default_leader(strwlen32_t(argv[0]));
                } catch (...) {
                        Print("Invalid destination endpoint '", argv[0], "'\n");
                        return 1;
                }

                if (verbose) {
                        Print("Discovering partitions\n");
                }

                // Discover partitions first
                reqId1 = tank_client.discover_partitions(topicPartition.first);
                if (!reqId1) {
                        Print("Unable to schedule discover request to source\n");
                        return 1;
                }

                reqId2 = dest.discover_partitions(topicPartition.first);
                if (!reqId2) {
                        Print("Unable to schedule discover request to destination\n");
                        return 1;
                }

                struct partition_ctx final {
                        uint16_t id;
                        bool     pending;
                        uint64_t nextBase, last;
                        uint64_t next;
			uint32_t fetch_size;
                };

                simple_allocator                                                                           allocator{4096};
                std::unordered_map<uint16_t, partition_ctx *>                                              map;
                std::vector<partition_ctx *>                                                               pending;
                std::vector<std::pair<TankClient::topic_partition, std::pair<uint64_t, uint32_t>>>         inputs;
                std::vector<uint8_t>                                                                       outputsOrder;
                uint16_t                                                                                   src_partitions_cnt{0};
                std::vector<std::pair<TankClient::topic_partition, std::vector<TankClient::consumed_msg>>> collected_withseqnum;
                std::vector<std::pair<TankClient::topic_partition, std::vector<TankClient::msg>>>          collected;
                std::vector<bool>                                                                          collections_tracker;
                static constexpr bool                                                                      trace{false};
		static constexpr size_t min_fetch_size = 16 * 1024 * 1024;

                while (tank_client.should_poll()) {
                        tank_client.poll(1e3);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                                return 1;
                        }

                        if (!tank_client.discovered_partitions().empty()) {
                                const auto &v = tank_client.discovered_partitions().front();

                                TANK_EXPECT(v.clientReqId == reqId1);
                                src_partitions_cnt = v.watermarks.size();

                                if (verbose) {
                                        Print("Discovered ", src_partitions_cnt, " partitions from source\n");
                                }
                        }
                }

                partition_ctx **all{nullptr};

                DEFER({
                        if (all) {
                                std::free(all);
                        }
                });

                while (dest.should_poll()) {
                        dest.poll(1e3);

                        for (const auto &it : dest.faults()) {
                                consider_fault(it);
                                return 1;
                        }

                        if (!dest.discovered_partitions().empty()) {
                                const auto &v = dest.discovered_partitions().front();
                                const auto  n = v.watermarks.size();

                                TANK_EXPECT(v.clientReqId == reqId2);

                                pending.reserve(n);
                                if (n) {
                                        all = static_cast<partition_ctx **>(malloc(sizeof(partition_ctx *) * n));
                                }

                                for (unsigned i = 0; i < v.watermarks.size(); ++i) {
                                        if (!partition_specified || partition == i) {
                                                const auto p         = v.watermarks.offset + i;
                                                auto       partition = allocator.Alloc<partition_ctx>();

                                                if (verbose) {
                                                        Print("From source(accept) ", i, " from ", p->second + 1, "\n");
                                                }

                                                partition->id         = i;
                                                partition->pending    = true;
                                                partition->nextBase   = UINT64_MAX;
                                                partition->last       = 0;
                                                partition->next       = p->second + 1;
                                                partition->fetch_size = min_fetch_size;

                                                pending.emplace_back(partition);
                                                all[partition->id] = partition;
                                                map.emplace(partition->id, partition);
                                        }
                                }

                                if (verbose) {
                                        Print("Discovered ", map.size(), " partitions from dest\n");
                                }
                        }
                }

                if (pending.empty()) {
                        Print("No partitions discovered - nothing to mirror\n");
                        return 1;
		}

		if (partition_specified) {
			if (partition >= src_partitions_cnt) {
				Print("Selected partition invalid. Available partitions in source broker [0, "_s32, src_partitions_cnt, ")\n");
				return 1;
			} 

			if (map.empty()) {
				Print("Selected partition invalid. Not specified in the destination broker\n");
				return 1;
			}
		} else if (map.size() != src_partitions_cnt) {
                        Print("Total partitions mismatch, ", dotnotation_repr(src_partitions_cnt), " partitions discovered on source, ", dotnotation_repr(map.size()), " partitions discovered on destination\n");
                        return 1;
                }

                Print("Will now mirror ", dotnotation_repr(map.size()), " partitions of ", ansifmt::bold, topicPartition.first, ansifmt::reset, "\n");
                Print("You can safely abort mirroring by stoping this tank-cli process (e.g CTRL-C or otherwise). Next mirror session will pick up mirroring from where this session ended\n");

                for (reqId1 = 0;;) {
                        if (!reqId1 && !pending.empty() && !dest.should_poll()) {
                                inputs.clear();
                                for (const auto it : pending) {
                                        if (verbose) {
                                                Print("Scheduling for partition ", it->id, " from ", it->next, "\n");
                                        }

                                        it->pending = false;
                                        inputs.emplace_back(
                                            std::make_pair(TankClient::topic_partition{
                                                               topicPartition.first,
                                                               it->id},
                                                           std::make_pair(it->next, it->fetch_size)));
                                }

                                reqId1 = tank_client.consume(inputs, 100, 1);

                                if (!reqId1) {
                                        Print("Failed to issue consume request\n");
                                        return 1;
                                }

                                pending.clear();
                        }

                        tank_client.poll(8000);

                        if (!tank_client.faults().empty()) {
                                for (const auto &it : tank_client.faults()) {
                                        if (it.type == TankClient::fault::Type::BoundaryCheck) {
                                                auto       partition = all[it.partition];
                                                const auto to        = it.adjust_seqnum_by_boundaries(partition->next);

                                                if (verbose) {
                                                        Print("Adjusted seqnunce number for partition from ", it.partition, " ", partition->next, " => ", to, "\n");
                                                }

                                                partition->next = to;
                                                if (!partition->pending) {
                                                        partition->pending = true;
                                                        pending.emplace_back(partition);
                                                }
                                        } else {
                                                consider_fault(it);
                                                return 1;
                                        }
                                }

                                reqId1 = 0;
                        }

                        if (tank_client.consumed().empty()) {
                                continue;
                        }

                        reqId1 = 0;
                        collected.clear();
                        collected_withseqnum.clear();
                        collections_tracker.clear();


                        for (const auto &it : tank_client.consumed()) {
                                const auto partition = all[it.partition];
                                const auto n         = it.msgs.size();

				partition->fetch_size = std::max<size_t>(min_fetch_size, it.next.minFetchSize);

                                if (0 == n) {
                                        if (!partition->pending) {
                                                partition->pending = true;
                                                pending.emplace_back(partition);
                                        }

                                        continue;
                                }

                                const auto data     = it.msgs.offset;
                                auto       expected = partition->last + 1;

                                if (trace) {
                                        SLog("n = ", n, ", expected = ", expected, "\n");
                                }

                                TANK_EXPECT(it.topic);
                                for (size_t i{0}; i < n;) {
                                        size_t     sum         = 0;
                                        const auto upto        = std::min<size_t>(n, i + bundleMsgsSetCntThreshold);
                                        bool       need_sparse = false;
                                        auto       k           = i;

                                        // need to figure out if we are going
                                        // to need a *sparse* bundle or not here
                                        do {
                                                const auto &m = data[k];

                                                need_sparse |= (expected != m.seqNum);
                                                expected = m.seqNum + 1;

                                                sum += m.key.size() + m.content.size();
                                        } while (++k < upto && sum < bundleMsgsSetSizeThreshold);

                                        const auto span = k - i;

                                        if (trace && false) {
                                                SLog("span = ", span, ", need_sparse = ", need_sparse, "\n");
                                        }

                                        if (need_sparse) {
                                                std::vector<TankClient::consumed_msg> msgs;

                                                msgs.reserve(span);
                                                while (i < k) {
                                                        msgs.emplace_back(data[i++]);
                                                }

                                                TANK_EXPECT(!msgs.empty());
                                                collected_withseqnum.emplace_back(std::make_pair(std::make_pair(it.topic, it.partition), std::move(msgs)));
                                                collections_tracker.emplace_back(true);
                                        } else {
                                                std::vector<TankClient::msg> msgs;

                                                msgs.reserve(span);
                                                do {
                                                        const auto &m = data[i++];

                                                        msgs.emplace_back(TankClient::msg{
                                                            .content = m.content,
                                                            .ts      = m.ts,
                                                            .key     = m.key});
                                                } while (i < k);

                                                TANK_EXPECT(!msgs.empty());
                                                collected.emplace_back(std::make_pair(
                                                    std::make_pair(it.topic, it.partition), std::move(msgs)));

                                                collections_tracker.emplace_back(false);
                                        }
                                }


                                partition->last       = data[n - 1].seqNum;
                                partition->next       = it.next.seqNum;
                        }

                        const auto cnt = collections_tracker.size();
                        uint32_t   collected_i{0}, collected_withseqnum_i{0};

                        TANK_EXPECT(cnt == collected.size() + collected_withseqnum.size());

                        if (trace) {
                                SLog("cnt = ", cnt, "\n");
                        }

                        // we need to do this in order
                        // for now, we can't guarantee that they will be processed in order and in sequence, rather they will
                        // likely be processed in parallel, and thus, we can't guarantee that the messages will be persisted in the correct order
                        // so for now, we 'll just do it one request/time.
                        // We may support chaining requests in the future in the client though
                        for (size_t i = 0; i < cnt;) {
                                const auto base = i;
                                const auto b    = collections_tracker[i];
                                uint32_t   req_id;

                                do {
                                        //
                                } while (++i < cnt && collections_tracker[i] == b);

                                const auto span = i - base;

                                if (trace) {
                                        SLog("FOR ", static_cast<bool>(b), " ", span, "\n");
                                }

                                if (b) {
                                        req_id = dest.produce_with_seqnum(collected_withseqnum.data() + collected_withseqnum_i, span);
                                        collected_withseqnum_i += span;
                                } else {
                                        req_id = dest.produce(collected.data() + collected_i, span);
                                        collected_i += span;
                                }

                                if (!req_id) {
                                        Print("Failed to produce to destination\n");
                                        return 1;
                                }

                                while (dest.should_poll()) {
                                        dest.poll(1000);

                                        if (!dest.faults().empty()) {
                                                for (const auto &it : dest.faults()) {
                                                        consider_fault(it);
                                                }
                                                return 1;
                                        }

                                        for (const auto &it : dest.produce_acks()) {
                                                const auto partition = it.partition;
                                                auto       p         = all[partition];

                                                if (!p->pending) {
                                                        p->pending = true;
                                                        pending.emplace_back(p);
                                                }
                                        }
                                }
                        }
                }

                return 0;
        } else if (cmd.BeginsWith(_S("reload_conf"))) {
                const auto req_id = tank_client.reload_partition_conf(topicPartition.first, topicPartition.second);

                if (!req_id) {
                        Print("Unable to schedule partition configuration reload request\n");
                        return 1;
                }

                while (tank_client.should_poll()) {
                        tank_client.poll(1e3);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                        }

                        for (const auto &it : tank_client.reloaded_partition_configs()) {
                                Print("Reloaded configuration of ", it.topic, "/", it.partition, "\n");
                        }
                }

                return 0;
        } else if (cmd.Eq(_S("discover_partitions"))) {
                const auto reqId = tank_client.discover_partitions(topicPartition.first);

                if (!reqId) {
                        Print("Unable to schedule discover partition request\n");
                        return 1;
                }

                while (tank_client.should_poll()) {
                        tank_client.poll(1e3);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                        }

                        for (const auto &it : tank_client.discovered_partitions()) {
                                uint16_t   i{0};
                                text_table tt;

                                TANK_EXPECT(it.clientReqId == reqId);

                                tt.set_headers("Partition", "First Available", "Last Assigned");
                                for (const auto wm : it.watermarks) {
                                        tt.add_row(i++, wm->first, wm->second);
                                }

                                Print(tt.draw());
                        }
                }

                return 0;
        } else if (cmd.Eq(_S("set")) || cmd.Eq(_S("produce")) || cmd.Eq(_S("publish"))) {
                char     path[PATH_MAX];
                size_t   bundleSize{1};
                bool     asSingleMsg{false}, asKV{false};
                uint64_t baseSeqNum{0};
		uint64_t seqnum_advance_step{1}, ts_advance_step{0};
		bool verbose{false};

		if (1 == argc) {
			goto help_produce;
		}

                optind  = 0;
                path[0] = '\0';
                while ((r = getopt(argc, argv, "+s:f:F:hS:Ka:t:v")) != -1) {
                        switch (r) {
				case 't':
					ts_advance_step = str_view32(optarg).as_uint64();
					break;

				case 'a':
					seqnum_advance_step = str_view32(optarg).as_uint64();
					if (!seqnum_advance_step) {
						Print("Invalid seqnum_advance_step\n");
					}
					break;

                                case 'K':
                                        asKV = true;
                                        break;

                                case 'S':
                                        baseSeqNum = strwlen32_t(optarg).AsUint64();
                                        break;

                                case 's':
                                        bundleSize = strwlen32_t(optarg).AsUint32();
                                        if (!bundleSize) {
                                                Print("Invalid bundle size specified\n");
                                                return 1;
                                        }
                                        break;

				case 'v':
					verbose = true;
					break;

                                case 'F':
                                        asSingleMsg = true;
                                case 'f': {
                                        const auto l = strlen(optarg);

                                        require(l < sizeof(path));
                                        memcpy(path, optarg, l);
                                        path[l] = '\0';
                                } break;

                                case 'h':
				help_produce:
					Print("Usage: ", app, " produce [options] <messages list>\n");
					Print(Buffer{}.append(left_aligned(5, "Produces 1 or more messages to the specific TANK <topic>/<partition>"_s32, 76)), "\n");

					Print("\nOptions:\n\n"_s32);
					Print(Buffer{}.append(align_to(3), "-s size"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "The bundle size; how many messages to be grouped into one bundle before producing that to the broker\nThe default value is 1, which means each new message published into its own bundle"_s32, 76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-f file"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "The messages are read from `file`, which is expected to contain one message per line. You can use \"-\" for stdin.\nIf this option is set, the messages list is ignored"_s32, 76)), "\n\n");


					Print(Buffer{}.append(align_to(3), "-F file"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Like -f, except that the contents of the file are treated a single message"_s32, 76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-K"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Treat each message in the messages list as a key=value pair, instead of as content(value) of a message"_s32, 76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-S sequence number"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Explicitly sets the base sequence number\nThis is useful for creating sparse bundles"_s32, 76)), "\n\n");

					Print(Buffer{}.append(align_to(3), "-v"_s32), "\n");
					Print(Buffer{}.append(left_aligned(5, "Displays profiler timings"_s32, 76)), "\n");

                                        return 1;

                                default:
                                        return 1;
                        }
                }

                argc -= optind;
                argv += optind;

                static constexpr size_t      pollInterval{20};
                std::vector<TankClient::msg> msgs;
                size_t                       pendingAckAcnt{0};
                std::set<uint32_t>           pendingResps;
		size_t total{0}, requests{0};
                const auto                   poll = [&]() {
                        tank_client.poll(8e2);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                                if (retry && it.type == TankClient::fault::Type::Network) {
                                        Timings::Milliseconds::Sleep(400);
                                        pendingResps.clear();
                                } else {
                                        return false;
                                }
                        }

                        for (const auto &it : tank_client.produce_acks()) {
                                pendingResps.erase(it.clientReqId);
                        }

                        return true;
                };
                const auto publish_msgs = [&]() {
                        uint32_t reqId;

                        total += msgs.size();
                        if (baseSeqNum) {
                                std::vector<TankClient::consumed_msg>                                                      all;
                                uint64_t                                                                                   next = baseSeqNum;
                                std::vector<std::pair<TankClient::topic_partition, std::vector<TankClient::consumed_msg>>> v;

                                all.reserve(msgs.size());
                                for (const auto &it : msgs) {
                                        all.emplace_back(TankClient::consumed_msg{
                                            .seqNum  = next,
                                            .key     = it.key,
                                            .ts      = it.ts,
                                            .content = it.content,
                                        });

                                        next += seqnum_advance_step;
                                }

                                v.emplace_back(std::make_pair(topicPartition, std::move(all)));
                                reqId      = tank_client.produce_with_seqnum(v);
                                baseSeqNum = 0;
                        } else {
                                reqId = tank_client.produce({{topicPartition, msgs}});
                        }

                        if (!reqId) {
                                Print("Failed to schedule messages to broker\n");
                                return false;
                        } else {
                                pendingResps.insert(reqId);
                        }

                        ++requests;
                        pendingAckAcnt += msgs.size();

                        if (pendingAckAcnt >= pollInterval) {
                                while (tank_client.should_poll()) {
                                        if (!poll()) {
                                                return false;
                                        }
                                }

                                pendingAckAcnt = 0;
                        }

                        return true;
                };

                if (path[0]) {
                        static constexpr size_t bufSize{64 * 1024};
                        int                     fd;
                        auto *const             buf = (char *)malloc(bufSize);
                        range32_t               range;
                        strwlen32_t             s;

                        Defer({
                                free(buf);
                                if (fd != STDIN_FILENO && fd != -1)
                                        close(fd);
                        });

                        if (!strcmp(path, "-")) {
                                fd = STDIN_FILENO;
                                require(fd != -1);
                        } else {
                                fd = open(path, O_RDONLY | O_LARGEFILE);

                                if (fd == -1) {
                                        Print("Failed to open(", path, "): ", strerror(errno), "\n");
                                        return 1;
                                }
                        }

                        const auto consider_input = [&](const bool last) {
				auto ts = Timings::Milliseconds::SysTime();

                                while (range) {
                                        s.Set(buf + range.offset, range.len);

                                        if (const auto *const p = s.Search('\n')) {
                                                const uint32_t n    = p - s.p;
                                                auto           data = (char *)malloc(n);

                                                memcpy(data, s.p, n);
                                                msgs.push_back({{data, n}, ts, {}});
                                                range.TrimLeft(n + 1);

						ts += ts_advance_step;

                                                if (msgs.size() == bundleSize) {
                                                        const auto r = publish_msgs();

                                                        for (auto &it : msgs) {
                                                                std::free(const_cast<char *>(it.content.p));
							}

                                                        msgs.clear();

                                                        if (!r)
                                                                return false;
                                                }
                                        } else
                                                break;
                                }

                                if (last && msgs.size()) {
                                        const auto r = publish_msgs();

                                        for (auto &it : msgs) {
                                                std::free(const_cast<char *>(it.content.p));
					}

                                        msgs.clear();

                                        if (!r) {
                                                return false;
					}
                                }

                                if (!range) {
                                        range.reset();
				}

                                return true;
                        };

                        range.reset();

			const auto before = Timings::Microseconds::Tick();

                        if (asSingleMsg) {
                                IOBuffer content;

                                for (;;) {
                                        if (content.capacity() < 8192)
                                                content.reserve(65536);

                                        const auto r = read(fd, content.end(), content.capacity());

                                        if (r == -1) {
                                                Print("Failed to read data:", strerror(errno), "\n");
                                                return 1;
                                        } else if (!r)
                                                break;
                                        else
                                                content.AdvanceLength(r);
                                }

                                if (verbose)
                                        Print("Publishing message of size ", size_repr(content.size()), "\n");

                                msgs.push_back({content.AsS32(), Timings::Milliseconds::SysTime(), {}});

                                const auto reqId = tank_client.produce(
                                    {{topicPartition, msgs}});

                                if (!reqId) {
                                        Print("Failed to schedule messages to broker\n");
                                        return 1;
                                } else {
                                        pendingResps.insert(reqId);
                                }
                        } else {
                                for (;;) {
                                        const auto capacity = bufSize - range.stop();
                                        auto       r        = read(fd, buf + range.offset, capacity);

                                        if (r == -1) {
                                                Print("Failed to read data:", strerror(errno), "\n");
                                                return 1;
                                        } else if (!r)
                                                break;
                                        else {
                                                range.len += r;
                                                if (!consider_input(false))
                                                        return 1;
                                        }
                                }

                                if (!consider_input(true))
                                        return 1;
                        }

                        while (tank_client.should_poll()) {
                                if (!poll()) {
                                        return 1;
                                }
                        }

			if (verbose) {
				Print("Took ", duration_repr(Timings::Microseconds::Since(before)), " for ", dotnotation_repr(total), " messages, ", dotnotation_repr(requests), " requests\n");
			}
                } else if (!argc) {
                        Print("No messages specified, and no input file was specified with -f. Please see ", app, " produce -h\n");
                        return 1;
                } else {
			const auto before = Timings::Microseconds::Tick();
			auto ts = Timings::Milliseconds::SysTime();

                        for (uint32_t i{0}; i != argc; ++i) {
                                if (asKV) {
                                        const strwlen32_t s(argv[i]);
                                        const auto        r = s.divided('=');

                                        msgs.push_back({r.second, ts, {r.first.p, uint8_t(r.first.len)}});
                                } else {
                                        msgs.push_back({strwlen32_t(argv[i]), ts, {}});
                                }

                                ts += ts_advance_step;
                                if (msgs.size() == bundleSize) {
                                        if (!publish_msgs()) {
                                                return 1;
                                        }

                                        msgs.clear();
                                }
                        }

                        if (msgs.size()) {
                                if (!publish_msgs()) {
                                        return 1;
                                }

                                msgs.clear();
                        }

                        while (tank_client.should_poll()) {
                                if (!poll()) {
                                        return 1;
				}
                        }

			if (verbose) {
				Print("Took ", duration_repr(Timings::Microseconds::Since(before)), " for ", dotnotation_repr(total), " messages, ", dotnotation_repr(requests), " requests\n");
			}
                }
        } else if (cmd.Eq(_S("benchmark")) || cmd.Eq(_S("bm"))) {
                optind = 0;
                while ((r = getopt(argc, argv, "+h")) != -1) {
                        switch (r) {

                                case 'h':
                                        Print("BENCHMARK [options] type [options]\n");
                                        Print("Executes a benchmark on the selected broker/topic/partition\n");
                                        Print("Type can be:\n");
                                        Print("p2c:  Measures latency when producing from client to broker and consuming(tailing) the broker that message\n");
                                        Print("p2b:  Measures latency when producing from client to broker\n");
                                        Print("Options include:\n");
                                        return 0;

                                default:
                                        return 1;
                        }
                }
                argc -= optind;
                argv += optind;

                if (!argc) {
                        Print("Benchmark type not selected. Please use -h option for more\n");
                        return 1;
                }

                const strwlen32_t type(argv[0]);

                if (type.Eq(_S("p2c"))) {
                        // Measure latency when publishing from publisher to broker and from broker to consume
                        // Submit messages to the broker and wait until you get them back
                        size_t size{128}, cnt{1}, batchSize{1};

                        optind = 0;
                        while ((r = getopt(argc, argv, "+hc:s:RB:")) != -1) {
                                switch (r) {
                                        case 'R':
                                                tank_client.set_compression_strategy(TankClient::CompressionStrategy::CompressNever);
                                                break;

                                        case 'c':
                                                cnt = strwlen32_t(optarg).AsUint32();
                                                break;

                                        case 's':
                                                size = strwlen32_t(optarg).AsUint32();
                                                break;

                                        case 'B':
                                                batchSize = strwlen32_t(optarg).AsUint32();
                                                break;

                                        case 'h':
                                                Print("Performs a produce to consumer via Tank latency test. It will produce messages while also 'tailing' the selected topic and will measure how long it takes for the messages to reach the broker, stored, forwarded to the client and received\n");
                                                Print("Options include:\n");
                                                Print("-s message content length (default 11 bytes)\n");
                                                Print("-c total messages to publish (default 1 message)\n");
                                                Print("-R: do not compress bundle\n");
                                                Print("-B: batch size(default 1)\n");
                                                return 0;

                                        default:
                                                return 1;
                                }
                        }
                        argc -= optind;
                        argv += optind;

                        auto *p = (char *)malloc(size + 16);

                        DEFER({ free(p); });

                        // A mostly random collection of bytes for each message
                        for (uint32_t i{0}; i != size; ++i)
                                p[i] = (i + (i & 3)) & 127;

                        const strwlen32_t            content(p, size);
                        std::vector<TankClient::msg> msgs;

                        if (tank_client.consume({{topicPartition, {UINT64_MAX, 1e4}}}, 10e3, 0) == 0) {
                                Print("Unable to schedule consumer request\n");
                                return 1;
                        }

                        msgs.reserve(cnt);

                        for (uint32_t i{0}; i < cnt;) {
                                const auto n = i + batchSize;

                                while (i != n && i < cnt) {
                                        msgs.push_back({content, 0, {}});
                                        ++i;
                                }
                        }

                        const auto start{Timings::Microseconds::Tick()};

                        if (tank_client.produce(
                                {{
                                    topicPartition,
                                    msgs,
                                }}) == 0) {
                                Print("Unable to schedule publisher request\n");
                                return 1;
                        }

                        while (tank_client.should_poll()) {
                                tank_client.poll(1e3);

                                for (const auto &it : tank_client.faults()) {
                                        consider_fault(it);
                                        return 1;
                                }

                                if (tank_client.consumed().size()) {
                                        Print("Got data after publishing ", dotnotation_repr(cnt), " message(s) of size ", size_repr(content.len), " (", size_repr(cnt * content.len), "), took ", duration_repr(Timings::Microseconds::Since(start)), "\n");
                                        return 0;
                                }
                        }
                } else if (type.Eq(_S("p2b"))) {
                        // Measure latency when publishing from publisher to broker
                        size_t size{128}, cnt{1}, batchSize{1};
                        bool   compressionDisabled{false};

                        optind = 0;
                        while ((r = getopt(argc, argv, "+hc:s:RB:")) != -1) {
                                switch (r) {
                                        case 'B':
                                                batchSize = strwlen32_t(optarg).AsUint32();
                                                break;

                                        case 'R':
                                                compressionDisabled = true;
                                                tank_client.set_compression_strategy(TankClient::CompressionStrategy::CompressNever);
                                                break;

                                        case 'c':
                                                cnt = strwlen32_t(optarg).AsUint32();
                                                break;

                                        case 's':
                                                size = strwlen32_t(optarg).AsUint32();
                                                break;

                                        case 'h':
                                                Print("Performs a produce to tank latency test. It will produce messages while also 'tailing' the selected topic and will measure how long it takes for the messages to reach the broker, stored, and acknowledged to the client\n");
                                                Print("Options include:\n");
                                                Print("-s message contrent length (default 11 bytes)\n");
                                                Print("-c total messages to publish (default 1 message)\n");
                                                Print("-R: do not compress bundle\n");
                                                Print("-B: batch size(default 1)\n");
                                                return 0;

                                        default:
                                                return 1;
                                }
                        }
                        argc -= optind;
                        argv += optind;

                        auto data = std::make_unique<char[]>(size + 16);
                        auto p{data.get()};

                        // A random collection of bytes for each message
                        uint64_t state{0};

                        for (uint32_t i{0}; i != size; ++i) {
                                state = lcrng(state);

                                p[i] = (state >> 32) & 0xff;
                        }

                        Print("Will publish ", dotnotation_repr(cnt), " messages, in batches of ", dotnotation_repr(batchSize), " messages (", dotnotation_repr(cnt / batchSize), " batch(es)), each message content is ", size_repr(size), compressionDisabled ? " (compression disabled)" : "", "\n");

                        const strwlen32_t                                                                 content(p, size);
                        std::vector<std::pair<TankClient::topic_partition, std::vector<TankClient::msg>>> batch;

                        for (uint32_t i{0}; i < cnt;) {
                                std::vector<TankClient::msg> msgs;

                                msgs.reserve(cnt);
                                for (const auto n = std::min<uint32_t>(i + batchSize, cnt); i != n; ++i)
                                        msgs.push_back({content, 0, {}});

                                batch.push_back({topicPartition, std::move(msgs)});
                        }

                        const auto start{Timings::Microseconds::Tick()};

                        if (0 == tank_client.produce(batch)) {
                                Print("Unable to schedule publisher request\n");
                                return 1;
                        }

                        while (tank_client.should_poll()) {
                                tank_client.poll(1e3);

                                for (const auto &it : tank_client.faults()) {
                                        consider_fault(it);
                                        return 1;
                                }

                                if (tank_client.produce_acks().size()) {
                                        Print("Got ACK after publishing ", dotnotation_repr(cnt), " message(s) of size ", size_repr(content.len), " (", size_repr(cnt * content.len), "), took ", duration_repr(Timings::Microseconds::Since(start)), "\n");
                                        return 0;
                                }
                        }
                } else {
                        Print("Unknown benchmark type\n");
                        return 1;
                }
        } else {
                Print("Command '", cmd, "' not supported. Please see ", app, " -h\n");
                return 1;
        }

        return EX_OK;
}

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


        if (s.size() != 16) {
                return 0;
	}

        c = s.Prefix(4);
        s.StripPrefix(4);
        tm.tm_year = c.AsUint32() - 1900;
        c          = s.Prefix(2);
        s.StripPrefix(2);
        tm.tm_mon = c.AsUint32() - 1;
        c         = s.Prefix(2);
        s.StripPrefix(2);
        tm.tm_mday = c.AsUint32();

        c = s.Prefix(2);
        s.StripPrefix(2);

        s.StripPrefix(1);
        tm.tm_hour = c.AsUint32();
        c          = s.Prefix(2);
        s.StripPrefix(2);
        tm.tm_min = c.AsUint32();

        s.StripPrefix(1);
        c = s.Prefix(2);
        s.StripPrefix(2);
        tm.tm_sec = c.AsUint32();

        tm.tm_isdst = -1;

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
        int               r;
        TankClient        tank_client;
        const char *const app = argv[0];
        bool              verbose{false}, retry{false};

        if (argc == 1)
                goto help;

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
                                } else
                                        topic.append(s);

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

                                partition = v;
                        } break;

                        case 'h':
                        help:
                                Print(app, " [common options] command [command options] [command arguments]\n");
                                Print("Common options include:\n");
                                Print("-b broker endpoint: The endpoint of the Tank broker. If not specified, assumed localhost:11011\n");
                                Print("-t topic: The selected topic\n");
                                Print("-p partition: The selected partition\n");
                                Print("-S bytes: set tank client's socket send buffer size\n");
                                Print("-R bytes: set tank client's socket receive buffer size\n");
                                Print("-v : enable verbose output\n");
                                Print("Commands available: consume, produce, benchmark, discover_partitions, mirror, create_topic, reload_config\n");
                                return 0;

                        default:
                                Print("Please use ", app, " -h for options\n");
                                return 1;
                }
        }

        if (!topic.size()) {
                Print("Topic not specified. Use -t to specify topic\n");
                return 1;
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
                Print("Invalid broker endpoint specified '", endpoint, "':", e.what(), "\n");
                return 1;
        }

        if (!argc) {
                Print("Command not specified. Please use ", app, " -h for available commands\n");
                return 1;
        }

        const strwlen32_t                 cmd(argv[0]);
        const TankClient::topic_partition topicPartition(topic.AsS8(), partition);
        const auto                        consider_fault = [](const TankClient::fault &f) {
                switch (f.type) {
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

                        case TankClient::fault::Type::SystemFail:
                                Print("System Error\n");
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
                        Size
                };
                uint8_t   displayFields{1u << uint8_t(Fields::Content)};
                size_t    defaultMinFetchSize{128 * 1024 * 1024};
                uint32_t  pendingResp{0};
                bool      statsOnly{false}, asKV{false};
                IOBuffer  buf;
                range64_t time_range{0, UINT64_MAX};
                bool      drainAndExit{false};
                uint64_t  endSeqNum{UINT64_MAX};

                optind = 0;
                while ((r = getopt(argc, argv, "+SF:hBT:KdE:s:")) != -1) {
                        switch (r) {
                                case 'E':
                                        endSeqNum = strwlen32_t(optarg).AsUint64();
                                        break;

                                case 'd':
                                        drainAndExit = true;
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
                                                } else if (*p == '.') {
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
                                                Print("Failed to parse ", s, "\n");
                                                return 1;
                                        }

                                        if (c == '-') {
                                                if (const auto end = parse_timestamp(s2); !end) {
                                                        Print("Failed to parse ", s2, "\n");

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
                                                if (it.Eq(_S("seqnum")))
                                                        displayFields |= 1u << uint8_t(Fields::SeqNum);
                                                else if (it.Eq(_S("key")))
                                                        displayFields |= 1u << uint8_t(Fields::Key);
                                                else if (it.Eq(_S("content")))
                                                        displayFields |= 1u << uint8_t(Fields::Content);
                                                else if (it.Eq(_S("ts")))
                                                        displayFields |= 1u << uint8_t(Fields::TS);
                                                else if (it.Eq(_S("size")))
                                                        displayFields |= 1u << uint8_t(Fields::Size);
                                                else {
                                                        Print("Unknown field '", it, "'\n");
                                                        return 1;
                                                }
                                        }
                                        break;

                                case 'h':
                                        Print("CONSUME [options] [from]\n");
                                        Print("Consumes/retrieves messages from Tank\n");
					Print("From(sequence number) is required unless -T is used\n");
                                        Print("Options include:\n");
                                        Print("-F display format: Specify a ',' separated list of message properties to be displayed. Properties include: \"seqnum\", \"key\", \"content\", \"ts\". By default, only the content is displayed\n");
                                        Print("-S: statistics only\n");
                                        Print("-E seqNum: Stop at sequence number specified\n");
                                        Print("-d: drain and exit. As soon as all available messages have been consumed, exit (i.e do not tail)\n");
                                        Print("-T: optionally, filter all consumes messages by specifying a time range\n");
					Print("  You can use -T to select an absolute timestamp, using `-T timestamp`, a time range using `-T start-end`, or a start time and an offset using `-T start+span`, where span is a number followed by either S(for seconds), M(for minutes), H(for hours), or D(for days).\n");
					Print("  Examples:\n");
					Print("    -T 2018090115:20:30 : The first message's timestamp will >= that time\n");
					Print("    -T 2018090115:20:30-2018090520:10:30 : The first message's timestamp will >= that time, and the last message's timestamp will be < the second timestamp\n");
					Print("    -T 2018090100:00:00-24h : Will read all messages in 24 hours starting from that timesamp\n");
					Print("    -T T-1h+2m : Will read the first 2 minutes worth of messages starting from 1 hour ago\n");
					Print("  Currently, the only supported timestamp format notations are YYYYMMDDHH:MM:SS and T-n(S|H|M|D). More formats may be supported in the future\n");
					Print("  tank-cli will use binary search to quickly determine the appropriate sequence number\n");
                                        Print("\"from\" specifies the first message we are interested in.\n");
                                        Print("If from is \"beginning\" or \"start\","
                                              " it will start consuming from the first available message in the selected topic. If it is \"eof\" or \"end\", it will "
                                              "tail the topic for newly produced messages, otherwise it must be an absolute 64bit sequence number\n");
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
                        } else
                                next = from.AsUint64();
                }

                size_t     totalMsgs{0}, sumBytes{0};
                const auto b            = Timings::Microseconds::Tick();
                auto       minFetchSize = defaultMinFetchSize;

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

                                        EXPECT(tank_client.discovered_partitions().size() == 1);
                                        EXPECT(it.topic == topicPartition.first);

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

                                                EXPECT(tank_client.consumed().size() == 1);

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

                for (const auto time_range_end = time_range.offset + time_range.size();;) {
                        if (!pendingResp) {
                                if (verbose) {
                                        Print("Requesting from ", next, "\n");
				}

                                pendingResp = tank_client.consume({{topicPartition, {next, minFetchSize}}}, drainAndExit ? 0 : 8e3, 0);

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


			pendingResp = 0;
                        for (const auto &it : tank_client.consumed()) {
                                if (drainAndExit && it.msgs.empty() && minFetchSize >= it.next.minFetchSize) {
                                        // Drained if we got no message in the response, and if the size we specified
                                        // is <= next.minFetchSize. This is important because we could get no messages
                                        // because the message is so large the minFetchSize we provided for the request was too low
                                        goto out;
                                }

                                if (statsOnly) {
                                        Print(it.msgs.len, " messages\n");
                                        totalMsgs += it.msgs.size();

                                        if (!time_range.offset) {
                                                if (verbose) {
                                                        for (const auto m : it.msgs) {
                                                                if (time_range.Contains(m->ts)) {
                                                                        Print(m->seqNum, ": ", size_repr(m->content.size()), "\n");
                                                                        sumBytes += m->content.len + m->key.len + sizeof(uint64_t);
                                                                }
                                                        }

                                                } else {
                                                        for (const auto m : it.msgs) {
                                                                if (time_range.Contains(m->ts)) {
                                                                        sumBytes += m->content.len + m->key.len + sizeof(uint64_t);
                                                                }
                                                        }
                                                }

                                        } else {
                                                if (verbose) {
                                                        for (const auto m : it.msgs) {
                                                                Print(m->seqNum, ": ", size_repr(m->content.size()), "\n");
                                                                sumBytes += m->content.len + m->key.len + sizeof(uint64_t);
                                                        }
                                                } else {
                                                        for (const auto m : it.msgs) {
                                                                sumBytes += m->content.len + m->key.len + sizeof(uint64_t);
                                                        }
                                                }
                                        }
                                } else {
                                        size_t sum{0};
                                        bool   should_abort{false};

                                        if (!time_range.offset) {
                                                for (const auto m : it.msgs) {
                                                        sum += m->content.size();
                                                }

                                                sum += it.msgs.size() * 2;
                                        } else {
                                                for (const auto m : it.msgs) {
                                                        if (time_range.Contains(m->ts)) {
                                                                sum += m->content.size();
                                                                sum += 2;
                                                        }
                                                }
                                        }

                                        buf.clear();
                                        buf.reserve(sum);
                                        for (const auto m : it.msgs) {
                                                if (m->seqNum > endSeqNum || m->ts > time_range_end) {
                                                        should_abort = true;
                                                        break;
                                                } else if (time_range.Contains(m->ts)) {
                                                        if (asKV) {
                                                                buf.append(m->seqNum, " [", m->key, "] = [", m->content, "]");
                                                        } else if (displayFields) {
                                                                if (displayFields & (1u << uint8_t(Fields::TS)))
                                                                        buf.append(Date::ts_repr(Timings::Milliseconds::ToSeconds(m->ts)), ':');
                                                                if (displayFields & (1u << uint8_t(Fields::SeqNum)))
                                                                        buf.append("seq=", m->seqNum, ':');
                                                                if (displayFields & (1u << uint8_t(Fields::Size)))
                                                                        buf.append("size=", m->content.size(), ':');
                                                                if (displayFields & (1u << uint8_t(Fields::Content)))
                                                                        buf.append(m->content);
                                                        } else {
                                                                buf.append(m->content);
                                                        }

                                                        buf.append('\n');
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

                                        if (should_abort) {
                                                goto out;
                                        }
                                }

                                minFetchSize = Max<size_t>(it.next.minFetchSize, defaultMinFetchSize);
                                next         = it.next.seqNum;
                        }
                }

        out:
                if (statsOnly) {
                        Print(dotnotation_repr(totalMsgs), " messages consumed in ", duration_repr(Timings::Microseconds::Since(b)), ", ", size_repr(sumBytes), " bytes consumed\n");
		}
        } else if (cmd.Eq(_S("create_topic"))) {
                Buffer config;

                optind = 0;
                while ((r = getopt(argc, argv, "+ho:")) != -1) {
                        switch (r) {
                                case 'h':
                                        Print("create_topic [options] total_partitions\n");
                                        Print("Will create a new topic named '", topicPartition.first, "', with total_partitions total partitions\n");
                                        Print("Options include:\n");
                                        Print("-o config-options: A ',' separated list of key=value options, that will be stored in the configuration of the new topic\n");
                                        Print("\tSee https://github.com/phaistos-networks/TANK/wiki/Configuration for available configuration options\n");
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

                        for (const auto &it : tank_client.faults())
                                consider_fault(it);

                        for (const auto &it : tank_client.created_topics()) {
                                require(it.clientReqId == reqId);
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
                                        Print("mirror [options] endpoint\n");
                                        Print("Will mirror the selected topic's partitions to the broker identified by <endpoint>.\n");
                                        Print("You should have created the partitions(directories) in the destination before you attempt to mirror from source to destination.\n");
                                        Print("Options include:\n");
                                        Print("-C cnt: Each bundle produced will contain no more that `cnt` messages. Default is ", bundleMsgsSetCntThreshold, "\n");
                                        Print("-S size: Each bundle produced will be no larger(in terms of bytes) than `size` bytes. Default is ", bundleMsgsSetSizeThreshold, "\n");
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

                if (verbose)
                        Print("Discovering partitions\n");

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

                struct partition_ctx {
                        uint16_t id;
                        bool     pending;
                        uint64_t nextBase, last;
                        uint64_t next;
                };

                simple_allocator                                                                                       allocator{4096};
                uint16_t                                                                                               srcPartitionsCnt{0};
                std::unordered_map<uint16_t, partition_ctx *>                                                          map;
                std::vector<partition_ctx *>                                                                           pending;
                std::vector<std::pair<TankClient::topic_partition, std::pair<uint64_t, uint32_t>>>                     inputs;
                std::vector<std::pair<TankClient::topic_partition, std::vector<TankClient::msg>>>                      outputs;
                std::vector<std::pair<TankClient::topic_partition, std::pair<uint64_t, std::vector<TankClient::msg>>>> outputsForBase;
                std::vector<uint8_t>                                                                                   outputsOrder;

                while (tank_client.should_poll()) {
                        tank_client.poll(1e3);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                                return 1;
                        }

                        if (tank_client.discovered_partitions().size()) {
                                const auto &v = tank_client.discovered_partitions().front();

                                require(v.clientReqId == reqId1);
                                srcPartitionsCnt = v.watermarks.len;

                                if (verbose)
                                        Print("Discovered topics from source\n");
                        }
                }

                while (dest.should_poll()) {
                        dest.poll(1e3);

                        for (const auto &it : dest.faults()) {
                                consider_fault(it);
                                return 1;
                        }

                        if (dest.discovered_partitions().size()) {
                                const auto &v = dest.discovered_partitions().front();

                                require(v.clientReqId == reqId2);
                                pending.reserve(v.watermarks.len);
                                for (const auto p : v.watermarks) {
                                        auto partition = allocator.Alloc<partition_ctx>();

                                        partition->id       = pending.size();
                                        partition->pending  = true;
                                        partition->nextBase = UINT64_MAX;
                                        partition->last     = 0;
                                        partition->next     = p->second + 1;
                                        pending.push_back(partition);
                                        map.insert({partition->id, partition});
                                }

                                if (verbose)
                                        Print("Discovered topics from dest\n");
                        }
                }

                if (pending.empty()) {
                        Print("No partitions discovered - nothing to mirror\n");
                        return 1;
                } else if (srcPartitionsCnt != pending.size()) {
                        Print("Partitions mismatch, ", dotnotation_repr(srcPartitionsCnt), " partitions discovered in source, ", dotnotation_repr(pending.size()), " in destination\n");
                        return 1;
                }

                Print("Will now mirror ", dotnotation_repr(srcPartitionsCnt), " partitions of ", ansifmt::bold, topicPartition.first, ansifmt::reset, "\n");
                Print("You can safely abort mirroring by stoping this tank-cli process (e.g CTRL-C or otherwise). Next mirror session will pick up mirroring from where this session ended\n");

                for (reqId1 = 0;;) {
                        if (!reqId1 && pending.size() && !dest.should_poll()) {
                                inputs.clear();
                                for (const auto it : pending) {
                                        if (verbose)
                                                Print("Scheduling for partition ", it->id, " from ", it->next, "\n");

                                        it->pending = false;
                                        inputs.push_back({{topicPartition.first, it->id}, {it->next, 4 * 1024 * 1024}});
                                }

                                reqId1 = tank_client.consume(inputs, 4e3, 1);
                                if (!reqId1) {
                                        Print("Failed to issue consume request\n");
                                        return 1;
                                }

                                pending.clear();
                        }

                        if (tank_client.should_poll()) {
                                tank_client.poll(1e2);

                                if (tank_client.faults().size()) {
                                        for (const auto &it : tank_client.faults())
                                                consider_fault(it);

                                        return 1;
                                }

                                if (tank_client.consumed().size()) {
                                        outputs.clear();
                                        outputsForBase.clear();
                                        outputsOrder.clear();

                                        for (const auto &it : tank_client.consumed()) {
                                                auto partition = map[it.partition];

                                                if (it.msgs.len) {
                                                        std::vector<TankClient::msg> msgs;
                                                        size_t                       sum{0};
                                                        auto                         first = it.msgs.offset->seqNum;

                                                        msgs.reserve(it.msgs.len);
                                                        for (const auto m : it.msgs) {
                                                                if (msgs.size() == bundleMsgsSetCntThreshold || sum > bundleMsgsSetSizeThreshold) {
                                                                        if (partition->nextBase != first) {
                                                                                outputsForBase.push_back({{topicPartition.first, it.partition}, {first, std::move(msgs)}});
                                                                                outputsOrder.push_back(1);
                                                                        } else {
                                                                                outputs.push_back({{topicPartition.first, it.partition}, std::move(msgs)});
                                                                                outputsOrder.push_back(0);
                                                                        }
                                                                        partition->nextBase = partition->last + 1;

                                                                        sum = 0;
                                                                        msgs.clear();
                                                                        first = m->seqNum;
                                                                }

                                                                sum += m->key.len + m->content.len + 32;
                                                                partition->last = m->seqNum;
                                                                msgs.push_back({m->content, m->ts, m->key});
                                                        }

                                                        if (partition->nextBase != first) {
                                                                outputsForBase.push_back({{topicPartition.first, it.partition}, {first, std::move(msgs)}});
                                                                partition->nextBase = partition->last + 1;
                                                                outputsOrder.push_back(1);
                                                        } else {
                                                                outputs.push_back({{topicPartition.first, it.partition}, std::move(msgs)});
                                                                outputsOrder.push_back(0);
                                                        }
                                                } else {
                                                        // could have been if timed out
                                                        if (!partition->pending) {
                                                                partition->pending = true;
                                                                pending.push_back(partition);
                                                        }
                                                }

                                                partition->next = it.next.seqNum;
                                        }

                                        // we need to do this in order
                                        if (outputsOrder.size()) {
                                                uint32_t   outputsIt{0}, outputsForBaseIt{0};
                                                const auto end = outputsOrder.end();

                                                for (auto it = outputsOrder.begin(); it != end;) {
                                                        const auto base = it;
                                                        const auto t    = *it;

                                                        for (++it; it != end && *it == t; ++it)
                                                                continue;

                                                        const auto span = it - base;

                                                        switch (t) {
                                                                case 0:
                                                                        reqId2 = dest.produce(outputs.data() + outputsIt, span);
                                                                        outputsIt += span;
                                                                        break;

                                                                case 1:
                                                                        reqId2 = dest.produce_with_base(outputsForBase.data() + outputsForBaseIt, span);
                                                                        outputsForBaseIt += span;
                                                                        break;
                                                        }

                                                        if (!reqId2) {
                                                                Print("Failed to produce to destination\n");
                                                                return 1;
                                                        }
                                                }
                                        }
                                        reqId1 = 0;
                                }
                        }

                        if (dest.should_poll()) {
                                dest.poll(1e2);

                                if (dest.faults().size()) {
                                        for (const auto &it : dest.faults())
                                                consider_fault(it);

                                        return 1;
                                }

                                for (const auto &it : dest.produce_acks()) {
                                        const auto partition = it.partition;
                                        auto       p         = map[partition];

                                        if (!p->pending) {
                                                p->pending = true;
                                                pending.push_back(p);
                                        }
                                }
                        }
                }

                return 0;
	} else if (cmd.BeginsWith(_S("reload_conf"))) {
		const auto req_id= tank_client.reload_partition_conf(topicPartition.first, topicPartition.second);

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

                        for (const auto &it : tank_client.faults())
                                consider_fault(it);

                        for (const auto &it : tank_client.discovered_partitions()) {
                                uint16_t i{0};

                                EXPECT(it.clientReqId == reqId);


#if defined(SWITCH_PHAISTOS) || 1
                                text_table tt;

				tt.set_headers("Partition", "First Available", "Last Assigned");
				for (const auto wm : it.watermarks) {
					tt.add_row(i++, wm->first, wm->second);
				}

				Print(tt.draw());
#else
                                Print(dotnotation_repr(it.watermarks.len), " partitions for '", topicPartition.first, "'\n");

                                Print(ansifmt::bold, "Partition", ansifmt::set_col(10), "First Available", ansifmt::set_col(30), "Last Assigned", ansifmt::reset, "\n");
                                for (const auto wm : it.watermarks)
                                        Print(ansifmt::bold, i++, ansifmt::reset, ansifmt::set_col(10), dotnotation_repr(wm->first), ansifmt::set_col(30), dotnotation_repr(wm->second), "\n");
#endif

                        }
                }

                return 0;
        } else if (cmd.Eq(_S("set")) || cmd.Eq(_S("produce")) || cmd.Eq(_S("publish"))) {
                char     path[PATH_MAX];
                size_t   bundleSize{1};
                bool     asSingleMsg{false}, asKV{false};
                uint64_t baseSeqNum{0};

                optind  = 0;
                path[0] = '\0';
                while ((r = getopt(argc, argv, "+s:f:F:hS:K")) != -1) {
                        switch (r) {
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

                                case 'F':
                                        asSingleMsg = true;
                                case 'f': {
                                        const auto l = strlen(optarg);

                                        require(l < sizeof(path));
                                        memcpy(path, optarg, l);
                                        path[l] = '\0';
                                } break;

                                case 'h':
                                        Print("PRODUCE [options] [message1 message2...]\n");
                                        Print("Produce messages to Tank\n");
                                        Print("Options include:\n");
                                        Print("-s number: The bundle size; how many messages to be grouped into a bundle before producing that to the broker. Default is 1, which means each new message is published as a single bundle\n");
                                        Print("-f file: The messages are read from `file`, which is expected to contain the mesasges in every new line. The `file` can be \"-\" for stdin. If this option is provided, the messages list is ignored\n");
                                        Print("-F file: Like '-f file', except that the contents of the file will be stored as a single message\n");
                                        Print("-K: treat each message in the message list as a key=value pair, instead of as the content(value) of a message\n");
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
                const auto                   poll = [&]() {
                        tank_client.poll(8e2);

                        for (const auto &it : tank_client.faults()) {
                                consider_fault(it);
                                if (retry && it.type == TankClient::fault::Type::Network) {
                                        Timings::Milliseconds::Sleep(400);
                                        pendingResps.clear();
                                } else
                                        return false;
                        }

                        for (const auto &it : tank_client.produce_acks())
                                pendingResps.erase(it.clientReqId);

                        return true;
                };
                const auto publish_msgs = [&]() {
                        uint32_t reqId;

                        if (verbose)
                                Print("Publishing ", msgs.size(), " messages\n");

                        if (baseSeqNum) {
                                reqId      = tank_client.produce_with_base({{topicPartition, {baseSeqNum, msgs}}});
                                baseSeqNum = 0;
                        } else {
                                reqId = tank_client.produce({{topicPartition, msgs}});
                        }

                        if (!reqId) {
                                Print("Failed to schedule messages to broker\n");
                                return false;
                        } else
                                pendingResps.insert(reqId);

                        pendingAckAcnt += msgs.size();

                        if (pendingAckAcnt >= pollInterval) {
                                while (tank_client.should_poll()) {
                                        if (!poll())
                                                return false;
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

                                while (range) {
                                        s.Set(buf + range.offset, range.len);

                                        if (const auto *const p = s.Search('\n')) {
                                                const uint32_t n    = p - s.p;
                                                auto           data = (char *)malloc(n);

                                                memcpy(data, s.p, n);
                                                msgs.push_back({{data, n}, Timings::Milliseconds::SysTime(), {}});
                                                range.TrimLeft(n + 1);

                                                if (msgs.size() == bundleSize) {
                                                        const auto r = publish_msgs();

                                                        for (auto &it : msgs)
                                                                std::free(const_cast<char *>(it.content.p));

                                                        msgs.clear();

                                                        if (!r)
                                                                return false;
                                                }
                                        } else
                                                break;
                                }

                                if (last && msgs.size()) {
                                        const auto r = publish_msgs();

                                        for (auto &it : msgs)
                                                std::free(const_cast<char *>(it.content.p));

                                        msgs.clear();

                                        if (!r)
                                                return false;
                                }

                                if (!range)
                                        range.reset();

                                return true;
                        };

                        range.reset();

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
                                } else
                                        pendingResps.insert(reqId);
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
                                if (!poll())
                                        return 1;
                        }
                } else if (!argc) {
                        Print("No messages specified, and no input file was specified with -f. Please see ", app, " produce -h\n");
                        return 1;
                } else {
                        for (uint32_t i{0}; i != argc; ++i) {
                                if (asKV) {
                                        const strwlen32_t s(argv[i]);
                                        const auto        r = s.Divided('=');

                                        msgs.push_back({r.second, Timings::Milliseconds::SysTime(), {r.first.p, uint8_t(r.first.len)}});
                                } else {
                                        msgs.push_back({strwlen32_t(argv[i]), Timings::Milliseconds::SysTime(), {}});
                                }

                                if (msgs.size() == bundleSize) {
                                        if (!publish_msgs())
                                                return 1;
                                        msgs.clear();
                                }
                        }

                        if (msgs.size()) {
                                if (!publish_msgs())
                                        return 1;

                                msgs.clear();
                        }

                        while (tank_client.should_poll()) {
                                if (!poll())
                                        return 1;
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

                        auto *p = (char *)malloc(size + 16);

                        Defer({ free(p); });

                        // A mostly random collection of bytes for each message
                        for (uint32_t i{0}; i != size; ++i)
                                p[i] = (i + (i & 3)) & 127;

                        const strwlen32_t            content(p, size);
                        std::vector<TankClient::msg> msgs;

                        if (tank_client.consume({{topicPartition, {UINT64_MAX, 1e4}}}, 10e3, 0) == 0) {
                                Print("Unable to schedule consumer request\n");
                                return 1;
                        }

                        Defer({ free(p); });

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

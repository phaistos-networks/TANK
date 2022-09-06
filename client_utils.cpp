#include "tank_client.h"
#ifdef LEAN_SWITCH
#include <tl/optional.hpp>
#endif

static void _handle_fault(const TankClient::fault &it) {
        switch (it.type) {
                case TankClient::fault::Type::UnknownTopic:
                        throw Switch::data_error("Fault while waiting responses: Unknown topic '"_s32, it.topic, '\'');
                case TankClient::fault::Type::UnknownPartition:
                        throw Switch::data_error("Fault while waiting responses: Unknown topic partition '"_s32, it.topic, "' / "_s32, it.partition);

                case TankClient::fault::Type::Access:
                        throw Switch::data_error("Fault while waiting responses: Access");

                case TankClient::fault::Type::BoundaryCheck:
                        throw Switch::data_error("Fault while waiting responses: Boundary Check");

                case TankClient::fault::Type::InvalidReq:
                        throw Switch::data_error("Fault while waiting responses: Invalid Request");

                case TankClient::fault::Type::SystemFail:
                        throw Switch::data_error("Fault while waiting responses: System Fault");

                case TankClient::fault::Type::AlreadyExists:
                        throw Switch::data_error("Fault while waiting responses: Resource already exists");
                case TankClient::fault::Type::NotAllowed:
                        throw Switch::data_error("Fault while waiting responses: Operation Not Allowed");

                case TankClient::fault::Type::Timeout:
                        throw Switch::data_error("Fault while waiting responses: Operation timed out");

                case TankClient::fault::Type::UnsupportedReq:
                        throw Switch::data_error("Fault while waiting responses: Request Not Supported");

                case TankClient::fault::Type::InsufficientReplicas:
                        throw Switch::data_error("Fault while waiting responses: Isuffficient Replicas available");

                case TankClient::fault::Type::Network:
                        throw Switch::data_error("Fault while waiting responses: Network Fault");
        }
}

void TankClient::wait_scheduled(const uint32_t req_id) {
        TANK_EXPECT(req_id);

        while (should_poll()) {
                poll();

                if (not faults().empty()) [[unlikely]] {
                        if (1 == faults().size()) {
                                _handle_fault(faults().front());
                        }

                        throw Switch::data_error("Fault while waiting responses");
                }

                for (const auto &it : produce_acks()) {
                        if (it.clientReqId == req_id) {
                                return;
                        }
                }

                for (const auto &it : consumed()) {
                        if (it.clientReqId == req_id) {
                                return;
                        }
                }
        }
}

uint64_t TankClient::sequence_number_by_event_time(const topic_partition &topic_partition, const uint64_t event_time, const uint64_t cut_off_threshold) {
        // @robert's idea
        if (should_poll()) {
                // this utility function is going to affect all requests scheduled
                // so one should't invoke this function unless there are no pending responses
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                throw Switch::runtime_error("Requests already in progress");
#else
                throw std::runtime_error("Requests already in progress");
#endif
        }

        // First, discover partitions in order to determine the [first, last] sequence numbers reange of the partition
        enum {
                trace = false,
        };
        tl::optional<std::pair<uint64_t, uint64_t>> offsets_space;

        if (discover_partitions(topic_partition.first) == 0) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                throw Switch::runtime_error("Failed to request topic's partitions list");
#else
                throw std::runtime_error("Failed to request topic's partitions list");
#endif
        }

        if (trace) {
                SLog("Determining partitions list for ", topic_partition.first, "/", topic_partition.second, "\n");
        }

        while (should_poll()) {
                try {
                        poll();
                } catch (...) {
                        throw;
                }

                if (not faults().empty()) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                        throw Switch::runtime_error("Unable to discover partitions - request failed");
#else
                        throw std::runtime_error("Unable to discover partitions - request failed");
#endif
                }

                if (not discovered_partitions().empty()) {
                        const auto &it = discovered_partitions().front();

                        TANK_EXPECT(discovered_partitions().size() == 1);
                        TANK_EXPECT(it.topic == topic_partition.first);

                        if (topic_partition.second >= it.watermarks.size()) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                                throw Switch::runtime_error("Partition not defined in topic partitions");
#else
                                throw std::runtime_error("Partition not defined in topic partitions");
#endif
                        }

                        offsets_space = it.watermarks.offset[topic_partition.second];
                        break;
                }
        }

        if (not offsets_space) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                throw Switch::runtime_error("Unable to determine offsets space for (topic, partition)");
#else
                throw std::runtime_error("Unable to determine offsets space for (topic, partition)");
#endif
        }

        // use binary search to determine the appropriate base sequence number
        const auto v = offsets_space.value();

        if (v.second == v.first) {
                return v.first;
        }

        uint64_t       low     = v.first;
        uint64_t       high    = v.second - 1;
        const uint64_t cut_off = event_time - cut_off_threshold;

        while (low <= high) {
                // guard against overflow
                // i.e don't use (low + high) / 2
                uint64_t msg_ts;
                auto     mid = low + (high - low) / 2;

                if (trace) {
                        SLog("low = ", low, ", high = ", high, ", mid = ", mid, "\n");
                }

                // determine the timestamp of the first message with (seq_num >= mid)
                for (uint32_t req_id{0}, fetch_size{32 * 1024};;) {
                        if (0 == req_id) {
                                if (trace) {
                                        SLog("Consuming ", topic_partition, " at ", mid, ", fetch_size = ", fetch_size, "\n");
                                }

                                req_id = consume_from(topic_partition, mid, fetch_size, 0, 0, unsigned(ConsumeFlags::prefer_local_node));

                                if (0 == req_id) {
                                        reset();
                                        continue;
                                }
                        }

                        try {
                                poll(2000);
                        } catch (...) {
                                throw;
                        }

                        if (!faults().empty()) {
                                for (const auto &it : faults()) {
                                        if (it.type == fault::Type::BoundaryCheck) {
                                                mid = it.adjust_seqnum_by_boundaries(mid);
                                        } else {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                                                throw Switch::runtime_error("Failed to access message");
#else
                                                throw std::runtime_error("Failed to access message");
#endif
                                        }
                                }

                                req_id = 0;
                                continue;
                        }

                        if (trace) {
                                SLog("consumed:", consumed().size(), "\n");
                        }

                        if (consumed().empty()) {
                                continue;
                        }

                        const auto &part = consumed().front();

                        if (trace) {
                                SLog("drained = ", part.drained, ", msgs.size() = ", part.msgs.size(), "\n");
                        }

                        if (not part.msgs.empty()) {
                                msg_ts = part.msgs.offset->ts;

                                if (trace) {
                                        SLog("Got ",
                                             Date::ts_repr(Timings::Milliseconds::ToSeconds(msg_ts)),
                                             " need ", Date::ts_repr(Timings::Milliseconds::ToSeconds(event_time)), "\n");
                                }

                                break;
                        }

                        req_id     = 0;
                        fetch_size = std::max<uint32_t>(fetch_size, part.next.min_fetch_size);
                        mid        = part.next.seq_num;
                }

                if (msg_ts < event_time and msg_ts >= cut_off) {
                        if (trace) {
                                SLog("Stopping now at ", Date::ts_repr(msg_ts / 1000),
                                     ", event_time = ", Date::ts_repr(event_time / 1000),
                                     " ", Date::ts_repr(cut_off / 1000), "\n");
                        }

                        break;
                }

                if (event_time < msg_ts) {
                        high = mid - 1;
                } else {
                        low = mid + 1;
                }
        }

        if (trace) {
                SLog("Returning low = ", low, "\n");
        }

        return low;
}

std::pair<time_t, time_t> TankClient::parse_time_window(const str_view32 s, const bool accept_yyyyymmdd) {
        // T
        // (yyyyy.mm.dd|today|yesterday)[@hh[:mm[:sec]]
        // optional followed by
        // +count<unit>
        const auto  b = s.data();
        const char *p = b;
        const auto  e = p + s.size();
        time_t      start;
        bool        have_tm = false;
        struct tm   tm;
#if 1
	static constexpr const auto fault = []() noexcept {
		return std::pair<time_t, time_t>{};
	};
#else
	static constexpr const auto fault = [](const unsigned ref = __builtin_LINE()) noexcept {
		SLog("Failed at ", ref, "\n");

		return std::pair<time_t, time_t>{};
	};
#endif

        while (p < e and ((*p >= 'a' and *p <= 'z') or (*p >= 'A' and *p <= 'Z'))) {
                ++p;
        }

        if (const str_view32 m(b, std::distance(b, p)); m) {
                if (m.EqNoCaseU(_S("TODAY"))) {
                        start = Date::day_first_second(time(nullptr));
                } else if (m.EqNoCaseU(_S("YESTERDAY"))) {
                        start = Date::day_first_second(Date::DaysAgo(time(nullptr), 1u));
                } else if (m.EqNoCaseU(_S("T"))) {
                        start = time(nullptr);
                        goto parse_end;
                } else {
                        return fault();
                }
        } else {
                // is it a YYYYMMDD?
                uint32_t v = 0;

                while (p < e and (*p >= '0' and *p <= '9')) {
                        v = v * 10 + (*(p++) - '0');
                }

                if (accept_yyyyymmdd and std::distance(b, p) == "YYYYMMDD"_len) {
                        if (not Date::valid_yyyymmdd(v)) {
                                return fault();
                        } else {
                                const auto [y, m, d] = Date::from_yyyymmdd(v);

                                tm.tm_isdst = -1;
                                tm.tm_year  = y - 1900;
                                tm.tm_mon   = m - 1;
                                tm.tm_mday  = d;
                                tm.tm_hour  = 0;
                                tm.tm_min   = 0;
                                tm.tm_sec   = 0;

                                have_tm = true;
                        }
                } else {
                        if (v < 1900 or v > 2200) [[unlikely]] {
                                return fault();
                        }

                        if (p >= e or *p != '.') [[unlikely]] {
                                return fault();
                        }

                        const auto year = v;

                        for (v = 0, ++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                                v = v * 10 + (*p - '0');
                        }

                        const auto mon = v;

                        if (mon < 1 or mon > 12) [[unlikely]] {
                                return fault();
                        }

                        if (p >= e or *p != '.') [[unlikely]] {
                                return fault();
                        }

                        for (v = 0, ++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                                v = v * 10 + (*p - '0');
                        }

                        const auto mday = v;

                        if (mday < 1 or mday > 31) [[unlikely]] {
                                return fault();
                        }

                        if (mday > Date::DaysOfMonth(mon, year)) [[unlikely]] {
                                return fault();
                        }

                        tm.tm_isdst = -1;
                        tm.tm_year  = year - 1900;
                        tm.tm_mon   = mon - 1;
                        tm.tm_mday  = mday;
                        tm.tm_hour  = 0;
                        tm.tm_min   = 0;
                        tm.tm_sec   = 0;

                        have_tm = true;
                }
        }

        if (p < e and *p == '@') {
                // parse hour (offset to start)
                uint32_t v;

                if (++p == e) [[unlikely]] {
                        return fault();
                }

                if (*p >= '0' and *p <= '9') {
                        v = *p - '0';
                } else {
                        return fault();
                }

                if (not have_tm) {
                        localtime_r(&start, &tm);
                        have_tm = true;
                }

                for (++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                        v = v * 10 + (*p - '0');
                }

                if (v > 23) [[unlikely]] {
                        return fault();
                }

                tm.tm_isdst = -1;
                tm.tm_hour  = v;
                tm.tm_min   = 0;
                tm.tm_sec   = 0;

                if (p != e and *p == ':') {
                        if (++p == e) [[unlikely]] {
                                return fault();
                        }

                        if (*p >= '0' and *p <= '9') {
                                v = *p - '0';
                        } else {
                                return fault();
                        }

                        for (++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                                v = v * 10 + (*p - '0');
                        }

                        if (v > 59) [[unlikely]] {
                                return fault();
                        }
                        tm.tm_min = v;

                        if (p != e and *p == ':') {
                                if (++p == e) [[unlikely]] {
                                        return fault();
                                }

                                if (*p >= '0' and *p <= '9') {
                                        v = *p - '0';
                                } else {
                                        return fault();
                                }

                                for (++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                                        v = v * 10 + (*p - '0');
                                }

                                if (v > 59) [[unlikely]] {
                                        return fault();
                                }

                                tm.tm_sec = v;
                        }
                }
        }

parse_end:
        if (have_tm) {
                start = mktime(&tm);
        }

        static const auto from_unit = [](const str_view32 s) noexcept -> uint32_t {
                if (not s or s.EqNoCaseU(_S("S")) or s.EqNoCaseU(_S("SECONDS"))) {
                        return 1;
                } else if (s.EqNoCaseU(_S("M")) or s.EqNoCaseU(_S("MINUTES"))) {
                        return 60;
                } else if (s.EqNoCaseU(_S("H")) or s.EqNoCaseU(_S("HOURS"))) {
                        return 3600;
                } else if (s.EqNoCaseU(_S("D")) or s.EqNoCaseU(_S("DAYS"))) {
                        return 86400;
                } else {
                        return 0;
                }
        };

        if (p < e and *p == '-') {
                uint32_t v;

                if (++p == e) [[unlikely]] {
                        return fault();
                }

                if (*p >= '0' and *p <= '9') {
                        v = *p - '0';
                } else {
                        return fault();
                }

                for (++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                        v = v * 10 + (*p - '0');
                }

                str_view32 unit;

                for (unit.p = p; p < e and ((*p >= 'a' and *p <= 'z') or (*p >= 'A' and *p <= 'Z')); ++p) {
                        continue;
                }

                unit.set_end(p);

                if (const auto c = from_unit(unit); 0 == c) {
                        return fault();
                } else {
                        start -= v * c;
                }
        }

        time_t end = std::numeric_limits<time_t>::max();

        if (p < e and *p == '+') {
                uint32_t v;

                if (++p == e) [[unlikely]] {
                        return fault();
                }

                if (*p >= '0' and *p <= '9') {
                        v = *p - '0';
                } else {
                        return fault();
                }

                for (++p; p < e and (*p >= '0' and *p <= '9'); ++p) {
                        v = v * 10 + (*p - '0');
                }

                str_view32 unit;

                for (unit.p = p; p < e and ((*p >= 'a' and *p <= 'z') or (*p >= 'A' and *p <= 'Z')); ++p) {
                        continue;
                }

                unit.set_end(p);

                if (const auto c = from_unit(unit); 0 == c) {
                        return fault();
                } else {
                        end = start + v * c;
                }
        }

        if (p != e) {
                return fault();
        }

        return {start, end};
}


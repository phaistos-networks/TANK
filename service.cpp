#include "service_common.h"
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/types.h>
#if __cplusplus > 201703L
#include <filesystem>
#endif

partition_config                config;
PubSubQueue<mainthread_closure> mainThreadClosures;
Service *                       this_service;
Buffer                          basePath_;
bool                            read_only{false};

std::mutex                       mboxLock;
std::vector<std::pair<int, int>> mbox;
std::condition_variable          mbox_cv;

int Rename(const char *oldpath, const char *newpath);
int Unlink(const char *pathname);

void topic_partition_log::consider_ro_segments() {
        enum { trace = false };

        if (compacting.load()) {
                // Busy compacting
                if (trace) {
                        SLog("Compacting\n");
                }

                return;
        }

        uint64_t   sum{0};
        const auto _now = time(nullptr);

        if (_now == ((time_t)-1)) {
                throw Switch::system_error("time() failed");
        }

        const uint32_t nowTS = _now;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_blue, "Considering segments sum=", sum, ", total = ", roSegments->size(), " limits { roSegmentsCnt ", config.roSegmentsCnt, ", roSegmentsSize ", config.roSegmentsSize, "}", ansifmt::reset, "\n");
        }

        if (config.logCleanupPolicy == CleanupPolicy::DELETE) {
                if (trace) {
                        SLog("DELETE policy\n");
                }

                for (auto it : *roSegments) {
                        sum += it->fileSize;
                }

                while (!roSegments->empty() &&
                       ((config.roSegmentsCnt && roSegments->size() > config.roSegmentsCnt) ||
                        (config.roSegmentsSize && sum > config.roSegmentsSize) ||
                        (roSegments->front()->createdTS &&
                         config.lastSegmentMaxAge &&
                         roSegments->front()->createdTS + config.lastSegmentMaxAge < nowTS))) {
                        Buffer basePath;

                        basePath.append(basePath_, "/", partition->owner->name(), "/", partition->idx, "/");

                        auto       segment     = roSegments->front();
                        const auto basePathLen = basePath.size();

                        if (trace) {
                                SLog(ansifmt::bold, ansifmt::color_red, "Removing ", segment->baseSeqNum, ansifmt::reset, "\n");
                        }

                        basePath.append("/", segment->baseSeqNum, "-", segment->lastAvailSeqNum, "_", segment->createdTS, ".ilog");
                        if (Unlink(basePath.data()) == -1) {
                                Print("Failed to unlink ", basePath, ": ", strerror(errno), "\n");
                        } else if (trace) {
                                SLog("Removed ", basePath, "\n");
                        }

                        basePath.resize(basePathLen);
                        basePath.append("/", segment->baseSeqNum, ".index");
                        if (Unlink(basePath.data()) == -1) {
                                Print("Failed to unlink ", basePath, ": ", strerror(errno), "\n");
                        } else if (trace) {
                                SLog("Removed ", basePath, "\n");
                        }

                        basePath.resize(basePathLen);

                        segment->fdh.reset();

                        sum -= segment->fileSize;
                        delete segment;

                        roSegments->erase(roSegments->begin()); // pop_front()
                }

                if (!roSegments->empty()) {
                        firstAvailableSeqNum = roSegments->front()->baseSeqNum;
                } else {
                        firstAvailableSeqNum = cur.baseSeqNum;
                }

                if (trace) {
                        SLog("firstAvailableSeqNum now = ", firstAvailableSeqNum, "\n");
                }

        } else if (config.logCleanupPolicy == CleanupPolicy::CLEANUP) {
                const auto firstDirtyOffset = first_dirty_offset();
                uint64_t   dirtyBytes{0};

                if (trace) {
                        SLog("CLEANUP policy ", firstDirtyOffset, "\n");
                }

                for (auto it : *roSegments) {
                        if (it->baseSeqNum >= firstDirtyOffset) {
                                dirtyBytes += it->fileSize;
                        }

                        sum += it->fileSize;
                }

                const double cleanable_ratio = sum ? (double(dirtyBytes) / double(sum)) : 0;

                if (trace) {
                        SLog(ansifmt::color_blue, "dirtyBytes = ", dirtyBytes, ", sum = ", sum, ", cleanable_ratio = ", cleanable_ratio, ansifmt::reset, "\n");
                }

                if (cleanable_ratio >= config.logCleanRatioMin) {
                        this_service->schedule_compaction(Buffer::build(basePath_, "/", partition->owner->name(), "/", partition->idx, "/").data(), this);
                }
        }
}

void topic_partition_log::schedule_flush(const uint32_t now) {
        cur.flush_state.pendingFlushMsgs = 0;
        cur.flush_state.nextFlushTS      = now + config.flushIntervalSecs;

        // This is obviously not optimal; we should have used a bounded queue, or some lock/wait-free construct
        // but given this is a rare event, it's not worth it yet
        mboxLock.lock();
        mbox.push_back({cur.fdh->fd, cur.index.fd});
        mboxLock.unlock();

        mbox_cv.notify_one();
}

bool topic_partition::foreach_msg(std::function<bool(topic_partition::msg &)> &l) const {
        const auto scan_vma = [&](const auto fileData, const auto fileSize, auto seqNum) {
                topic_partition::msg                     msg;
                IOBuffer                                 db;
                uint64_t                                 firstMsgSeqNum, lastMsgSeqNum;
                range_base<const uint8_t *, std::size_t> msgSetContent;

                for (const auto *p = static_cast<const uint8_t *>(fileData), *const e = p + fileSize; p != e;) {
                        const auto bundleLen          = Compression::decode_varuint32(p);
                        const auto nextBundle         = p + bundleLen;
                        const auto bundleFlags        = *p++;
                        const auto codec              = bundleFlags & 3;
                        const bool sparseBundleBitSet = bundleFlags & (1u << 6);
                        const auto msgsSetSize        = ((bundleFlags >> 2) & 0xf) ?: Compression::decode_varuint32(p);

                        if (sparseBundleBitSet) {
                                firstMsgSeqNum = decode_pod<uint64_t>(p);

                                if (msgsSetSize != 1) {
                                        lastMsgSeqNum = firstMsgSeqNum + Compression::decode_varuint32(p) + 1;
                                } else {
                                        lastMsgSeqNum = firstMsgSeqNum;
                                }
                        }

                        if (codec) {
                                db.clear();
                                if (!Compression::UnCompress(Compression::Algo::SNAPPY, p, nextBundle - p, &db)) {
                                        throw Switch::system_error("failed to decompress message set");
                                }

                                msgSetContent.set(reinterpret_cast<const uint8_t *>(db.data()), db.size());
                        } else {
                                msgSetContent.set(p, nextBundle - p);
                        }

                        // advance ptr to the next bundle
                        p = nextBundle;

                        // parse current bundle
                        uint32_t msgIdx{0};

                        msg.ts = 0;
                        for (const auto *p = msgSetContent.offset, *const e = p + msgSetContent.size(); p != e; ++msgIdx, ++seqNum) {
                                const auto flags{*p++};

                                if (sparseBundleBitSet) {
                                        if (msgIdx == 0) {
                                                seqNum = firstMsgSeqNum;
                                        } else if (msgIdx == msgsSetSize - 1) {
                                                seqNum = lastMsgSeqNum;
                                        } else if (flags & uint8_t(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                                // incremented in for() (in previous loop iteration)
                                        } else {
                                                // we encode delta from last - 1, but we already ++seqNum in for() (in previous iteration)
                                                seqNum += Compression::decode_varuint32(p);
                                        }
                                }

                                if (!(flags & uint8_t(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                        msg.ts = decode_pod<uint64_t>(p);
                                }

                                if (flags & uint8_t(TankFlags::BundleMsgFlags::HaveKey)) {
                                        msg.key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                        p += msg.key.size() + sizeof(uint8_t);
                                } else {
                                        msg.key.reset();
                                }

                                if (const auto msgLen = Compression::decode_varuint32(p)) {
                                        msg.data.set(reinterpret_cast<const char *>(p), msgLen);
                                        p += msgLen;
                                } else {
                                        msg.data.reset();
                                }

                                msg.seqNum = seqNum;
                                if (false == l(msg)) {
                                        // no need to consume any more messages
                                        return false;
                                }
                        }
                }

                return true;
        };

        auto log = _log.get();

        TANK_EXPECT(log);

        for (const auto seg : *log->roSegments) {
                const auto fileSize{seg->fileSize};
                auto       fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, seg->fdh->fd, -1);

                if (fileData == MAP_FAILED) {
                        throw Switch::data_error("Failed to mmap() partition log segment");
                }

                DEFER({
                        munmap(fileData, fileSize);
                });

                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

                if (!scan_vma(fileData, fileSize, seg->baseSeqNum)) {
                        return false;
                }
        }

        if (const auto fileSize = log->cur.fileSize) {
                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, log->cur.fdh->fd, -1);

                if (fileData == MAP_FAILED) {
                        throw Switch::data_error("Failed to mmap() partition log segment");
                }

                DEFER({
                        munmap(fileData, fileSize);
                });

                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

                if (!scan_vma(fileData, fileSize, log->cur.baseSeqNum)) {
                        return false;
                }
        }

        return true;
}

static uint32_t parse_duration(strwlen32_t in) {
        uint32_t sum{0};

        do {
                uint32_t i{0}, n{0}, scale{1};

                for (i = 0; i != in.len && isdigit(in.p[i]); ++i)
                        n = n * 10 + (in.p[i] - '0');

                if (!i) {
                        throw Switch::data_error("Unable to parse duration format");
                }

                in.StripPrefix(i);

                if (in.StripPrefix(_S("weeks")) || in.StripPrefix(_S("week")) || in.StripPrefix(_S("w")))
                        scale = 86400 * 7;
                else if (in.StripPrefix(_S("years")) || in.StripPrefix(_S("year")) || in.StripPrefix(_S("y")))
                        scale = 86400 * 365;
                else if (in.StripPrefix(_S("months")) || in.StripPrefix(_S("month")) || in.StripPrefix(_S("mon")))
                        scale = 86400 * 365;
                else if (in.StripPrefix(_S("days")) || in.StripPrefix(_S("day")) || in.StripPrefix(_S("d")))
                        scale = 86400;
                else if (in.StripPrefix(_S("hours")) || in.StripPrefix(_S("hour")) || in.StripPrefix(_S("h")))
                        scale = 3600;
                else if (in.StripPrefix(_S("minutes")) || in.StripPrefix(_S("minute")) || in.StripPrefix(_S("mins")) || in.StripPrefix(_S("min")))
                        scale = 60;
                else if (in.StripPrefix(_S("seconds")) || in.StripPrefix(_S("second")) || in.StripPrefix(_S("secs")) || in.StripPrefix(_S("sec")) || in.StripPrefix(_S("s")))
                        scale = 1;

                // Optinally, separated by ',' or '+'
                in.StripPrefix(_S(","));
                in.StripPrefix(_S("+"));
                sum += n * scale;
        } while (in);

        return sum;
}

static uint64_t parse_size(strwlen32_t in) {
        uint64_t sum{0};

        do {
                uint32_t i{0};
                uint64_t n{0}, scale{1};

                for (i = 0; i != in.len && isdigit(in.p[i]); ++i) {
                        n = n * 10 + (in.p[i] - '0');
                }

                if (!i) {
                        throw Switch::data_error("Unable to parse size format");
                }

                in.StripPrefix(i);

                if (in.StripPrefix(_S("terabytes")) || in.StripPrefix(_S("terabyte")) || in.StripPrefix(_S("tbs")) || in.StripPrefix(_S("tb")) || in.StripPrefix(_S("t"))) {
                        scale = 1024ul * 1024ul * 1024ul * 1024ul;
                } else if (in.StripPrefix(_S("gigabytes")) || in.StripPrefix(_S("gibabyte")) || in.StripPrefix(_S("gbs")) || in.StripPrefix(_S("gb")) || in.StripPrefix(_S("g"))) {
                        scale = 1024u * 1024u * 1024ul;
                } else if (in.StripPrefix(_S("megabytes")) || in.StripPrefix(_S("megabyte")) || in.StripPrefix(_S("mbs")) || in.StripPrefix(_S("mb")) || in.StripPrefix(_S("m"))) {
                        scale = 1024 * 1024;
                } else if (in.StripPrefix(_S("kilobytes")) || in.StripPrefix(_S("kilobyte")) || in.StripPrefix(_S("kbs")) || in.StripPrefix(_S("kb")) || in.StripPrefix(_S("k"))) {
                        scale = 1024;
                } else if (in.StripPrefix(_S("bytes")) || in.StripPrefix(_S("byte")) || in.StripPrefix(_S("kbs")) || in.StripPrefix(_S("b"))) {
                        scale = 1;
                }

                // Optinally, separated by ',' or '+'
                in.StripPrefix(_S(","));
                in.StripPrefix(_S("+"));
                sum += n * scale;
        } while (in);

        return sum;
}

void Service::parse_partition_config(const strwlen32_t contents, partition_config *const l) {
        for (auto &&line : contents.split('\n')) {
                strwlen32_t k, v;

                if (auto p = line.Search('#')) {
                        line.SetEnd(p);
                }

                if (!line) {
                        continue;
                }

                std::tie(k, v) = line.divided('=');
                k.TrimWS();
                v.TrimWS();

                if (!IsBetweenRange<size_t>(v.len, 1, 128)) {
                        throw Switch::data_error("Unexpected value for ", k);
                }

                // We are now using Kafka's configuration keys and semantics - for the most part - for simplicity
                // Keep it Simple.
                if (k && v) {
                        if (k.EqNoCase(_S("retention.segments.count"))) {
                                l->roSegmentsCnt = v.AsUint32();
                                if (l->roSegmentsCnt < 2 && l->roSegmentsCnt) {
                                        throw Switch::range_error("Invalid value for ", k);
                                }
                        } else if (k.EqNoCase(_S("log.cleanup.policy"))) {
                                if (v.EqNoCase(_S("cleanup"))) {
                                        l->logCleanupPolicy = CleanupPolicy::CLEANUP;
                                } else if (v.EqNoCase(_S("delete"))) {
                                        l->logCleanupPolicy = CleanupPolicy::DELETE;
                                } else {
                                        throw Switch::range_error("Unexpected value for ", k, ": available options are cleanup and delete");
                                }
                        } else if (k.EqNoCase(_S("log.cleaner.min.cleanable.ratio"))) {
                                l->logCleanRatioMin = v.AsDouble();

                                if (l->logCleanRatioMin < 0 || l->logCleanRatioMin > 1) {
                                        throw Switch::range_error("Invalid value for ", k);
                                }
                        } else if (k.EqNoCase(_S("log.retention.secs"))) {
                                l->lastSegmentMaxAge = parse_duration(v);
                        } else if (k.EqNoCase(_S("log.retention.bytes"))) {
                                l->roSegmentsSize = parse_size(v);
                                if (l->roSegmentsSize < 128 && l->roSegmentsSize) {
                                        throw Switch::range_error("Invalid value for ", k);
                                }
                        } else if (k.EqNoCase(_S("log.segment.bytes"))) {
                                l->maxSegmentSize = parse_size(v);
                                if (l->maxSegmentSize < 64) {
                                        throw Switch::range_error("Invalid value for ", k);
                                }
                        } else if (k.EqNoCase(_S("log.index.interval.bytes"))) {
                                l->indexInterval = parse_size(v);
                                if (l->indexInterval < 128) {
                                        throw Switch::range_error("Invalid value for ", k);
                                }
                        } else if (k.EqNoCase(_S("log.index.size.max.bytes"))) {
                                l->maxIndexSize = parse_size(v);
                                if (l->maxIndexSize < 128) {
                                        throw Switch::range_error("Invalid value for ", k);
                                }
                        } else if (k.EqNoCase(_S("log.roll.jitter.secs"))) {
                                l->maxRollJitterSecs = parse_duration(v);
                        } else if (k.EqNoCase(_S("log.roll.secs"))) {
                                l->curSegmentMaxAge = parse_duration(v);
                        } else if (k.EqNoCase(_S("flush.messages"))) {
                                // The number of messages accumulated on a log partition before messages are flushed on disk
                                l->flushIntervalMsgs = v.AsUint32();
                        } else if (k.EqNoCase(_S("flush.secs"))) {
                                // The amount of time the log can have dirty data before a flush is forced
                                l->flushIntervalSecs = parse_duration(v);
                        } else {
                                Print("Unknown topic/partition configuration key '", k, "'\n");
                        }
                }
        }
}

void Service::parse_partition_config(const char *const path, partition_config *const l) {
        int fd = this_service->safe_open(path, O_RDONLY | O_LARGEFILE | O_NOATIME);

        if (-1 == fd) {
                throw Switch::system_error("Failed to access topic/partition config file(", path, "):", strerror(errno));
        }

        if (const auto fileSize = lseek64(fd, 0, SEEK_END)) {
                TANK_EXPECT(fileSize != off64_t(-1));

                auto fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);

                TANKUtil::safe_close(fd);
                if (fileData == MAP_FAILED) {
                        throw Switch::system_error("Failed to access topic/partition config file(", path, ") of size ", fileSize, ":", strerror(errno));
                }

                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);
                DEFER({ munmap(fileData, fileSize); });

                parse_partition_config(strwlen32_t(static_cast<char *>(fileData), fileSize), l);
        }
}

std::shared_ptr<topic_partition> Service::define_partition(const uint16_t idx, topic *t) {
        TANK_EXPECT(t);
        auto partition = std::make_shared<topic_partition>(t);

        // it is important that we serialize access to partitions_v here
        // for if (false == cluster_aware()), we use std::async() to initialise partitions in parallel
        // and those takss may invoke init_local_partition() which in turns invokes define_partition()
        partition->idx = idx;

        partitions_v_lock.lock();
        partition->distinctId = ++nextDistinctPartitionId;
        partitions_v.emplace_back(partition.get());
        partitions_v_lock.unlock();

        return partition;
}

std::shared_ptr<topic_partition> Service::init_local_partition(const uint16_t idx, topic *topic, const partition_config &partitionConf, const bool is_new) {
        TANK_EXPECT(topic);
        auto partition = define_partition(idx, topic);

#ifndef TANK_SRV_LAZY_PARTITION_INIT
        // rely on lazy initialization
        // i.e only when requested, not earlier
        // this is important for faster startup
        try {
                open_partition_log(partition.get(), partitionConf);
        } catch (...) {
                throw;
        }
#else
        if (is_new) {
                // just create the directory if it's not there
                char basePath[PATH_MAX];

                snprintf(basePath, sizeof(basePath), "%.*s/%s%.*s/%u/",
                         static_cast<int>(::basePath_.size()), ::basePath_.data(),
                         (topic->flags & unsigned(topic::Flags::under_construction)) ? "." : "",
                         static_cast<int>(topic->name_.size()),
                         topic->name_.data(), partition->idx);

#ifdef LEAN_SWITCH
                if (-1 == mkdir(basePath, 0775)) {
                        throw Switch::system_error("Failed to mkdir(", basePath, "): ", strerror(errno));
                }

#else
                if (not SwitchFS::BuildPath(basePath)) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                        throw Switch::system_error("Failed to mkdir(", basePath, "): ", strerror(errno));
#else
                        throw std::filesystem::filesystem_error("mkdir() failed", std::error_code{});
#endif
                }
#endif
#endif
}

return partition;
}

void Service::disable_tank_srv() {
        disable_listener();

        cancel_timer(&try_become_cluster_leader_timer.node);

        // shutdown all idle TANK connections
        for (auto it = idle_connections.prev; it != &idle_connections;) {
                auto c    = switch_list_entry(connection, classification.ll, it);
                auto prev = it->prev;

                if (c->type == connection::Type::TankClient) {
                        shutdown(c);
                }

                it = prev;
        }
}

void Service::put_outgoing_queue(outgoing_queue *const q) {
        TANK_EXPECT(q);

        while (auto p = q->front_) {
                auto next = p->next;

                release_payload(p);
                q->front_ = next;
        }

        outgoingQueuesPool.push_back(q);
}

void Service::introduce_self(connection *const c, bool &have_cork) {
	static constexpr const bool trace{false};
        uint8_t b[sizeof(uint8_t) + sizeof(uint32_t)];
        auto    q  = c->outQ;
        int     fd = c->fd;

        b[0]                                               = uint8_t(TankAPIMsgType::Ping); // msg = ping
        *reinterpret_cast<uint32_t *>(b + sizeof(uint8_t)) = 0;                             // no payload

        if (trace) {
                SLog("PINGING\n");
        }

        if (q && !q->empty()) {
                if (trace) {
                        SLog("Activating Cork\n");
                }

                have_cork = true;
                Switch::SetTCPCork(fd, 1);
        }

        if (write(fd, b, sizeof(b)) != sizeof(b)) {
                // this may fail, but that's OK
        }

        c->as.tank.flags &= ~unsigned(connection::As::Tank::Flags::PendingIntro);
}

Service::Service() {
        patch_list                                  = static_cast<range32_t *>(malloc(sizeof(range32_t) * (TANK_Limits::max_topic_partitions * 2 + 512)));
        partitions_requested_eof_patch_list_indices = static_cast<uint32_t *>(malloc(sizeof(uint32_t) * (TANK_Limits::max_topic_partitions + 64)));

        switch_dlist_init(&allConnections);

        _interrupt_efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (_interrupt_efd == -1) {
                Print(ansifmt::bold, ansifmt::color_red, "eventfd() failed:", ansifmt::reset, " ", strerror(errno), "\n");
                Print("Aborting Immediately\n");
                exit(1);
        }

        poller.insert(_interrupt_efd, EPOLLIN, &_interrupt_efd);

        memset(&timers_ebtree_root, 0, sizeof(timers_ebtree_root));
        timers_ebtree_next         = std::numeric_limits<uint64_t>::max();
        cleanup_tracker_timer.type = timer_node::ContainerType::CleanupTracker;
}

Service::~Service() {
        std::free(patch_list);
        std::free(partitions_requested_eof_patch_list_indices);

        if (_interrupt_efd != -1) {
                TANKUtil::safe_close(_interrupt_efd);
        }

        while (!bufs.empty()) {
                delete bufs.back();
                bufs.pop_back();
        }

        while (!outgoingQueuesPool.empty()) {
                delete outgoingQueuesPool.back();
                outgoingQueuesPool.pop_back();
        }

        for (auto it : waitctx_deferred_gc) {
                put_waitctx(it);
        }

        waitctx_deferred_gc.clear();

        for (auto c : pending_reusable_conns) {
                delete c;
        }

        for (auto &it : topics) {
                for (auto p : *it.second->partitions_) {
                        for (auto &it : p->waiting_list) {
                                std::free(it.first);
                        }

                        p->waiting_list.clear();
                }
        }

        for (auto &it : waitCtxPool) {
                while (!it.empty()) {
                        std::free(it.back());
                        it.pop_back();
                }
        }
}

void Service::schedule_cleanup() {
        if (!cleanup_tracker_timer.is_linked()) {
                cleanup_tracker_timer.node.key = now_ms + 128;
                register_timer(&cleanup_tracker_timer.node);
        }
}

int Service::safe_open(const char *path, int flags, mode_t mode) {
        for (int fd;;) {
                fd = open(path, flags, mode);

                if (-1 == fd) {
                        if ((ENFILE == errno || EMFILE == errno) && try_shutdown_idle(1)) {
                                continue;
                        } else if (EINTR == errno) {
                                continue;
                        } else {
                                return -1;
                        }
                } else {
                        return fd;
                }
        }
}

topic *Service::topic_by_name(const strwlen8_t name) const {
        if (const auto it = topics.find(name); it != topics.end()) {
                return it->second.get();
        } else {
                return nullptr;
        }
}

void Service::tear_down() {
        // notify the thread to exit so that we can join()
        mboxLock.lock();
        mbox.emplace_back(-1, 1);
        mboxLock.unlock();
        mbox_cv.notify_one();

        if (auto t = sync_thread.release()) {
                t->join();
                delete t;
        }

        if (compactions.compaction_thread) {
                compactions.pendingCompactions.push_back(new pending_compaction{.log = nullptr});
                compactions.workCond.notify_one();
                compactions.compaction_thread->join();
        }
}

void Service::cleanup_scheduled_logs() {
        // TODO(markp): maybe we should just have another thread to do this
        // although this is a very infrequent operation and shouldn't take more than a few microseconds
        IOBuffer b;
        int      fd;

        for (auto it : cleanup_tracker) {
                const auto topic = it->partition->owner->name();

                b.Serialize(topic.len);
                b.Serialize(topic.p, topic.len);
                b.Serialize<uint16_t>(it->partition->idx);
                b.Serialize<uint64_t>(it->lastCleanupMaxSeqNum);
        }

        fd = safe_open(Buffer::build(basePath_, "/.cleanup.log.int").data(), O_WRONLY | O_TRUNC | O_CREAT, 0775);

        if (fd == -1) {
                Print("Failed to update cleanup log:", strerror(errno), "\n");
        } else if (write(fd, b.data(), b.size()) != b.size()) {
                Print("Failed to update cleanup log:", strerror(errno), "\n");
                TANKUtil::safe_close(fd);
        } else {
                TANKUtil::safe_close(fd);

                if (Rename(Buffer::build(basePath_, "/.cleanup.log.int").data(), Buffer::build(basePath_, "/.cleanup.log").data()) == -1) {
                        Print("Failed to update cleanup log:", strerror(errno));
                }
        }
}

bool topic_partition::safe_to_reset() const noexcept {
        return enabled() == false;
}

void Service::track_accessed_partition(topic_partition *const p, const time_t now) {
        // TODO: maybe only do so if (p->flags & unsigned(topic_partition::Flags::NoDataFiles) == 0)
        if (p->access.ll.empty()) {
                if (active_partitions.empty()) {
                        next_active_partitions_check = now_ms + Timings::Seconds::ToMillis(8);
                }

                active_partitions.push_back(&p->access.ll);
        }

        p->access.last_access = now;
}

void Service::consider_active_partitions() {
        for (auto it = active_partitions.next; it != &active_partitions;) {
                auto       next  = it->next;
                auto       part  = containerof(topic_partition, access.ll, it);
                const auto delta = curTime - part->access.last_access;

                TANK_EXPECT(!part->access.ll.empty());

                if (delta > 60 * 5) {
                        // this may not happend if the partition is currently being compacted
                        close_partition_log(part);
                }

                it = next;
        }

        next_active_partitions_check = active_partitions.empty()
                                           ? std::numeric_limits<uint64_t>::max()
                                           : now_ms + Timings::Seconds::ToMillis(32);
}

#include "service_common.h"
#if __cplusplus > 201703L
#include <filesystem>
#endif

extern std::mutex                       mboxLock;
extern std::vector<std::pair<int, int>> mbox;
extern std::condition_variable          mbox_cv;

namespace {
        [[maybe_unused]] std::atomic<bool> bootstrap_failed{false};
}

int Service::start(int argc, char **argv) {
        static constexpr bool trace{false};
        sockaddr_in           sa;
        int                   r;
        struct stat64         st;
        size_t                totalPartitions{0};

        this_service = this;
        startup_ts   = time(nullptr);

#ifndef LEAN_SWITCH
        // See: https://github.com/markpapadakis/BacktraceResolver
        Switch::trapCommonUnexpectedSignals();
#endif

        if (argc > 1 && !strcmp(argv[1], "verify")) {
                // https://github.com/phaistos-networks/TANK/wiki/Managing-Segments-files
                return verify(argv + 2, argc - 2);
        }

        prom_endpoint.unset();

        if (argc == 1) {
                goto help;
        }

        while ((r = getopt(argc, argv, "p:l:hvP:rC:")) != -1) {
                switch (r) {
                        case 'C': {
                                auto [id_repr, cluster_name] = str_view32(optarg).divided('@');
                                str_view32 endpoint_repr;

                                if (const auto p = cluster_name.Search('|')) {
                                        endpoint_repr = cluster_name.SuffixFrom(p + 1);
                                        cluster_name.set_end(p);
                                }

                                if (!id_repr || !id_repr.all_of_digits()) {
                                        Print("Invalid cluster ID\n");
                                        return 1;
                                } else if (!cluster_name || cluster_name.size() > 32) {
                                        Print("Invalid cluster name\n");
                                        return 1;
                                }

                                const auto cluster_id = id_repr.as_uint32();

                                if (cluster_id > std::numeric_limits<uint16_t>::max()) {
                                        Print("Invalid cluster node id, allowed values [0, ", std::numeric_limits<uint16_t>::max(), "]\n");
                                        return 1;
                                }

                                cluster_state._name_len = cluster_name.size();
                                cluster_name.CopyTo(cluster_state._name);
                                cluster_state.local_node.id = cluster_id;

                                if (!endpoint_repr) {
                                        consul_state.srv.endpoint.addr4 = IP4Addr(127, 0, 0, 1);
                                        consul_state.srv.endpoint.port  = 8500;
                                } else {
                                        const auto e = Switch::ParseSrvEndpoint(endpoint_repr, {}, 8500);

                                        if (!e) {
                                                Print("Unable to parse Consul Agent endpoint ", e, "\n");
                                                return 1;
                                        }

                                        consul_state.srv.endpoint = e;
                                }

                                if (int fd = open("tank_consul.token", O_RDONLY); - 1 == fd) {
                                        Print(ansifmt::color_red, "Failed to access tank_consul.token: ", ansifmt::reset, strerror(errno), "\n");
                                        Print("The consul agent API token should be stored in that file so that interfacing with the Consul Cluster can be accomplished.\n");
                                        return 1;
                                } else if (const auto file_size = lseek(fd, 0, SEEK_END); file_size == 0 || file_size > 64) {
                                        TANKUtil::safe_close(fd);
                                        Print("Not valid consul agent API token found in tank_consul.token.\n");
                                        Print("That file should only contain a valid consul API token\n");
                                        return 1;
                                } else {
                                        char buf[file_size];

                                        if (pread(fd, buf, file_size, 0) != file_size) {
                                                Print(ansifmt::color_red, "Failed to access tank_consul.token: ", ansifmt::reset, strerror(errno), "\n");
                                                TANKUtil::safe_close(fd);
                                                return 1;
                                        }

                                        const auto token = str_view32(buf, file_size).ws_trimmed();

                                        memcpy(consul_state._token, token.data(), token.size());
                                        consul_state._token_len = token.size();
                                        TANKUtil::safe_close(fd);
                                }
                        } break;

                        case 'r':
                                read_only = true;
                                break;

                        case 'P':
                                prom_endpoint = Switch::ParseSrvEndpoint({optarg}, "http"_s8, 9102);
                                if (!prom_endpoint) {
                                        Print("Failed to parse endpoint for prometheus metrics from ", optarg, "\n");
                                        return 1;
                                }
                                break;

                        case 'p':
                                basePath_.clear();
                                basePath_.append(strwlen32_t(optarg, strlen(optarg)));
                                break;

                        case 'l':
                                tank_listen_ep = Switch::ParseSrvEndpoint({optarg}, _S8("tank"), 11011);
                                if (!tank_listen_ep) {
                                        Print("Failed to parse endpoint from ", optarg, "\n");
                                        return 1;
                                }
                                break;

                        case 'h':
                        help:
                                Print("Usage: ./tank <settings> [-h] [-v] [-C <spec>]\n\n");
                                Print("Settings:\n");
                                Print(Buffer{}.append(align_to(5), "-p <path>"_s32, align_to(24), "Specifies the base path for all TANK data"_s32), "\n");
                                Print(Buffer{}.append(align_to(5), "-l <endpoint>"_s32, align_to(24), "Specifies the endpoint to to listen for incoming connections."_s32), "\n", Buffer{}.append(align_to(24), "Endpoint notation is [address:]port"_s32), "\n");
                                Print(Buffer{}.append(align_to(5), "-C <spec>"_s32, align_to(24), "Have this node join a TANK cluster. spec notation is nodeid@cluster_name"_s32), "\n", Buffer{}.append(left_aligned(24, "This is how TANK clusters are built. One or more TANK nodes can form clusters. Multiple TANK clusters can be defined. Each TANK node is identified by a unique node id that is specified using this option.\nCurrently, Consul is supported for leadership election and metadata storage, so TANK will connect to the local Consul node.\nFor more information about Consul, please see https://www.consul.io/ and TANK's Documentation", 76), "\n\n"));

                                Print("\nOther Options:\n");
                                Print(Buffer{}.append(align_to(5), "-v"_s32, align_to(24), "Enable verbose messages output"_s32), "\n");
                                Print(Buffer{}.append(align_to(5), "-h"_s32, align_to(24), "This help message"_s32), "\n");
                                Print("\n\nExamples:\n");
                                Print(Buffer{}.append(left_aligned(7, "./tank -p /data/TANK -l :11011 :Starts a TANK node where the TANK data files are stored in /tmp/TANK and accepts requests at localhost:11011"_s32, 76)), "\n\n");
                                Print(Buffer{}.append(left_aligned(7, "./tank -p /data/TANK -l 10.5.5.10:11011 :Starts a TANK node where the TANK data files are stored in /tmp/TANK and accepts requests at 10.5.5.10:11011"_s32, 76)), "\n\n");
                                Print(Buffer{}.append(left_aligned(7, "./tank -p /data/TANK -l 10.5.5.10:11011 -C 1@my_cluster: Starts a TANK node where the TANK data files are stored in /tmp/TANK and accepts requests at 10.5.5.10:11011, and joins or creates the TANK cluster 'my_cluster'\nThe node maybe become the cluster leader and may replicate data from existing topic parttiions immediately. Please see TANK's documentation for Clusters terminology and guides"_s32, 76)), "\n");
                                return 0;

                        case 'v':
                                Print("TANK v", TANK_VERSION / 100, ".", TANK_VERSION % 100, ", (C) Phaistos Networks, S.A | http://phaistosnetworks.gr/\n");
                                Print("You can always get the latest release from the GitHub hosted repository at https://github.com/phaistos-networks/TANK\n");
                                return 0;

                        default:
                                return 1;
                }
        }

        if (!tank_listen_ep) {
                Print("Listen address not specified. Use -l address to specify it\n");
                return 1;
        } else if (cluster_aware() && tank_listen_ep.addr4 == INADDR_ANY) {
                Print("Expected address:port for cluster aware TANK mode\n");
                return 1;
        }

        if (trace) {
                SLog("cluster_aware() = ", cluster_aware(), ", basePath_ = ", basePath_, "\n");
        }

        curTime = time(nullptr);

        if (basePath_.empty()) {
                Print("Base path not specified. Use -p path to specify it\n");
                return 1;
        } else if (stat64(basePath_.data(), &st) == -1) {
                Print("Failed to stat(", basePath_, "): ", strerror(errno), ". Please verify base path\n");
                return 1;
        } else if (!(st.st_mode & S_IFDIR)) {
                Print(basePath_, " is not a directory\n");
                return 1;
        } else if (!cluster_aware()) {
                try {
                        // We will parallelize this across multiple threads so that we can support many thousands of topics and partitions
                        // without incurring a long startup-sequence time
                        const auto                                                        basePathLen = basePath_.size();
                        std::vector<std::pair<topic *, size_t>>                           pendingPartitions;
                        uint64_t                                                          before;
                        simple_allocator                                                  a{8192};
                        std::vector<strwlen8_t>                                           collectedTopics;
                        std::mutex                                                        collectLock;
                        std::vector<std::pair<std::pair<strwlen8_t, uint16_t>, uint64_t>> cleanupCheckpoints;
                        char                                                              fullPath[PATH_MAX];

                        if (trace) {
                                before = Timings::Microseconds::Tick();
                        }

                        if (trace) {
                                SLog("Attempting to iterate names in ", basePath_, "\n");
                        }

                        Print("Initializing topics and partitions from ", basePath_, " ..\n");

                        try {
                                for (const auto &&name : DirectoryEntries(basePath_.data())) {
                                        if (name.Eq(_S(".cleanup.log"))) {
                                                int fd = open(Buffer::build(basePath_, "/", name).data(), O_RDONLY | O_LARGEFILE);

                                                if (fd == -1) {
                                                        Print(ansifmt::bold, ansifmt::color_red, "Failed to access ", name, ansifmt::reset, ": ", strerror(errno), "\n");
                                                        Print("Aborting Now\n");
                                                        return 1;
                                                }

                                                const auto fileSize = lseek64(fd, 0, SEEK_END);

                                                if (!fileSize) {
                                                        TANKUtil::safe_close(fd);
                                                        continue;
                                                }

                                                auto      fileData = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
                                                str_view8 topicName;

                                                TANKUtil::safe_close(fd);
                                                if (fileData == MAP_FAILED) {
                                                        RFLog("mmap() failed:", strerror(errno), "\n");
                                                        return 1;
                                                }

                                                DEFER(
                                                    {
                                                            munmap(fileData, fileSize);
                                                    });

                                                madvise(fileData, fileSize, MADV_SEQUENTIAL | MADV_DONTDUMP);

                                                for (const auto *p = static_cast<uint8_t *>(fileData), *const e = p + fileSize; p < e;) {
                                                        topicName.Set((char *)p + 1, *p);

                                                        p += topicName.size() + sizeof(uint8_t);

                                                        const auto partition = decode_pod<uint16_t>(p);
                                                        const auto seqNum    = decode_pod<uint64_t>(p);

                                                        if (seqNum) {
                                                                cleanupCheckpoints.push_back({{{a.CopyOf(topicName.p, topicName.len), topicName.len}, partition}, seqNum});
                                                        }
                                                }
                                        } else if (name == "."_s8 || name == ".."_s8) {
                                                continue;
                                        } else {
                                                struct stat64 st;

                                                basePath_.ToCString(fullPath, sizeof(fullPath));
                                                fullPath[basePath_.size()] = '/';
                                                name.ToCString(fullPath + basePath_.size() + 1);

                                                if (stat64(fullPath, &st) == -1)
                                                        throw Switch::system_error("Failed to stat(", fullPath, "): ", strerror(errno));
                                                else if (!S_ISDIR(st.st_mode)) {
                                                        // Just ignore whatever isn't a directory
                                                        if (trace) {
                                                                SLog("Ignoring [", name, "]: not a directory\n");
                                                        }
                                                } else {
                                                        if (name.front() == '.') {
                                                                if (false && false == read_only) {
                                                                        Print(ansifmt::color_red, "Found stray topic '", name, "'. Will attempt to delete it", ansifmt::reset, "\n");
                                                                        rm_tankdir(fullPath);
                                                                } else {
                                                                        // TODO: https://github.com/phaistos-networks/TANK/issues/70
                                                                        Print(ansifmt::color_red, "Found stray topic '", name, "'. Will NOT attempt to delete it", ansifmt::reset, "\n");
                                                                }
                                                        } else {
                                                                collectedTopics.emplace_back(a.CopyOf(name.p, name.len), name.len);
                                                        }
                                                }
                                        }
                                }
                        } catch (const std::exception &e) {
                                Print(ansifmt::bold, ansifmt::color_red, "Initialization failed:", ansifmt::reset, e.what(), ". Aborting startup-sequence\n");
                                return 1;
                        }

                        if (trace) {
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)),
                                     " for initial walk ", collectedTopics.size(), " topics\n");
                        }

                        try {
                                for (const auto name : collectedTopics) {
                                        // we are not going to be using std::async()
                                        // it creates a new thread for each call, which is far from optimal
                                        // for now, we will not parallelize the work, but eventually we will use a fixed-size thread pool
                                        // to register all those topics, although it shouldn't really matter match
                                        basePath_.resize(basePathLen);
                                        basePath_.append('/', name, '/');

                                        // XXX: make sure you access basePath_.data() after Buffer::append()
                                        auto       path = basePath_.c_str();
                                        const auto len  = basePath_.size();

                                        if (stat64(path, &st) == -1) {
#if defined(TANK_THROW_SWITCH_EXCEPTIONS) || __cplusplus <= 201703L
                                                throw Switch::system_error("stat(", str_view32(path, len), ") failed:", strerror(errno));
#else
                                                throw std::filesystem::filesystem_error(std::string{}
                                                                                            .append("stat(")
                                                                                            .append(path, len)
                                                                                            .append(") failed:")
                                                                                            .append(strerror(errno)),
                                                                                        std::error_code{});
#endif
                                        } else if (st.st_mode & S_IFDIR) {
                                                uint32_t         partitionsCnt{0}, min{UINT32_MAX}, max{0};
                                                partition_config partitionConfig;

                                                try {
                                                        for (const auto &&name : DirectoryEntries(path)) {
                                                                basePath_.resize(len);
                                                                basePath_.append(name);

                                                                if (name.Eq(_S("config"))) {
                                                                        // topic overrides defaults
                                                                        parse_partition_config(basePath_.c_str(), &partitionConfig);
                                                                } else if (name.IsDigits()) {
#if 0 // for faster-startup, we assume names comprised of digits are indeed partitioons/directories
									if (stat64(path, &st) == -1) {
										throw Switch::system_error("Failed to stat(", basePath_, "): ", strerror(errno));
									}

									if (!(st.st_mode & S_IFDIR)) {
										continue;
									}
#endif

                                                                        const auto id = name.as_uint32();

                                                                        min = std::min<uint32_t>(min, id);
                                                                        max = std::max<uint32_t>(max, id);
                                                                        ++partitionsCnt;
                                                                }
                                                        }
                                                } catch (...) {
                                                        throw;
                                                }

                                                try {
                                                        if (partitionsCnt) {
                                                                if (min != 0 && max != partitionsCnt - 1) {
                                                                        throw Switch::system_error("Unexpected partitions list; expected [0, ", partitionsCnt - 1, "]");
                                                                }

                                                                auto t = Switch::make_sharedref<topic>(name, partitionConfig);

                                                                TANK_EXPECT(t->use_count() == 1);

                                                                collectLock.lock();
                                                                pendingPartitions.emplace_back(t.get(), partitionsCnt);
                                                                register_topic(t.get());
                                                                collectLock.unlock();
                                                        } else {
                                                                path[len] = '\0'; // this is important

                                                                if (false && false == read_only) {
                                                                        Print(ansifmt::color_red, "No partions found in ", path,
                                                                              ": Will delete topic", ansifmt::reset, "\n");

                                                                        rm_tankdir(path);
                                                                } else {
                                                                        // TODO: https://github.com/phaistos-networks/TANK/issues/70
                                                                        Print(ansifmt::color_red, "No partions found in ", path,
                                                                              ": Will NOT delete topic", ansifmt::reset, "\n");
                                                                }
                                                        }
                                                } catch (...) {
                                                        throw;
                                                }
                                        }
                                }
                        } catch (const std::exception &e) {
                                Print(ansifmt::bold, ansifmt::color_red, "Initialization failed:", ansifmt::reset,
                                      e.what(), ". Aborting startup-sequence\n");
                                return 1;
                        }

                        basePath_.resize(basePathLen);

                        if (trace) {
                                SLog("Took ", duration_repr(Timings::Microseconds::Since(before)),
                                     " for topics, pendingPartitions.size() = ", pendingPartitions.size(), "\n");
                        }

                        before = Timings::Microseconds::Tick();
                        if (!pendingPartitions.empty()) {
                                static constexpr bool                                                   trace{false};
                                std::vector<std::pair<topic *, Switch::shared_refptr<topic_partition>>> list;

                                if (trace) {
                                        SLog("pendingPartitions.size() = ", pendingPartitions.size(), "\n");
                                }

                                for (auto &it : pendingPartitions) {
                                        auto t = it.first;

                                        for (uint16_t i{0}; i < it.second; ++i) {
                                                try {
                                                        const auto partition = i;
                                                        auto       p         = init_local_partition(partition, t, t->partitionConf, false);

                                                        if (p) {
                                                                list.emplace_back(t, std::move(p));
                                                        }

                                                } catch (std::exception &e) {
                                                        Print(ansifmt::bold, ansifmt::color_red, "Initialization failed:", ansifmt::reset, e.what(), ". Aborting startup-sequence\n");
                                                        return 1;
                                                }
                                        }
                                }

                                if (trace) {
                                        SLog("Done with partitions\n");
                                }

                                if (trace) {
                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " for all partitions\n");
                                }

                                std::sort(list.begin(), list.end(), [](const auto &a, const auto &b) noexcept {
                                        return uintptr_t(a.first) < uintptr_t(b.first);
                                });

                                const auto                     n = list.size();
                                std::vector<topic_partition *> partitions;

                                if (trace) {
                                        before = Timings::Microseconds::Tick();
                                }

                                for (uint32_t i{0}; i < n;) {
                                        auto t = list[i].first;

                                        do {
                                                partitions.push_back(list[i].second.release());
                                        } while (++i < n && list[i].first == t);

                                        t->register_partitions(partitions.data(), partitions.size());
                                        totalPartitions += partitions.size();
                                        partitions.clear();
                                }

                                for (const auto &it : cleanupCheckpoints) {
                                        const auto topic     = it.first.first;
                                        const auto partition = it.first.second;
                                        auto       seqNum    = it.second;

                                        if (auto t = topic_by_name(topic)) {
                                                if (partition < t->partitions_->size()) {
                                                        auto log = partition_log(t->partitions_->at(partition));

                                                        if (seqNum < log->firstAvailableSeqNum) {
                                                                seqNum = log->firstAvailableSeqNum - 1;
                                                        }

                                                        log->lastCleanupMaxSeqNum = seqNum;
                                                        cleanup_tracker.push_back(log); // TODO(markp): If we support topic/partitions deletes, we need to remove from cleanup_tracker
                                                }
                                        }
                                }

                                if (trace) {
                                        SLog("Took ", duration_repr(Timings::Microseconds::Since(before)), " to initialize all partitions\n");
                                }
                        }
                } catch (const std::exception &e) {
                        Print(ansifmt::bold, ansifmt::color_red, "Failed to initialize topics and partitions:", e.what(), ansifmt::reset, "\n");
                        return 1;
                }
        }

        Print(ansifmt::bold, "<=TANK=>", ansifmt::reset, " v", TANK_VERSION / 100, ".", TANK_VERSION % 100,
              " | ", dotnotation_repr(topics.size()), " topics registered, ",
              dotnotation_repr(totalPartitions), " partitions; will listen for new connections at ", tank_listen_ep, "\n");

        if (prom_endpoint) {
                Print("> Prometheus Exporter enabled, will respond to http://", prom_endpoint, "/metrics\n");
        }

        if (read_only) {
                Print("> Read-Only mode; some functionality/APIs will be unavailable\n");
        }

        Print("(C) Phaistos Networks, S.A. - ", ansifmt::color_green, "http://phaistosnetworks.gr/", ansifmt::reset, ". Licensed under the Apache License\n\n");

        if (topics.empty()) {
                if (!cluster_aware()) {
                        Print("No topics found in ", basePath_, ". You may want to create a few, like so:\n");
                        Print("mkdir -p ", basePath_, "/events/0 ", basePath_, "/orders/0 \n");
                        Print("This will create topics events and orders and define one partition with id 0 for each of them.\nRestart TANK after you have created a few topics/partitions\n");
                        Print(R"EOF(Or, you can use tank-cli's "create topic" command to create new topics instead)EOF", "\n");
                }
        }

        if (false == cluster_aware() && !enable_listener()) {
                // we are going to until we have bootstrapped
                return 1;
        }

        if (prom_endpoint) {
                prom_listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

                if (-1 == prom_listen_fd) {
                        Print("socket() failed:", strerror(errno), "\n");
                        return 1;
                }

                memset(&sa, 0, sizeof(sa));
                sa.sin_addr.s_addr = prom_endpoint.addr4;
                sa.sin_port        = htons(prom_endpoint.port);
                sa.sin_family      = AF_INET;

                if (Switch::SetReuseAddr(prom_listen_fd, 1) == -1) {
                        Print("SO_REUSEADDR: ", strerror(errno), "\n");
                        return 1;
                } else if (bind(prom_listen_fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) == -1) {
                        Print("bind() failed:", strerror(errno), ". Cannot accept Prometheus connections.\n");
                        return 1;
                } else if (listen(prom_listen_fd, 128)) {
                        Print("listen() failed:", strerror(errno), "\n");
                        return 1;
                }
        }

        sync_thread.reset(new std::thread([] {
                std::vector<std::pair<int, int>> local;
                sigset_t                         mask;

                sigfillset(&mask);
                pthread_sigmask(SIG_SETMASK, &mask, nullptr);
                for (;;) {
                        {
                                std::unique_lock<std::mutex> g(mboxLock);

                                while (mbox.empty()) {
                                        mbox_cv.wait(g);
                                }

                                local = std::move(mbox);
                                TANK_EXPECT(mbox.empty());
                        }

                        for (auto &it : local) {
                                if (-1 == it.first) {
                                        return;
                                }

                                fsync(it.first);
                                fsync(it.second);
                        }

                        local.clear();
                }
        }));

        if (prom_listen_fd != -1) {
                poller.insert(prom_listen_fd, POLLIN, &prom_listen_fd);
        }

        main_thread_id = pthread_self();
        curTime        = time(nullptr);

        // setup signals delivery
        sigset_t   mask;
        const auto signal_handler = [](int sig) { pending_signals.fetch_or(1ull << sig); };

        signal(SIGINT, signal_handler);
        signal(SIGUSR1, signal_handler);

        sigemptyset(&mask);
        sigaddset(&mask, SIGPIPE);
        sigaddset(&mask, SIGHUP);
        sigdelset(&mask, SIGUSR1);
        if (-1 == pthread_sigmask(SIG_BLOCK, &mask, nullptr)) {
                Print("pthread_sigmask():", strerror(errno), "\n");
                return 1;
        }

        // we need to make sure now_ms is initialized before we register_with_cluster()
        now_ms = Timings::Milliseconds::Tick();

        if (cluster_aware()) {
                if (int fd = open(Buffer{}.append(basePath_, "/.cluster_session_"_s32, cluster_state.local_node.id).c_str(), O_RDONLY); - 1 == fd) {
                        if (errno != ENOENT) {
                                Print("Failed to access cluster_session: ", strerror(errno), "\n");
                                return 1;
                        }
                } else {
                        const auto file_size = lseek(fd, 0, SEEK_END);

                        if (file_size != 36) {
                                TANKUtil::safe_close(fd);
                                Print("Unexpected cluster_session file contents\n");
                                return 1;
                        }

                        if (pread(fd, consul_state._session_id, file_size, 0) != file_size) {
                                Print("Failed to access cluster session: ", strerror(errno), "\n");
                                TANKUtil::safe_close(fd);
                                return 1;
                        } else {
                                // We have a session already, so we 'll try to renew it first
                                // and if it fails only then create a new session
                                //
                                // see register_with_cluster()
                                consul_state._session_id_len = file_size;
                                TANKUtil::safe_close(fd);
                        }
                }

                register_with_cluster();
        }

        return reactor_main();
}

#include <ext/Catch/catch.hpp>
#include "service.h"

bool adjust_range_start(lookup_res &res, const uint64_t abs_seq_num, std::unordered_map<uint64_t, adjust_range_start_cache_value> *const cache);

struct materialized_msg final {
        str_view8  key;
        str_view32 data;
        uint64_t   ts;
};

static bool set_partition_replicas(topic_partition *p, cluster_node **nodes, const size_t nodes_cnt, std::vector<node_partition_rel_update> *const updates) {
        static constexpr bool trace{false};
	const size_t K_max_replicas =128;
        TANK_EXPECT(p);

        cluster_node *new_set[K_max_replicas];
        size_t        new_i{0}, new_e{nodes_cnt};
        size_t        old_i{0}, old_e{p->cluster.replicas.nodes.size()};
        size_t        out{0};
        bool          dirty{false};

        while (new_i < new_e && old_i < old_e) {
                auto new_node = nodes[new_i];
                auto old_node = p->cluster.replicas.nodes[old_i];

                if (new_node->id == old_node->id) {
                        // partition is still being replicated to node
                        new_set[out++] = old_node;

                        ++new_i;
                        ++old_i;
                } else if (old_node->id < new_node->id) {
                        // partition is no longer replicated to node
                        updates->emplace_back(node_partition_rel_update{.n = old_node, .p = p, .assign = false});

                        if constexpr (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx, " no longer replicated to node ", old_node->id, "\n");
                        }

                        dirty = true;
                        ++old_i;
                } else {
                        // partition will be replicated to node
                        updates->emplace_back(node_partition_rel_update{.n = new_node, .p = p, .assign = true});

                        if constexpr (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx, " NOW replicated to node ", new_node->id, "\n");
                        }

                        dirty          = true;
                        new_set[out++] = new_node;
                        ++new_i;
                }
        }

        if (old_i < old_e) {
                dirty = true;

                do {
                        // partition is no longer replicated to node
                        auto n = p->cluster.replicas.nodes[old_i];

                        if constexpr (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx, " no longer replicated to node ", n->id, "\n");
                        }

                        updates->emplace_back(node_partition_rel_update{.n = n, .p = p, .assign = false});
                } while (++old_i < old_e);
        }

        if (new_i < new_e) {
                dirty = true;

                do {
                        // partition will be replicated to node
                        auto n = nodes[new_i];

                        if constexpr (trace) {
                                SLog("Partition ", p->owner->name(), "/", p->idx, " NOW replicated to node ", n->id, "\n");
                        }

                        updates->emplace_back(node_partition_rel_update{.n = n, .p = p, .assign = true});
                        new_set[out++] = n;
                } while (++new_i < new_e);
        }

        TANK_EXPECT(out <= K_max_replicas);

        if (dirty) {
                // list of nodes this partition is being replicated to has been modified
                std::sort(new_set, new_set + out, [](const auto a, const auto b) noexcept { return a->id < b->id; });

                p->cluster.replicas.nodes.clear();
                p->cluster.replicas.nodes.insert(p->cluster.replicas.nodes.end(), new_set, new_set + out);

                if constexpr (trace) {
                        SLog("Now partition ", p->owner->name(), "/", p->idx, " replicated to ", p->cluster.replicas.nodes.size(), " cluster nodes\n");
                }

                return true;
        }

        return false;
}

static materialized_msg materialize_msg(const lookup_res &lr, const uint64_t requested_seqnum) {
        static IOBuffer  b;
        const auto       span = std::min<size_t>(10 * 1024 * 1024, lr.fileOffsetCeiling - lr.fileOffset);
        materialized_msg res;

        b.clear();
        REQUIRE(lr.fault == lookup_res::Fault::NoFault);
        b.reserve(span);

        const auto r               = pread(lr.fdh->fd, b.data(), span, lr.fileOffset);
        auto       log_base_seqnum = lr.absBaseSeqNum;

        REQUIRE(r == span);

        const auto *p = reinterpret_cast<const uint8_t *>(b.data()), *const e = p + span;
        const auto chunk_end = e;

        while (p < e) {
                const auto                          bundle_len       = Compression::decode_varuint32(p);
                const auto                          bundle_end       = p + bundle_len;
                const auto                          bundle_hdr_flags = decode_pod<uint8_t>(p);
                const auto                          codec            = bundle_hdr_flags & 3;
                const auto                          sparse_bundle    = bundle_hdr_flags & (1u << 6);
                uint32_t                            msgset_size      = (bundle_hdr_flags >> 2) & 0xf ?: Compression::decode_varuint32(p);
                uint64_t                            msgset_end;
                range_base<const uint8_t *, size_t> msgset_content;
                uint64_t                            first_msg_seqnum, last_msg_seqnum;

                if (sparse_bundle) {
                        first_msg_seqnum = decode_pod<uint64_t>(p);

                        if (msgset_size != 1) {
                                last_msg_seqnum = first_msg_seqnum + Compression::decode_varuint32(p) + 1;
                        } else {
                                last_msg_seqnum = first_msg_seqnum;
                        }

                        log_base_seqnum = first_msg_seqnum;
                        msgset_end      = last_msg_seqnum + 1;
                } else {
                        msgset_end = log_base_seqnum + msgset_size;
                }

                if (requested_seqnum >= msgset_end) {
                        p               = bundle_end;
                        log_base_seqnum = msgset_end;

                        continue;
                }

                if (codec) {
                        IMPLEMENT_ME();
                } else {
                        msgset_content.set(p, std::min(std::distance(p, chunk_end), std::distance(p, bundle_end)));
                }

                p = bundle_end;
                uint64_t   ts{0};
                uint32_t   msg_idx{0};
                const auto min_accepted_seqnum = requested_seqnum;

                for (const auto *p = msgset_content.offset, *const msgset_end = p + msgset_content.size();; ++msg_idx, ++log_base_seqnum) {
                        const auto msg_flags = decode_pod<uint8_t>(p);
                        str_view8  key;

                        if (sparse_bundle) {
                                if (msg_flags & unsigned(TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)) {
                                } else if (0 == msg_idx) {
                                        log_base_seqnum = first_msg_seqnum;
                                } else if (msg_idx == msgset_size - 1) {
                                        log_base_seqnum = last_msg_seqnum;
                                } else {
                                        const auto delta = Compression::decode_varuint32(p);

                                        log_base_seqnum += delta;
                                }
                        }

                        const auto msg_abs_seqnum = log_base_seqnum;

                        if (0 == (msg_flags & unsigned(TankFlags::BundleMsgFlags::UseLastSpecifiedTS))) {
                                ts = decode_pod<uint64_t>(p);
                        }

                        if (msg_flags & unsigned(TankFlags::BundleMsgFlags::HaveKey)) {
                                key.set(reinterpret_cast<const char *>(p) + 1, *p);
                                p += sizeof(uint8_t) + key.size();
                        } else {
                                key.reset();
                        }

                        const auto       len = Compression::decode_varuint32(p);
                        const str_view32 content(reinterpret_cast<const char *>(p), len);

                        // SLog("For ", msg_abs_seqnum, " [", key, "] [", content, "]\n");

                        if (msg_abs_seqnum == requested_seqnum) {
                                res.key  = key;
                                res.data = content;
                                res.ts   = ts;

                                return res;
                        }
                }
        }

        return res;
}

TEST_CASE_METHOD(Service, "RW") {
        partition_config      config{.maxSegmentSize = 128 * 1024 * 1024, .maxIndexSize = 128 * 1024 * 1024, .curSegmentMaxAge = 8192 * 1000};
        const str_view8       topic_name("foo");
        auto                  t = Switch::make_sharedref<topic>(str_view8(topic_name.Copy(), topic_name.size()), config);
        Buffer                k, c;
        SwitchFS::deltree_ctx deltree_ctx;
        REQUIRE(t->partitions_);
        REQUIRE(t->partitions_->empty());

        t->cluster.rf_ = 1;

        basePath_.clear();
        basePath_.append("/tmp/TDEBUG/"_s32);
        SwitchFS::DelTree(basePath_.c_str(), &deltree_ctx);
        SwitchFS::BuildPath(Buffer{}.append(basePath_.as_s32(), "/foo/").c_str());

        auto part = init_local_partition(0, t.get(), config);

        REQUIRE(t->partitions_->empty());

        t->register_partition(part.release());
        REQUIRE(t->partitions_->size() == 1);

        auto p = t->partition(0);
        REQUIRE(p != nullptr);

        std::vector<topic_partition *> partitions{p};

        SECTION("ISR") {
                INFO("Becoming Cluster Aware");
                cluster_state._name[0]  = 'X';
                cluster_state._name_len = 1;

                auto n = new cluster_node(1);
                REQUIRE(p->cluster.isr.size() == 0);
                REQUIRE(n->isr.list.size() == 0);
                REQUIRE_FALSE(p->cluster.isr.dirty);
                REQUIRE(!p->cluster.replicas.count(1));

                INFO("Set node as the replica of partition");
                std::vector<cluster_node *>            replicas{n};
                std::vector<node_partition_rel_update> updates;

                REQUIRE(p->cluster.replicas.nodes.empty());
                set_partition_replicas(p, replicas.data(), replicas.size(), &updates);

                REQUIRE(p->cluster.replicas.nodes.size() == 1);
                REQUIRE(p->cluster.replicas.count(1));

                INFO("Bind node with partition's ISR");
                auto isr_e = isr_bind(p, n, __LINE__);
                REQUIRE(p->cluster.isr.size() == 1);
                REQUIRE(n->isr.list.size() == 1);
                REQUIRE(p->cluster.isr.dirty);

                SECTION("isr_dispose") {
                        isr_dispose(isr_e);
                        REQUIRE(p->cluster.isr.size() == 0);
                        REQUIRE(p->cluster.isr.dirty);
                        REQUIRE(n->isr.list.size() == 0);
                }

                SECTION("consider_pending_client_produce_responses") {
                        consider_pending_client_produce_responses(p);
                }

                SECTION("peer_consumed_local_partition") {
                        INFO("Creating a new produce response");
                        auto &q  = p->cluster.pending_client_produce_acks_tracker.pending;
                        auto  pr = get_produce_response();

                        REQUIRE(pr->participants.empty());
                        REQUIRE(pr->client_ctx.connection_ll.empty());
                        REQUIRE(!pr->client_ctx.ch.get());

                        INFO("Registering partition as a participant for the produce response");
                        pr->participants.emplace_back(produce_response::participant{
                            .topic_name = p->owner->name(),
                            .topic      = p->owner,
                            .p          = p,
                            .partition  = p->idx});
                        REQUIRE(pr->participants.size() == 1);
                        pr->deferred.pending_partitions = pr->participants.size();

                        INFO("Registering with q");
                        q.emplace_back(topic_partition::Cluster::PendingClientProduceAcks::pending_ack{
                            .bundle_desc.last_msg_seqnum = 1,
                            .bundle_desc.next.handle     = nullptr,
                            .bundle_desc.next.size       = 0,
                            .isr_nodes_acknowledged_bm   = 0,
                            .required_acks               = 2,
                            .deferred_resp               = pr,
                            .deferred_resp_gen           = pr->gen,
                            .pr_participant_idx          = static_cast<uint8_t>(0)});

                        SECTION("consider_pending_client_produce_responses") {
                                INFO("shouldn't do anything");
                                consider_pending_client_produce_responses(p);
                        }

                        SECTION("consider_pending_client_produce_responses after adjusting required_acks") {
                                INFO("Setting required_acks to 2");
                                REQUIRE_FALSE(q.empty());
                                (*q.begin()).required_acks = 3;

                                INFO("Now that we set required_acks to 2 i.e > isr.size() it consider_pending_client_produce_responses() should do the right thing");
                                consider_pending_client_produce_responses(p);
                        }
                }

                SECTION("cluster-aware") {
                        // become cluster aware
                        cluster_state._name[0]  = 'X';
                        cluster_state._name_len = 1;

                        SECTION("wait") {
                                auto                    c = get_connection();
                                size_t                  i = 1;
                                Buffer                  k, d;
                                std::vector<wait_ctx *> woken_up;

                                k.clear();
                                d.clear();
                                k.append("key:", i);
                                d.append("world of warcraft:", i);

                                c->fd = 1;
                                c->as.tank.reset();
                                REQUIRE(c->as.tank.waitCtxList.size() == 0);

                                SECTION("verify.hwmark_fh == nullptr") {
                                        INFO("Registering Consumer Wait");
                                        register_consumer_wait(TankAPIMsgType::Consume, c,
                                                               1,
                                                               0,
                                                               100,
                                                               partitions.data(), partitions.size());

                                        REQUIRE(p->waiting_list.size() == 1);
                                        REQUIRE(c->as.tank.waitCtxList.size() == 1);

                                        auto wctx = switch_list_entry(wait_ctx, list, c->as.tank.waitCtxList.next);

                                        REQUIRE(wctx->total_partitions == 1);
                                        REQUIRE(wctx->partitions[0].partition == p);
                                        REQUIRE(wctx->capturedSize == 0);
                                        REQUIRE(wctx->c == c);

                                        auto &pctx = wctx->partitions[0];

                                        REQUIRE(pctx.fdh == nullptr);
                                        REQUIRE(pctx.partition == p);
                                        REQUIRE(pctx.range.offset == 0);
                                        REQUIRE(pctx.range.size() == 0);
                                        REQUIRE(pctx.hwmark_threshold != std::numeric_limits<uint64_t>::max());
                                        REQUIRE(pctx.seqNum == 1); // hwmark + 1

                                        INFO("Appending first message");
                                        auto log = partition_log(p);
                                        auto ar  = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        INFO("Will consider_append_res()");
                                        consider_append_res(p, ar, &woken_up);
                                        REQUIRE(woken_up.empty()); // will not wake up because consider_append_res() will ignore because of HWM
                                        REQUIRE(p->waiting_list.size() == 1);

                                        // this will succeed regardless of minBytes
                                        // because it will need to read_from_local()
                                        INFO("Expecting success regardless of minBytes because it will read_from_local()");
                                        woken_up.clear();
                                        consider_highwatermark_update(p, 1, &woken_up);
                                        REQUIRE(woken_up.size() == 1);
                                        REQUIRE(p->waiting_list.size() == 0);
                                }

                                // this is similar to the previous section
                                SECTION("verify.hwmark_fh != cur_fh") {
                                        INFO("PublishingInitial Message");
                                        auto log = partition_log(p);
                                        auto ar  = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        set_hwmark(p, 1);

                                        log->roll(time(nullptr));
                                        // we will roll so the next message past the HWM will be in a different segment

                                        INFO("Registering Consumer Wait");
                                        register_consumer_wait(TankAPIMsgType::Consume, c,
                                                               1,
                                                               0,
                                                               100,
                                                               partitions.data(), partitions.size());

                                        REQUIRE(p->waiting_list.size() == 1);
                                        REQUIRE(c->as.tank.waitCtxList.size() == 1);

                                        auto wctx = switch_list_entry(wait_ctx, list, c->as.tank.waitCtxList.next);

                                        REQUIRE(wctx->total_partitions == 1);
                                        REQUIRE(wctx->partitions[0].partition == p);
                                        REQUIRE(wctx->capturedSize == 0);
                                        REQUIRE(wctx->c == c);

                                        auto &pctx = wctx->partitions[0];

                                        REQUIRE(pctx.fdh == nullptr);
                                        REQUIRE(pctx.partition == p);
                                        REQUIRE(pctx.range.offset == 0);
                                        REQUIRE(pctx.range.size() == 0);
                                        REQUIRE(pctx.hwmark_threshold != std::numeric_limits<uint64_t>::max());
                                        REQUIRE(pctx.seqNum == 2); // hwmark + 1

                                        INFO("Appending another message");
                                        ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        INFO("Considering Append Res");
                                        consider_append_res(p, ar, &woken_up);
                                        REQUIRE(woken_up.empty()); // will not wake up because consider_append_res() will ignore because of HWM
                                        REQUIRE(p->waiting_list.size() == 1);

                                        INFO("Considering Highwater Mark Update. Will not wake up any because hwmark(1) <= hwmark_threshold(1)");
                                        woken_up.clear();
                                        consider_highwatermark_update(p, 1, &woken_up);
                                        REQUIRE(woken_up.empty());
                                        REQUIRE(p->waiting_list.size() == 1);

                                        INFO("Bumping HWMark to 2 and expecting it to work");
                                        set_hwmark(p, 2);
                                        woken_up.clear();
                                        consider_highwatermark_update(p, 2, &woken_up);
                                        REQUIRE(woken_up.size() == 1);
                                        REQUIRE(p->waiting_list.empty());

                                        REQUIRE(pctx.range.offset == 0); // because we rolled and now at offset 0 for HW(2)
                                        REQUIRE(pctx.range.size() == 37);
                                        REQUIRE(pctx.fdh != nullptr);
                                }

                                SECTION("verify.hwmark_fh == cur_fh") {
                                        INFO("Appending first message");
                                        auto log = partition_log(p);
                                        auto ar  = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        INFO("Setting HWMark to 1");
                                        set_hwmark(p, 1);

                                        INFO("Appending Another Message so that capturedRange and range.size() won't be == 0");
                                        ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        INFO("Will register_consumer_wait()");
                                        register_consumer_wait(TankAPIMsgType::Consume, c,
                                                               1,
                                                               0,
                                                               100,
                                                               partitions.data(), partitions.size());

                                        REQUIRE(p->waiting_list.size() == 1);
                                        REQUIRE(c->as.tank.waitCtxList.size() == 1);

                                        auto wctx = switch_list_entry(wait_ctx, list, c->as.tank.waitCtxList.next);

                                        REQUIRE(wctx->total_partitions == 1);
                                        REQUIRE(wctx->partitions[0].partition == p);
                                        REQUIRE(wctx->capturedSize > 0);
                                        REQUIRE(wctx->c == c);

                                        auto &pctx = wctx->partitions[0];

                                        REQUIRE(pctx.fdh);
                                        REQUIRE(pctx.partition == p);
                                        REQUIRE(pctx.range.offset > 0); // because we set HWM to 1
                                        REQUIRE(pctx.range.size() == wctx->capturedSize);
                                        REQUIRE(pctx.seqNum == 2); // because hwmark was 1 before register_consumer_wait()
                                        REQUIRE(pctx.hwmark_threshold != std::numeric_limits<uint64_t>::max());

                                        INFO("Will consider_highwatermark_update() : should not wake up because expected partition hwmark to be > 1");
                                        woken_up.clear();
                                        consider_highwatermark_update(p, 1, &woken_up);
                                        REQUIRE(woken_up.empty());

                                        INFO("Appending another message");
                                        ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        INFO("Considering Append Res(will not wake)");
                                        consider_append_res(p, ar, &woken_up);
                                        REQUIRE(woken_up.empty()); // will not wake up because consider_append_res() will ignore because of HWM
                                        REQUIRE(p->waiting_list.size() == 1);

                                        INFO("Bumping HWM to 2");
                                        set_hwmark(p, 2);

                                        INFO("Will consider_highwatermark_update() : Expecting to fail because not enough content");
                                        woken_up.clear();
                                        consider_highwatermark_update(p, 2, &woken_up);
                                        REQUIRE(woken_up.empty());

                                        SECTION("path 1") {
                                                INFO("Another message for content update reasons. Will extend range but will not wake up because HWM dep.");
                                                ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());
                                                INFO("consider_append_res() for this new message");
                                                woken_up.clear();
                                                consider_append_res(p, ar, &woken_up);

                                                INFO("Will consider_highwatermark_update() : Expecting to work because enough content");
                                                woken_up.clear();
                                                consider_highwatermark_update(p, 2, &woken_up);
                                                REQUIRE(woken_up.size() == 1);
                                        }

                                        SECTION("path 2") {
                                                INFO("Force roll");
                                                log->roll(time(nullptr));

                                                INFO("Another message for content update reasons");
                                                ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());
                                                INFO("consider_append_res() for this new message. We expect to wake up because switched to a new segment");
                                                woken_up.clear();
                                                consider_append_res(p, ar, &woken_up);
                                                REQUIRE(woken_up.size() == 1);
                                                REQUIRE(p->waiting_list.empty());
                                        }
                                }
                        }
                }

                SECTION("NOT-cluster-aware") {
                        cluster_state._name_len = 0;

                        SECTION("wait") {
                                auto                    c = get_connection();
                                size_t                  i = 1;
                                Buffer                  k, d;
                                std::vector<wait_ctx *> woken_up;

                                k.clear();
                                d.clear();
                                k.append("key:", i);
                                d.append("world of warcraft:", i);

                                c->fd = 1;
                                c->as.tank.reset();
                                REQUIRE(c->as.tank.waitCtxList.size() == 0);
				REQUIRE(!cluster_aware());

                                SECTION("verify") {
                                        INFO("register_consumer_wait()");
                                        register_consumer_wait(TankAPIMsgType::Consume, c,
                                                               1,
                                                               0,
                                                               100,
                                                               partitions.data(), partitions.size());

                                        REQUIRE(p->waiting_list.size() == 1);
                                        REQUIRE(c->as.tank.waitCtxList.size() == 1);

                                        auto wctx = switch_list_entry(wait_ctx, list, c->as.tank.waitCtxList.next);

                                        REQUIRE(wctx->total_partitions == 1);
                                        REQUIRE(wctx->partitions[0].partition == p);
                                        REQUIRE(wctx->capturedSize == 0);
                                        REQUIRE(wctx->c == c);

                                        auto &pctx = wctx->partitions[0];

                                        REQUIRE(pctx.fdh == nullptr);
                                        REQUIRE(pctx.range.offset == 0);
                                        REQUIRE(pctx.range.size() == 0);
                                        CHECK(pctx.hwmark_threshold == std::numeric_limits<uint64_t>::max());
                                        REQUIRE(pctx.partition == p);

                                        INFO("consider_highwatermark_update() should ignore this because this is not a cluster_aware() configuration");
                                        consider_highwatermark_update(p, 1, &woken_up);
                                        REQUIRE(woken_up.empty());

                                        INFO("Appending the first message");
                                        auto log = partition_log(p);
                                        auto ar  = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                        INFO("consider_append_res() will not wake up because of minBytes");
                                        consider_append_res(p, ar, &woken_up);
                                        REQUIRE(woken_up.empty());

                                        SECTION("override minbytes and expect wake up") {
                                                wctx->minBytes = 30;
                                                INFO("consider_append_res() will wake up because we updated minBytes");
                                                consider_append_res(p, ar, &woken_up);
                                                REQUIRE(woken_up.size() == 1);
                                                REQUIRE(p->waiting_list.empty());
                                        }

                                        SECTION("Add a few more messages") {
                                                INFO("Appending a new message");
                                                ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                                INFO("consider_append_res() will not wake up anything because capturedSize(74) < minBytes(100)");
                                                consider_append_res(p, ar, &woken_up);
                                                REQUIRE(woken_up.empty());
                                                REQUIRE(p->waiting_list.size() == 1);
                                                REQUIRE(wctx->capturedSize == 74);

                                                INFO("Adding a message");
                                                ar = log->append_msg(time(nullptr), k.as_s8(), d.as_s32());

                                                INFO("consider_append_res() will wake up because capturedSize(111) >= minBytes(100)");
                                                consider_append_res(p, ar, &woken_up);
                                                REQUIRE(woken_up.size() == 1);
                                                REQUIRE(p->waiting_list.empty());
                                        }
                                }
                        }
                }

                SECTION("basics") {
                        auto log = partition_log(p);

                        REQUIRE(log);
                        auto lr = p->read_from_local(false, 0, 100 * 1024);

                        REQUIRE(lr.fault == lookup_res::Fault::AtEOF);
                        lr = p->read_from_local(false, 1, 100 * 1024);
                        REQUIRE(lr.fault == lookup_res::Fault::AtEOF);
                        lr = p->read_from_local(false, 2, 100 * 1024);
                        REQUIRE(lr.fault == lookup_res::Fault::BoundaryCheck);

                        INFO("Becoming Cluster Aware");
                        cluster_state._name[0]  = 'X';
                        cluster_state._name_len = 1;

                        INFO("Appending 512 messages");
                        for (size_t i{1}; i < 512; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto append_res = log->append_msg(time(nullptr), k.as_s8(), c.as_s32());

                                REQUIRE(append_res.fdh.get() != nullptr);

                                auto lr = p->read_from_local(false, i, 1024 * 1024);
                                adjust_range_start(lr, i, nullptr);
                                const auto msg = materialize_msg(lr, i);
                                REQUIRE(msg.key == k.as_s8());
                                REQUIRE(msg.data == c.as_s32());
                        }

                        INFO("Reading a single message");
                        lr = log->read_cur(120, 1024 * 100, 150);
                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                        INFO("Rolling");
                        log->roll(time(nullptr));

                        INFO("Reading the message again");
                        p->read_from_local(false, 120, 1024 * 100);

                        INFO("Appending more messages");
                        for (size_t i{512}; i < 820; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto append_res = log->append_msg(time(nullptr), k.as_s8(), c.as_s32());

                                REQUIRE(append_res.fdh.get() != nullptr);

                                lr = p->read_from_local(false, i, 1024 * 1024);
                                adjust_range_start(lr, i, nullptr);
                                const auto msg = materialize_msg(lr, i);
                                REQUIRE(msg.key == k.as_s8());
                                REQUIRE(msg.data == c.as_s32());
                        }

                        INFO("Rolling");
                        log->roll(time(nullptr));

                        INFO("Appending More Messages");
                        for (size_t i = 820; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto append_res = log->append_msg(time(nullptr), k.as_s8(), c.as_s32());

                                REQUIRE(append_res.fdh.get() != nullptr);

                                lr = p->read_from_local(false, i, 1024 * 1024);
                                adjust_range_start(lr, i, nullptr);
                                const auto msg = materialize_msg(lr, i);
                                REQUIRE(msg.key == k.as_s8());
                                REQUIRE(msg.data == c.as_s32());
                        }

                        INFO("Adjusting HWM to 150");
                        set_hwmark(p, 150);
                        INFO("Verifying read_from_local()");
                        lr = p->read_from_local(true, 120, 1024 * 100);
                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                        INFO("Adjusting HWM to 670");
                        set_hwmark(p, 670);
                        INFO("Verifying read_from_local()");
                        lr = p->read_from_local(true, 625, 1024 * 100);
                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                        adjust_range_start(lr, 625, nullptr);
                        materialize_msg(lr, 625);

#pragma mark read_from_local() with fetch_only_committed = false
                        INFO("read_from_local() with fetch_only_committed == false");
                        for (size_t i{1}; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto lr = p->read_from_local(false, i, 1024 * 1024);
                                adjust_range_start(lr, i, nullptr);

                                const auto msg = materialize_msg(lr, i);
                                REQUIRE(msg.key == k.as_s8());
                                REQUIRE(msg.data == c.as_s32());
                        }

                        std::unordered_map<uint64_t, adjust_range_start_cache_value> cache;

                        INFO("Force into Cache");
                        for (size_t i{1}; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto lr = p->read_from_local(false, i, 1024 * 1024);
                                adjust_range_start(lr, i, &cache);

                                const auto msg = materialize_msg(lr, i);
                                REQUIRE(msg.key == k.as_s8());
                                REQUIRE(msg.data == c.as_s32());
                        }

                        INFO("Verify Cache");
                        for (size_t i{1}; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto lr = p->read_from_local(false, i, 1024 * 1024);
                                adjust_range_start(lr, i, &cache);

                                const auto msg = materialize_msg(lr, i);
                                REQUIRE(msg.key == k.as_s8());
                                REQUIRE(msg.data == c.as_s32());
                        }

#pragma mark read_from_local() with fetch_only_committed = true
                        INFO("read_from_local() with fetch_only_committed == true");
                        const auto hwmark = partition_hwmark(p);

                        for (size_t i{1}; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto lr = p->read_from_local(true, i, 1024 * 1024);

                                if (i <= hwmark) {
                                        if (unlikely(lr.fault != lookup_res::Fault::NoFault)) {
                                                FAIL("Expected success for " << i << " hwmark = " << hwmark);
                                        }

                                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                                        adjust_range_start(lr, i, nullptr);

                                        const auto msg = materialize_msg(lr, i);
                                        REQUIRE(msg.key == k.as_s8());
                                        REQUIRE(msg.data == c.as_s32());
                                } else {
                                        REQUIRE(lr.fault == lookup_res::Fault::PastMax);
                                }
                        }

                        cache.clear();

                        INFO("Force Into Cache");
                        for (size_t i{1}; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto lr = p->read_from_local(true, i, 1024 * 1024);

                                if (i <= hwmark) {
                                        if (unlikely(lr.fault != lookup_res::Fault::NoFault)) {
                                                FAIL("Expected success for " << i << " hwmark = " << hwmark);
                                        }

                                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                                        adjust_range_start(lr, i, &cache);

                                        const auto msg = materialize_msg(lr, i);
                                        REQUIRE(msg.key == k.as_s8());
                                        REQUIRE(msg.data == c.as_s32());
                                } else {
                                        REQUIRE(lr.fault == lookup_res::Fault::PastMax);
                                }
                        }

                        INFO("Verify Cache");
                        for (size_t i{1}; i < 1024; ++i) {
                                k.clear();
                                c.clear();
                                k.append("key:", i);
                                c.append("world of warcraft:", i);

                                auto lr = p->read_from_local(true, i, 1024 * 1024);
                                if (i <= hwmark) {
                                        if (unlikely(lr.fault != lookup_res::Fault::NoFault)) {
                                                FAIL("Expected success for " << i << " hwmark = " << hwmark);
                                        }

                                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                                        adjust_range_start(lr, i, &cache);

                                        const auto msg = materialize_msg(lr, i);
                                        REQUIRE(msg.key == k.as_s8());
                                        REQUIRE(msg.data == c.as_s32());
                                } else {
                                        REQUIRE(lr.fault == lookup_res::Fault::PastMax);
                                }
                        }

                        INFO("Reading from 0 and expecting first message");
                        lr = p->read_from_local(false, 0, 100 * 1024);
                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                        auto msg = materialize_msg(lr, 1);

                        REQUIRE(msg.key == "key:1"_s8);
                        REQUIRE(msg.data == "world of warcraft:1"_s32);

                        INFO("Reading from 1 and expecting first message");
                        lr = p->read_from_local(false, 1, 100 * 1024);
                        REQUIRE(lr.fault == lookup_res::Fault::NoFault);

                        msg = materialize_msg(lr, 1);

                        REQUIRE(msg.key == "key:1"_s8);
                        REQUIRE(msg.data == "world of warcraft:1"_s32);
                }
        }
}

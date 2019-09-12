#include "service_common.h"

// guarding against malformed requests because we really don' want rogue connections to kill TANK
// XXX: the same partition cannot be present in the same produce request
// TODO: we need to guard against that
// This, among other reasons, is because of how and why we use topic_partition::waiting_list, where
// each value is a pair of (wait_ctx, uint8_t), the second being the index in wait_ctx::partitions[]
// so we can't have the same partition more than once there.
bool Service::process_produce(const TankAPIMsgType msg, connection *const c, const uint8_t *p, const size_t len) {
        static constexpr bool trace{false};
	static constexpr bool trace_faults{true};
        if (unlikely(len < sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint8_t))) {
                return shutdown(c, __LINE__);
        }

        const auto *const end = p + len;

        if (unlikely(p + (*p) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint8_t) >= end)) {
                return shutdown(c, __LINE__);
        }

        [[maybe_unused]] const auto ca             = cluster_aware();
        auto                        self           = cluster_state.local_node.ref;
        [[maybe_unused]] const auto client_version = decode_pod<uint16_t>(p);
        const auto                  request_id     = decode_pod<uint32_t>(p);
        p += decode_pod<uint8_t>(p); // skip client id

        if (unlikely(p + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint8_t) > end)) {
                return shutdown(c, __LINE__);
        }

        [[maybe_unused]] const auto         op_required_acks = decode_pod<uint8_t>(p);
        [[maybe_unused]] const auto         ack_timeout      = decode_pod<uint32_t>(p); // TODO:
        const auto                          topics_cnt       = decode_pod<uint8_t>(p);
        auto                                pr               = get_produce_response();
        robin_hood::unordered_map<str_view8, bool> intern_map;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, "NEW produce request ", request_id, ", required_acks = ", op_required_acks, ", ack_timeout = ", ack_timeout, ", topics_cnt = ", topics_cnt, ansifmt::reset, "\n");
        }

        // the response needs to include the topics/partitions in the order they were provided in the request
        for (size_t i{0}; i < topics_cnt; ++i) {
                if (unlikely(p + (*p) + sizeof(uint8_t) + sizeof(uint8_t) > end)) {
                        put_produce_response(pr);
                        return shutdown(c, __LINE__);
                }

                const str_view8 topic_name(reinterpret_cast<const char *>(p) + 1, *p);

                p += topic_name.size() + sizeof(uint8_t);

                auto       topic = topic_by_name(topic_name);
                const auto cnt   = decode_pod<uint8_t>(p);

		if (trace) {
			SLog("For topic [", topic_name, "], ", cnt, " partitions\n");
		}

                if (!topic) {
			const auto res = intern_map.emplace(topic_name, true);

                        if (trace) {
                                SLog("Unknown topic [", topic_name, "]\n");
                        }

			if (res.second) {
				// we need to retain that name
				// so we 'll intern it here
				const_cast<str_view8 *>(&res.first->first)->p = topic_name.Copy();
			}


                        for (size_t i{0}; i < cnt; ++i) {
				const auto partition_id = decode_pod<uint16_t>(p);

                        	pr->participants.emplace_back(produce_response::participant{.topic = nullptr, .topic_name = res.first->first, .partition = partition_id });

                                if (unlikely(p > end)) {
                                        put_produce_response(pr);
                                        return shutdown(c, __LINE__);
                                }

                                if (unlikely(!Compression::check_decode_varuint32(p, end))) {
                                        put_produce_response(pr);
                                        return shutdown(c, __LINE__);
                                }

                                const auto bundle_len = Compression::decode_varuint32(p);

                                if (msg == TankAPIMsgType::ProduceWithSeqnum) {
                                        p += sizeof(uint64_t);
                                }

                                p += bundle_len;

                                if (unlikely(p > end)) {
                                        put_produce_response(pr);
                                        return shutdown(c, __LINE__);
                                }
                        }
                        continue;
                }

                if (unlikely(p + cnt * sizeof(uint16_t) > end)) {
                        put_produce_response(pr);
                        return shutdown(c, __LINE__);
                }

                for (size_t i{0}; i < cnt; ++i) {
                        const auto partition_id = decode_pod<uint16_t>(p);

                        if (unlikely(!Compression::check_decode_varuint32(p, end))) {
                                put_produce_response(pr);
                                return shutdown(c, __LINE__);
                        }

                        const auto                                bundle_len = Compression::decode_varuint32(p);
                        auto                                      partition = topic->enabled_partition(partition_id);
                        uint64_t                                  first_msg_seq_num;

                        if (trace) {
                                SLog("For partition ", partition_id, ", bundle_len = ", bundle_len, ", partition = ", ptr_repr(partition), "\n");
                        }

                        if (msg == TankAPIMsgType::ProduceWithSeqnum) {
				// first message base sequence number is explicitly provided
				// it is *not* part of the bundle.
				// it is provided for convenience, so that we won't have to decode the first bundle, and its message set
				// (which could have required decompressing it) to determine that.
                                if (unlikely(p + sizeof(uint64_t) > end)) {
                                        put_produce_response(pr);
                                        return shutdown(c, __LINE__);
                                }

                                first_msg_seq_num = decode_pod<uint64_t>(p);

				if (trace) {
					SLog("first_msg_seq_num = ", first_msg_seq_num, "\n");
				}
                        } else {
                                first_msg_seq_num = 0;
                        }

                        const range_base<const uint8_t *, size_t> bundle{p, bundle_len};

                        p += bundle_len;

                        if (unlikely(p > end)) {
				if (trace) {
					SLog(ansifmt::color_red, ansifmt::inverse, "Partial Bundle!?", ansifmt::reset, "\n");
				}

                                put_produce_response(pr);
                                return shutdown(c, __LINE__);
                        }

                        pr->participants.emplace_back(produce_response::participant{
                            .topic                    = topic,
                            .topic_name               = topic->name(),
                            .partition                = partition_id,
                            .p                        = partition,
                            .update.bundle            = bundle,
                            .update.first_msg_seq_num = first_msg_seq_num});
                }
        }

        // link to the client connection
        pr->client_ctx.ch.set(c);
        pr->client_ctx.req_id = request_id;
        c->as.tank.produce_responses_list.push_back(&pr->client_ctx.connection_ll);

	if (trace) {
		SLog(pr->participants.size(), " participants\n");
	}

        // TODO: should we verify the  participants, and if we have any issues with any of them, abort it, or only
        // process participants that are valid and ignore others?
        for (size_t pi = 0; pi < pr->participants.size(); ++pi) {
                auto &it = pr->participants[pi];

                if (read_only) {
                        it.res = produce_response::participant::OpRes::ReadOnly;
                        continue;
                }

                if (!it.topic) {
                        it.res = produce_response::participant::OpRes::UnknownTopic;
                        continue;
                }

                auto partition = it.p;

                if (!partition) {
                        it.res = produce_response::participant::OpRes::UnknownPartition;
                        continue;
                }

                auto topic = partition->owner;
		uint8_t required_acks;

                if (ca) {
                        required_acks = topic->compute_required_peers_acks(partition, op_required_acks);

			if (trace) {
				SLog("required_acks = ", required_acks, " from compute_required_peers_acks(", op_required_acks, ")\n");
			}

                        const auto total_required_acks = required_acks + 1; // +1 for this node

                        if (required_acks > 200 /* XXX: */) {
                                if (trace || trace_faults) {
                                        SLog("Cannot service request, required_acks = ", required_acks, "\n");
                                }

                                it.res = produce_response::participant::OpRes::InsufficientReplicas;
                                continue;
                        } else if (total_required_acks > topic->cluster.rf_) {
                                // See comments in DEFERRED_REPAIRS
                                if (trace || trace_faults) {
                                        SLog("Cannot service request, (total_required_acks=", total_required_acks, ") > (topic.cluster.rf = ",
                                             topic->cluster.rf_, "), isr size = ", partition->cluster.isr.size(),
                                             ", replicas = ", partition->cluster.replicas.nodes.size(), "\n");
                                }

                                it.res = produce_response::participant::OpRes::InsufficientReplicas;
                                continue;
                        } else if (const auto isr_size = partition->cluster.isr.size(); total_required_acks > isr_size) {
                                if (trace || trace_faults) {
                                        SLog("Cannot service request, total_required_acks = ", total_required_acks, ", isr_size = ", isr_size, "\n");
                                }

                                it.res = produce_response::participant::OpRes::InsufficientReplicas;
                                continue;
                        }

                        const auto leader = partition->cluster.leader.node;

                        if (!leader) {
                                if (trace) {
                                        SLog("NO leader\n");
                                }

                                it.res = produce_response::participant::OpRes::NoLeader;
                                continue;
                        }

                        if (leader != self) {
                                if (trace) {
                                        SLog("Different leader\n");
                                }

                                it.res = produce_response::participant::OpRes::OtherLeader;
                                continue;
                        }
                } else {
                        required_acks = 0;
                }

                const auto                  bundle                = it.update.bundle;
                const auto *                p                     = bundle.offset;
                [[maybe_unused]] const auto e                     = p + bundle.size();
                const auto                  bundle_flags          = decode_pod<uint8_t>(p);
                const auto                  sparse_bundle_bit_set = bundle_flags & (1u << 6);
                const uint32_t              msg_set_size          = ((bundle_flags >> 2) & 0xf) ?: Compression::decode_varuint32(p);
                auto                        first_msg_seq_num     = it.update.first_msg_seq_num;
                uint64_t                    last_msg_seq_num;

                if (sparse_bundle_bit_set) {
                        first_msg_seq_num = decode_pod<uint64_t>(p);

                        if (msg_set_size != 1) {
                                last_msg_seq_num = first_msg_seq_num + Compression::decode_varuint32(p) + 1;
                        } else {
                                last_msg_seq_num = first_msg_seq_num;
                        }

                } else {
                        last_msg_seq_num = 0;
                }


                auto log = partition_log(partition);

                if (!log) {
                        if (trace) {
                                SLog("partition.log is NA\n");
                        }

                        it.res = produce_response::participant::OpRes::IO_Fault;
                        continue;
                }

                if (last_msg_seq_num) {
                        if (unlikely(last_msg_seq_num < first_msg_seq_num)) {
                                if (trace) {
                                        SLog("Invalid seqnum\n");
                                }

                                it.res = produce_response::participant::OpRes::InvalidSeqNums;
                                continue;
                        } else if (first_msg_seq_num <= partition_hwmark(partition)) {
                                if (trace) {
                                        SLog("Invalid seqnum\n");
                                }

                                it.res = produce_response::participant::OpRes::InvalidSeqNums;
                                continue;
                        }
                } else if (first_msg_seq_num && first_msg_seq_num <= partition_hwmark(partition)) {
                        if (trace) {
                                SLog("Invalid seqnum\n");
                        }

                        it.res = produce_response::participant::OpRes::InvalidSeqNums;
                        continue;
                }


                auto       res                     = log->append_bundle(curTime, bundle.offset, bundle.size(), msg_set_size, first_msg_seq_num, last_msg_seq_num);
                const auto bundle_last_msg_seq_num = res.msgSeqNumRange.offset + res.msgSeqNumRange.size() - 1;

                if (!res.fdh) {
                        if (trace) {
                                SLog("append_bundle() failed\n");
                        }

                        if (res.dataRange.size() == std::numeric_limits<uint32_t>::max()) {
                                it.res = produce_response::participant::OpRes::InvalidSeqNums;
                        } else {
                                it.res = produce_response::participant::OpRes::IO_Fault;
                        }
                        continue;
                }

                if (!ca) {
                        // obviously
			if (trace) {
				SLog("Not cluster-aware, comminitting now\n");
			}

                        set_hwmark(partition, bundle_last_msg_seq_num);
                        goto wakeup_any;
                } else if (required_acks == 0) {
			// it is important that we use the priority queue and consider_append_res() here
			// otherwise we can trivially violate the ordering invariant
			//
			// XXX: UPDATE: this is *not* enough
			// we need to first invoke consider_append_res() so that
			// capturedSize and range are updated, and then
			// insert into pending_client_produce_acks_tracker() followed by a consider_append_res() 
			// UPDATE: if HWM_UPDATE_BASED_ON_ACKS is not defined, we instead need to set_hwmark() and consider_highwatermark_update()
			//
			// TODO: is there a way to combine those for performance?
			if (trace) {
				SLog("No peer acks required,will ack and consider immediately {res.dataRange =", res.dataRange, ", res.msgSeqNumRange = ", res.msgSeqNumRange, "}\n");
			}

			// first use consider_append_res() which will likely update capturedSize and range
                        now_awake.clear();
                        consider_append_res(partition, res, &now_awake);

			if (trace) {
				SLog("To awake ", now_awake.size(), "\n");
			}

                        for (auto ctx : now_awake) {
                                wakeup_wait_ctx(ctx, c);
                        }
                        now_awake.clear();

			// and then update HWMark
#ifdef HWM_UPDATE_BASED_ON_ACKS
			// we need to respect the complex semantics here so we first
			// need to push it into the PQ and then consider_acknowledged_product_reqs()
                        partition->cluster.pending_client_produce_acks_tracker.acknowledged.push(topic_partition::Cluster::pending_ack_bundle_desc{
                            .last_msg_seqnum = bundle_last_msg_seq_num,
                            .next.handle     = log->cur.fdh.get(),
                            .next.size       = log->cur.fileSize});

                        consider_acknowledged_product_reqs(partition);
#else
			if (topic->cluster.rf_ == 1) {
				// only the leader is to store this bundle
                        	set_hwmark(partition, bundle_last_msg_seq_num);

				if (trace) {
					SLog("Now bumped HWM\n");
				}

				consider_highwatermark_update(partition, bundle_last_msg_seq_num);
			}
#endif
                } else {
                wakeup_any:
                        // we may end up waking up any consumers(replicas, or clients)
			if (trace) {
				SLog("Will collect consume requests to wake up {res.dataRange = ", res.dataRange, ", res.msgSeqNumRange = ", res.msgSeqNumRange, "}\n");
			}

                        now_awake.clear();
                        consider_append_res(partition, res, &now_awake);

			if (trace) {
				SLog("Will wake up ", now_awake.size(), "\n");
			}

                        for (auto ctx : now_awake) {
                                wakeup_wait_ctx(ctx, c);
                        }
                        now_awake.clear();
                }

                if (trace) {
                        SLog("res.msgSeqNumRange = ", res.msgSeqNumRange,
                             ", required_acks = ", required_acks,
                             ", cluster{replicas = ", partition->cluster.replicas.nodes.size(),
			     ", rf = ", topic->cluster.rf_,
                             ", ISR = ", partition->cluster.isr.size(), "}}\n");

                        for (const auto it : partition->cluster.isr.list) {
                                const auto isr_e = containerof(isr_entry, partition_ll, it);
                                const auto node  = isr_e->node();

                                EXPECT(isr_e->partition() == partition);

                                SLog(">>In ISR ", node->id, "@", node->ep, "\n");
                        }
                }

                it.res = produce_response::participant::OpRes::OK;

                topic->metrics.bytes_in += bundle.size();
                topic->metrics.msgs_in += msg_set_size;

                if (required_acks == 0) {
                        it.res = produce_response::participant::OpRes::OK;
                        continue;
                }

                if (pr->deferred.expiration.ll.empty()) {
			// link this (D)PR for expiration
			// if no ack. time out has been explicitly provided, we default to 8s
                        pr->deferred.expiration.ts = now_ms + (ack_timeout ?: 8 * 1000); 

                        if (deferred_produce_responses_expiration_list.empty()) {
                                deferred_produce_responses_next_expiration = pr->deferred.expiration.ts;
                        }

                        deferred_produce_responses_expiration_list.push_back(&pr->deferred.expiration.ll);
                }

                // associate the DPR with this partition
                pr->deferred.pending_partitions++;

                // wait for ack. for this (partition, bundle_last_msg_seq_num)
                auto &q = partition->cluster.pending_client_produce_acks_tracker.pending;

                q.emplace_back(topic_partition::Cluster::PendingClientProduceAcks::pending_ack{
                    .bundle_desc.last_msg_seqnum = bundle_last_msg_seq_num,
                    .bundle_desc.next.handle     = log->cur.fdh.get(),
                    .bundle_desc.next.size       = log->cur.fileSize,
#ifdef HWM_UPDATE_BASED_ON_ACKS
                    .isr_nodes_acknowledged_bm   = 0,
#endif
                    .required_acks               = required_acks,
                    .deferred_resp               = pr,
                    .deferred_resp_gen           = pr->gen,
                    .pr_participant_idx          = static_cast<uint8_t>(pi)});

                it.res = produce_response::participant::OpRes::Pending;
        }

        if (pr->deferred.expiration.ll.empty()) {
                // can respond immediately, not a DPR
                if (trace) {
                        SLog("Can respond now\n");
                }

                return try_generate_produce_response(pr);
        } else {
                // will respond eventually, it's now a deferred pending response(DPR)
                if (trace) {
                        SLog("Will defer response\n");
                }

                return true;
        }
}

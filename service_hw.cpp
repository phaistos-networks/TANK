#include "service_common.h"

void Service::set_hwmark(topic_partition *const __restrict__ p, const uint64_t seqnum) {
	enum {
		trace = false,
	};
        TANK_EXPECT(p);

        p->highwater_mark.seq_num = seqnum;

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "Updating hwmark for ", p->owner->name(), "/", p->idx, " to ", seqnum, ansifmt::reset, "\n");
        }

        if (cluster_aware()) {
                // also track the current segment and file offset
                if (auto l = p->_log.get()) {
			if (seqnum >  l->lastAssignedSeqNum) [[unlikely]] {
				// This can happen if TANK is misconfigured, where for example
				// a replica(not the leader) of a partition has content (e.g for topic/0 
				// but thep partition leader of topic/0 doesnt).
                                Print(ansifmt::bold, ansifmt::color_red, "Attempted to set hwmark to ", seqnum,
                                      ", whereas, lastAssignedSeqNum = ", l->lastAssignedSeqNum, ansifmt::reset, "\n");
                                std::abort();
			}

                        assert(seqnum <= l->lastAssignedSeqNum);

                        p->highwater_mark.file.handle = l->cur.fdh;
                        p->highwater_mark.file.size   = l->cur.fileSize;

                } else {
                        p->highwater_mark.file.handle.reset();
                        p->highwater_mark.file.size = 0;
                }
        }
}

void Service::set_hwmark(topic_partition *p, const uint64_t seqnum, fd_handle *fh, const uint32_t file_size) {
        static constexpr bool trace{false};
        TANK_EXPECT(p);

        if (trace) {
                SLog("Updating hwmark of ", p->owner->name(), "/", p->idx, " to ", seqnum, "\n");
        }

        p->highwater_mark.seq_num = seqnum;

        if (cluster_aware()) {
                p->highwater_mark.file.handle.reset(fh);
                p->highwater_mark.file.size = file_size;
        }
}

uint64_t topic_partition::hwmark() const noexcept {
        return highwater_mark.seq_num;
}

uint64_t Service::partition_hwmark(topic_partition *const p) TANK_NOEXCEPT_IF_NORUNTIME_CHECKS {
        TANK_EXPECT(p);

        if (cluster_aware()) {
                // if we get a PRODUCE message, where
                // the last message in the bundle's message set would be 100(seq.number)
                // and among our 4 nodes in ISR, 3 have
                // confirmed that they persisted the update(by issuing a CONSUME for seq_num > 100)
                // wheras the other haven't done so yet (and we haven't timed it out of the ISR)
                // then the highwater mark shouldn't be 100, it should be whatever sequence number
                // is common among all nodes in the ISR
                return p->hwmark();
        }

        return partition_log(p)->lastAssignedSeqNum;
}

void Service::update_hwmark(topic_partition *p, const topic_partition::Cluster::pending_ack_bundle_desc bd) {
	enum{
	trace = false,
	};
        TANK_EXPECT(p);
        const auto                  hwmark = bd.last_msg_seqnum;
        [[maybe_unused]] const auto before = p->hwmark();
        TANK_EXPECT(hwmark);
        TANK_EXPECT(hwmark >= before);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, ansifmt::bgcolor_brown, "Updating HWMark for ", p->owner->name(), "/", p->idx,
                     " from ", before, " to ", hwmark, ansifmt::reset, "\n");
        }

        // XXX:
        // what if bd.handle is no longer valid?
        // pending_ack_bundle_desc doesn't retain the file handle
        // maybe we should check if that's partition_log(p)->cur.fdh.get()
        // do something else?
        if (bd.next.handle != partition_log(p)->cur.fdh.get()) {
                IMPLEMENT_ME();
        }

        set_hwmark(p, hwmark, bd.next.handle, bd.next.size);
        consider_highwatermark_update(p, hwmark);
}

void Service::update_hwmark(topic_partition *p, const uint64_t hwmark) {
	enum {
	trace =false,
	};
        TANK_EXPECT(p);
        [[maybe_unused]] const auto before = p->hwmark();
        TANK_EXPECT(hwmark);
        TANK_EXPECT(hwmark >= before);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, ansifmt::bgcolor_gray, "Updating HWMark for ", p->owner->name(), "/", p->idx,
                     " from ", before, " to ", hwmark, ansifmt::reset, "\n");
        }

        set_hwmark(p, hwmark);

        if (trace) {
                SLog("Did set_hwmark(), will consider_highwatermark_update()\n");
        }

        consider_highwatermark_update(p, hwmark);
}

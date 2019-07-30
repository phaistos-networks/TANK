#include "service_common.h"
#include <base64.h>

/*

+---------------+-------------------------------------+---------------------------------------------------------------------------------+
| NAMESPACE     | CONTENTS                            | PURPOSE                                                                         |
+---------------+-------------------------------------+---------------------------------------------------------------------------------+
| topology/     | <topic_name>/<partition>            | Value holds the list of replicas.                                 		|
|               |                                     | This represents the current cluster topology.                                   |
|               |                                     | Whenever a new topic or partition is created, or othrs are deleted, the        |
|               |                                     | changes are applied in this this namespace                                      |
+---------------+-------------------------------------+---------------------------------------------------------------------------------+
| leaders/      | CLUSTER or <topic_name>/<partition> | We care for Session, and Flags                                                  |
|               |                                     | Flags holds the node id that's the leader for that the partition(or, the        |
|               |                                     | whole cluster leader).                                                          |
|               |                                     | If Session is valid, then that replica is indeed alive and leader for that key. |
|               |                                     | Otherwise, we can safely disregard that information, and try to acquire         |
|               |                                     | leadership(i.e lock with a session) of whatever is appropriate, or if we are    |
|               |                                     | the cluster's leader, redistribute partition replicas and assign leaders.       |
+---------------+-------------------------------------+---------------------------------------------------------------------------------+
| nodes/        | replicaIDs                          | We care for both the Value and the Session.                                     |
|               |                                     | The Value should be the endpoint of that node, and if                           |
|               |                                     | Session is present and valid, it means there is an exclusive owner              |
|               |                                     | who has locked that key -- i.e that node is alive and has reserved that         |
|               |                                     | node id. This is very important.                                                |
+---------------+-------------------------------------+---------------------------------------------------------------------------------+
| ISR/          | <topic_name>/partition              | Each key's value is a a list of u16 replica IDs that make up the ISR            |
|               |                                     | for that partition.                                                             |
+---------------+-------------------------------------+---------------------------------------------------------------------------------+
| conf-updates/ | .cluster OR <topic_name>            | Whenever a topic or cluster's configuration is updated, we use                  |
|               |                                     | a transaction to update two keys. We "touch" a conf/updates                     |
|               |                                     | key, and we update the contents in configs/                                     |
|               |                                     |                                                                                 |
|               |                                     | We care for ModifyIndex of whatever keys in conf-updates. Whenever              |
|               |                                     | anything is touched(even if the value is not updated), its                      |
|               |                                     | ModifyIndex is incremented, and because when anything is updated                |
|               |                                     | in conf-updates/ we get to read all of those (monitored, recursively)           |
|               |                                     | we can check against ModifyIndex for actual updates                             |
+---------------+-------------------------------------+---------------------------------------------------------------------------------+

Important Consul Semantics:
- When a key is acquired, if you monitor it (itself, or its prefix) via long-polling, as soon as
	the session is released, then the long-polling request will be provided with
	a response, and there will now be no Session for that key. This is very handy and important.
	The ModifyIndex for that key will _also_ be incremented/updated.
- You can "touch" keys -- update them without changing the value or with value being reset to ""
- You should use a Key's Flags. This can be set to an arbitrary int64 value, so you can use it
	it for e.g revision updates, etc.
- 
*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Bootstrap process is tricky, we need to do this properly
// 1. Fetch (topology, leaders, ISR, configuration, Nodes) as bootstrap updates (but do not apply_cluster_state_updates() when we
//	retrieve the KVs for any of those namespaces)
// 2. As soon as we have retrieved all configurations, we try to CreateSession
// 3. When we retrieve the successfull response for CreateSession, we schedule an AcquireNodeID which
//	will acquire the nodes/ID key but without any value(i.e the local endpoint won't be set). This is important
// 4. After we have acquired the node id (w/o having set the endpoint) and cluster_node_id_acquired() we
// 	consider the registration to be complete and we force_cluster_updates_apply() which will
// 	make sure apply_cluster_state_updates() will be invoked ASAP
// 5. Once apply_cluster_state_updates() processed all the collected boostrap state updates and invokes conclude_bootstrap_updates()
//	we then acquire our node ID again but this time we will set the endpoint as the value so that other nodes will
//	know that we are ready and available
// 6. After that response to the AcquireNodeID request has been processed, only then we can check if we know
//	there is a cluster leader and if not, attempt to become leaders outselves.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool Service::register_with_cluster() {
        TANK_EXPECT(cluster_aware());
        TANK_EXPECT(consul_state.srv.state != ConsulState::Srv::State::Available);

        // it is important that we register this node with cluster_state.all_nodes
        // because when we get back responses during registration e.g leadersip or topology
        // we will need access to (cluster_state.local_node.ref)
        auto n = cluster_state.local_node.ref = cluster_state.find_or_create_node(cluster_state.local_node.id);

        n->available_ = true;
        n->ep         = tank_listen_ep;

        // We need to reset here in case
        // this has been invoked after the initial bootstrap for any reason
        consul_state.flags &= ~(unsigned(ConsulState::Flags::ConfirmedSession) |
                                unsigned(ConsulState::Flags::RegistrationComplete) |
                                unsigned(ConsulState::Flags::StateRetrieved) |
                                unsigned(ConsulState::Flags::ConfigsRetrieved) |
                                unsigned(ConsulState::Flags::BootstrapStateUpdatesProcessed));

        consul_state.ack_cnt     = 0;
        cluster_state.registered = false;

        // just in case
        cancel_timer(&consul_state.renew_timer.node);

        // important: we need to explicitly set cluster_state.updates.cluster_leader here
        // so that apply_cluster_state_updates() wilkl get to consider cluster leadership even if /leaders is missing
        cluster_state.updates.cluster_leader.defined = false;
        cluster_state.updates.nodes.clear();
        cluster_state.updates.pm.clear();
        cluster_state.updates.pm.clear();
        cluster_state.updates.a.reuse();

        // important: need to be in all_available_nodes
        cluster_state.all_available_nodes.emplace_back(n);

        SLog(ansifmt::bold, ansifmt::inverse, ansifmt::color_red, "CLUSTER: registration", ansifmt::reset, "\n");

        // we don't know yet
        consul_state.srv.state = ConsulState::Srv::State::Unknown;

        // we would like to know how long it will take to register
        consul_state.reg_init_ts = now_ms;

        // start by initializing the namespace
        // chain those together, they will complete in a few ms
        auto main_req = consul_state.get_req(consul_request::Type::InitNS_Topology);
        auto req      = (main_req->then = consul_state.get_req(consul_request::Type::InitNS_Leaders));

        req = (req->then = consul_state.get_req(consul_request::Type::InitNS_ISR));
        req = (req->then = consul_state.get_req(consul_request::Type::InitNS_ConfUpdates));
        req = (req->then = consul_state.get_req(consul_request::Type::InitNS_Nodes));
        schedule_consul_req(main_req, true);

        return true;
}

void Service::enqueue_consul_req(consul_request *const req) {
	static constexpr bool trace{false};
        TANK_EXPECT(req);
        TANK_EXPECT(!req->is_released());

        // need to make sure OverHandled is unset
        req->flags &= ~unsigned(consul_request::Flags::OverHandled);

#ifdef TANK_RUNTIME_CHECKS
        for (auto it = req->then; it; it = it->then) {
                TANK_EXPECT(!it->is_released());
                TANK_EXPECT(0 == (it->flags & unsigned(consul_request::Flags::OverHandled)));
        }
#endif


        req->next                       = consul_state.deferred_reqs_head;
        consul_state.deferred_reqs_head = req;

        if (trace) {
                size_t n{0};

                for (auto it = consul_state.deferred_reqs_head; it; it = it->next) {
                        ++n;
                }

                SLog(ansifmt::bold, ansifmt::color_green, ansifmt::inverse, "ENQUEUED consul request", ansifmt::reset, " (", n, ")\n");
        }
}

bool Service::schedule_consul_req(consul_request *req, const bool urgent) {
        // we are going to defer them for the next reactor loop iteration
        // because we may want to schedule a consul request in e.g process_ready_consul_resp()
        // and if we only have that single connection to consul, we 'd need to create another, but by deferring them
        // process_ready_consul_resp() would have made the connection idle, and thus when we get to schedule_consul_req_impl() we 'd
        // make use of that now idle conection
        TANK_EXPECT(req);
        TANK_EXPECT(!req->is_released());

        for (auto it = req->then; it; it = it->then) {
                TANK_EXPECT(!it->is_released());
        }

        if (urgent) {
                req->flags |= unsigned(consul_request::Flags::Urgent) | unsigned(consul_request::Flags::Deferred);
        } else {
                req->flags |= unsigned(consul_request::Flags::Deferred);
        }

        enqueue_consul_req(req);
        return true;
}

bool Service::schedule_consul_req_impl(consul_request *req, const bool urgent) {
        static constexpr bool trace{false};

        TANK_EXPECT(req);
        TANK_EXPECT(!req->is_released());

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_cyan, "Will attempt to schedule a new consul request urgent=", urgent, ansifmt::reset, "\n");
        }

        if (!consul_state.idle_conns.empty()) {
                // Great, we can reuse an existing connection
                auto c = switch_list_entry(connection, as.consul.conns_ll, consul_state.idle_conns.prev);

                c->verify();

                if (trace) {
                        SLog("Can reuse connection\n");
                }

                TANK_EXPECT(c->is_consul());
                TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Idle);

                // will not c->as.consul.conns_ll.detach_and_reset() here
                // bind_consul_req() will invoke initiate_consul_req() which will invoke make_active()
                // and that will remove it from consul_state.idle_conns
                return bind_consul_req(req, c);
        }

        if (!urgent) {
                // consider all active consul connections
                // and look for one that's not blocked waiting for a response
                // if there is one such connection, we can will enqueue the request to be processed
                // as soon any of those connections become idle again, and return
                if (trace) {
                        SLog("Not urgent, looking for active consul connections\n");
                }

                for (auto it = consul_state.active_conns.prev; it != &consul_state.active_conns; it = it->next) {
                        auto c = switch_list_entry(connection, as.consul.conns_ll, it);

                        c->verify();
                        TANK_EXPECT(c->is_consul());
                        TANK_EXPECT(c->state.classification() == connection::ClassificationTracker::Type::Active);

                        auto other_req = c->as.consul.cur.req;

                        TANK_EXPECT(other_req);
                        TANK_EXPECT(!other_req->is_released());

                        if (false == other_req->will_long_poll()) {
                                // OK, there is a connection that will likely become idle soon so we
                                // will schedule this for when that happens
                                consul_state.pending_reqs.push_back(req);
                                return true;
                        }
                }

                if (trace) {
                        SLog("Cannot rely on active consul connections\n");
                }
        }

        if (trace) {
                SLog("Will create a new consul connection\n");
        }

        // we need to create a new connection afterall
        auto c = acq_new_consul_connection();

        if (!c) {
		if (trace) {
			SLog("*FAILED* to acq_new_consul_connection()\n");
		}

                return false;
        }

        return bind_consul_req(req, c);
}

void Service::disable_consul_srv() {
        // we can't access the consul service
        // so we have no idea if we are still the servers -- we basically don't know anything
        //
        // we 'll stop accepting connections, and we 'll terminate all idle tank connections
        // and we 'll try again in a while the whole register_with_cluster() dance
        static constexpr bool trace{false};

        if (trace) {
                SLog("Disabling consul agent interface\n");
        }

        TANK_EXPECT(!consul_state.renew_timer.is_linked());

        cancel_timer(&consul_state.renew_timer.node);

        disable_tank_srv();

        // just in case
        disable_listener();

        while (!consul_state.pending_reqs.empty()) {
                auto req = consul_state.pending_reqs.front();

                consul_state.pending_reqs.pop_front();
                for (auto r = req->then; r;) {
                        auto then = r->then;

                        consul_req_over(r);
                        consul_state.put_req(r, __LINE__);
                        r = then;
                }

                consul_req_over(req);
                consul_state.put_req(req, __LINE__);
        }

        // get rid of all pending and deferred requests
        while (auto req = consul_state.deferred_reqs_head) {
                auto next = req->next;

                for (auto r = req->then; r;) {
                        auto then = r->then;

                        consul_req_over(r);
                        consul_state.put_req(r, __LINE__);
                        r = then;
                }

                consul_req_over(req);
                consul_state.put_req(req, __LINE__);
                consul_state.deferred_reqs_head = next;
        }

        cluster_state.registered = false;

        // we 'll try again in a few seconds
        TANK_EXPECT(!consul_state.srv.retry_reg_timer.is_linked());

        consul_state.srv.retry_reg_timer.node.key = now_ms + 4 * 1000;
        register_timer(&consul_state.srv.retry_reg_timer.node);
}

void Service::try_reschedule_one_consul_req() {
        if (!consul_state.pending_reqs.empty()) {
                auto req = consul_state.pending_reqs.front();

                consul_state.pending_reqs.pop_front();
                enqueue_consul_req(req);
        }
}

bool Service::handle_consul_flush(connection *c) {
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        auto req = c->as.consul.cur.req;

        TANK_EXPECT(req);

        c->as.consul.state = connection::As::Consul::State::WaitRespFirstBytes;
        if (req->will_long_poll()) {
                make_long_polling(c);
        }

        return true;
}

void Service::session_released() {
        static constexpr bool trace{false};

        if (trace) {
                SLog("## Session released\n");
        }

        consul_state._session_id_len = 0;
        unlink(Buffer{}.append(basePath_, "/.cluster_session_"_s32, cluster_state.local_node.id).c_str());

        if (reactor_state == ReactorState::ReleaseSess) {
                if (all_conns_idle()) {
                        reactor_state = ReactorState::Idle;
                } else {
                        reactor_state = ReactorState::WaitAllConnsIdle;
                }
        }
}

void Service::schedule_consul_session_renewal() {
	static constexpr bool trace{false};

	if (trace) {
		SLog("Attempting to schedule session renewal linked:", consul_state.renew_timer.is_linked(), "\n");
	}

        if (!consul_state.renew_timer.is_linked()) {
                consul_state.renew_timer.node.key = now_ms + 4 * 1000;
                register_timer(&consul_state.renew_timer.node);
        }
}

void Service::request_cluster_config() {
        static constexpr bool trace{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, ansifmt::inverse, "REQUESTING CLUSTER CONF", ansifmt::reset, "\n");
        }

        schedule_consul_req(consul_state.get_req(consul_request::Type::RetrieveClusterConfig), true);
}

void Service::request_topic_config(topic *t) {
        static constexpr bool trace{false};
        auto                  req = consul_state.get_req(consul_request::Type::RetrieveTopicConfig);

        if (trace) {
                SLog(ansifmt::bold, ansifmt::color_red, ansifmt::inverse, "REQUESTING TOPIC ", t->name(), " CONF", ansifmt::reset, "\n");
        }

        req->tref = t;
        schedule_consul_req(req, false);
}

void Service::consul_ns_retrieval_complete() {
        // now fetch _all_ configurations before we can
        // renew or create a session
        SLog(ansifmt::bold, ansifmt::color_red, ansifmt::bgcolor_green, "CLUSTER: NS retrieval complete", ansifmt::reset, "\n");

        consul_state.flags |= unsigned(ConsulState::Flags::StateRetrieved);

        if (consul_state.flags & unsigned(ConsulState::Flags::LastRegFailed)) {
                consul_state.flags &= ~unsigned(ConsulState::Flags::LastRegFailed);

                Print(ansifmt::color_red, "Connection and registration with consul agent succeeded", ansifmt::reset, "\n");
        }

        schedule_consul_req(consul_state.get_req(consul_request::Type::RetrieveConfigs), true);
}

void Service::process_consul_configs(const std::vector<std::pair<str_view8, std::pair<str_view32, uint64_t>>> &v) {
        static constexpr bool trace{false};

        for (const auto &it : v) {
                base64_dec_buffer.clear();

                if (Base64::Decode(reinterpret_cast<const uint8_t *>(it.second.first.data()), it.second.first.size(), &base64_dec_buffer) < 0) {
                        continue;
                }

                const auto content = base64_dec_buffer.as_s32();

                if (it.first.Eq(_S(".cluster"))) {
                        process_cluster_config(content, it.second.second);
                } else {
                        process_topic_config(it.first, content, it.second.second);
                }
        }

        if (!(consul_state.flags & unsigned(ConsulState::Flags::ConfigsRetrieved))) {
                consul_state.flags |= unsigned(ConsulState::Flags::ConfigsRetrieved);

                if (trace) {
                        SLog(ansifmt::bold, ansifmt::color_red, ansifmt::bgcolor_green, "CLUSTER: configs retrieval complete", ansifmt::reset, "\n");
                }
        }
}

void Service::consul_sess_confirmed() {
        // persist the session(it's OK if this fails)
        static constexpr const bool trace{false};

        if (trace) {
                SLog(ansifmt::bold, ansifmt::inverse, ansifmt::color_red, "CLUSTER: session confirmed", ansifmt::reset, "\n");
        }

        if (int fd = open(Buffer{}.append(basePath_, "/.cluster_session_"_s32, cluster_state.local_node.id).c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG); - 1 == fd) {
                Print("Failed to persist cluster session:", strerror(errno), "\n");
        } else if (write(fd, consul_state._session_id, consul_state._session_id_len) != consul_state._session_id_len) {
                Print("Failed to persist cluster session:", strerror(errno), "\n");
                close(fd);
        } else {
                if (trace) {
                        SLog("** Persisted Session\n");
                }

                close(fd);
        }

        // we now need to register ourselves in /nodes
        // we need the session id because we need exclusive ownership of that id
        // we don't want another node share that ID with us.
        schedule_consul_req(consul_state.get_req(consul_request::Type::AcquireNodeID), true);
}

void Service::monitor_consul_namespaces() {
        // It's important that we begin monitoring the various namespaces _before_ we
        // acquire a node id, because before we acquire the ID, we need to enable_listener()
        // and processing the topology is expensive, so we need to do this before we join the cluster
        //
        // Chain them; ORDER IS IMPORTANT
        // 1. Nodes 		: reconcile_cluster_nodes()
        // 2. Topology 		: reconcile_cluster_topology()
        // 3. ISRs 		: reconcile_cluster_ISR()
        // 4. Leadership 	: reconcile_leadership()
        // 5. Configuration 	: consider_updated_consul_configs()
        // see service_cluster_reconcile.cpp
        auto main_req = consul_state.get_req(consul_request::Type::MonitorNodes);
        auto p        = (main_req->then = consul_state.get_req(consul_request::Type::MonitorTopology));

        p = (p->then = consul_state.get_req(consul_request::Type::MonitorISRs));
        p = (p->then = consul_state.get_req(consul_request::Type::MonitorLeaders));
        // 1. MonitorConfUpdatesNoApply because we need ModifyIndex, but won't really go fetch anything just update the gen for each topic or cluster_state.local_conf.last_update_gen
        // 	- Once we get the request schedulke a MonitorConfUpdates. Whenever we get responses from that, we will fetch updated keys from configs/
        // 2. RetrieveConfigs will fetch all keys from configs/.  We didn't want to fetch each key inddividually from the original MonitorConfUpdatesNoApply
        p = (p->then = consul_state.get_req(consul_request::Type::MonitorConfUpdatesNoApply));

        consul_state.ack_cnt = 0; // Reset it -- increments by each successful monitor response
        schedule_consul_req(main_req, true);
}

bool Service::initiate_consul_req(connection *c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        auto req = c->as.consul.cur.req;

        TANK_EXPECT(req);
        TANK_EXPECT(req->flags & unsigned(consul_request::Flags::Assigned));

        auto *const q       = c->outQ ?: (c->outQ = get_outgoing_queue());
        auto        payload = get_data_vector_payload();

#define ACCEPT_ENCODING "Accept-Encoding: gzip\r\n"
//#define ACCEPT_ENCODING 

	if (trace) {
		SLog("initiate_consul_req() for ", unsigned(req->type), "\n");
		SLog("q.front_ = ", ptr_repr(q->front_), ", q.back_ = ", ptr_repr(q->back_), "\n");
	}

	q->verify();

        switch (req->type) {
                case consul_request::Type::CreateSession: {
                        // TODO: specify health id
                        // we need to rely on more than just TTLs, we need to specify a health check in consul
                        // which will be used to check nodes more frequently
                        auto b = (payload->buf = get_buf());

                        TANK_EXPECT(b->empty());
                        // https://www.consul.io/api/session.html#release
                        // neither `release` nor `delete` Behavior seems to do what we need
                        // for now, sticking with release
                        b->append(R"stanza({"LockDelay": "2s", "TTL": "10s", "Name": ")stanza"_s32);
                        b->append("tank_session_"_s32, Timings::Microseconds::Tick(), "_",
                                  static_cast<unsigned>(getpid()));
                        b->append(R"stanza(", "Behavior": "release"})stanza"_s32);

                        payload->append("PUT /v1/session/create HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: "_s32);
                        payload->append(str_view32(payload->small_buf, sprintf(payload->small_buf, "%zu", static_cast<std::size_t>(b->size()))));
                        payload->append("\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                        payload->append(b->as_s32());
                } break;

                case consul_request::Type::InitNS_Topology: {
                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/topology/?cas=0 HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::InitNS_Nodes: {
                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/nodes/?cas=0 HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::InitNS_Leaders: {
                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/leaders/?cas=0 HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::InitNS_ConfUpdates: {
                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/conf-updates/?cas=0 HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::AcquireNodeID: {
                        auto       b        = (payload->buf = get_buf());
                        const auto repr_len = sprintf(payload->small_buf, "%u", cluster_state.local_node.id);
                        auto       sb       = payload->small_buf + repr_len;

                        if (consul_state.flags & unsigned(ConsulState::Flags::BootstrapStateUpdatesProcessed)) {
                                // initially, we 'll acquire the ID but we will *not* set our endpoint
                                // so that other nodes will not rush to replicate from us
                                //
                                // we will acquire the node ID again once we have processed the bootstrap updates
                                b->append(tank_listen_ep);
                        }

                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/nodes/"_s32);
                        payload->append(str_view32(payload->small_buf, repr_len));
                        payload->append("?acquire="_s32);
                        TANK_EXPECT(consul_state.session_id().size() == 36);
                        payload->append(consul_state.session_id().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: "_s32);
                        payload->append(str_view32(sb, sprintf(sb, "%zu", static_cast<std::size_t>(b->size()))));
                        payload->append("\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                        payload->append(b->as_s32());
                } break;

                // XXX:
                // releasing here results in resetting the value to empty
                // because we are not providing any content
                // there's probably a way to release the lock without affecting the content
                case consul_request::Type::ReleaseNodeLock: {
                        const auto repr_len = sprintf(payload->small_buf, "%u", cluster_state.local_node.id);

                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/nodes/"_s32);
                        payload->append(str_view32(payload->small_buf, repr_len));
                        payload->append("?release="_s32);
                        TANK_EXPECT(consul_state.session_id().size() == 36);
                        payload->append(consul_state.session_id().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0"_s32);
                        payload->append("\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                // see comemnts for ReleaseNodeLock
                case consul_request::Type::ReleaseClusterLeadership: {
                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/leaders/CLUSTER?release="_s32);
                        TANK_EXPECT(consul_state.session_id().size() == 36);
                        payload->append(consul_state.session_id().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\n"_s32);
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::CreatePartititons: {
                        auto             b = (payload->buf = get_buf());
                        const str_view32 topic_name(req->new_partitions.topic_name.data, req->new_partitions.topic_name.size);

                        b->append('[');
                        for (size_t i = req->new_partitions.first_partition_index, e = i + req->new_partitions.partitions_cnt; i < e; ++i) {
                                b->append(R"json({"KV": {"Verb": "set", "Key": ")json"_s32);
                                b->append(cluster_state.tank_ns, "/clusters/"_s32, cluster_state.name(), "/topology/"_s32, topic_name, '/', i);
                                b->append(R"json(", "Value": ")json"_s32);
                                // no value initially
                                // once this new key is created, the cluster leader will pick up the updates and
                                // will assign a leader and replicas to it
                                b->append(R"json(", "Session" : ")json"_s32);
                                b->append(consul_state.session_id());
                                b->append(R"json("}},)json"_s32);
                        }

                        if (b->back() == ',') {
                                b->pop_back();
                        }
                        b->append(']');

                        const auto len = sprintf(payload->small_buf, "%u", b->size());

                        payload->append("PUT /v1/txn HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: "_s32);
                        payload->append(str_view32(payload->small_buf, len));
                        payload->append("\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                        payload->append(b->as_s32());
                } break;

                case consul_request::Type::TryBecomeClusterLeader: {
                        const auto _len = sprintf(payload->small_buf, "%u", cluster_state.local_node.id);

                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/leaders/CLUSTER?acquire="_s32);
                        TANK_EXPECT(consul_state.session_id().size() == 36);
                        payload->append(consul_state.session_id().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: "_s32);
                        payload->append(str_view32(payload->small_buf + _len, sprintf(payload->small_buf + _len, "%u", _len)));
                        payload->append("\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                        payload->append(str_view32(payload->small_buf, _len));

#if 0
			{
				IOBuffer b;

				for (size_t i = 0; i < payload->iov_cnt; ++i) {
					b.append(str_view32(reinterpret_cast<const char *>(payload->iov[i].iov_base), payload->iov[i].iov_len));
				}

				SLog(b.as_s32(), "\n");
			}
#endif
                } break;

                case consul_request::Type::PartitionsTX: {
                        const auto len = sprintf(payload->small_buf, "%u", req->tx_repr->size());

                        payload->append("PUT /v1/txn HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: "_s32);
                        payload->append(str_view32(payload->small_buf, len));
                        payload->append("\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                        payload->append(req->tx_repr->as_s32());
                } break;

                case consul_request::Type::InitNS_ISR: {
                        payload->append("PUT /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/ISR/?cas=0 HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::DestroySession: {
                        payload->append("PUT /v1/session/destroy/"_s32);
                        payload->append(consul_state.session_id().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        payload->append("User-Agent: TANK\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::RenewSession: {
                        payload->append("PUT /v1/session/renew/"_s32);
                        payload->append(consul_state.session_id().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        payload->append("User-Agent: TANK\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append(ACCEPT_ENCODING "Content-Length: 0\r\nConnection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::MonitorTopology: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/topology/?recurse=true"_s32);
                        if (consul_state.topology_monitor_modify_index) {
                                payload->append("&index="_s32);
                                payload->append(str_view32(payload->small_buf,
                                                           sprintf(payload->small_buf, "%" PRIu64, consul_state.topology_monitor_modify_index)));
                        }
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::MonitorNodes: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/nodes/?recurse=true"_s32);
                        if (consul_state.nodes_monitor_modify_index) {
                                payload->append("&index="_s32);
                                payload->append(str_view32(payload->small_buf,
                                                           sprintf(payload->small_buf, "%" PRIu64, consul_state.nodes_monitor_modify_index)));
                        }
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::MonitorLeaders: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/leaders/?recurse=true"_s32);
                        if (consul_state.leaders_monitor_modify_index) {
                                payload->append("&index="_s32);
                                payload->append(str_view32(payload->small_buf,
                                                           sprintf(payload->small_buf, "%" PRIu64, consul_state.leaders_monitor_modify_index)));
                        }
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::RetrieveClusterConfig: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/configs/.cluster"_s32);
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::RetrieveConfigs: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/configs/?recurse=true"_s32);
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);

#if 0
                        {
                                Buffer b;

                                for (unsigned i = 0; i < payload->iov_cnt; ++i) {
                                        b.append(str_view32(static_cast<const char *>(payload->iov[i].iov_base), payload->iov[i].iov_len));
                                }

                                b.Print();
                        }
#endif

                } break;

                case consul_request::Type::RetrieveTopicConfig: {
                        TANK_EXPECT(req->tref);

                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/configs/"_s32);
                        payload->append(req->tref->name().as_s32());
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::MonitorConfUpdates:
                        [[fallthrough]];
                case consul_request::Type::MonitorConfUpdatesNoApply: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/conf-updates/?recurse=true"_s32);
                        if (req->type == consul_request::Type::MonitorConfUpdates && consul_state.conf_updates_monitor_modify_index) {
                                payload->append("&index="_s32);
                                payload->append(str_view32(payload->small_buf,
                                                           sprintf(payload->small_buf, "%" PRIu64, consul_state.conf_updates_monitor_modify_index)));
                        }
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;

                case consul_request::Type::MonitorISRs: {
                        payload->append("GET /v1/kv/"_s32);
                        payload->append(cluster_state.tank_ns);
                        payload->append("/clusters/"_s32);
                        payload->append(cluster_state.name());
                        payload->append("/ISR/?recurse=true"_s32);
                        if (consul_state.isrs_monitor_modify_index) {
                                payload->append("&index="_s32);
                                payload->append(str_view32(payload->small_buf,
                                                           sprintf(payload->small_buf, "%" PRIu64, consul_state.isrs_monitor_modify_index)));
                        }
                        payload->append(" HTTP/1.1\r\n"_s32);
                        if (const auto token = consul_state.token()) {
                                payload->append("X-Consul-Token: "_s32);
                                payload->append(token.as_s32());
                                payload->append("\r\n"_s32);
                        }
                        payload->append("Connection: keep-alive\r\nHost: "_s32);
                        payload->append(consul_state.srv.hn);
                        payload->append("\r\n\r\n"_s32);
                } break;
        }

        q->push_back(payload);
	TANK_EXPECT(!q->empty());

        c->as.consul.state = connection::As::Consul::State::TxReq;

        make_active(c);
		
	if (trace) {
		SLog("Payload ready, connection requires availability:", c->state.flags & (1u << uint8_t(connection::State::Flags::NeedOutAvail)), ", oq.size() = ", q->size(), "\n");
	}

        return try_tx(c);
}

void Service::set_consul_conn_cur_req(connection *const c, consul_request *req) {
        TANK_EXPECT(req);
        TANK_EXPECT(c);

        c->verify();

        TANK_EXPECT(c->is_consul());
        TANK_EXPECT(c->as.consul.cur.req == nullptr);
        TANK_EXPECT(!req->is_released());

        req->flags |= unsigned(consul_request::Flags::Assigned);
        c->as.consul.cur.req = req;
}

bool Service::bind_consul_req(consul_request *req, connection *const c) {
	static constexpr bool trace{false};

        c->verify();

        set_consul_conn_cur_req(c, req);

        switch (c->as.consul.state) {
                case connection::As::Consul::State::Connecting:
                        // we 'll try as soon as we can
			if (trace) {
				SLog("Will try as soon as we can (now connecting)\n");
			}
                        return true;

                case connection::As::Consul::State::Idle:
			if (trace) {
				SLog("Can initiate_consul_req() now (idle)\n");
			}

                        return initiate_consul_req(c);

                default:
                        // unexpected
                        std::abort();
        }
}

bool Service::consider_idle_consul_connection(connection *const c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());

        c->verify();
        c->as.consul.state = connection::As::Consul::State::Idle;

        if (trace) {
                SLog("cur.req = ", ptr_repr(c->as.consul.cur.req), " pending_reqs.empty() = ", consul_state.pending_reqs.empty(), "\n");
        }

        if (c->as.consul.cur.req) {
                // likely set in complete_consul_req()
                if (trace) {
                        SLog("Connection has request, we can initiate_consul_req()\n");
                }

                return initiate_consul_req(c);
        } else if (false == consul_state.pending_reqs.empty()) {
                auto req = consul_state.pending_reqs.front();

                consul_state.pending_reqs.pop_front();

                if (trace) {
                        SLog("Pending request pending_reqs.empty = ", consul_state.pending_reqs.empty(), "s\n");
                }
                return bind_consul_req(req, c);
        } else {
                if (trace) {
                        SLog("Will make idle\n");
                }

                make_idle(c, __LINE__);
                return true;
        }
}

bool Service::process_ready_consul_resp(connection *const c, IOBuffer *b) {
        // c->fd can be -1 if it has been invoked in cleanup_connection()
        // see connection::As::Consul::Flags::WaitShutdown
	static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        auto &consul = c->as.consul;
        TANK_EXPECT(consul.state == connection::As::Consul::State::ReadContent || consul.state == connection::As::Consul::State::ReadCompressedContent);
        auto &resp = consul.cur.resp;

	if (trace) {
		SLog("**READY** consul response\n");
	}

        cancel_timer(&consul.attached_timer.node);

        if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::Draining)) {
		if (trace) {
			SLog("Was draining: nothing to process\n");
		}
        } else {
                TANK_EXPECT(b);
                const auto content = b->as_s32().SuffixFrom(b->offset());

		if (trace) {
			SLog("To process ", resp.rc, " ", size_repr(content.size()), "\n");
		}

                process_ready_consul_resp_impl(c, content);
        }

        tear_down_consul_resp(c);
        return complete_consul_req(c);
}

// invoked as soon as we read in a consule's response headers
bool Service::consider_consul_resp_headers(connection *const c) {
        static constexpr bool trace{false};
        TANK_EXPECT(c);
        TANK_EXPECT(c->is_consul());
        auto &resp = c->as.consul.cur.resp;

        if (trace) {
                SLog(ansifmt::color_brown, "Got response (rem bytes:", resp.remaining_content_bytes,
                     ", keep_alive = ", bool(resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::KeptAlive)),
                     ", gzip encoded = ", bool(resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding)),
                     ", chunks transfers = ", bool(resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer)),
                     " for req ", unsigned(c->as.consul.cur.req->type), ansifmt::reset, "\n");
        }

        if (resp.flags & unsigned(connection::As::Consul::Cur::Response::Flags::GZIP_Encoding)) {
                c->as.consul.state = connection::As::Consul::State::ReadCompressedContent;
        } else {
                c->as.consul.state = connection::As::Consul::State::ReadContent;
        }

        // we could have drained i.e { resp.flags |= unsigned(connection::As::Consul::Cur::Response::Flags::Draining); }
        // if e.g (resp.rc > 400) but we really want both the rc and the content

        if ((resp.flags & (unsigned(connection::As::Consul::Cur::Response::Flags::KeptAlive) | unsigned(connection::As::Consul::Cur::Response::Flags::ChunksTransfer))) ||
            resp.remaining_content_bytes) {
                // we either know how many bytes we need to read, or it's a keep alive
                // and we expect to ready no content because (0 == remaining_content_bytes)
                switch (c->as.consul.state) {
                        case connection::As::Consul::State::ReadContent:
                                return recv_consul_resp_content(c);

                        case connection::As::Consul::State::ReadCompressedContent:
                                return recv_consul_resp_content_gzip(c);

                        default:
                                std::abort();
                }
        } else {
                // can't really do anything other than wait until the peer
                // shuts down the connection on its end (HTTP/1.0 or Connection: close response)
                c->as.consul.flags |= unsigned(connection::As::Consul::Flags::WaitShutdown);
                return true;
        }
}

// Generates 0+ more consul requests in order to apply replicas set and partitions leaders updates
// order is imortant
consul_request *Service::schedule_topology_update_tx(const std::vector<NodesPartitionsUpdates::reduced_rs> &          reduced,
                                                     const std::vector<NodesPartitionsUpdates::expanded_rs> &         expanded,
                                                     const std::vector<NodesPartitionsUpdates::leadership_promotion> &promotions,
                                                     topic_partition **dirty_isr_list, const size_t dirty_isr_list_size) {
        [[maybe_unused]] static constexpr bool trace{false};
        TANK_EXPECT(cluster_aware());

        if (reduced.empty() && expanded.empty() && promotions.empty() && 0 == dirty_isr_list) {
                return nullptr;
        }

        if (trace) {
                SLog(ansifmt::bold, ansifmt::bgcolor_green, "UPDATE:", ansifmt::reset, "\n");

                for (const auto &it : reduced) {
                        SLog("\t\t", ansifmt::color_green, "Reduced RS of ", it.p->owner->name(), "/", it.p->idx, " from ", it.p->cluster.replicas.nodes.size(), " to ", it.new_size, ansifmt::reset, "\n");
                }

                for (const auto &it : expanded) {
                        SLog("\t\t", ansifmt::color_green, "Expanded RS of ", it.p->owner->name(), "/", it.p->idx, " from ", it.original_size, " to ", it.p->cluster.replicas.nodes.size(), ansifmt::reset, "\n");
                }

                for (const auto &it : promotions) {
                        SLog("\t\t", ansifmt::color_green, "Set leader of ", it.p->owner->name(), "/", it.p->idx, " to ", it.n ? it.n->id : 0, ansifmt::reset, "\n");
                }

                for (size_t i{0}; i < dirty_isr_list_size; ++i) {
                        const auto p = dirty_isr_list[i];

                        SLog("\t\tPersist ISR of ", p->owner->name(), "/", p->idx, " ", p->cluster.isr.size(), "\n");
                }
        }

        // https://www.consul.io/api/txn.html
        // no more than 64 ops can be included in a single transaction
        // we may need multiple transactions
        std::vector<std::unique_ptr<IOBuffer>> reprs;
        static constexpr const size_t          tx_max_ops{64};
        size_t                                 n{tx_max_ops};
        char                                   node_repr[64];

        // expanded replicas set
        for (const auto it : expanded) {
                auto p = it.p;

                if (n == tx_max_ops) {
                        auto b = new IOBuffer();

                        b->append("[\n"_s32);
                        reprs.emplace_back(b);
                        n = 0;
                }
                ++n;

                auto b = reprs.back().get();

                base64_dec_buffer.clear();
                for (const auto n : p->cluster.replicas.nodes) {
                        base64_dec_buffer.append(n->id, ',');
                }
                if (!base64_dec_buffer.empty()) {
                        base64_dec_buffer.pop_back();
                }

                b->append(R"json({"KV": {"Verb": "set", "Key": ")json"_s32);
                b->append(cluster_state.tank_ns, "/clusters/"_s32, cluster_state.name(), "/topology/"_s32, p->owner->name(), '/', p->idx);
                b->append(R"json(", "Value": ")json"_s32);
                Base64::Encode(reinterpret_cast<const uint8_t *>(base64_dec_buffer.data()), base64_dec_buffer.size(), b);
                b->append(R"json(", "Session" : ")json"_s32);
                b->append(consul_state.session_id());
                b->append(R"json("}},)json"_s32);

                p->cluster.replicas.nodes.resize(it.original_size);
        }

        // reduced replicas set
        for (const auto it : reduced) {
                auto p = it.p;

                if (n == tx_max_ops) {
                        auto b = new IOBuffer();

                        b->append("[\n"_s32);
                        reprs.emplace_back(b);
                        n = 0;
                }
                ++n;

                auto b = reprs.back().get();

                base64_dec_buffer.clear();
                for (size_t i{0}; i < it.new_size; ++i) {
                        const auto n = p->cluster.replicas.nodes[i];

                        base64_dec_buffer.append(n->id, ',');
                }
                if (!base64_dec_buffer.empty()) {
                        base64_dec_buffer.pop_back();
                }

                b->append(R"json({"KV": {"Verb": "set", "Key": ")json"_s32);
                b->append(cluster_state.tank_ns, "/clusters/"_s32, cluster_state.name(), "/topology/"_s32, p->owner->name(), '/', p->idx);
                b->append(R"json(", "Value": ")json"_s32);
                Base64::Encode(reinterpret_cast<const uint8_t *>(base64_dec_buffer.data()), base64_dec_buffer.size(), b);
                b->append(R"json(", "Session" : ")json"_s32);
                b->append(consul_state.session_id());
                b->append(R"json("}},)json"_s32);
        }

        // partitions leaders promotions and demotions
        for (const auto it : promotions) {
                auto       p   = it.p;
                const auto id  = it.n ? it.n->id : 0;
                const auto len = sprintf(node_repr, "%u", id);

                if (n == tx_max_ops) {
                        auto b = new IOBuffer();

                        b->append("[\n"_s32);
                        reprs.emplace_back(b);
                        n = 0;
                }
                ++n;

                auto b = reprs.back().get();

                b->append(R"json({"KV": {"Verb": "set", "Key": ")json"_s32);
                b->append(cluster_state.tank_ns, "/clusters/"_s32, cluster_state.name(), "/leaders/"_s32, p->owner->name(), '/', p->idx);
                b->append(R"json(", "Value": ")json"_s32);
                Base64::Encode(reinterpret_cast<const uint8_t *>(node_repr), len, b);
                b->append(R"json(", "Session" : ")json"_s32);
                b->append(consul_state.session_id());
                b->append(R"json("}},)json"_s32);
        }

        // partitions ISRs updates
        for (size_t i{0}; i < dirty_isr_list_size; ++i) {
                auto p = dirty_isr_list[i];

                if (n == tx_max_ops) {
                        auto b = new IOBuffer();

                        b->append("[\n"_s32);
                        reprs.emplace_back(b);
                        n = 0;
                }
                ++n;

                auto b = reprs.back().get();

                b->append(R"json({"KV": {"Verb": "set", "Key": ")json"_s32);
                b->append(cluster_state.tank_ns, "/clusters/"_s32, cluster_state.name(), "/ISR/"_s32, p->owner->name(), '/', p->idx);
                b->append(R"json(", "Value": ")json"_s32);

                base64_dec_buffer.clear();
                for (const auto it : p->cluster.isr.list) {
                        const auto node = switch_list_entry(isr_entry, partition_ll, it)->node();

                        base64_dec_buffer.append(node->id, ',');
                }
                if (!base64_dec_buffer.empty()) {
                        base64_dec_buffer.pop_back();
                }

                Base64::Encode(reinterpret_cast<const uint8_t *>(base64_dec_buffer.data()), base64_dec_buffer.size(), b);
                b->append(R"json(", "Session" : ")json"_s32);
                b->append(consul_state.session_id());
                b->append(R"json("}},)json"_s32);

                p->cluster.flags &= ~unsigned(topic_partition::Cluster::Flags::ISRDirty);
        }

        // generate the request now(likely one will suffice)
        consul_request *last{nullptr}, *first{nullptr};

        for (auto &it : reprs) {
                it->pop_back();
                it->append(']');

                if (trace) {
                        it->Print();
                }

                auto req = consul_state.get_req(consul_request::Type::PartitionsTX);

                TANK_EXPECT(req->then == nullptr);

                req->tx_repr = it.release();
                if (last) {
                        last->then = req;
                } else {
                        first = req;
                }
                last = req;
        }

        // XXX: what happens if any of those requests fail for any reason?
        if (first) {
                // sanity check
                size_t n{0};

                for (auto it = first; it; it = it->then) {
                        ++n;
                }
                TANK_EXPECT(n == reprs.size());
        }

        return first;
}

// invoked when the request content has been retrieved (_not_ after the response
// has been processed) or when we are draining the service and we are releasing the request without processing
void Service::consul_req_over(consul_request *req) {
        TANK_EXPECT(req);

        if (0 == (req->flags & unsigned(consul_request::Flags::OverHandled))) {
                req->flags ^= unsigned(consul_request::Flags::OverHandled);

                if (req->type == consul_request::Type::TryBecomeClusterLeader) {
                        consul_state.flags &= ~unsigned(ConsulState::Flags::AttemptBecomeClusterLeader);
                }
        }
}

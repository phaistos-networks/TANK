#include "client_common.h"

#define _BUFFERS_POOL_THRESHOLD 32
// if you are under ridiculous memory pressure, you may want to enable EXPECT_MEMORY_PRESSURE
//#define EXPECT_MEMORY_PRESSURE 1

IOBuffer *TankClient::get_buffer() {
        ++rsrc_tracker.buffer;

        if (!reusable_buffers.empty()) {
                auto b = reusable_buffers.back().release();

                reusable_buffers.pop_back();
		b->clear();
                return b;
        } else {
                return new IOBuffer();
        }
}

void TankClient::put_buffer(IOBuffer *const b) {
        --rsrc_tracker.buffer;

#ifdef EXPECT_MEMORY_PRESSURE
        delete b;
#else
        TANK_EXPECT(b);

	if (b->Reserved() > 16 * 1024 * 1024) {
		delete b;
	} else if (reusable_buffers.size() > _BUFFERS_POOL_THRESHOLD) {
                delete b;
        } else {
                b->clear();
                reusable_buffers.emplace_back(std::move(b));
        }
#endif
}

TankClient::managed_buf *TankClient::get_managed_buffer() {
        ++rsrc_tracker.managed_buf;

        if (!reusable_managed_buffers.empty()) {
                auto b = reusable_managed_buffers.back().release();

                reusable_managed_buffers.pop_back();
                b->clear();
                b->rc = 1;
                return b;
        } else {
                return new managed_buf();
        }
}

void TankClient::put_managed_buffer(managed_buf *b) {
        TANK_EXPECT(b);
        TANK_EXPECT(rsrc_tracker.managed_buf);
        --rsrc_tracker.managed_buf;

        reusable_managed_buffers.emplace_back(std::move(b));
}

TankClient::msgs_bucket *TankClient::get_msgs_bucket() {
        auto b = static_cast<msgs_bucket *>(malloc(sizeof(msgs_bucket)));

        TANK_EXPECT(b);
        b->next = nullptr;

        return b;
}

void TankClient::put_msgs_bucket(msgs_bucket *b) {
        std::free(b);
}

std::unique_ptr<TankClient::api_request> TankClient::get_api_request(const uint64_t expiration) {
        std::unique_ptr<api_request> req;

        ++rsrc_tracker.api_request;

        // it is important that we always update
        // now_ms before any method that in turn calls get_api_request()
        // may hace been invoked outside the reactor loop, and we need now_ms to be correct
        now_ms = Timings::Milliseconds::Tick();

        if (not reusable_api_requests.empty()) {
                req = std::move(reusable_api_requests.back());
                reusable_api_requests.pop_back();
        } else {
                req.reset(new api_request());
        }

        req->reset();
        req->api_reqs_expirations_tree_node.key = expiration ? now_ms + expiration : 0;

#ifdef TANK_RUNTIME_CHECKS
        req->init_ms = now_ms;
#endif

        return req;
}

void TankClient::put_api_request(std::unique_ptr<api_request> req) {
        TANK_EXPECT(req);
        TANK_EXPECT(req->managed_bufs.empty());

        TANK_EXPECT(rsrc_tracker.api_request);
        --rsrc_tracker.api_request;

#ifdef EXPECT_MEMORY_PRESSURE
        // let it go
#else
        reusable_api_requests.emplace_back(std::move(req));
#endif
}

TankClient::broker_api_request *TankClient::get_broker_api_request() {
        broker_api_request *req;

        ++rsrc_tracker.broker_api_request;

        if (!reusable_broker_api_requests.empty()) {
                req = reusable_broker_api_requests.back();
                reusable_broker_api_requests.pop_back();
        } else {
#ifdef EXPECT_MEMORY_PRESSURE
                req = static_cast<broker_api_request *>(malloc(sizeof(broker_api_request)));
#else
                req = static_cast<broker_api_request *>(reqs_allocator.Alloc(sizeof(broker_api_request)));
#endif
        }
        req->reset();
        return req;
}

void TankClient::put_broker_api_request(broker_api_request *req) {
        TANK_EXPECT(req);
        TANK_EXPECT(rsrc_tracker.broker_api_request);
        --rsrc_tracker.broker_api_request;

#ifdef EXPECT_MEMORY_PRESSURE
        std::free(req);
#else
        reusable_broker_api_requests.emplace_back(req);
#endif
}

TankClient::request_partition_ctx *TankClient::get_request_partition_ctx() {
        request_partition_ctx *ctx;

        ++rsrc_tracker.request_partition_ctx;

        if (not reusable_request_partition_contexts.empty()) {
                ctx = reusable_request_partition_contexts.back();
                reusable_request_partition_contexts.pop_back();
        } else {
#ifdef EXPECT_MEMORY_PRESSURE
                ctx = static_cast<request_partition_ctx *>(malloc(sizeof(request_partition_ctx)));
#else
                ctx = static_cast<request_partition_ctx *>(reqs_allocator.Alloc(sizeof(request_partition_ctx)));
#endif
        }

        ctx->reset();
        return ctx;
}

void TankClient::put_request_partition_ctx(request_partition_ctx *ctx) {
        TANK_EXPECT(ctx);
        TANK_EXPECT(rsrc_tracker.request_partition_ctx);
        --rsrc_tracker.request_partition_ctx;

#ifdef EXPECT_MEMORY_PRESSURE
        std::free(ctx);
#else
        reusable_request_partition_contexts.emplace_back(ctx);
#endif
}

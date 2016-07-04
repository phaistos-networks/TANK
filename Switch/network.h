#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

static inline constexpr uint32_t _ntohl(const uint32_t orig)
{
        return ({
                const auto b1 = (orig >> 24) & 255;
                const auto b2 = (orig >> 16) & 255;
                const auto b3 = (orig >> 8) & 255;
                const auto b4 = (orig & 255);

                (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
        });
}
static inline constexpr uint32_t IP4Addr(const uint8_t a, const uint8_t b, const uint8_t c, const uint8_t d)
{
        return _ntohl((a << 24) | (b << 16) | (c << 8) | d);
}

namespace Switch
{
        struct endpoint
        {
                uint32_t addr4;
                uint16_t port;

                void unset()
                {
                        addr4 = INADDR_NONE; // 255.255.255.255
                        port = 0;
                }

                inline operator bool() const
                {
                        return addr4 != INADDR_NONE && port;
                }

                void set(const uint32_t a, const uint16_t p)
                {
                        addr4 = a;
                        port = p;
                }

                auto operator==(const endpoint &o) const
                {
                        return addr4 == o.addr4 && port == o.port;
                }

                auto operator!=(const endpoint &o) const
                {
                        return addr4 != o.addr4 || port != o.port;
                }

                operator uint32_t() const
                {
                        return addr4;
                }

                auto operator<(const endpoint &o) const
                {
                        return addr4 < o.addr4 || (addr4 == o.addr4 && port < o.port);
                }
        };

        inline int SetTCPCork(int fd, const int v)
        {
#ifdef __linux__
                return setsockopt(fd, IPPROTO_TCP, TCP_CORK, &v, sizeof(v));
#else
                return setsockopt(fd, IPPROTO_TCP, TCP_NOPUSH, &v, sizeof(v));
#endif
        }

        inline int SetReuseAddr(int fd, const int v)
        {
                return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(v));
        }

        inline uint32_t ParseHostAddress(const strwlen8_t repr, bool &succ)
        {
                const char *p = repr.p, *const end = repr.End();
                uint32_t res, octet;
                uint8_t *const octets = (uint8_t *)&res;

                for (uint32_t i{0};; ++i, ++p)
                {
                        if (p == end || !isdigit(*p))
                        {
                                succ = false;
                                return 0;
                        }

                        for (octet = *p++ - '0'; p != end && isdigit(*p); ++p)
                                octet = octet * 10 + (*p - '0');

                        if (unlikely(octet > 255))
                        {
                                succ = false;
                                return 0;
                        }

                        octets[i] = octet;
                        if (i != 3)
                        {
                                if (p == end || *p != '.')
                                {
                                        succ = false;
                                        return 0;
                                }
                        }
                        else
                        {
                                if (p != end && *p)
                                {
                                        succ = false;
                                        return 0;
                                }
                                else
                                {
                                        succ = true;
                                        return res;
                                }
                        }
                }
        }

        inline endpoint ParseSrvEndpoint(strwlen32_t addr, const strwlen8_t proto, const uint16_t srvPort)
        {
                endpoint res;

                if (addr.BeginsWithNoCase(proto.p, proto.len) && addr.SuffixFrom(proto.len).BeginsWith(_S("://")))
                        addr.StripPrefix(proto.len + 3);

                const auto r = addr.Divided(':');

		if (!r.first)
			res.addr4 = ntohl(INADDR_ANY);
		else if (r.first.len > 128)
                        return {0, 0};
		else
                {
                        bool succ;

                        res.addr4 = ParseHostAddress({r.first.p, r.first.len}, succ);
                        if (!succ)
                                return {0, 0};
                }

                if (r.second)
                {
                        if (!r.second.IsDigits())
                                return {0, 0};

                        const auto port = r.second.AsUint32();
                        if (port > 65536)
                                return {0, 0};
                        else
                                res.port = port;
                }
                else
	                res.port = srvPort;

                return res;
        }
}

class EPoller
{
      private:
        int epollFd;
        struct epoll_event *returnedEvents;
        uint32_t returnedEventsSize, maxReturnedEvents;
        struct epoll_event _localEventsStorage[8]; /* this helps if we wish to avoid allocating from the heap */

      public:
        EPoller(const uint32_t _maxReturnedEvents = 256)
            : maxReturnedEvents{_maxReturnedEvents}
        {
                epollFd = epoll_create(4096);
                returnedEvents = nullptr;
                returnedEventsSize = 0;
                returnedEvents = (epoll_event *)malloc(sizeof(epoll_event) * maxReturnedEvents);
        }

        ~EPoller(void)
        {
                if (likely(epollFd != -1))
                        close(epollFd);

                if (returnedEvents && returnedEvents != _localEventsStorage)
                        free(returnedEvents);
        }

        [[gnu::always_inline]] static auto _epoll_ctl(int epfd, int op, int fd, struct epoll_event *const event)
        {
                return epoll_ctl(epfd, op, fd, event);
        }

        inline void AddFd(int fd, const uint32_t events)
        {
                struct epoll_event e;

                e.events = events;
                e.data.fd = fd;

                _epoll_ctl(epollFd, EPOLL_CTL_ADD, fd, &e);
        }

        inline void AddFd(int fd, const uint32_t events, void *uData)
        {
                struct epoll_event e;

                e.events = events;
                e.data.ptr = uData;

                _epoll_ctl(epollFd, EPOLL_CTL_ADD, fd, &e);
        }

        inline void DelFd(int fd)
        {
                _epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr);
        }

        inline void SetEvents(int fd, const uint32_t events)
        {
                struct epoll_event e;

                e.events = events;
                e.data.fd = fd;

                _epoll_ctl(epollFd, EPOLL_CTL_MOD, fd, &e);
        }

        inline void SetDataAndEvents(int fd, void *data, const uint32_t events)
        {
                struct epoll_event e;

                e.events = events;
                e.data.ptr = data;

                _epoll_ctl(epollFd, EPOLL_CTL_MOD, fd, &e);
        }

        [[gnu::always_inline]] int Poll(const int timeoutInMSecs)
        {
                return epoll_wait(epollFd, returnedEvents, maxReturnedEvents, timeoutInMSecs);
        }

        [[gnu::always_inline]] const struct epoll_event *Events(void) const
        {
                return returnedEvents;
        }
};

namespace std
{
	template<>
	struct hash<Switch::endpoint>
	{
		using argument_type = Switch::endpoint;
                using result_type = std::size_t;

                result_type operator()(const argument_type &e) const
		{
			return std::hash<uintptr_t>{}((uint64_t(e.addr4) << 32u) | e.port);
		}
	};
}

static inline void PrintImpl(Buffer &v, const Switch::endpoint &e)
{
        const uint8_t *const octets = (uint8_t *)&e.addr4;

        v.AppendFmt("%u.%u.%u.%u:%u", octets[0], octets[1], octets[2], octets[3], e.port);
}


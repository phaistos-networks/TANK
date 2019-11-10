#include "service_common.h"
#include <sys/time.h>
#include <sys/resource.h>

int log_fd;

void init_log() {
#ifndef LEAN_SWITCH
        log_fd = open("/tmp/tank_srv.log", O_WRONLY | O_CREAT | O_LARGEFILE | O_APPEND, 0775);

        if (log_fd != -1) {
                Buffer m;

                m.append(Date::ts_repr(time(nullptr)), " TANK service initialization in progress\n");
                write(log_fd, m.data(), m.size());
        }
#else
        log_fd = -1;
#endif
}

// deals with the weak symbol getentropy@@GLIBC_2.25
int getentropy(void *buffer, size_t length) {
        return 0;
}

int main(int argc, char *argv[]) {
        signal(SIGHUP, SIG_IGN);

        if (struct rlimit rl{.rlim_cur = RLIM_INFINITY, .rlim_max = RLIM_INFINITY}; setrlimit(RLIMIT_CORE, &rl) == -1) {
                Print("Warning: setrlimit() failed:", strerror(errno), "\n");
        }

        init_log();

        return Service{}.start(argc, argv);
}

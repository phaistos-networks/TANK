#include "service_common.h"

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

int main(int argc, char *argv[]) {
        init_log();

        return Service{}.start(argc, argv);
}

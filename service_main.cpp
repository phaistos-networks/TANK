#include "service_common.h"
#include <sys/time.h>
#include <sys/resource.h>

// deals with the weak symbol getentropy@@GLIBC_2.25
int getentropy(void *buffer, size_t length) {
        return 0;
}

int main(int argc, char *argv[]) {
        signal(SIGHUP, SIG_IGN);

        if (struct rlimit rl{.rlim_cur = RLIM_INFINITY, .rlim_max = RLIM_INFINITY}; setrlimit(RLIMIT_CORE, &rl) == -1) {
                Print("Warning: setrlimit() failed:", strerror(errno), "\n");
        }

        return Service{}.start(argc, argv);
}

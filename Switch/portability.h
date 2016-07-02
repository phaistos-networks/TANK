#pragma once
#include <inttypes.h>

#ifdef __linux__
#define SWITCH_HAVE_MALLOC_USABLE_SIZE 1
#else
#define off64_t off_t
#define pread64 pread
#define pwrite64 pwrite
#define dirent64 dirent
#define readdir64_r readdir_r
#define readdir64 readdir
#endif

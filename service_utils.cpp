#include "service.h"

void Service::rm_tankdir(const char *tank_path) {
        static const auto visit = [](const auto &self, char *path, unsigned path_len) -> void {
                EXPECT(path_len + 1 < PATH_MAX);

                if (path_len && path[path_len - 1] != '/') {
                        path[path_len++] = '/';
                }

                path[path_len] = '\0';

                enum {
                        dry_run = 1,
                };
                auto *const path_end = path + path_len;
                struct stat st;

                for (auto name : DirectoryEntries(path)) {
                        if (name == "."_s8 || name == ".."_s8) {
                                continue;
                        }

                        const auto name_len = name.size();

                        EXPECT(path_len + name_len + 1 < PATH_MAX);

                        name.ToCString(path_end);
                        if (stat(path, &st) == -1) {
                                std::abort();
                        }

                        if (st.st_mode & S_IFDIR) {
                                path_end[name.size()] = '/';
                                self(self, path, path_len + name_len + 1);
                        } else {
                                if (-1 == unlink(path)) {
                                        if (errno != ENOENT) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                                                throw Switch::system_error("Unable to unlink() file");
#else
                                                throw std::runtime_error("Unable to unlink() file");
#endif
                                        }
                                }
                        }
                }

                path[path_len] = '\0'; // important, was modified in the loop
                if (-1 == rmdir(path)) {
#ifdef TANK_THROW_SWITCH_EXCEPTIONS
                        throw Switch::system_error("Failed to rmdir() directory");
#else
                        throw std::runtime_error("Failed to rmdir() directory");
#endif
                }
        };

        char       path[PATH_MAX];
        const auto len = strlen(tank_path);

        memcpy(path, tank_path, len);
        visit(visit, path, len);
}

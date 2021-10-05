#include "service_common.h"

int Service::verify(char *paths[], const int cnt) {
        size_t n{0}, tot{0};
        bool   anyFailed{false};

        for (int i{0}; i < cnt; ++i) {
                const char *const path = paths[i];
		const auto fullPath = str_view32::make_with_cstr(path);
                const auto        ext = fullPath.Extension();

                if (ext.Eq(_S("ilog")) || ext.Eq(_S("log")) || ext.Eq(_S("index"))) {
                        int fd = open(path, O_RDONLY | O_LARGEFILE);

                        if (fd == -1) {
				Print("open(", path, ") failed:", strerror(errno), "\n");
                                return 1;
                        }

                        DEFER({ close(fd); });

                        Print("Verifying ", fullPath, " (", size_repr(lseek64(fd, 0, SEEK_END)), ") ..\n");

                        try {
                                if (ext.Eq(_S("ilog")) || ext.Eq(_S("log"))) {
                                        const auto r = Service::verify_log(fd);

                                        Print("> ", dotnotation_repr(r), " msgs\n");
                                        tot += r;
                                } else if (ext.Eq(_S("index"))) {
                                        auto name = fullPath;

                                        if (const auto p = name.SearchR('/')) {
                                                name = name.SuffixFrom(p + 1);
					}

                                        Service::verify_index(fd, name.Divided('_').second.Eq(_S("64")));
                                }
                        } catch (const std::exception &e) {
                                Print(ansifmt::bold, ansifmt::color_red, "Failed to verify (", fullPath, ansifmt::reset, ")\n");
                                anyFailed = true;
                        }

                        ++n;
                } else {
                        Print("Ignoring ", fullPath, "\n");
                }
        }

        if (!anyFailed) {
                Print(ansifmt::color_green, "All ", dotnotation_repr(n), " files verified OK, ", dotnotation_repr(tot), " messages", ansifmt::reset, "\n");
                return 0;
        } else {
                return 1;
        }
}

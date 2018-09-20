#pragma once
#include "switch.h"
#include <dirent.h>

class DirectoryEntries {
      private:
        DIR *const dh;

      public:
        struct iterator {
		using de_t = struct dirent; // WAS: dirent64 
                DIR *const       dh;
		de_t *de, storage;

                iterator(DIR *const h, de_t *d) 
                    : dh(h), de(d) {
                }

                inline bool operator!=(const iterator &o) const {
                        return de != o.de;
                }

                inline strwlen8_t operator*() const {
                        return strwlen8_t(de->d_name, strlen(de->d_name));
                }

                inline iterator &operator++() {
			// readdir64_r and readdir_r are deprecated
			de = readdir(dh);
                        return *this;
                }
        };

      public:
        DirectoryEntries(const char *const path)
            : dh(opendir(path)) {
                if (unlikely(!dh)) {
                        throw Switch::exception("Failed to access directory ", path, ":", strerror(errno));
		}
        }

        ~DirectoryEntries() {
                if (dh) {
                        closedir(dh);
		}
        }

        struct iterator begin() const {
                return iterator(dh, dh ? readdir(dh) : nullptr);
        }

        struct iterator end() const {
                return iterator(dh, nullptr);
        }

        operator bool() const {
                return dh;
        }
};

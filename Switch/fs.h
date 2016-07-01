#pragma once
#include "switch.h"
#include <dirent.h>

class DirectoryEntries
{
      private:
        DIR *const dh;

      public:
        struct iterator
        {
                DIR *const dh;
                struct dirent64 *de, storage;

                iterator(DIR *const h, struct dirent64 *const d)
                    : dh(h), de(d)
                {
                }

                inline bool operator!=(const struct iterator &o) const
                {
                        return de != o.de;
                }

                inline strwlen8_t operator*() const
                {
                        return strwlen8_t(de->d_name, strlen(de->d_name));
                }

                inline iterator &operator++()
                {
                        if (unlikely(readdir64_r(dh, &storage, &de)))
                                de = nullptr;
                        return *this;
                }
        };

      public:
        DirectoryEntries(const char *const path)
            : dh(opendir(path))
        {
                if (unlikely(!dh))
                        throw Switch::exception("Failed to access directory ", path);
        }

        ~DirectoryEntries()
        {
                if (dh)
                        closedir(dh);
        }

        struct iterator begin() const
        {
                return iterator(dh, dh ? readdir64(dh) : nullptr);
        }

        struct iterator end() const
        {
                return iterator(dh, nullptr);
        }

        operator bool() const
        {
                return dh;
        }
};

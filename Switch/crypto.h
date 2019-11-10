#pragma once
#include "switch.h"

struct CRC32Generator final {
      private:
        uint32_t crc32;

      private:
        void update_impl(const void *, const size_t) noexcept;

      public:
        CRC32Generator(void)
            : crc32(~0L) {
        }

        auto &reset() {
                crc32 = ~0L;
                return *this;
        }

        template <typename T>
        inline auto &update(const T &v) {
                update_impl(&v, sizeof(v));
                return *this;
        }

        inline auto &update(const void *ptr, const uint32_t len) {
                update_impl(ptr, len);
                return *this;
        }

        inline auto get() const noexcept {
                return ~crc32;
        }
};

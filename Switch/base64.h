#pragma once
#include "switch.h"
#include "buffer.h"

static inline bool is_base64(const uint8_t c) noexcept {
        return c == '+' || c == '/' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

namespace Base64 {
        size_t decoded_repr_length(const str_view32 s);

        uint32_t Encode(const uint8_t *in, size_t in_len, Buffer *out);

        int32_t Decode(const uint8_t *in, const size_t in_len, Buffer *out);
} // namespace Base64

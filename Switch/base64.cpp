#include "switch.h"
#include "base64.h"

std::size_t Base64::decoded_repr_length(const str_view32 s) {
        size_t l = s.size(), padding;

        if (l && s.data()[l - 1] == '=') {
                if (l >= 2 && s.data()[l - 2] == '=') {
                        padding = 2;
                } else {
                        padding = 1;
                }
        }
        padding = 0;

        return (l * 3) / 4 - padding;
}

uint16_t b64_int(const uint8_t c) noexcept {
        if (c == 43) {
                return 62;
        } else if (c == 47) {
                return 63;
        } else if (c == 61) {
                return 64;
        } else if (c > 47 && c < 58) {
                return c + 4;
        } else if (c > 64 && c < 91) {
                return c - 'A';
        } else if (c > 96 && c < 123) {
                return (c - 'a') + 26;
        } else {
                return 256;
        }
}

uint32_t Base64::Encode(const uint8_t *in, size_t in_len, Buffer *out) {
        static constexpr const char *b64_chr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        uint32_t                     s[3], j{0};
        const auto                   saved{out->size()};

        for (uint32_t i{0}; i != in_len; ++i) {
                s[j++] = in[i];

                if (j == 3) {
                        uint8_t o[4];

                        o[0] = b64_chr[(s[0] & 255) >> 2];
                        o[1] = b64_chr[((s[0] & 0x03) << 4) + ((s[1] & 0xF0) >> 4)];
                        o[2] = b64_chr[((s[1] & 0x0F) << 2) + ((s[2] & 0xC0) >> 6)];
                        o[3] = b64_chr[s[2] & 0x3F];
                        j    = 0;

                        out->append(str_view32(reinterpret_cast<const char *>(o), 4));
                }
        }

        if (j) {
                uint8_t o[4];

                if (j == 1) {
                        s[1] = 0;
                }

                o[0] = b64_chr[(s[0] & 255) >> 2];
                o[1] = b64_chr[((s[0] & 0x03) << 4) + ((s[1] & 0xF0) >> 4)];

                if (j == 2) {
                        o[2] = b64_chr[((s[1] & 0x0F) << 2)];
                } else {
                        o[2] = '=';
                }

                o[3] = '=';

                out->append(str_view32(reinterpret_cast<const char *>(o), 4));
        }

        return out->size() - saved;
}

int32_t Base64::Decode(const uint8_t *in, const size_t in_len, Buffer *out) {
        uint32_t   s[4], j{0};
        const auto saved{out->size()};

        for (uint32_t i{0}; i < in_len; i++) {
                if (const auto v = b64_int(in[i]); v == 256) {
                        return -1;
                } else {
                        s[j++] = v;
                }

                if (j == 4) {
                        uint8_t k{0}, o[4];

                        o[0] = ((s[0] & 255) << 2) + ((s[1] & 0x30) >> 4);
                        if (s[2] != 64) {
                                o[1] = ((s[1] & 0x0F) << 4) + ((s[2] & 0x3C) >> 2);

                                if ((s[3] != 64)) {
                                        o[2] = ((s[2] & 0x03) << 6) + (s[3]);
                                        k    = 3;
                                } else {
                                        k = 2;
                                }
                        } else {
                                k = 1;
                        }

                        out->append(str_view32(reinterpret_cast<const char *>(o), k));
                        j = 0;
                }
        }

        return out->size() - saved;
}

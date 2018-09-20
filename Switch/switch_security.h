#pragma once
#include "switch.h"
#include "switch_ranges.h"
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/rsa.h>

namespace switch_security
{

#pragma mark ciphers - symmetric key encryption
        struct ciphers
        {
                struct block_cipher
                {
                        static void verify_key_iv(const std::size_t key_size, const std::size_t iv_size, const std::size_t input_key_size, const std::size_t input_iv_size)
                        {
                                EXPECT(input_key_size * (sizeof(uint8_t) << 3) == key_size);
                                EXPECT(input_iv_size * (sizeof(uint8_t) << 3) == iv_size);
                        }

                        const std::size_t block_size_in_bytes;
                        const EVP_CIPHER *cipher;
                        const range_base<const uint8_t *, std::size_t> key, iv;
                        EVP_CIPHER_CTX *ctx{nullptr};

                        void reset_ctx()
                        {
                                if (ctx)
                                        EVP_CIPHER_CTX_free(ctx);

                                ctx = EVP_CIPHER_CTX_new();
                                if (!ctx)
                                        throw Switch::data_error("Failed to initialize context");
                        }

                        block_cipher(const std::size_t block_size_, const EVP_CIPHER *cipher_,
                                     const range_base<const uint8_t *, std::size_t> key_, const range_base<const uint8_t *, std::size_t> iv_)
                            : block_size_in_bytes{block_size_ >> 3}, cipher{cipher_}, key{key_}, iv{iv_}
                        {
                        }

                        ~block_cipher()
                        {
                                if (ctx)
                                        EVP_CIPHER_CTX_free(ctx);
                        }

                        Buffer decrypt(const range_base<const uint8_t *, std::size_t> ciphertext); // returns plaintext

			Buffer decrypt(const str_view32 ciphertext)
			{
				return decrypt({reinterpret_cast<const uint8_t *>(ciphertext.data()), ciphertext.size()});
			}

                        Buffer encrypt(const str_view32 plaintext); // returns ciphertext
                };

                struct aes256 final
                    : public block_cipher
                {
                        aes256(const range_base<const uint8_t *, std::size_t> key, const range_base<const uint8_t *, std::size_t> iv)
                            : block_cipher(128, EVP_aes_256_cbc(), key, iv)
                        {
                                verify_key_iv(256, 128, key.size(), iv.size());
                        }
                };

                struct aes128 final
                    : public block_cipher
                {
                        aes128(const range_base<const uint8_t *, std::size_t> key, const range_base<const uint8_t *, std::size_t> iv)
                            : block_cipher(128, EVP_aes_128_cbc(), key, iv)
                        {
                                verify_key_iv(128, 128, key.size(), iv.size());
                        }
                };
        };

#pragma mark HMAC
        struct hmac final
        {
                HMAC_CTX ctx;
                uint8_t ds{0};

                hmac(const EVP_MD *md, const void *key, const int key_len);

                auto digest_size() noexcept
                {
                        return ds;
                }

                // digest_out should be at least digest_size() in bytes
                void finalize(uint8_t *digest_out);

                inline void update(const void *data, const std::size_t len)
                {
                        HMAC_Update(&ctx, reinterpret_cast<const uint8_t *>(data), len);
                }

                ~hmac();

                static void PBKDF2(const str_view32 password, const void *salt, const std::size_t salt_len, const std::size_t iterations,
                                   const EVP_MD *md,
                                   const std::size_t key_out_capacity,
                                   uint8_t *key_out);
        };

#pragma mark RSA
        struct rsa final
        {
                std::unique_ptr<RSA, decltype(&::RSA_free)> r{nullptr, ::RSA_free};

              private:
                rsa(RSA *ptr)
                    : r(ptr, ::RSA_free)
                {
                }

              public:
                rsa()
                {
                }

                inline bool have_pubkey() const noexcept
                {
                        return r && r->e && r->n;
                }

                inline bool have_privkey() const noexcept
                {
                        return r && r->d && r->p && r->q;
                }

                static rsa generate(const uint32_t bits = 4096);

                static rsa make_from_pubkcy_pkcs(const uint8_t *content, const std::size_t len);

                static rsa make_from_privkey_pkcs(const uint8_t *content, const std::size_t len);

                std::size_t pub_key_repr_pkcs(uint8_t *storage)
                {
                        require(have_pubkey());
                        return i2d_RSAPublicKey(r.get(), &storage);
                }

                std::size_t priv_key_repr_pkcs(uint8_t *storage)
                {
                        require(have_privkey());
                        return i2d_RSAPrivateKey(r.get(), &storage);
                }

                void print() const
                {
                        RSA_print_fp(stdout, r.get(), 0);
                }

                int modulus_size() const noexcept
                {
                        return RSA_size(r.get());
                }

                int priv_decrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding = RSA_PKCS1_PADDING);

                void pub_encrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding = RSA_PKCS1_PADDING);

                void priv_encrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding = RSA_PKCS1_PADDING);

                int pub_decrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding = RSA_PKCS1_PADDING);

                bool sign(const int type, const uint8_t *m, const std::size_t m_len, uint8_t *sigret);

                int verify(const int type, const uint8_t *m, const uint8_t m_len, uint8_t *sigbuf, const std::size_t siglen);
        };

        static void gen_rnd(const std::size_t size, uint8_t *out)
        {
                if (1 != RAND_bytes(out, size))
                        throw Switch::data_error("Failed to generate random sequence");
        }

        static void gen_pseudo_rnd(const std::size_t size, uint8_t *out)
        {
                if (1 != RAND_pseudo_bytes(out, size))
                        throw Switch::data_error("Failed to generate random sequence");
        }

	static inline auto gen_rnd(const std::size_t size)
        {
                auto res = std::make_unique<uint8_t[]>(size);

                gen_rnd(size, res.get());
                return res;
        }

	static inline auto gen_pseudo_rnd(const std::size_t size)
	{
                auto res = std::make_unique<uint8_t[]>(size);

		gen_pseudo_rnd(size, res.get());
		return res;
	}
}

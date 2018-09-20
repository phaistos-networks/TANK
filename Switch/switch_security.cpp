#include "switch_security.h"

Buffer switch_security::ciphers::block_cipher::decrypt(const range_base<const uint8_t *, std::size_t> ciphertext)
{
        reset_ctx();

        if (1 != EVP_DecryptInit_ex(ctx, cipher, nullptr, reinterpret_cast<const uint8_t *>(key.offset), reinterpret_cast<const uint8_t *>(iv.offset)))
                throw Switch::runtime_error("Failed to initialize EVP ctx");

        if (1 != EVP_CIPHER_CTX_set_padding(ctx, 1))
                throw Switch::runtime_error("Failed to initialize EVP ctx");

        // shouldn't need to do this, i.e size should be set to ciphertext.size()
        // because it's already padded and aligned.
        const auto size = (ciphertext.size() + block_size_in_bytes) & ~(block_size_in_bytes - 1);
        Buffer res;

        res.reserve(size + 1);

        auto out = reinterpret_cast<uint8_t *>(res.data());
        int len;
        const auto *p = reinterpret_cast<const uint8_t *>(ciphertext.offset), *const e = p + ciphertext.size();

        if (1 != EVP_DecryptUpdate(ctx, out, &len, p, std::distance(p, e)))
        {
                EVP_CIPHER_CTX_free(ctx);
                ctx = nullptr;
                throw Switch::data_error("Failed to decrypt");
        }
        out += len;

        if (1 != EVP_DecryptFinal_ex(ctx, out, &len))
        {
                EVP_CIPHER_CTX_free(ctx);
                ctx = nullptr;
                throw Switch::data_error("Failed to decrypt");
        }
        out += len;

        const auto plaintext_len = std::distance(reinterpret_cast<uint8_t *>(res.data()), out);

        EVP_CIPHER_CTX_free(ctx);
        ctx = nullptr;

        EXPECT(plaintext_len <= size);
        res.resize(plaintext_len);
        return res;
}

Buffer switch_security::ciphers::block_cipher::encrypt(const str_view32 plaintext)
{
        reset_ctx();

        if (1 != EVP_EncryptInit_ex(ctx, cipher, nullptr, reinterpret_cast<const uint8_t *>(key.offset), reinterpret_cast<const uint8_t *>(iv.offset)))
                throw Switch::runtime_error("Failed to initialize EVP ctx");

        if (1 != EVP_CIPHER_CTX_set_padding(ctx, 1))
        {
                // Buy default, encryption operations are padded using standard block padding and the padding
                // is checked and removed when decrypting.
                //
                // Disabing the padding here would mean that no padding is performed, by the size of plaintext would
                // have to be a multiple of the block size, or encryption would fail
                //
                // We are going to enable it explicitly here anyway
                throw Switch::runtime_error("Failed to initialize EVP ctx");
        }

        // e.g AES is a block cipher with 128-bit blocks(16 bytes)
        // Note that AES-256 also uses 128-bit blocks; the "256" is about the key length, not the block length.
        const auto size = (plaintext.size() + block_size_in_bytes) & ~(block_size_in_bytes - 1);
        Buffer res;

        res.reserve(size + 1);

        auto out = reinterpret_cast<uint8_t *>(res.data());
        int len;
        const auto *p = reinterpret_cast<const uint8_t *>(plaintext.data()), *const e = p + plaintext.size();

        if (1 != EVP_EncryptUpdate(ctx, out, &len, p, std::distance(p, e)))
        {
                EVP_CIPHER_CTX_free(ctx);
                ctx = nullptr;
                throw Switch::data_error("Failed to encrypt");
        }
        out += len;

        if (1 != EVP_EncryptFinal_ex(ctx, out, &len))
        {
                EVP_CIPHER_CTX_free(ctx);
                ctx = nullptr;
                throw Switch::data_error("Failed to encrypt");
        }
        out += len;

        const auto ciphertext_len = std::distance(reinterpret_cast<uint8_t *>(res.data()), out);

        EVP_CIPHER_CTX_free(ctx);
        ctx = nullptr;

        EXPECT(ciphertext_len <= size);
        res.resize(ciphertext_len);
        return res;
}

switch_security::hmac::hmac(const EVP_MD *md, const void *key, const int key_len)
{
        ds = md->md_size;
        HMAC_CTX_init(&ctx);
        HMAC_Init_ex(&ctx, key, key_len, md, nullptr);
}

void switch_security::hmac::finalize(uint8_t *digest_out)
{
        unsigned len = digest_size();

        HMAC_Final(&ctx, digest_out, &len);
}

switch_security::hmac::~hmac()
{
        HMAC_CTX_cleanup(&ctx);
}

void switch_security::hmac::PBKDF2(const str_view32 password, const void *salt, const std::size_t salt_len, const std::size_t iterations,
                                   const EVP_MD *md,
                                   const std::size_t key_out_capacity,
                                   uint8_t *key_out)
{
        require(key_out_capacity >= md->md_size);

        if (1 != PKCS5_PBKDF2_HMAC(password.data(), password.size(),
                                   reinterpret_cast<const uint8_t *>(salt), salt_len,
                                   iterations,
                                   md,
                                   key_out_capacity, key_out))
        {
                throw Switch::data_error("Failed to PKCS5_PBKDF2_HMAC()");
        }
}


switch_security::rsa switch_security::rsa::generate(const uint32_t bits)
{
	require(bits >= 1024 && bits <= 8192);

	std::unique_ptr<RSA, decltype(&::RSA_free)> r{RSA_new(), ::RSA_free};
	std::unique_ptr<BIGNUM, decltype(&::BN_free)> bn(BN_new(), ::BN_free);

	BN_set_word(bn.get(), RSA_F4);
	if (1 != RSA_generate_key_ex(r.get(), 2048, bn.get(), nullptr))
		throw Switch::data_error("Failed to generate RSA keys pair");

	if (!RSA_check_key(r.get()))
		throw Switch::data_error("Failed to verify RSA keys pair");

	return rsa(r.release());
}

switch_security::rsa switch_security::rsa::make_from_pubkcy_pkcs(const uint8_t *content, const std::size_t len)
{
        std::unique_ptr<RSA, decltype(&::RSA_free)> r{RSA_new(), ::RSA_free};
        auto local{r.get()};
        auto res = d2i_RSAPublicKey(&local, &content, len);

        EXPECT(res == local);
        return rsa(r.release());
}

switch_security::rsa switch_security::rsa::make_from_privkey_pkcs(const uint8_t *content, const std::size_t len)
{
        std::unique_ptr<RSA, decltype(&::RSA_free)> r{RSA_new(), ::RSA_free};
        auto local{r.get()};
        auto res = d2i_RSAPrivateKey(&local, &content, len);

        EXPECT(res == local);
        return rsa(r.release());
}

int switch_security::rsa::priv_decrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding)
{
        // to must point to a memory section large enough to hold the plaintext data
        // which is smaller than modulus_size()
        // padding is the padding mode used to encrypt the data

        require(have_privkey());
        return RSA_private_decrypt(content_len, content, to, r.get(), padding);
}

void switch_security::rsa::pub_encrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding)
{
        require(have_pubkey());

        switch (padding)
        {
                case RSA_PKCS1_PADDING:
                        require(content_len < modulus_size() - 11);
                        break;

                case RSA_PKCS1_OAEP_PADDING:
                        require(content_len < modulus_size() - 41);
                        break;

                case RSA_NO_PADDING:
                        require(content_len == modulus_size());
                        break;
        }

        auto res = RSA_public_encrypt(content_len, content, to, r.get(), padding);

        require(res == modulus_size());
}

void switch_security::rsa::priv_encrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding)
{
        require(have_privkey());
        auto res = RSA_private_encrypt(content_len, content, to, r.get(), padding);

        require(res == modulus_size());
}

int switch_security::rsa::pub_decrypt(const uint8_t *content, const std::size_t content_len, uint8_t *to, int padding)
{
        require(have_pubkey());
        return RSA_public_decrypt(content_len, content, to, r.get(), padding);
}

bool switch_security::rsa::sign(const int type, const uint8_t *m, const std::size_t m_len, uint8_t *sigret)
{
        require(have_privkey());
        unsigned siglen = modulus_size();

        return RSA_sign(type, m, m_len, sigret, &siglen, r.get()) == 1;
}

int switch_security::rsa::verify(const int type, const uint8_t *m, const uint8_t m_len, uint8_t *sigbuf, const std::size_t siglen)
{
        require(have_pubkey());
        return RSA_verify(type, m, m_len, sigbuf, siglen, r.get());
}

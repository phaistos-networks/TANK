#pragma once
#include <malloc.h>
#include <stdarg.h>

class Buffer
{
      public:
        using iterator = char *;
        using const_iterator = const char *;
        using reference = char &;
        using const_reference = const char &;
        using value_type = char;

        static constexpr uint32_t npos = UINT32_MAX;

      public: // std::string API
        iterator begin() noexcept
        {
                return buffer;
        }

        const_iterator cbegin() const noexcept
        {
                return buffer;
        }

        iterator end() noexcept
        {
                return buffer + length_;
        }

        const_iterator end() const noexcept
        {
                return buffer + length_;
        }

        uint32_t size() const noexcept
        {
                return length_;
        }

        uint32_t max_size() const noexcept
        {
                return UINT32_MAX - 1;
        }

        void resize(const uint32_t n)
        {
                if (unlikely(n < max_size()))
                        SetLength(n);
                else
                        EnsureCapacity(n);
        }

        uint32_t capacity() const noexcept
        {
                return max_size();
        }

        void reserve(const uint32_t n)
        {
                EnsureCapacity(n + 1);
        }

        void clear() noexcept
        {
                length_ = 0;
        }

        bool empty() const noexcept
        {
                return !length_;
        }

        void shrink_to_fit()
        {
                // NO-OP for now
        }

        char &at(const uint32_t pos)
        {
                return buffer[pos];
        }

        const char &at(const uint32_t pos) const
        {
                return buffer[pos];
        }

        char &back()
        {
                return buffer[length_ - 1];
        }

        const char &back() const
        {
                return buffer[length_ - 1];
        }

        char &front()
        {
                return *buffer;
        }

        const char &front() const
        {
                return *buffer;
        }

        inline Buffer &operator+=(const char c)
        {
                Append(c);
                return *this;
        }

        inline Buffer &operator+=(const char *const str)
        {
                Append(str);
                return *this;
        }

        inline Buffer &operator+=(std::initializer_list<char> il)
        {
                EnsureCapacity(il.size());
                for (const auto it : il)
                        Append(it);
                return *this;
        }

        void push_back(const char c)
        {
                Append(c);
        }

        Buffer &erase(const uint32_t pos = 0, const uint32_t len = npos)
        {
                DeleteChunk(pos, len != npos ? len : length_ - pos);
                return *this;
        }

        Buffer &erase(const_iterator p)
        {
                DeleteChunk(p - buffer, 1);
                return *this;
        }

        Buffer &erase(const_iterator first, const_iterator last)
        {
                DeleteChunk(first - buffer, last - first);
                return *this;
        }

        void pop_back()
        {
                AdjustLength(1);
        }

        const char *c_str() const noexcept
        {
                return buffer;
        }

      public:
        [[gnu::always_inline]] inline uint32_t Reserved() const
        {
                return likely(buffer) ? malloc_usable_size(buffer) : 0;
        }

        [[gnu::always_inline]] inline auto length() const
        {
                return length_;
        }

        [[gnu::always_inline]] inline auto data() const
        {
                return buffer;
        }

        inline strwlen8_t AsS8() const
        {
                return strwlen8_t(buffer, length_);
        }

        inline strwlen16_t AsS16() const
        {
                return strwlen16_t(buffer, length_);
        }

        inline strwlen32_t AsS32() const
        {
                return strwlen32_t(buffer, length_);
        }

        inline void FreeBuf()
        {
                WillUpdate();
                if (likely(buffer))
                {
                        ::free(buffer);
                        buffer = nullptr;
                        SetReserved(0);
                        length_ = 0;
                }
        }

        [[gnu::always_inline]] inline bool IsNullTerminated() const
        {
                return buffer && buffer[length_] == '\0';
        }

        [[gnu::always_inline]] inline operator char *()
        {
                return buffer ?: const_cast<char *>(""); // Too many uses so we need to do it
        }

        [[gnu::always_inline]] inline bool operator==(const char *const data) const
        {
                if (buffer == data && data == nullptr)
                        return true;
                else if (likely(buffer))
                        return AsS32().Eq(data, strlen(data));
		else
			return false;
        }

        auto operator!=(const Buffer &o) const
        {
                return AsS32() != o.AsS32();
        }

        [[gnu::always_inline]] inline bool operator!=(const char *const data) const
        {
                return !(operator==(data));
        }

        [[gnu::always_inline]] inline bool operator==(const Buffer &other) const
        {
                return length_ == other.length_ ? !memcmp(buffer, other.buffer, length_) : false;
        }

        inline Buffer &operator=(const char *const data)
        {
                Flush();
                Append(data);

                return *this;
        }

        inline Buffer &operator=(const Buffer &other)
        {
                Flush();
                Append(other.data(), other.length());

                return *this;
        }

        inline Buffer &operator+(const char *const data)
        {

                Append(data);
                return *this;
        }

        inline void Append(const strwlen8_t *const s)
        {
                Append(s->p, s->len);
        }

        inline void Append(const strwlen16_t *const s)
        {
                Append(s->p, s->len);
        }

        inline void Append(const strwlen32_t *const s)
        {
                Append(s->p, s->len);
        }

        inline void Append(const strwlen8_t &s)
        {
                Append(s.p, s.len);
        }

        inline void Append(const strwlen16_t &s)
        {
                Append(s.p, s.len);
        }

        inline void Append(const strwlen32_t &s)
        {
                Append(s.p, s.len);
        }

        inline Buffer &operator+(const Buffer &other)
        {
                Append(other.data(), other.length());
                return *this;
        }

        inline Buffer &operator+=(const Buffer &other)
        {
                Append(other.data(), other.length());
                return *this;
        }

        [[gnu::always_inline]] inline char *At(const uint32_t offset) const
        {
                WillReference(offset);

                return buffer + offset;
        }

        [[gnu::always_inline]] inline char *End() const
        {
                return buffer + length_;
        }

        [[gnu::always_inline]] inline uint32_t OffsetOf(const char *const ptr)
        {
                return ptr - buffer;
        }

        inline uint8_t LastChar() const
        {
                return likely(length_) ? buffer[length_ - 1] : 0;
        }

        inline char FirstChar() const
        {
                return likely(length_) ? *buffer : 0;
        }

        inline char operator[](const uint32_t index) const
        {
                if (likely(index <= length_))
                        return buffer[index];

                abort();
                return 0;
        }

        /** Releases the internal buffer, and resets the structure, causing the internal buffer to 
		 *  be reallocated on further requests . Useful in some scenarios where we need to 
		 *  get for oursleves the internal buffer
		 */

        inline void ReleaseBuffer()
        {
                buffer = nullptr;
                SetReserved(0);
                length_ = 0;
        }

        inline char *GetAndReleaseBuffer()
        {
                char *const b = buffer;

                buffer = nullptr;
                SetReserved(0);
                length_ = 0;

                return b;
        }

        inline void FreeResources()
        {
                FreeBuf();
                length_ = 0;
        }

        void Exchange(Buffer *other);

        int32_t IndexOf(const char *const needle);

        uint32_t CountOf(const char c) const
        {
                return strwlen32_t(buffer, length_).CountOf(c);
        }

        int32_t Replace(const strwlen32_t from, const strwlen32_t to);

        uint32_t Replace(const char needle, const char with);

        Buffer()
            : length_{0}, buffer{nullptr}
        {
        }

        Buffer(Buffer &&o)
        {
                length_ = o.length_;
                SetReserved(o.Reserved());

                buffer = o.buffer;
                SetOwnsBuffer(o.OwnsBuffer());

                o.length_ = 0;
                o.SetReserved(0);
                o.buffer = nullptr;
                o.SetOwnsBuffer(false);
        }

        Buffer &operator=(Buffer &&o)
        {
                if (buffer && OwnsBuffer())
                        free(buffer);

                length_ = o.length_;
                SetReserved(o.Reserved());

                buffer = o.buffer;
                SetOwnsBuffer(o.OwnsBuffer());

                o.length_ = 0;
                o.SetReserved(0);
                o.buffer = nullptr;
                o.SetOwnsBuffer(false);

                return *this;
        }

        Buffer(const uint32_t initSize);

        Buffer(const char *const p, const uint32_t l);

        Buffer(const strwlen32_t content)
            : Buffer(content.p, content.len)
        {
        }

        Buffer(const strwlen16_t content)
            : Buffer(content.p, content.len)
        {
        }

        Buffer(const strwlen8_t content)
            : Buffer(content.p, content.len)
        {
        }

        Buffer(const Buffer &other)
            : Buffer(other.data(), other.length())
        {
        }

        virtual ~Buffer()
        {
                if (likely(OwnsBuffer()) && buffer)
                        free(buffer);
        }

        void AppendFmt(const char *fmt, ...)
            __attribute__((format(printf, 2, 3)))
        {
                WillUpdate();

                va_list args;
                const int32_t capacity = Capacity();

                va_start(args, fmt);
                const auto len = vsnprintf(buffer + length_, capacity, fmt, args);

                if (unlikely(len < 0))
                {
                        (void)va_end(args);
                        return;
                }
                else if (len >= capacity)
                {
                        EnsureCapacity(len + 1);

                        va_end(args);
                        va_start(args, fmt);

                        const auto r = vsnprintf(buffer + length_, len + 1, fmt, args);

                        if (unlikely(r < 0))
                        {
                                va_end(args);
                                return;
                        }
                }
                va_end(args);

                length_ += len;
                buffer[length_] = '\0';
        }

        static inline uint32_t ComputeNewSize(const uint32_t requestedSize)
        {
                if (requestedSize < 0x4000)
                        return ((requestedSize + (requestedSize >> 3)) | 15) + 1;
                else if (requestedSize < 0x80000)
                        return ((requestedSize + requestedSize) | 0xfff) + 1;
                else
                        return ((requestedSize + (requestedSize >> 3)) | 0xfff) + 1;
        }

#pragma mark Will/Did delegates
        [[gnu::always_inline]] inline void WillTouch(const uint32_t o, const uint32_t l) const
        {
                //expect(o + l < Reserved());
                (void)o;
                (void)l;
        }

        [[gnu::always_inline]] inline void WillSetLength(const uint32_t l) const {
            //expect_reason(OwnsBuffer() && length_ < Reserved(), "Unexpected");
        }

            [[gnu::always_inline]] inline void WillReference(const uint32_t offset) const {
                //expect_reason(offset == 0 || (likely(OwnsBuffer()) && offset < Reserved()), "Unexpected DataWithOffset()");
            }

                [[gnu::always_inline]] inline void WillUpdate()
        {
                // Enable this if you want to track down an issue; otherwise it's too expensive, and, it makes using this pattern an issue
                // expect_reason(OwnsBuffer(), "Attempting to update an immutable Buffer");
        }

        void EnsureSize(const uint32_t newMin)
        {
                const uint32_t r = Reserved();

                WillUpdate();

                if (unlikely(newMin > r))
                {
                        uint32_t newSize = ComputeNewSize(newMin);

                        try
                        {
                                if (buffer == nullptr || unlikely(!OwnsBuffer()))
                                        buffer = (char *)malloc(sizeof(char) * newSize);
                                else if (length_ == 0)
                                {
                                        ::free(buffer);
                                        buffer = (char *)malloc(sizeof(char) * newSize);
                                }
                                else
                                {
                                        buffer = (char *)realloc(buffer, newSize);
                                }
                        }
                        catch (...)
                        {
                                std::abort();
                        }

                        if (unlikely(!buffer))
                                std::abort();

                        newSize = malloc_usable_size(buffer);

                        SetReserved(newSize);
                        SetOwnsBuffer(true);

                        buffer[length_] = '\0';
                }
        }

        [[gnu::always_inline]] inline void EnsureCapacity(const uint32_t n)
        {
                EnsureSize(length_ + n + 1); // +1 for trailing \0
        }

        [[gnu::always_inline]] inline void WillInsert(const uint32_t n)
        {
                EnsureCapacity(n);
        }

        inline void Append(const char *const data, const uint32_t len)
        {
                WillInsert(len);
                memcpy(buffer + length_, data, len);
                length_ += len;
                buffer[length_] = '\0';
        }

        inline void Append(const char *const data)
        {
                Append(data, strlen(data));
        }

        inline void Append(const Buffer &buf)
        {
                Append(buf.data(), buf.length());
        }

        void Append(const char c, const uint32_t cnt);

        void Append(const char c)
        {
                WillInsert(1);
                buffer[length_++] = c;
                buffer[length_] = '\0';
        }

        void Append(const uint8_t c)
        {
                WillInsert(1);

                buffer[length_++] = c;
                buffer[length_] = '\0';
        }

        void Append(const float f)
        {
                AppendFmt("%lf", f);
        }

        void Append(const int i)
        {
                AppendFmt("%i", i);
        }

        void Append(const uint32_t i)
        {
                AppendFmt("%u", i);
        }

        [[gnu::always_inline]] inline void SetLengthAndTerm(const uint32_t l)
        {
                length_ = l;

                WillSetLength(length_);
                buffer[length_] = '\0';
        }

        inline void SetLength(const uint32_t newLength)
        {
                WillSetLength(newLength);

                Drequire(newLength < Reserved() && buffer);

                length_ = newLength;
                buffer[length_] = '\0';
        }

        void TrimWS();

        void TrimWSAll();

        inline bool IsBlank() const
        {
                return strwlen32_t(buffer, length_).IsBlank();
        }

        inline void AdjustLength(const uint32_t factor)
        {
                assert(length_ >= factor);

                length_ -= factor;
                *(buffer + length_) = '\0';
        }

        inline void ReplaceLastCharWith(const char newChar)
        {
                if (length_)
                        buffer[length_ - 1] = newChar;
        }

        inline void AdvanceLength(const uint32_t factor)
        {
                assert(length_ + factor < Reserved());
                assert(OwnsBuffer());

                length_ += factor;
                buffer[length_] = '\0';
        }

        void DeleteChunk(const uint32_t pos, uint32_t gapLength)
        {
                WillUpdate();

                if (gapLength + pos > length_)
                        gapLength = length_ - pos;

                if (gapLength > 0)
                {
                        memmove(buffer + pos, buffer + pos + gapLength, length_ - pos - gapLength);
                        length_ -= gapLength;
                }
        }

        // Includes space reserved for \0
        [[gnu::always_inline]] inline uint32_t ActualCapacity() const
        {
                Drequire(OwnsBuffer());
                return Reserved() - length_;
        }

        [[gnu::always_inline]] inline uint32_t Capacity() const
        {
                const uint32_t r = Reserved();

                return r > length_ ? (r - length_) - 1 : 0;
        }

        inline void SetData(char *const d)
        {
                buffer = d;
        }

        inline void Reset()
        {
                if (length_)
                {
                        if (likely(buffer))
                                *buffer = '\0';

                        length_ = 0;
                }
        }

        inline void Flush()
        {
                Reset();
        }

        [[gnu::always_inline]] inline void SetZeroLength()
        {
                length_ = 0;
        }

        inline void PushIntUnsafe(const int v)
        {
                WillUpdate();

                *(int *)(buffer + length_) = v;
                length_ += sizeof(int);
        }

        inline uint32_t AsUint32() const
        {
                return strwlen32_t(buffer, length_).AsUint32();
        }

        uint64_t AsUint64() const
        {
                return strwlen32_t(buffer, length_).AsUint64();
        }

        inline int32_t AsInt32() const
        {
                return strwlen32_t(buffer, length_).AsInt32();
        }

        bool IsAllDigits() const
        {
                for (const char *p = buffer, *const e = p + length_; p != e; ++p)
                {
                        if (!isdigit(*p))
                                return false;
                }
                return true;
        }

        inline void PushIntsPairUnsafe(const int v1, const int v2)
        {
                WillUpdate();

                *(int *)(buffer + length_) = v1;
                length_ += sizeof(int);
                *(int *)(buffer + length_) = v2;
                length_ += sizeof(int);
        }
        void SetDataAndLength(char *const d, const uint32_t l)
        {
                // Make sure you know what you are doing
                buffer = d;
                length_ = l;
        }

        void SetDataAndSize(char *const d, const uint32_t s)
        {
                buffer = d;
                length_ = 0;
                SetReserved(s);
        }

        void *MakeSpace(const uint32_t s)
        {
                void *ptr;

                WillInsert(s);
                ptr = buffer + length_;

                if (likely(s))
                {
                        length_ += s;
                        buffer[length_] = '\0';
                }

                return ptr;
        }

        void PadUptoWith(uint32_t n, const char c)
        {
                WillUpdate();

                if (n <= length_)
                        return;

                EnsureCapacity(n - length_ + 8);
                memset(buffer + length_, c, n - length_);
                length_ = n;
                buffer[length_] = '\0';
        }

      public:
        template <typename... Arg>
        static Buffer build(Arg &&... args)
        {
                Buffer b;

                ToBuffer(b, std::forward<Arg>(args)...);
                return b;
        }

        template <typename... Arg>
        auto &append(Arg &&... args)
        {
                ToBuffer(*this, std::forward<Arg>(args)...);
                return *this;
        }

        inline auto &append(const Buffer &str)
        {
                Append(str);
                return *this;
        }

        inline auto &append(const Buffer &str, const uint32_t pos, const uint32_t len)
        {
                Append(str.buffer + pos, len);
                return *this;
        }

        inline auto &append(const uint32_t n, const char c)
        {
                EnsureCapacity(n);

                for (uint32_t i{0}; i != n; ++i)
                        buffer[length_ + i] = c;

                length_ += n;
                buffer[length_] = '\0';
                return *this;
        }

        inline auto &append(std::initializer_list<char> il)
        {
                EnsureCapacity(il.size());
                for (const auto it : il)
                        Append(it);

                return *this;
        }

        [[gnu::always_inline]] inline virtual bool OwnsBuffer() const
        {
                // Ref: SGL ImmutableString
                // We need to know if we own the buffer, for otherwise Reserved() will fail (malloc_usable_size(const char*) will abort)

                return true;
        }

      protected:
        char *buffer;
        uint32_t length_;

        [[gnu::always_inline]] inline void SetOwnsBuffer(const bool v)
        {
                (void)v; // NO-OP for now
        }

        inline void SetReserved(const uint32_t newSize)
        {
                (void)newSize;
        }
};

class IOBuffer
    : public Buffer
{
      private:
        uint32_t position;

      private:
        static const uint32_t sizeOf;

      public:
        // Specialized for IOBuffer()
        // Buffer::EnsureCapacity() asks for length_ + n + 1 because
        // it needs to set the trailing \0
        //
        // UPDATE: we _need_ +1 for \0, otherwise Buffer::SetLength() fails because
        // newLen == Reserved(), if e.g we Reserve(8) and newLen == 8
        [[gnu::always_inline]] inline void WillInsert(const uint32_t n)
        {
                EnsureSize(length_ + n); // no need to +1 for trailing \0
        }

        void SetLengthAndTerm(const uint32_t l)
        {
                Drequire(l + 1 <= Reserved());

                Buffer::SetLengthAndTerm(l);
        }

        void SetLength(const uint32_t nl)
        {
                Drequire(nl <= Reserved());

                length_ = nl;
        }

        void makeNullTerminated()
        {
                if (length_ < Reserved())
                {
                        buffer[length_] = '\0';
                        return;
                }
                else
                {
                        WillInsert(1);
                        buffer[length_] = '\0';
                }
        }

        uint8_t *RoomFor(const size_t s)
        {
                WillInsert(s);

                auto *const ptr = buffer + length_;

                length_ += s;

                return (uint8_t *)ptr;
        }

        void DumpStrings() const;

        [[gnu::always_inline]] inline char *AtOffset() const
        {
                return buffer + position;
        }

        void FreeBuf()
        {
                Buffer::FreeBuf();
                position = 0;
        }

        void SetDataAndLength(char *const d, const uint32_t l)
        {
                buffer = d;
                length_ = l;
                position = 0;
        }

        IOBuffer()
            : position{0}
        {
        }

        IOBuffer(const uint32_t len)
            : Buffer{len}, position{0}
        {
        }

        IOBuffer(const char *const p, const uint32_t len)
            : Buffer{p, len}, position{0}
        {
        }

        IOBuffer(IOBuffer &&b)
            : Buffer(std::move(b)), position{b.position}
        {
                b.ReleaseBuffer();
                b.SetOffset(uint64_t(0));
        }

        IOBuffer(const IOBuffer &b)
            : Buffer(b.buffer, b.length_), position{b.position}
        {
        }

        auto &operator=(const IOBuffer &b)
        {
                position = b.position;
                length_ = 0;
                Serialize(b.buffer, b.length_);

                return *this;
        }

        auto &operator=(IOBuffer &&o)
        {
                FreeBuf();

                buffer = o.buffer;
                length_ = o.length_;
                position = o.position;
                SetReserved(o.Reserved());

                o.ReleaseBuffer();
                o.position = 0;

                return *this;
        }

        inline void Reset()
        {
                position = 0;
                Buffer::Reset();
        }

        inline void QuickFlush()
        {
                length_ = 0;
                position = 0;
        }

        inline void Flush()
        {
                Buffer::Flush();
                position = 0;
        }

        void clear()
        {
                Buffer::Flush();
                position = 0;
        }

        auto Offset() const
        {
                return position;
        }

        int ReadFromSocket(const int fd, const uint32_t chunkSize = 8192);

        int ReadFromFile(const int fd, off64_t offset = 0, int32_t bytesToRead = -1);

        int ReadFromFile(FILE *fh);

        int ReadFromFile(const char *const path, const off64_t offset = 0, const int32_t bytesToRead = -1);

        int SaveInFile(const char *const path, const off64_t offset = 0, const int32_t bytesToWrite = -1) const;

        int SaveInFile(int fd, const off64_t offset = 0, int32_t bytesToWrite = -1) const;

        int SaveInFileWithPermissions(const char *const, const mode_t mode) const;

        int WriteToSocket(const int fd);

        int WriteRangeToSocket(const int fd, uint32_t &bytesLeft);

        inline void SetPosition(const uint32_t pos)
        {
                position = pos;
        }

        inline void ResetPosition()
        {
                position = 0;
        }

        inline void AdvancePosition(const uint32_t pos)
        {
                position += pos;
        }

        inline void SetOffset(const uint64_t o)
        {
                position = (uint32_t)o;
        }

        inline void SetOffset(const char *const p)
        {
                position = p - buffer;
        }

        inline uint64_t GetOffset() const
        {
                return position;
        }

        inline void AdvanceOffset(const uint64_t s)
        {
                position += s;
        }

        void AdvanceOffsetTo(const char *const p)
        {
                position = p - buffer;
        }

        void ShiftOffset(const int32_t by)
        {
                position += by;
        }

        void AdjustOffsetAndLength(const uint32_t n)
        {
                position += n;
                length_ -= n;
        }

        inline uint32_t Position() const
        {
                return position;
        }

        [[gnu::always_inline]] inline bool IsPositionAtEnd() const
        {
                return position == length_;
        }

        inline operator char *()
        {
                return data() ?: const_cast<char *>("");
        }

        inline Buffer &operator=(const char *const data)
        {
                Buffer::operator=(data);
                this->position = 0;

                return *this;
        }

        Buffer &operator=(const strwlen32_t content)
        {
                length_ = 0;
                Append(content);
                return *this;
        }

        Buffer &operator=(const strwlen16_t content)
        {
                length_ = 0;
                Append(content);
                return *this;
        }

        Buffer &operator=(const strwlen8_t content)
        {
                length_ = 0;
                Append(content);
                return *this;
        }

        inline bool RetrieveBinarySafe(void *const d, const uint32_t d_size)
        {
                memcpy(d, buffer + position, d_size);
                position += d_size;

                return true;
        }

        inline void SerializeVarUInt32(const uint32_t n);

        inline uint32_t UnserializeVarUInt32(void);

        [[gnu::always_inline]] inline void Serialize(const uint8_t v)
        {
                *(typename std::remove_const<decltype(v)>::type *)RoomFor(sizeof(v)) = v;
        }

        [[gnu::always_inline]] inline void Serialize(const uint16_t v)
        {
                *(typename std::remove_const<decltype(v)>::type *)RoomFor(sizeof(v)) = v;
        }

        [[gnu::always_inline]] inline void Serialize(const uint32_t v)
        {
                *(typename std::remove_const<decltype(v)>::type *)RoomFor(sizeof(v)) = v;
        }

        [[gnu::always_inline]] inline void Serialize(const uint64_t v)
        {
                *(typename std::remove_const<decltype(v)>::type *)RoomFor(sizeof(v)) = v;
        }

        [[gnu::always_inline]] inline void Serialize(const float v)
        {
                *(typename std::remove_const<decltype(v)>::type *)RoomFor(sizeof(v)) = v;
        }

        [[gnu::always_inline]] inline void Serialize(const double v)
        {
                *(typename std::remove_const<decltype(v)>::type *)RoomFor(sizeof(v)) = v;
        }

        inline void Serialize(const strwlen8_t s)
        {
                Serialize(s.len);
                Serialize(s.p, s.len);
        }

        inline void Serialize(const strwlen16_t s)
        {
                SerializeVarUInt32(s.len);
                Serialize(s.p, s.len);
        }

        inline void Serialize(const strwlen32_t s)
        {
                SerializeVarUInt32(s.len);
                Serialize(s.p, s.len);
        }

        template <typename T>
        [[gnu::always_inline]] inline void Serialize(const T v)
        {
                memcpy(RoomFor(sizeof(T)), &v, sizeof(v));
        }

        template <typename T>
        [[gnu::always_inline]] inline void Serialize(const T *v)
        {
                memcpy(RoomFor(sizeof(T)), v, sizeof(T));
        }

        [[gnu::always_inline]] inline void Serialize(const void *const p, const uint32_t size)
        {
                memcpy(RoomFor(size), p, size);
        }

        [[gnu::always_inline]] inline void *Peek(const uint32_t size)
        {
                return buffer + position;
        }

        [[gnu::always_inline]] inline bool IsAtEnd() const
        {
                return position == length_;
        }

        template <typename T>
        [[gnu::always_inline]] inline T Unserialize()
        {
                const T r = *(T *)(buffer + position);

                position += sizeof(T);
                return r;
        }

        template <typename T>
        [[gnu::always_inline]] inline void Unserialize(T *const dst)
        {
                memcpy(dst, buffer + position, sizeof(T));
                position += sizeof(T);
        }

        template <typename T>
        [[gnu::always_inline]] inline bool UnserializeSafe(T *const dst)
        {
                return RetrieveBinarySafe(dst, sizeof(T));
        }

        [[gnu::always_inline]] inline void Unserialize(void *const p, const uint32_t size)
        {
                memcpy(p, buffer + position, size);
                position += size;
        }

        [[gnu::always_inline]] inline bool UnserializeSafe(void *const p, const uint32_t size)
        {
                memcpy(p, buffer + position, size);
                position += size;

                return true;
        }

        inline uint32_t ToEndSpanLen() const
        {
                return length_ - position;
        }

        inline strwlen32_t SuffixFromOffset() const
        {
                return {buffer + position, length_ - position};
        }
};

template <typename VT, typename LT>
static inline void PrintImpl(Buffer &out, const range_base<VT, LT> &r)
{
	out.AppendFmt("[%lu, %lu)", uint64_t(r.Left()), uint64_t(r.Right()));
}

struct _srcline_repr
{
        uint32_t line;
        const char *func;
        const char *file;

        _srcline_repr(const uint32_t l, const char *const f, const char *const _file)
            : line{l}, func{f}, file{_file}
        {
        }

        _srcline_repr(const _srcline_repr &o)
            : line{o.line}, func{o.func}, file{o.file}
        {
        }

        auto &operator=(const _srcline_repr &o)
        {
                line = o.line;
                func = o.func;
                file = o.file;

                return *this;
        }
};

#define srcline_repr() _srcline_repr(__LINE__, __FUNCTION__, __FILE__)
static inline void PrintImpl(Buffer &out, const _srcline_repr &r)
{
        out.AppendFmt("<%s:%u %s>", r.file, r.line, r.func);
}


struct duration_repr
{
        const uint64_t time;

        duration_repr(const uint64_t timeInMicroseconds)
            : time{timeInMicroseconds}
        {
        }

        duration_repr(const struct timeval &tv)
            : time{uint64_t(tv.tv_sec * 1000000UL + tv.tv_usec)}
        {
        }

        strwlen8_t Get(char *const out) const
        {
                uint8_t len;

                if (time < 1000)
                        len = sprintf(out, "%uus", (uint32_t)time);
                else if (time < 1000000)
                {
                        len = sprintf(out, "%.3f", (double)time / 1000000.0);
                        while (len && (out[len - 1] == '0'))
                                --len;
                        if (len && out[len - 1] == '.')
                                --len;
                        out[len++] = 's';
                }
                else if (time < 60 * 1000000)
                {
                        len = sprintf(out, "%.3f", (double)time / 1000000.0);
                        while (len && (out[len - 1] == '0'))
                                --len;
                        if (len && out[len - 1] == '.')
                                --len;
                        out[len++] = 's';
                }
                else if (time < 3600UL * 1000000UL)
                {
                        len = sprintf(out, "%um%u", (uint32_t)(time / (60 * 1000000UL)), (uint32_t)((time % (60 * 1000000UL)) / 1000000UL));
                        while (len && out[len - 1] == '0')
                                --len;
                        if (len && out[len - 1] != 'm')
                                out[len++] = 's';
                }
                else
                {
                        const uint32_t seconds = time / 1000000UL;

                        if (const auto mins = (seconds % 3600) / 60)
                                len = sprintf(out, "%uh%um", seconds / 3600, mins);
                        else
                                len = sprintf(out, "%uh", seconds / 3600);
                }

                return {out, len};
        }
};

static inline void PrintImpl(Buffer &out, const duration_repr &r)
{
        out.reserve(32);
        out.AdvanceLength(r.Get(out.end()).len);
}

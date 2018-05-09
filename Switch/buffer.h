#pragma once
#ifdef __linux__
#include <malloc.h>
#endif
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <initializer_list>
#include "switch_compiler_aux.h"
#include <string>

size_t as_escaped_repr_length(const char *p, const size_t len);
uint32_t escape_impl(const char *const p, const uint32_t len, char *out, const uint32_t available);

class Buffer
{
      private:
        static const uint32_t sizeOf;

      private:
        static long double ToDoubleImpl(const char *p)
        {
                char *e;
                const auto res = strtold(p, &e);

                errno = 0;
                if ((res == HUGE_VAL || res == HUGE_VALL) && errno == ERANGE)
                        return NAN;
                else if (res == 0 && errno == ERANGE)
                        return NAN;
                else if (*e)
                        return NAN;
                else
                        return res;
        }

      protected:
        inline void SetAsEmpty()
        {
                length_ = 0;
                buffer = nullptr;
                SetReserved(0);
        }

      public: //std::string API
        using iterator = char *;
        using const_iterator = const char *;
        using reference = char &;
        using const_reference = const char &;
        using value_type = char;

        static constexpr uint32_t npos = UINT32_MAX;

      public: // std::string API
        inline iterator begin() noexcept
        {
                return buffer;
        }

        inline const_iterator cbegin() const noexcept
        {
                return buffer;
        }

        inline iterator end() noexcept
        {
                return buffer + length_;
        }

        inline const_iterator end() const noexcept
        {
                return buffer + length_;
        }

        inline uint32_t max_size() const noexcept
        {
                return UINT32_MAX - 1;
        }

        inline auto &clear() noexcept
        {
                if (length_)
                {
                        if (buffer)
                                *buffer = '\0';
                        length_ = 0;
                }
                return *this;
        }

        inline bool empty() const noexcept
        {
                return !length_;
        }

        void shrink_to_fit()
        {
                // NO-OP for now
        }

        inline char &at(const uint32_t pos) noexcept
        {
                return buffer[pos];
        }

        const char &at(const uint32_t pos) const noexcept
        {
                return buffer[pos];
        }

        inline char &back() noexcept
        {
                return buffer[length_ - 1];
        }

        inline const char &back() const noexcept
        {
                return buffer[length_ - 1];
        }

        inline char &front() noexcept
        {
                return *buffer;
        }

        inline const char &front() const noexcept
        {
                return *buffer;
        }

        inline Buffer &operator+=(const char c)
        {
                Append(c);
                return *this;
        }

	void AppendEscaped(const char *content, const size_t len)
        {
                if (unlikely(!content))
                        return;

		const auto required = as_escaped_repr_length(content, len);

                reserve(required + 16);
		length_ += escape_impl(content, len, buffer + length_, required);
                buffer[length_] = '\0';
        }

        inline Buffer &operator+=(const char *const str)
        {
                Append(str);
                return *this;
        }

        inline Buffer &operator+=(std::initializer_list<char> il)
        {
                reserve(il.size());
                for (const auto it : il)
                        Append(it);
                return *this;
        }

        void push_back(const char c)
        {
                Append(c);
        }

        Buffer &assign(const Buffer &str)
        {
                length_ = 0;
                Append(str);
                return *this;
        }

        Buffer &assign(const Buffer &str, const uint32_t pos, const uint32_t len)
        {
                length_ = 0;
                Append(str.buffer + pos, len);
                return *this;
        }

        Buffer &assign(const char *const s)
        {
                length_ = 0;
                Append(s);
                return *this;
        }

        Buffer &assign(const uint32_t n, const char c)
        {
                length_ = 0;
                reserve(n);

                for (uint32_t i = 0; i != n; ++i)
                        buffer[i] = c;

                length_ = n;
                buffer[length_] = '\0';
                return *this;
        }

        Buffer &assign(const_iterator first, const_iterator last)
        {
                const uint32_t n = last - first;

                Append(first, n);
                return *this;
        }

        Buffer &assign(Buffer &&str)
        {
                buffer = str.buffer;
                length_ = str.length_;
                SetReserved(str.Reserved());

                str.buffer = nullptr;
                str.length_ = 0;
                str.SetReserved(0);

                return *this;
        }

        Buffer &insert(const uint32_t pos, const Buffer &str)
        {
                InsertChunk(pos, str.AsS32());
                return *this;
        }

        Buffer &insert(const uint32_t pos, const Buffer &str, const uint32_t subpos, const uint32_t sublen)
        {
                InsertChunk(pos, strwlen32_t{str.buffer + subpos, sublen});
                return *this;
        }

        Buffer &insert(const uint32_t pos, const char *const s, const uint32_t n)
        {
                InsertChunk(pos, strwlen32_t(s, n));
                return *this;
        }

        Buffer &insert(const uint32_t pos, const uint32_t n, const char c)
        {
                InsertSpace(pos, n);
                for (uint32_t i = 0; i != n; ++i)
                        buffer[pos + i] = c;

                return *this;
        }

        void insert_space(const uint32_t o, const uint32_t n)
        {
                require(o <= length_);
                const auto newLen = length_ + n;
                const auto upto = o + n;

                reserve(newLen);
                memmove(buffer + upto, buffer + o, sizeof(char) * (length_ - o));
                resize(newLen);
        }

        Buffer &insert(const_iterator pos, const char c)
        {
                return insert(pos - buffer, 1, c);
        }

        Buffer &insert(iterator pos, const_iterator first, const_iterator last)
        {
                return insert(pos - buffer, first, last - first);
        }

        Buffer &insert(const_iterator pos, std::initializer_list<char> il)
        {
                const auto index = pos - buffer;

                InsertSpace(index, il.size());

                char *out = buffer + index;

                for (const auto it : il)
                        *out++ = it;

                return *this;
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

        Buffer &replace(const uint32_t pos, const uint32_t len, const Buffer &str)
        {
                ReplaceChunk({pos, len}, str.AsS32());
                return *this;
        }

        Buffer &replace(const_iterator i1, const_iterator i2, const Buffer &str)
        {
                ReplaceChunk(i1 - buffer, i2 - i1, str.buffer, str.length_);
                return *this;
        }

        Buffer &replace(const uint32_t pos, const uint32_t len, const Buffer &str, const uint32_t subpos, const uint32_t sublen)
        {
                ReplaceChunk(pos, len, str.buffer + subpos, sublen);
                return *this;
        }

        Buffer &replace(const_iterator i1, const_iterator i2, const char *const s)
        {
                ReplaceChunk(i1 - buffer, i2 - i1, strwlen32_t{s});
                return *this;
        }

        Buffer &replace(const uint32_t pos, const uint32_t len, const char *const s, const uint32_t n)
        {
                ReplaceChunk(pos, len, s, n);
                return *this;
        }

        Buffer &replace(const_iterator i1, const_iterator i2, const char *const s, const uint32_t n)
        {
                return replace(i1 - buffer, i2 - i1, s, n);
        }

        Buffer &replace(const uint32_t pos, const uint32_t len, const uint32_t n, const char c)
        {
                PrepareReplacement(pos, len, n);

                for (uint32_t i = 0; i != n; ++i)
                        buffer[pos + i] = c;

                return *this;
        }

        Buffer &replace(const_iterator i1, const_iterator i2, const uint32_t n, const uint32_t c)
        {
                return replace(i1 - buffer, i2 - buffer, n, c);
        }

        Buffer &replace(const_iterator i1, const_iterator i2, const_iterator i3, const_iterator i4)
        {
                ReplaceChunk(i1 - buffer, i2 - i1, i3, i4 - i3);
                return *this;
        }

        void pop_back()
        {
                shrink_by(1);
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

        inline bool null_terminated() const noexcept
        {
                return buffer && buffer[length_] == '\0';
        }

        void make_null_terminated()
        {
                makeNullTerminated();
        }

        inline const char *c_str() const noexcept
        {
                return buffer;
        }

        inline char *c_str() noexcept
        {
                return buffer;
        }

        uint32_t find(const Buffer &str, const uint32_t pos = 0) const noexcept
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).Search(str.AsS32()))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t find(const char *const s, const uint32_t pos = 0) const
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).Search(strwlen32_t(s)))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t find(const char *const s, const uint32_t pos, const uint32_t n) const
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).Search(strwlen32_t(s, n)))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t find(const char c, const uint32_t pos = 0) const
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).Search(c))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t rfind(const Buffer &str, const uint32_t pos = 0) const noexcept
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).SearchR(str.AsS32()))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t rfind(const char *const s, const uint32_t pos = 0) const
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).SearchR(strwlen32_t(s)))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t rfind(const char *const s, const uint32_t pos, const uint32_t n) const
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).SearchR(strwlen32_t(s, n)))
                        return p - buffer;
                else
                        return npos;
        }

        uint32_t rfind(const char c, const uint32_t pos = 0) const
        {
                if (const char *const p = strwlen32_t(buffer + pos, length_ - pos).SearchR(c))
                        return p - buffer;
                else
                        return npos;
        }

        // TODO: find_first_of()
        // TODO: find_last_of()
        // TODO: find_first_not_of()
        // TODO: find_last_not_of()

        Buffer substr(const uint32_t pos, const uint32_t len = npos) const
        {
                return Buffer(buffer + pos, len != npos ? length_ : len);
        }

        int compare(const Buffer &str) const noexcept
        {
                return AsS32().Cmp(str.AsS32());
        }

        int compare(const uint32_t pos, const uint32_t len, const Buffer &str) const
        {
                return strwlen32_t(buffer + pos, len).Cmp(str.AsS32());
        }

        int compare(const uint32_t pos1, const uint32_t cnt1, const Buffer &str, const uint32_t pos2, const uint32_t cnt2) const
        {
                return strwlen32_t(buffer + pos1, cnt1).Cmp(strwlen32_t(str.buffer + pos2, cnt2 - pos2));
        }

        int compare(const char *const s) const
        {
                return AsS32().Cmp(strwlen32_t(s));
        }

      public:
        // _includes_ space reserved for \0
        [[gnu::always_inline]] inline uint32_t Reserved() const
        {
                // no need to check for (buffer != nullptr), malloc_usable_size() does that
                // The problem with this arrangement is that if we are going to use, e.g SetData() to memory we
                // have e.g alloca()ed or mmmap()ed, this will fail
                // UPDATE: we sometimes use SetData() to explicitly set the buffer, and that memory was never allocated from the allocator; it could
                // have been allocated with alloca() or mmap(), and malloc_usable_size() on that ptr would either return bogus values or would result in crashes
                // we can use the unusted pointer upper bits for that -- this adds a small overhead, but it's very convenient
                // https://en.wikipedia.org/wiki/Tagged_pointer
                // UPDATE: not doing it
                return likely(buffer) ? malloc_usable_size(buffer) : 0;
        }

        inline auto reserved() const noexcept
        {
                return Reserved();
        }

        inline auto size() const noexcept
        {
                return length_;
        }

        inline const char *data() const noexcept
        {
                return buffer;
        }

        inline char *data() noexcept
        {
                return buffer;
        }

        inline auto data_at(const uint32_t o) const noexcept
        {
                return buffer + o;
        }

        char *Copy() const
        {
                if (length_)
                {
                        char *const res = (char *)malloc(length_);

                        memcpy(res, buffer, length_);
                        return res;
                }
                else
                        return nullptr;
        }

        // TODO: to text
        static char GetUpper(const char c) noexcept;

        [[gnu::always_inline]] inline static char UppercaseISO88597(const char c)
        {
                return GetUpper(c);
        }

        void Print() const
        {
                AsS32().Print();
        }

        inline strwlen8_t AsS8() const
        {
                return strwlen8_t(buffer, length_);
        }

        inline strwlen16_t AsS16() const
        {
                return strwlen16_t(buffer, length_);
        }

        inline strwlen32_t AsS32() const noexcept
        {
                return strwlen32_t(buffer, length_, strwlen32_t::NoMaxLenCheck{});
        }

        inline strwlen32_t as_s32() const noexcept
        {
                return strwlen32_t(buffer, length_, strwlen32_t::NoMaxLenCheck{});
        }


        inline uint32_t AsUInt32() const
        {
                return strwlen32_t(buffer, length_).AsUint32();
        }

        inline double AsFloat() const
        {
                if (likely(IsNullTerminated()))
                        return atof(buffer);
                else
                        return AsS32().AsDouble();
        }

        inline void SetToEmpty()
        {
                length_ = 0;
        }

        inline bool IsNullTerminated() const noexcept
        {
                return buffer && buffer[length_] == '\0';
        }

        inline void JustFreeBuf()
        {
                WillUpdate();
                if (likely(buffer))
                        ::free(buffer);
        }

        inline operator char *() noexcept
        {
                return buffer ?: const_cast<char *>(""); // Too many uses so we need to do it
        }

        inline operator const char *() const noexcept
        {
                return buffer ?: "";
        }

        inline bool operator==(const char *const data) const noexcept
        {
                return AsS32() == data;
        }

        inline auto operator!=(const Buffer &o) const noexcept
        {
                return AsS32() != o.AsS32();
        }

        inline bool operator!=(const char *const data) const noexcept
        {
                return !(operator==(data));
        }

        inline bool operator==(const Buffer &other) const noexcept
        {
                return length_ == other.length_ ? !memcmp(buffer, other.buffer, length_) : false;
        }

        inline bool IsEqInsensitive(const char *const str) const
        {
                return AsS32().EqNoCase(str);
        }

        inline bool IsNum() const noexcept
        {
                return AsS32().IsDigits();
        }

        inline bool Eq(const char *const p, const uint32_t l) const noexcept
        {
                return length_ == l ? !memcmp(buffer, p, l) : false;
        }

        inline bool IsEqual(const char *const data) noexcept
        {
                return (!data && !buffer) || (data && buffer ? AsS32().Eq(data) : false);
        }

        inline Buffer &operator=(const char *const data)
        {
                clear();
                Append(data);

                return *this;
        }

        auto ToCString(char *out, const size_t outCapacity) const
        {
                return AsS32().ToCString(out, outCapacity);
        }

        inline Buffer &operator=(const Buffer &other)
        {
                clear().Append(other.data(), other.size());

                return *this;
        }

        void AppendTimes(const char *text, const uint32_t len, const uint32_t times);

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
                Append(other.data(), other.size());
                return *this;
        }

        inline Buffer &operator+=(const Buffer &other)
        {
                Append(other.data(), other.size());
                return *this;
        }

        inline const char *At(const uint32_t offset) const
        {
                WillReference(offset);

                return buffer + offset;
        }

        inline char *At(const uint32_t offset)
        {
                WillReference(offset);

                return buffer + offset;
        }

        inline uint32_t OffsetOf(const char *const ptr) noexcept
        {
                return ptr - buffer;
        }

        inline char operator[](const uint32_t index) const
        {
                if (likely(index <= length_))
                        return buffer[index];

                std::abort();
                return 0;
        }

        auto release()
        {
                auto ptr = buffer;

                buffer = nullptr;
                SetReserved(0);
                length_ = 0;
                return ptr;
        }

        void reset()
        {
                if (buffer)
                {
                        ::free(buffer);
                        buffer = nullptr;
                        SetReserved(0);
                        length_ = 0;
                }
        }

        uint32_t CountOf(const char c) const
        {
                return AsS32().CountOf(c);
        }

        inline char CharAt(const uint32_t index) const
        {
                if (likely(buffer && index <= length_))
                        return buffer[index];

                assert(!"Invalid args for CharAt()");
                return 0;
        }

        auto RoomFor(const size_t s)
        {
                WillInsert(s);

                auto *const ptr = buffer + length_;

                length_ += s;
                return reinterpret_cast<uint8_t *>(ptr);
        }

        void InsertSpace(const uint32_t o, const uint32_t s)
        {
                assert(o <= length_);

                WillInsert(s);
                WillTouch(o, length_ - o);
                WillTouch(o, s);

                length_ += s;
                const auto dest = o + s;

                memmove(buffer + dest, buffer + o, length_ - dest);
                buffer[length_] = '\0';
        }

        int ReplacePortion(uint32_t start_offset, uint32_t end_offset, const char *data, const uint32_t l);

        int32_t Replace(const strwlen32_t from, const strwlen32_t to);

        uint32_t Replace(const char needle, const char with);

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

        Buffer()
            : length_{0}
        {
                buffer = nullptr;
                SetOwnsBuffer(false);
                SetReserved(0);
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
            : Buffer(other.data(), other.size())
        {
        }

        virtual ~Buffer()
        {
                if (likely(OwnsBuffer()) && buffer)
                        free(buffer);
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
                (void)o;
                (void)l;
        }

        [[gnu::always_inline]] inline void WillSetLength(const uint32_t l) const {

        }

            [[gnu::always_inline]] inline void WillReference(const uint32_t offset) const {}

                [[gnu::always_inline]] inline void WillUpdate()
        {
        }

        inline void AppendFmt(const char *fmt, ...)
            __attribute__((format(printf, 2, 3)));

        void EnsureSize(const uint32_t newMin)
        {
                const uint32_t r = Reserved();

                WillUpdate();

                if (unlikely(newMin > r))
                {
                        uint32_t newSize = ComputeNewSize(newMin);

                        assert(newSize >= newMin && newSize > length_);

                        try
                        {
                                if (buffer == nullptr || unlikely(!OwnsBuffer()))
                                        buffer = (char *)malloc(sizeof(char) * newSize);
                                else if (length_ == 0)
                                {
                                        free(buffer);
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

                        assert(Reserved() >= newSize);
                        assert(length_ + 1 <= Reserved());

                        if (unlikely(buffer == nullptr))
                        {
                                std::abort();
                        }

                        SetOwnsBuffer(true);

                        // Realloc did NOT copy '\0', for length_ doesn't account for it
                        // We are responsible for setting it whenever we realloc()
                        buffer[length_] = '\0';
                }
        }

        [[gnu::always_inline]] inline void reserve(const uint32_t n)
        {
                EnsureSize(length_ + n + 1); // +1 for trailing \0
        }

        [[gnu::always_inline]] inline void WillInsert(const uint32_t n)
        {
                reserve(n);
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
                Append(buf.data(), buf.size());
        }

        void Append(const char c);

        void Append(const char c, const uint32_t cnt);

        void Append(const uint8_t c);

        void Append(const float f);

        void Append(const int i);

        void Append(const uint32_t i);

        inline void __SetLength(const uint32_t newLength)
        {
                // tread carefuly
                length_ = newLength;
        }

        void SetLengthAndTerm(const uint32_t l)
        {
                length_ = l;

                WillSetLength(length_);
                buffer[length_] = '\0';
        }

        void resize(const uint32_t newLength)
        {
                WillSetLength(newLength);

                Drequire(newLength < Reserved() && buffer);

                length_ = newLength;
                buffer[length_] = '\0';
        }

        void TrimWS();

        void TrimWSAll();

        inline bool IsBlank() const noexcept
        {
                return AsS32().IsBlank();
        }

        inline void StripSuffix(const uint32_t n)
        {
                shrink_by(n);
        }

        void shrink_by(const uint32_t factor)
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
                Drequire(length_ + factor < reserved());
                Drequire(OwnsBuffer());

                length_ += factor;
                buffer[length_] = '\0';
        }

        inline void advance_size(const uint32_t by)
        {
                AdvanceLength(by);
        }

        void InsertChunk(const uint32_t pos, const char *data, const uint32_t dataLength);

        void InsertChunk(const uint32_t pos, const strwlen32_t v)
        {
                InsertChunk(pos, v.p, v.len);
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

        void erase(const range32_t range)
        {
                DeleteChunk(range.offset, range.len);
        }

        int32_t PrepareReplacement(const uint32_t pos, const uint32_t chunkLen, const uint32_t newChunkLen);

        int32_t ReplaceChunk(const uint32_t pos, const uint32_t chunkLen, const char *newChunkData, const uint32_t newChunkLen);

        int32_t ReplaceChunk(const uint32_t pos, const uint32_t chunkLen, const strwlen32_t s)
        {
                return ReplaceChunk(pos, chunkLen, s.p, s.len);
        }

        int32_t ReplaceChunk(const range32_t range, const strwlen32_t s)
        {
                return ReplaceChunk(range.offset, range.len, s.p, s.len);
        }

        inline strwlen32_t Substr(const range32_t r)
        {
                return {buffer + r.offset, r.len};
        }

        // Includes space reserved for \0
        [[gnu::always_inline]] inline uint32_t ActualCapacity() const
        {
                Drequire(OwnsBuffer());
                return Reserved() - length_;
        }

        inline auto capacity() const
        {
                const uint32_t r = Reserved();

                return r > length_ ? (r - length_) - 1 : 0;
        }

        inline void SetData(char *const d)
        {
                buffer = d;
        }

        inline auto AsUint32() const
        {
                return AsS32().AsUint32();
        }

        auto AsUint64() const
        {
                return AsS32().AsUint64();
        }

        inline auto AsInt32() const
        {
                return AsS32().AsInt32();
        }

        inline void set_data(const char *const d, const uint32_t l)
        {
                buffer = const_cast<char *>(d);
                length_ = l;
        }

        // Be careful
        void set_data_and_size(char *const d, const uint32_t s)
        {
                buffer = d;
                length_ = 0;
                SetReserved(s);
        }

        void StripInvalidTextCharacters();

        inline bool BeginsWithNoCase(const char *const p, const uint32_t l) const
        {
                return AsS32().BeginsWithNoCase(p, l);
        }

        inline bool BeginsWith(const char *p) const
        {
                return AsS32().BeginsWith(p);
        }

        inline auto BeginsWith(const char c) const
        {
                return AsS32().BeginsWith(c);
        }

        inline bool BeginsWith(const char *const p, const uint32_t l) const
        {
                return AsS32().BeginsWith(p, l);
        }

        inline bool EndsWith(const char *const p, const uint32_t l) const
        {
                return AsS32().EndsWith(p, l);
        }

        inline bool EndsWith(const char *const p)
        {
                return AsS32().EndsWith(p);
        }
        auto &pad(const uint32_t n)
        {
                memset(RoomFor(n), 0, n);
                return *this;
        }

        void PadUptoWith(uint32_t n, const char c)
        {
                WillUpdate();

                if (n <= length_)
                        return;

                reserve(n - length_ + 8);
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
        inline auto &append(Arg &&... args)
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

        inline auto &append(std::initializer_list<char> il)
        {
                reserve(il.size());
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
#ifndef USE_MALLOC_USABLE_SIZE
        uint32_t _reserved;
#endif
        [[gnu::always_inline]] inline void SetOwnsBuffer(const bool v)
        {
                (void)v; // NO-OP for now
        }

        inline void SetReserved(const uint32_t newSize)
        {
#ifdef USE_MALLOC_USABLE_SIZE
                (void)newSize;
#else
                _reserved = newSize;
#endif
        }
};

template <typename... Args>
static inline auto makeBuffer(Args &&... args)
{
        Buffer b;

        ToBuffer(b, std::forward<Args>(args)...);
        return b;
}

class IOBuffer
    : public Buffer
{
      private:
        uint32_t position;

      private:
        static const uint32_t sizeOf;

      public:
        // Specialized for IOBuffer()
        // Buffer::reserve() asks for length_ + n + 1 because
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

        void resize(const uint32_t n)
        {
                Drequire(n <= Reserved());
                length_ = n;
        }

        inline auto offsetToEndRange() const noexcept
        {
                return Switch::make_range(buffer + position, length_ - position);
        }

        inline auto data_at_offset() const noexcept
        {
                return buffer + position;
        }

        inline void CheckMemUsage(const uint32_t maxAllowed)
        {
                if (unlikely(Reserved() > maxAllowed))
                {
                        JustFreeBuf();
                        SetAsEmpty();
                        position = 0;
                }
        }

        inline void set_data(char *const d, const uint32_t l) noexcept
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
                b.release();
                b.reset_offset();
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
                reset();

                buffer = o.buffer;
                length_ = o.length_;
                position = o.position;
                SetReserved(o.Reserved());

                o.release();
                o.reset_offset();

                return *this;
        }

        void clear()
        {
                Buffer::clear();
                position = 0;
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

        inline void AdvancePosition(const uint32_t pos) noexcept
        {
                position += pos;
        }

        void SetOffset(const uint64_t o) noexcept
        {
                position = (uint32_t)o;
        }

        inline void SetOffset(const char *const p)
        {
                position = p - buffer;
        }

        inline void set_offset(const uint64_t o)
        {
                position = o;
        }

        inline void reset_offset() noexcept
        {
                position = 0;
        }

        inline void set_offset(const char *const p) noexcept
        {
                position = p - buffer;
        }

        inline auto offset() const noexcept
        {
                return position;
        }

        inline void advance_offset(const uint64_t s) noexcept
        {
                position += s;
        }

        void AdvanceOffsetTo(const char *const p) noexcept
        {
                position = p - buffer;
        }

        void ShiftOffset(const int32_t by)
        {
                position += by;
        }

        // Make sure you know what you are doing here
        void AdjustOffsetAndLength(const uint32_t n)
        {
                position += n;
                length_ -= n;
        }

        inline bool IsPositionAtEnd() const
        {
                return position == length_;
        }

        inline operator char *() noexcept
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

        inline void EnsureLengthRoundedTo(const uint32_t roundedTo)
        {
                pad(RoundToMultiple(capacity(), roundedTo));
        }

        inline void SetDataAt(const uint32_t offset, const void *const b, const uint32_t bSize)
        {
                assert(offset <= length_);

                const uint32_t upto = offset + bSize;
                if (upto < length_)
                {
                        memcpy(buffer + offset, b, bSize);
                        return;
                }

                const uint32_t need = upto - length_;

                WillInsert(need);

                memcpy(buffer + offset, b, bSize);
                length_ = upto;
                buffer[length_] = '\0';
        }

        inline void SetDataAtWithPadding(const uint32_t offset, const void *b, const uint32_t bSize, const uint8_t padWith)
        {
                const uint32_t upto = offset + bSize;

                if (upto > length_)
                {
                        EnsureSize(upto + 1);
                        memset(buffer + length_, padWith, upto - length_);
                }
                SetDataAt(offset, b, bSize);
        }

        inline void SerializePackedUInt32WithLenPrefix(const uint32_t n);
        inline uint32_t UnserializePackedUInt32WithLenPrefix();
        inline void SerializeVarUInt32(const uint32_t n);
        inline uint32_t UnserializeVarUInt32();
	inline void encode_varuint32(const uint32_t n);

        inline void encode_varbyte32(const uint32_t n)
        {
		WillInsert(5);

		auto ptr = buffer + length_;
		const auto b{ptr};

                varbyte_put32(ptr, n);
		length_ += ptr - b;
        }

        // we are no longer using PushBinary(). clang will optimize away memcpy() for size (1,2,4,8)
        // to e.g *(uin64_t *)ptr = v
        // Using PushBinary() is another call and doesn't give the chance to the compiler to deterministically(compile time)
        // compile it away. This works way better.
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

	[[gnu::always_inline]] void serialize(const void *p, const uint32_t size)
        {
                memcpy(RoomFor(size), p, size);
        }

        inline void *Peek(const uint32_t size) noexcept
        {
                return buffer + position;
        }

        inline bool at_end() const noexcept
        {
                return position == length_;
        }

        template <typename T>
        inline T Unserialize() noexcept
        {
                const T r = *(T *)(buffer + position);

                position += sizeof(T);
                return r;
        }

        template <typename T>
        inline void Unserialize(T *const dst) noexcept
        {
                memcpy(dst, buffer + position, sizeof(T));
                position += sizeof(T);
        }

        inline void Unserialize(void *const p, const uint32_t size)
        {
                memcpy(p, buffer + position, size);
                position += size;
        }

        inline auto offset_end_span() const noexcept
        {
                return length_ - position;
        }

        inline strwlen32_t suffix_from_offset() const noexcept
        {
                return {buffer + position, length_ - position, strwlen32_t::NoMaxLenCheck{}};
        }

        // http://en.wikipedia.org/wiki/Variadic_template
        // http://en.cppreference.com/w/cpp/language/parameter_pack
        // e.g
        // b->pack(10, var, var2...)
        auto &pack()
        {
                return *this;
        }

        void unpack()
        {
        }

        template <typename T, typename... Args>
        auto &pack(const T v, Args &&... args)
        {
                Serialize(v);
                pack(std::forward<Args>(args)...);
                return *this;
        }

        template <typename T, typename... Args>
        void unpack(T &v, Args &... args)
        {
                Unserialize(&v);
                unpack(args...);
        }

        [[gnu::always_inline]] inline bool IsAtEnd() const
        {
                return position == length_;
        }

        void reset()
        {
                Buffer::reset();
                position = 0;
        }
};

template <typename VT, typename LT>
static inline void PrintImpl(Buffer &out, const range_base<VT, LT> &r)
{
        out.AppendFmt("[%" PRIu64 ", %" PRIu64 ")", uint64_t(r.start()), uint64_t(r.stop()));
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

void Buffer::AppendFmt(const char *fmt, ...)
{
        WillUpdate();

        va_list args;
        const int32_t capacity = this->capacity();

        (void)va_start(args, fmt);
        const auto len = vsnprintf(buffer + length_, capacity, fmt, args);

        if (unlikely(len < 0))
        {
                (void)va_end(args);
                return;
        }
        else if (len >= capacity)
        {
                reserve(len + 1);

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

static inline void PrintImpl(Buffer &out, const duration_repr &r)
{
        out.reserve(32);
        out.AdvanceLength(r.Get(out.end()).len);
}

struct escaped_repr
{
        const strwlen32_t v;

        escaped_repr(const void *const p, const uint32_t len)
            : v((char *)p, len)
        {
        }

        template <typename LT, typename CT = char>
        escaped_repr(const strwithlen<LT, CT> s)
            : v(s.p, s.len)
        {
        }
};

static inline void PrintImpl(Buffer &out, const escaped_repr &e)
{
        out.AppendEscaped(e.v.p, e.v.len);
}

static inline void PrintImpl(Buffer &out, const std::string &s)
{
        out.Append(s.data(), s.size());
}


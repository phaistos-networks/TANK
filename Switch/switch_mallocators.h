#pragma once
#include <system_error>
#include <sys/mman.h>
#include "switch_ll.h"
#include "ansifmt.h"

struct simple_allocator {
        enum class BackingStore : uint8_t {
                MMAP = 1
        };

        int8_t *       first_{nullptr}, *last_{nullptr}, *cur_{nullptr};
        const uint32_t bankCapacity_;
        uint32_t       curBankUtilization_;

        static constexpr uint32_t buildBankCapacity(const uint32_t v, const bool f) {
                return v | (uint32_t(f) << 31);
        }

        static inline int8_t *next(int8_t *const ptr) {
                return (int8_t *)*(uintptr_t *)ptr;
        }

        static inline void setNext(int8_t *const a, const int8_t *const next) {
                *(uintptr_t *)a = uintptr_t(next);
        }

        inline uint32_t bankCapacity() const {
                return bankSize() - sizeof(uintptr_t);
        }

        inline uint32_t bankSize() const {
                return bankCapacity_ & INT32_MAX;
        }

        bool can_allocate(const std::size_t size) const noexcept {
                return bankSize() >= size;
        }

        simple_allocator &operator=(simple_allocator &&o) {
                require(bankCapacity_ == o.bankCapacity_);

                first_ = o.first_;
                last_  = o.last_;
                cur_   = o.cur_;

                curBankUtilization_ = o.curBankUtilization_;

                // we can't touch o again
                o.first_ = o.last_ = o.cur_ = nullptr;
                o.curBankUtilization_       = 0;
                return *this;
        }

        explicit simple_allocator(simple_allocator &&o)
            : bankCapacity_(o.bankCapacity_) {
                first_ = o.first_;
                last_  = o.last_;
                cur_   = o.cur_;

                curBankUtilization_ = o.curBankUtilization_;

                // we can't touch o again
                o.first_ = o.last_ = o.cur_ = nullptr;
                o.curBankUtilization_       = 0;
        }

        int MLock() {
                const auto capacity = bankSize();

                for (auto it = first_; it; it = next(it)) {
                        if (const auto r = mlock(it, capacity))
                                return r;
                }
                return 0;
        }

        int MUnlock() {
                const auto capacity = bankSize();

                for (auto it = first_; it; it = next(it)) {
                        if (const auto r = munlock(it, capacity))
                                return r;
                }

                return 0;
        }

        // if you want to make sure that a page holds at least e.g sizeof(foo), use simple_allocator::minBankSizeForSize(sizeof(foo))
        // This is because we reserve sizeof(uintptr_t) first bytes from a bank
        static inline size_t minBankSizeForSize(const uint32_t s) {
                return s + sizeof(uintptr_t);
        }

        simple_allocator(const uint32_t bc = 1024 * 1024)
            : bankCapacity_(buildBankCapacity(goodMallocSize(Clamp(RoundToMultiple(bc, 8), 64, 64 * 1024 * 1024)), false)) {
                curBankUtilization_ = bankSize();
        }

        simple_allocator(const uint32_t bc, const enum BackingStore)
            : bankCapacity_(buildBankCapacity(RoundToMultiple(bc, getpagesize()), true)) {
                curBankUtilization_ = bankSize();
        }

        ~simple_allocator() {
                _FlushBanks();
        }

        inline bool canFitInCurBank(const uint32_t size) const // for debugging
        {
                return curBankUtilization_ + size <= bankSize();
        }

        auto curBankAvail__() const // for debugging
        {
                return !first_ ? bankCapacity() : bankSize() - curBankUtilization_;
        }

        auto banksCount() const {
                uint32_t cnt{0};

                for (auto it = first_; it; it = next(it))
                        ++cnt;
                return cnt;
        }

        size_t footprint() const {
                return banksCount() * bankSize() + sizeof(*this);
        }

        void allocNewBank_(const uint32_t bs) {
                if (cur_ == last_) {
                        int8_t *newBank;

                        if (bankCapacity_ & (1u << 31)) {
                                newBank = (int8_t *)mmap(nullptr, bs, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

                                require(newBank != MAP_FAILED);
#ifdef MADV_NOHUGEPAGE
                                // This is important, read:
                                // Via @mattsta: https://twitter.com/mattsta/status/529327782351097856
                                // > Other products/vendors that recommend to disable THP: cloudera, vertica, varnish, mongo, websphere, maridb, intel, ...
                                // Redis now recomends:
                                // echo never > /sys/kernel/mm/transparent_hugepage/enabled
                                // Also refs:
                                // - http://dev.nuodb.com/techblog/linux-transparent-huge-pages-jemalloc-and-nuodb
                                // - http://www.percona.com/blog/2014/07/23/why-tokudb-hates-transparent-hugepages/
                                // > The classical problem with memory allocators is fragmentation. If you allocated a 2MB chunk from the OS
                                // > typically using mmap(), as the process runs it's likely some of that 2MB memory block will become free but
                                // > not all of it, hence it can't be given back to the OS completely. jamalloc and other allocators use a clever trick
                                // > where they use madvise(..., MADV_DONTNEED) to give back portions of memory allocated in such a way back to the OS.
                                //
                                // > With THP, the OS (and the CPU, really) work with pages of much larger size which only can be unmapped from the address
                                // > space in its entirety - which does not work when smaller objects are freed which produce smaller free "holes"
                                // > As a result, without being able to free memory efficientl, the mount of allocated memory may go up unbound until
                                // > the process starts to swap out - and in the end being killed by the OOMK at least under some workloads.
                                // > This is not a behavior you want to see from a database server.
                                madvise(newBank, bs, MADV_NOHUGEPAGE);
#endif
                        } else {
                                try {
                                        newBank = (int8_t *)malloc(bs);
                                } catch (...) {
                                        newBank = nullptr;
                                }

                                assert(newBank);

                                // We can't use capacity = malloc_usable_size(curBank) here, because we won't always get the same
                                // usable size for each bank allocated (no guarantees, may be more or less). So, unless we keep
                                // track of usability per bank, we can't use this.
                        }

                        setNext(newBank, nullptr);

                        if (last_)
                                setNext(last_, newBank);
                        else
                                first_ = newBank;

                        cur_ = last_ = newBank;
                } else {
                        cur_ = next(cur_);
                }
        }

        void *Alloc(const uint32_t size) {
                const auto bs = bankSize();

                // micro-optimization: we expect to be able to use current bank
                auto *const res = reinterpret_cast<char *>(cur_ + curBankUtilization_);

                curBankUtilization_ += size;
                if (unlikely(curBankUtilization_ > bs)) {
                        require(size <= bankCapacity());

                        // it is important that we do not inline allocNewBank_() code here
                        // in order to reduce ICache miss rate -- because we almost never need to execute that code
                        allocNewBank_(bs);
                        curBankUtilization_ = size + sizeof(uintptr_t);

                        return reinterpret_cast<char *>(cur_ + sizeof(uintptr_t));
                } else
                        return res;
        }

        // allocWithLock() and other WithLock() methods are very handy
        // if you really want to share an allocator among multiple threads, in a thread safe manner
        // this is not recommended, however it may be a good idea if the contention is low and if memory fragementation and pressure is causing problems
        template <typename T>
        void *allocWithLock(const uint32_t size, T &lock) {
                const auto bs = bankSize();

                if (unlikely(curBankUtilization_ + size > bs)) {
                        require(size <= bankCapacity());

                        lock.lock();

                        allocNewBank_(bs);
                        curBankUtilization_ = size + sizeof(uintptr_t);

                        auto *const res = reinterpret_cast<char *>(cur_ + sizeof(uintptr_t));

                        lock.unlock();

                        return res;
                } else {
                        lock.lock();

                        char *const ret = reinterpret_cast<char *>(cur_ + curBankUtilization_);

                        curBankUtilization_ += size;

                        lock.unlock();

                        return ret;
                }
        }

        inline void reuse() {
                if (first_) {
                        cur_                = first_;
                        curBankUtilization_ = sizeof(uintptr_t);
                } else {
                        cur_                = nullptr;
                        curBankUtilization_ = bankSize();
                }
        }

        void _FlushBanks() {
                if (bankCapacity_ & (1u << 31)) {
                        const auto capacity = bankSize();

                        for (auto it = first_; it;) {
                                auto n = next(it);

                                madvise(it, capacity, MADV_DONTNEED);
                                munmap(it, capacity);
                                it = n;
                        }
                } else {
                        for (auto it = first_; it;) {
                                auto n = next(it);

                                std::free(it);
                                it = n;
                        }
                }

                first_ = last_ = cur_ = nullptr;
        }

        void reset() {
                _FlushBanks();

                curBankUtilization_ = bankSize();
                cur_ = first_ = last_ = nullptr;
        }

        inline void *CAlloc(const uint32_t size) {
                void *const p = Alloc(size);

                if (likely(p))
                        memset(p, 0, size);

                return p;
        }

        template <typename LT, typename CT = char>
        auto make_copy(const strwithlen<LT, CT> &s) {
                return strwithlen<LT, CT>(CopyOf(s.data(), s.size()), s.size());
        }


        template <typename T>
        T *CopyOf(const T *const v, const uint32_t n) {
                T *const res = (T *)Alloc(n * sizeof(T));

                Drequire(res);
                memcpy(res, v, n * sizeof(T));
                return res;
        }

        template <typename LT, typename CT = char>
        CT *CopyOf(const strwithlen<LT, CT> &s) {
                return CopyOf(s.p, s.len);
        }

        template <typename T>
        T *CopyOf(const T *const v) {
                return CopyOf(v, 1);
        }

        template <typename T>
        inline T *New() {
                return (T *)Alloc(sizeof(T));
        }

        template <typename T>
        inline T *Alloc() {
                return (T *)Alloc(sizeof(T));
        }

        template <typename T>
        inline T *Alloc(const uint32_t cnt) {
                return (T *)Alloc(sizeof(T) * cnt);
        }

        template <typename T, typename... Args>
        inline T *construct(Args &&... args) {
                return new (Alloc(sizeof(T))) T(std::forward<Args>(args)...);
        }

        template <typename T, typename LT, typename... Args>
        inline T *constructWithLock(LT &lock, Args &&... args) {
                return new (allocWithLock(sizeof(T), lock)) T(std::forward<Args>(args)...);
        }

        template <typename T>
        inline void destroy(T *const ptr) {
                ptr->~T();
        }
};

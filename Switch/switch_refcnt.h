#pragma once
#include "switch_atomicops.h"

struct RCObject
{
	virtual void Release() = 0;

	virtual void Retain() = 0;

	virtual int32_t RetainCount() const = 0;

	virtual int32_t use_count() const
	{
		return RetainCount();
	}

	virtual ~RCObject()
	{

	}
};


template <class T>
struct RefCounted 
	: public RCObject
{
	private:
		int32_t rc = 1;

	public:
		~RefCounted()
		{
			assert(__atomic_load_n(&rc, MemoryModel::RELAXED) == 0);
		}

		void SetOneRef()
		{
			__atomic_store_n(&rc, 1, MemoryModel::RELAXED);
		}

		void ResetRefs()
		{
			// Useful when you allocat on stack - otherwise destructor will abort
			rc = 0;
		}

		[[gnu::always_inline]] inline int32_t RetainCount() const override
		{
			return rc;
		}

		[[gnu::always_inline]] inline void TryRelease()
		{	
			const auto p = (uintptr_t)this;

			if (likely(p))
				Release();
		}

		[[gnu::always_inline]] inline void Retain() override
		{
			const auto now = __atomic_add_fetch(&rc, 1, MemoryModel::RELAXED);

			require(now > 1); // can't go from 0 to 1
		}

		[[gnu::always_inline]] inline bool ReleaseAndTestNoRefs()
		{
			return  __atomic_sub_fetch(&rc, 1, MemoryModel::RELEASE) == 0;
		}


		[[gnu::always_inline]] inline void Release() override
		{
			const auto res = __atomic_sub_fetch(&rc, 1, MemoryModel::RELEASE);

			require(res >= 0);

			if (!res)
			{
				std::atomic_thread_fence(std::memory_order_acquire);
				delete static_cast<T *>(this);
			}
		}

		[[gnu::always_inline]] inline void ReleaseNoDealloc()
		{
			if (!__atomic_sub_fetch(&rc, 1, MemoryModel::RELEASE))
			{
				std::atomic_thread_fence(std::memory_order_acquire);
				static_cast<T *>(this)->~T();
			}
		}
};


namespace Switch
{
	template<class T>
	struct shared_refptr
	{
		T *v;

		shared_refptr(T *p, const bool)
			: v{p}
		{

		}

		shared_refptr(T *p)
			: v{p}
		{
			if (v)
				v->Retain();
		}

		~shared_refptr()
                {
                        if (v)
                                v->Release();
                }

		shared_refptr()
			: v{nullptr}
		{

		}

		shared_refptr(std::nullptr_t)
			: v{nullptr}
		{

		}

		shared_refptr(const shared_refptr &o)
			: v{o.v}
                {
                        if (v)
                                v->Retain();
                }

		shared_refptr(shared_refptr &&o)
			: v{o.v}
		{
			o.v = nullptr;
		}

		auto &operator=(const shared_refptr &o)
		{
			if (o.v)
				o.v->Retain();
			if (v)
				v->Release();
			v = o.v;
			return *this;
		}

                auto &operator=(shared_refptr &&o)
                {
                        if (v != o.v)
                        {
                                if (v)
                                        v->Release();
                                v = o.v;
                                o.v = nullptr;
                        }
                        return *this;
                }

		void reset(T *const o)
		{
			if (v != o)
			{
				if (v)
					v->Release();

				v = o;
				if (v)
					v->Retain();
			}
		}
		T *release()
		{
			auto res = v;

			v = nullptr;
			return res;
		}

		[[gnu::always_inline]] inline T *get() const
		{
			return v;
		}

		[[gnu::always_inline]] inline T &operator *() const
		{
			return *v;
		}

		[[gnu::always_inline]] inline T *operator->() const
		{
			return v;
		}

		uint32_t use_count() const
		{
			return v ? v->use_count() : 0;
		}

		operator bool() const
		{
			return v != nullptr;
		}

		auto unique() const
		{
			return use_count() == 1;
		}

                // handy -- although not in std::unique_ptr<> API
                inline operator T *()
                {
                        return v;
                }
        };

	template<typename T>
	static inline auto make_sharedref(T *v)
	{
		return shared_refptr<T>(std::move(v), true);
	}

	template<typename T>
	static inline auto make_sharedref_retained(T *v)
	{
		return shared_refptr<T>(v);
	}
};

template <typename T>
static inline void tryRelease(T *const p)
{
        if (p)
                p->Release();
}

#pragma once
#include <unordered_map>

namespace Switch
{
        template <class Key, class T>
        class unordered_map
            : public std::unordered_map<Key, T>
        {
		public:
			struct kv
			{
				T v;

				const T &value() const
				{
					return v;
				}
			};

		public:
			bool Add(const Key &k, const T &v)
			{
				return this->insert({k, v}).second;
			}

			bool Remove(const Key &k)
			{
				return this->erase(k);
			}

			kv detach(const Key &k)
			{
				auto it = this->find(k);

				if (it != this->end())
				{
					auto v = std::move(it->second);

					this->erase(it);
					return {v};
				}
				else
					return {};
			}
        };
}

namespace std
{
	template<>
	struct hash<void *>
	{
		using argument_type = void*;
                using result_type = std::size_t;

                std::size_t operator()(const void *const ptr) const
		{
			return std::hash<uintptr_t>{}(uintptr_t(ptr));
		}
	};
}



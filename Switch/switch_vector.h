#pragma once
#include <vector>

namespace Switch
{
        template <class T>
        class vector
            : public std::vector<T>
        {
		using Super = std::vector<T>;

              public:
                void RemoveByValue(const T v)
                {
			const auto e = Super::end();
                        auto it = std::find(Super::begin(), e, v);

                        if (it != e)
                                Super::erase(it, it + 1);
                }

                T &&Pop()
                {
                        auto last = std::move(Super::back());

                        Super::pop_back();
                        return std::move(last);
                }

                auto values()
                {
                        return Super::data();
                }

                typename std::vector<T>::iterator begin()
                {
                        return Super::begin();
                }

                typename std::vector<T>::iterator end()
                {
                        return Super::end();
                }

                typename std::vector<T>::const_iterator begin() const
                {
                        return Super::begin();
                }

                typename std::vector<T>::const_iterator end() const
                {
                        return Super::end();
                }

		void pop_front()
		{
			auto it = Super::begin();

			Super::erase(it, it + 1);
		}

		void Append(T *const list, const size_t n)
		{
			Super::reserve(n);
			for (size_t i{0}; i != n; ++i)
				Super::push_back(list[i]);
		}

		void PopByIndex(const size_t idx)
		{
			auto it = Super::begin() + idx;

			Super::erase(it, it + 1);
		}

        };
}

#pragma once
#include <vector>

namespace Switch
{
        template <class T>
        class vector
            : public std::vector<T>
        {
              public:
                void RemoveByValue(const T v)
                {
                        const auto e = this->end();
                        auto it = std::find(this->begin(), e, v);

                        if (it != e)
                        {
                                const auto n = this->size();

                                this->erase(it);
                                require(this->size() + 1 == n);
                        }
                }

                T Pop()
                {
                        auto last{this->back()};

                        this->pop_back();
                        return last;
                }

                auto values() noexcept
                {
                        return this->data();
                }

                void pop_front()
                {
                        auto it = this->begin();

                        this->erase(it);
                }

                void Append(T *const list, const size_t n)
                {
                        this->reserve(n);
                        for (size_t i{0}; i != n; ++i)
                                this->push_back(list[i]);
                }

                void PopByIndex(const size_t idx)
                {
                        this->erase(this->begin() + idx);
                }
        };
}

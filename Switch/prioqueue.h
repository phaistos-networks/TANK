#pragma once
#include <switch.h>

// A simple binary heap / prio.queue, of fixed capacity.
// It provides constant time lookup of the smallest(by default) element, at the expense of logarithmic insertion and extraction.
// A user-provided Compare can be supplied to chage the ordering, e.g using std::greater<T> would cause the LARGEST element to appear as the top().
// This is the inverse of std::priority_queue<> semantics, where the Compare function defines the order from end to top, this implementation's Compare 
// defines the order from top to end.
// e.g if you are using std::greater<T> and capacity is set to 3, it will track the lowest 3 values, with top() being the largest among them.
// e.g if you are using std::less<T> and capacity is set to 3, it will track the highest 3 values, with top() being the smallest among them
//
// It's more flexible and faster than std::priority_queue<>, and it also
// supports faster top value updates via update_top(). It was 32% on a specific use case benchmarked against std::priority_queue<>
//
// base-1 not base-0, because it affords some nice optimizations
// Based on Lucene's PriorityQueue design and impl.
namespace Switch
{
        template <typename T, class Compare = std::less<T>>
        class priority_queue
        {
              public:
                using const_reference = const T &;
                using reference = T &;
                using value_type = T;

              private:
                const uint32_t capacity_;
                uint32_t size_{0};
                T *const heap;

              private:
                bool up(const uint32_t orig)
                {
                        auto i{orig};
                        auto node = std::move(heap[i]);
                        const Compare cmp;

                        for (uint32_t j = i / 2; j && cmp(node, heap[j]); j /= 2)
                        {
                                heap[i] = std::move(heap[j]); // shift parents down
                                i = j;
                        }

                        heap[i] = std::move(node); // install saved node
                        return i != orig;
                }

#if 0 // should have been faster, but it's not
                void down(uint32_t i)
                {
                        uint32_t j = i << 1; // smaller child(left)

                        if (j > size_)
                                return;

                        auto node = std::move(heap[i]); // top node
                        uint32_t k = j + 1;             // (right)
                        const Compare cmp;

                        for (j = (k <= size_ && cmp(heap[k], heap[j])) ? k : j;
                             cmp(heap[j], node);
                             k = j + 1, j = (k <= size_ && cmp(heap[k], heap[j])) ? k : j)
                        {
                                heap[i] = std::move(heap[j]); // shift up child
                                i = j;
                                j = i << 1;

                                if (j > size_)
                                        break;
                        }

                        heap[i] = std::move(node);
                }
#else
                void down(uint32_t i)
                {
                        uint32_t j = i << 1;            // smaller child(left)

			if (j <= size_)
                        {
                        	auto node = std::move(heap[i]); // top node
                                uint32_t k = j + 1; // (right)
                                const Compare cmp;

                                if (k <= size_ && cmp(heap[k], heap[j]))
                                {
                                        // right child exists and smaller than left
                                        j = k;
                                }

                                while (j <= size_ && cmp(heap[j], node))
                                {
                                        heap[i] = std::move(heap[j]); // shift up child
                                        i = j;
                                        j = i << 1;
                                        k = j + 1;

                                        if (k <= size_ && cmp(heap[k], heap[j]))
                                                j = k;
                                }

                                heap[i] = std::move(node);
                        }
                }
#endif

              public:
                priority_queue(const uint32_t m)
                    : capacity_{m}, heap((T *)malloc(sizeof(T) * (0 == capacity_ ? 2 : capacity_ + 1))) // base-1 based, heap[0] is not used
                {
                        if (unlikely(!heap))
                                throw Switch::data_error("Failed to allocate memory");
                }

                ~priority_queue()
                {
                        std::free(heap);
                }

		// Using a stack here because recurssion is likely more expensive than
		// maintaining a thread_local stc::vector<>
		//
		// XXX: we are not checking if (false == empty()). Make sure your application does before
		// invoking this here method
		template<typename ProcessTop, typename CompareTop = std::equal_to<T>>
		void for_each_top(ProcessTop &&process, CompareTop &&cmp = CompareTop{})
                {
                        static thread_local std::vector<uint32_t> stackTLS;
                        auto &stack{stackTLS};
                        const auto s{size()};
                        const auto h{data()};
			const auto top{h[0]};

                        stack.clear();
                        process(top);
                        if (s >= 3)
                        {
                                stack.push_back(1);
                                stack.push_back(2);

                                do
                                {
                                        const auto i = stack.back();

                                        stack.pop_back();
                                        if (auto it = h[i]; cmp(top, it))
                                        {
                                                process(it);

                                                const auto left = ((i + 1) << 1) - 1;
                                                const auto right = left + 1;

                                                if (right < s)
                                                {
                                                        stack.push_back(left);
                                                        stack.push_back(right);
                                                }
                                                else if (left < s && cmp(top, (it = h[left])))
                                                {
                                                        process(it);
                                                }
                                        }
                                } while (stack.size());
                        }
                        else if (s == 2 && cmp(top, h[1]))
                                process(h[1]);
                }

                void clear()
                {
                        size_ = 0;
                }

                // Attempts to push a new value
                // It returns false if a value was dropped to make space for it, or `v` can't be inserted due to capacity_ constraints
                // if false is returned, prev is assigned either v or the value that was dropped to make space
                bool try_push(const T v, T &prev)
                {
                        if (size_ < capacity_)
                        {
                                push(v);
                                return true;
                        }
                        //WAS: else if (size_ && !Compare{}(v, heap[1]))	 
			// This wasn't correct because e.g if Compare is std::less<unsigned>
			// and (v == 5 && heap[1]) == 5,  !std::less<unsined>(5, 5) would be true
			// so we 'd end up invoking update_top(), whereas now that we just check
			// for std::less<unsigned>{}(5,5) it returns false, and
			// std::less<unsigned>{}(8, 5) would return true (we 'd need to replace
			// 	top=5 with 8)
                        else if (size_ && Compare{}(heap[1], v))	 // need to check if (size_t) because a queue can be empty
                        {
                                prev = std::move(heap[1]);
                                heap[1] = std::move(v);
                                update_top();
                                return false;
                        }
                        else
                        {
                                prev = v;
                                return false;
                        }
                }

                inline void push(const T &v)
                {
#if 0 // use try_push() if you need capacity_ checks
                        if (unlikely(size_ == capacity_))
                                throw Switch::data_error("Full");
#endif

                        heap[++size_] = v;
                        up(size_);
                }

                inline void push(T &&v)
                {
#if 0
                        if (unlikely(size_ == capacity_))
                                throw Switch::data_error("Full");
#endif
                        heap[++size_] = std::move(v);
                        up(size_);
                }

                // push_back() does NOT ensure the heap semantics are upheld
                // this is really only useful if you want to e.g populate a pq until
                // you reach a size and then use make_heap() and from then on, you use push()/pop()
                // Make sure you know what you are doing here
                inline void push_back(T &&v)
                {
                        heap[++size_] = std::move(v);
                }

                inline void push_back(const T &v)
                {
                        heap[++size_] = v;
                }

                void make_heap()
                {
                        struct
                        {
                                inline bool operator()(const T &a, const T &b) noexcept
                                {
                                        // we are comparing (b,a), not (a,b) because
                                        // the semantics are inverted in STL
                                        return Compare{}(b, a);
                                }
                        } hp;

                        std::make_heap(heap + 1, heap + 1 + size_, hp);
                }

                T pop() noexcept
                {
                        // won't check if (size == 0), so that we can use noexcept
                        // but you should check
                        const auto res = std::move(heap[1]);

                        heap[1] = std::move(heap[size_--]); // remember, base-1
                        down(1);
                        return res;
                }

                constexpr auto capacity() const noexcept
                {
                        return capacity_;
                }

                constexpr auto size() const noexcept
                {
                        return size_;
                }

                constexpr auto empty() const noexcept
                {
                        return 0 == size_;
                }

                // Not sure why you 'd need data() though, because
                // it will be out of order, but here it is (maybe you want to
                // store the values somewhere without draining the queue)
                constexpr auto data() const noexcept
                {
                        return heap + 1; // base 1
                }

                constexpr auto data() noexcept
                {
                        return heap + 1; // base 1
                }

                [[gnu::always_inline]] const_reference top() const noexcept
                {
                        // not check if (size == 0)
                        // so we can use noexcept, but make sure you know what you are doing
                        return heap[1];
                }

                [[gnu::always_inline]] reference top() noexcept
                {
                        return heap[1];
                }

                // invoke when the top changes value
                // still log(n) worse case, but it's x2 faster compared to
                // { auto v = pop(); o.update(); push(o); }
                [[gnu::always_inline]] void update_top() noexcept
                {
                        down(1);
                }

                inline void update_top(const_reference v)
                {
                        heap[1] = v;
                        down(1);
                }

                inline void update_top(T &&v)
                {
                        heap[1] = std::move(v);
                        down(1);
                }

                // Removes an existing value. Cost is linear with the size of queue.
                bool erase(const T v)
                {
                        for (uint32_t i{1}; i <= size_; ++i)
                        {
                                if (heap[i] == v)
                                {
                                        heap[i] = std::move(heap[size_--]);
                                        if (i <= size_)
                                        {
                                                if (!up(i))
                                                {
                                                        down(i);
                                                }
                                        }
                                        return true;
                                }
                        }
                        return false;
                }

		inline auto begin() const noexcept
		{
			return data() + 0;
		}

                inline auto end() const noexcept
                {
                        return data() + size_;
                }
        };
}

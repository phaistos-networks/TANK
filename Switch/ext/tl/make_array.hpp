///
// TL library - A collection of small C++ utilities
// Written in 2017 by Simon Brand (@TartanLlama)
//
// To the extent possible under law, the author(s) have dedicated all
// copyright and related and neighboring rights to this software to the
// public domain worldwide. This software is distributed without any warranty.
//
// You should have received a copy of the CC0 Public Domain Dedication
// along with this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
///
// A minimal implementation of make_array
///

#ifndef TL_MAKE_ARRAY_HPP
#define TL_MAKE_ARRAY_HPP

#include <array>
#include <type_traits>
#include <utility>

namespace tl {
    template <class... Ts>
    constexpr std::array<
                 typename std::decay<typename std::common_type<Ts...>::type
              >::type, sizeof...(Ts)>
    make_array(Ts&&... ts) {
        return std::array<
                 typename std::decay<typename std::common_type<Ts...>::type>::type, sizeof...(Ts)
               >{ std::forward<Ts>(ts)... };
    }
}

#endif

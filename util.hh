#pragma once

#include <ranges>
#include <string>
#include <vector>

namespace util {

template <class... Ts> struct cases : Ts... {
    using Ts::operator()...;
};

template <typename V, typename... Vs> auto concat(V&& first, Vs&&... rest)
{
    using T = typename std::remove_cvref_t<V>::value_type;
    std::vector<T> result;
    result.reserve(first.size() + (rest.size() + ...));

    auto append = [&]<typename U>(U&& vec) {
        if constexpr (std::is_lvalue_reference_v<U>)
            result.append_range(vec);
        else
            result.append_range(vec | std::views::as_rvalue);
    };

    append(std::forward<V>(first));
    (append(std::forward<Vs>(rest)), ...);
    return result;
}

template <typename T> struct construct {
    template <typename... Args> T operator()(Args&&... args) const
    {
        return T(std::forward<Args>(args)...);
    }
};

std::string replace_all(std::string_view s, std::string_view from, std::string_view to);

template <typename R, typename V> long index_of(R r, V v)
{
    return std::ranges::find(r, v) - r.begin();
}

} // namespace util

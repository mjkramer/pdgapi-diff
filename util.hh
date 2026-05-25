#pragma once

#include <string>

namespace util {

template <class... Ts> struct cases : Ts... {
    using Ts::operator()...;
};

std::string replace_all_copy(std::string_view s, std::string_view from, std::string_view to);

} // namespace util

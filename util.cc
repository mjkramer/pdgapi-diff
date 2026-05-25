#include "util.hh"

using namespace std;

namespace util {

std::string replace_all_copy(std::string_view s, std::string_view from,
                             std::string_view to)
{
    std::string result;
    if (from.empty())
        return std::string{s};
    result.reserve(s.size());
    std::size_t pos = 0, prev = 0;
    while ((pos = s.find(from, prev)) != std::string_view::npos) {
        result.append(s, prev, pos - prev);
        result.append(to);
        prev = pos + from.size();
    }
    result.append(s, prev);
    return result;
}
} // namespace util

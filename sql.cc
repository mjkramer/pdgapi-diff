#include <iomanip>

#include "sql.hh"

using namespace std;
using namespace sql;

std::ostream& operator<<(std::ostream& os, const Val& val)
{
    auto write = [&](auto&& v) {
        using T = decay_t<decltype(v)>;
        if constexpr (is_same_v<T, string>)
            os << quoted(v);
        else if constexpr (is_same_v<T, null_t>)
            os << "NULL";
        else
            os << v;
    };
    return os;
}

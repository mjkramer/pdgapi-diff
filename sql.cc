#include <algorithm>
#include <iomanip>

#include "SqlData.hh"

std::string SqlVal::str() const
{
    std::ostringstream os;
    auto write = [&](auto&& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string>)
            os << std::quoted(v);
        else if constexpr (std::is_same_v<T, void*>)
            os << "NULL";
        else
            os << v;
    };
    std::visit(write, *this);
    return os.str();
}

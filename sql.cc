#include <iomanip>
#include <ranges>

#include "sql.hh"

using namespace std;
using namespace sql;

namespace sql {

string to_str(const Val& val)
{
    ostringstream os;
    auto write = [&](auto&& v) {
        using T = decay_t<decltype(v)>;
        if constexpr (is_same_v<T, string>)
            os << quoted(v);
        else if constexpr (is_same_v<T, null_t>)
            os << "NULL";
        else
            os << v;
    };
    visit(write, val);
    return os.str();
}

Ident::Ident(const vector<string>& v) : m_keys(v)
{
}

Ident::Ident(const string& s)
{
    string_view delim("::");
    auto to_str = [](auto r) { return string(r.begin(), r.end()); };
    m_keys = s | views::split(delim) | views::transform(to_str) | ranges::to<vector>();
}

string Ident::str()
{
    string_view delim("::");
    return m_keys | views::join_with(delim) | ranges::to<string>();
}

long Ident::id_at(size_t idx) { return stoul(m_keys[idx]); }

const string& Ident::operator[](size_t idx) const { return m_keys[idx]; }

string& Ident::operator[](size_t idx) { return m_keys[idx]; }

}

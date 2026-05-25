#include <cmath>
#include <ranges>
#include <string>
#include <variant>

#include "sql.hh"

using namespace std;
using namespace sql;

namespace sql {

static bool isclose(double a, double b, double rel_tol = 1e-6, double abs_tol = 0.0)
{
    const auto max_abs = max(fabs(a), fabs(b));
    return fabs(a - b) <= max(rel_tol * max_abs, abs_tol);
}

bool operator==(const Val& lhs, const Val& rhs)
{
    auto compare = [&](auto&& l) {
        using T = decay_t<decltype(l)>;
        if (not holds_alternative<T>(rhs))
            return false;
        const auto r = get<T>(rhs);

        if constexpr (is_same_v<T, double>)
            return isclose(l, r);
        else
            return l == r;
    };

    return visit(compare, lhs);
}

bool operator==(const Row& lhs, const Row& rhs)
{
    for (const auto& [l, r] : ranges::views::zip(lhs, rhs)) {
        if (l != r)
            return false;
    }
    return true;
}

Ident::Ident(vector<string> v) : m_keys(std::move(v)) {}

Ident::Ident(const string& s)
{
    string_view delim("::");
    auto to_str = [](auto r) { return string(r.begin(), r.end()); };
    m_keys = s | views::split(delim) | views::transform(to_str) | ranges::to<vector>();
}

primkey_t Ident::id_at(size_t idx) const { return stoul(m_keys[idx]); }

const string& Ident::operator[](size_t idx) const { return m_keys[idx]; }

string& Ident::operator[](size_t idx) { return m_keys[idx]; }

} // namespace sql

#include "sql.hh"

#include <ranges>

using namespace sql;
using namespace util;

using namespace std;

string format_val(const Val& val, const Val* other, const string* diff_hl_color)
{
    if (not other)
        return format("{:q}", val);

    const auto s1 = format("{:q}", val);
    const auto s2 = format("{:q}", *other);
    const auto width = max(s1.size(), s2.size());

    const auto s = format("{:>{}}", s1, width);

    if (diff_hl_color and val != *other)
        return format("{}{}{}", *diff_hl_color, s, ANSI_RESET);
    return format("{}", s);
}

string format_row(const Row& row, const Row* other, const string* diff_hl_color)
{
    auto val = [&](auto i) {
        return format_val(row[i], other ? &((*other)[i]) : nullptr, diff_hl_color);
    };

    string_view delim(", ");
    return views::iota(size_t(0), row.size()) | views::transform(val) |
           views::join_with(delim) | ranges::to<string>();
}

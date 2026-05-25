#include "sql.hh"
#include "util.hh"

#include <ranges>

using namespace sql;
using namespace util;

using namespace std;

constexpr std::string ANSI_RESET = "\033[0m";
constexpr std::string ANSI_RED = "\033[31m";
constexpr std::string ANSI_GREEN = "\033[32m";
constexpr std::string ANSI_CYAN = "\033[36m";

static inline string joined(const vector<string>& v, const string& delim)
{
    return v | views::join_with(delim) | ranges::to<string>();
}

static string format_val(const Val& val, const Val* other = nullptr,
                         const string* diff_hl_color = nullptr)
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

static string format_row(const Row& row, const Row* other = nullptr,
                         const string* diff_hl_color = nullptr)
{
    auto val = [&](auto i) {
        return format_val(row[i], other ? &((*other)[i]) : nullptr, diff_hl_color);
    };

    string_view delim(", ");
    return views::iota(size_t(0), row.size()) | views::transform(val) |
           views::join_with(delim) | ranges::to<string>();
}

std::format_context::iterator
std::formatter<Val>::format(const Val& v, std::format_context& ctx) const
{
    auto fmt = [&](auto&& v) {
        using T = decay_t<decltype(v)>;
        if constexpr (is_same_v<T, string>)
            if (quote)
                return format_to(ctx.out(), "\"{}\"", v);
            else
                return format_to(ctx.out(), "{}", v);
        else if constexpr (is_same_v<T, null_t>)
            return format_to(ctx.out(), "NULL");
        else
            return format_to(ctx.out(), "{}", v);
    };
    return visit(fmt, v);
}

std::format_context::iterator
std::formatter<Ident>::format(const Ident& ident, std::format_context& ctx) const
{
    auto s = joined(ident.keys(), "::");
    return format_to(ctx.out(), "{}", s);
}

std::format_context::iterator
std::formatter<ColVec>::format(const ColVec& cols, std::format_context& ctx) const
{
    auto s = joined(cols, ", ");
    return format_to(ctx.out(), "{}", s);
}

std::format_context::iterator
std::formatter<Delta>::format(const Delta& delta, std::format_context& ctx) const
{
    auto c = cases{
      [&](const Insert& ins) {
          const string row_str = format_row(ins.row);
          return format_to(ctx.out(), "{}INSERT:{} {}", ANSI_GREEN, ANSI_RESET,
                           row_str);
      },
      [&](const Delete& del) {
          const string row_str = format_row(del.row);
          return format_to(ctx.out(), "{}DELETE:{} {}", ANSI_RED, ANSI_RESET, row_str);
      },
      [&](const Update& upd) {
          const string row1_str = format_row(upd.row, &upd.new_row, &ANSI_RED);
          const string row2_str = format_row(upd.new_row, &upd.row, &ANSI_GREEN);
          return format_to(ctx.out(), "{}UPDATE-:{} {}\n{}UPDATE+:{} {}", ANSI_RED,
                           ANSI_RESET, row1_str, ANSI_GREEN, ANSI_RESET, row2_str);
      }};
    return visit(c, delta);
}

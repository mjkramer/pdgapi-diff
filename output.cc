#include "sql.hh"
#include "util.hh"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

using namespace sql;
using namespace util;

using namespace std;

constexpr std::string ANSI_RESET = "\033[0m";
constexpr std::string ANSI_RED = "\033[31m";
constexpr std::string ANSI_GREEN = "\033[32m";
constexpr std::string ANSI_BLUE = "\033[34m";
constexpr std::string ANSI_CYAN = "\033[36m";

static inline string joined(const vector<string>& v, const string& delim)
{
    return v | views::join_with(delim) | ranges::to<string>();
}

// Split into runs of alphanumerics; every other character is its own token
static vector<string_view> tokenize(string_view s)
{
    vector<string_view> tokens;
    size_t i = 0;
    while (i < s.size()) {
        size_t j = i + 1;
        if (isalnum((unsigned char)s[i]))
            while (j < s.size() and isalnum((unsigned char)s[j]))
                ++j;
        tokens.push_back(s.substr(i, j - i));
        i = j;
    }
    return tokens;
}

// For each token in a, whether it is absent from the LCS of a and b
// (Claude-written)
static vector<bool> changed_tokens(const vector<string_view>& a,
                                   const vector<string_view>& b)
{
    // Above this many DP cells, just mark the whole middle as changed
    constexpr size_t MAX_CELLS = 1 << 22;

    vector<bool> changed(a.size(), false);

    size_t pre = 0;
    while (pre < a.size() and pre < b.size() and a[pre] == b[pre])
        ++pre;
    size_t suf = 0;
    while (suf < a.size() - pre and suf < b.size() - pre and
           a[a.size() - 1 - suf] == b[b.size() - 1 - suf])
        ++suf;

    const size_t n = a.size() - pre - suf, m = b.size() - pre - suf;
    if (n == 0)
        return changed;
    if (m == 0 or (n + 1) * (m + 1) > MAX_CELLS) {
        fill(changed.begin() + pre, changed.begin() + pre + n, true);
        return changed;
    }

    // lcs[i][j] = LCS length of a[pre+i..pre+n) and b[pre+j..pre+m)
    vector<uint32_t> lcs((n + 1) * (m + 1), 0);
    auto L = [&](size_t i, size_t j) -> uint32_t& { return lcs[i * (m + 1) + j]; };
    for (size_t i = n; i-- > 0;)
        for (size_t j = m; j-- > 0;)
            L(i, j) = a[pre + i] == b[pre + j] ? L(i + 1, j + 1) + 1
                                               : max(L(i + 1, j), L(i, j + 1));

    size_t i = 0, j = 0;
    while (i < n) {
        if (j < m and a[pre + i] == b[pre + j]) {
            ++i, ++j;
        } else if (j == m or L(i + 1, j) >= L(i, j + 1)) {
            changed[pre + i] = true;
            ++i;
        } else {
            ++j;
        }
    }
    return changed;
}

// Wrap the tokens of s that don't appear in other in reverse video
static string highlight_changes(const string& s, const string& other)
{
    constexpr string_view REVERSE_ON = "\033[7m", REVERSE_OFF = "\033[27m";

    const auto tokens = tokenize(s);
    const auto changed = changed_tokens(tokens, tokenize(other));

    string out;
    bool in_hl = false;
    for (const auto& [tok, chg] : views::zip(tokens, changed)) {
        if (chg != in_hl)
            out += chg ? REVERSE_ON : REVERSE_OFF;
        in_hl = chg;
        out += tok;
    }
    if (in_hl)
        out += REVERSE_OFF;
    return out;
}

static string format_val(const Val& val, const Val* other = nullptr,
                         const string* diff_hl_color = nullptr)
{
    if (not other)
        return format("{:q}", val);

    const auto s1 = format("{:q}", val);
    const auto s2 = format("{:q}", *other);
    const auto align_left = holds_alternative<string>(val);
    const auto width = max(s1.size(), s2.size());

    if (diff_hl_color and val != *other and holds_alternative<string>(val) and
        holds_alternative<string>(*other)) {
        const auto body = highlight_changes(get<string>(val), get<string>(*other));
        const auto pad = string(width - s1.size(), ' ');
        return format("{}\"{}\"{}{}", *diff_hl_color, body, pad, ANSI_RESET);
    }

    const auto s =
      align_left ? format("{:<{}}", s1, width) : format("{:>{}}", s1, width);

    if (diff_hl_color and val != *other)
        return format("{}{}{}", *diff_hl_color, s, ANSI_RESET);
    return format("{}", s);
}

static string format_row(const Row& row, const Row* other = nullptr,
                         const string* diff_hl_color = nullptr)
{
    auto val = [&](auto i) {
        const auto other_pv = other ? &((*other)[i]) : nullptr;
        const auto val_str = format_val(row[i], other_pv, diff_hl_color);
        const auto prefix = i == 0 ? ANSI_BLUE : "";
        const auto suffix = i == 0 ? ANSI_RESET : "";
        return format("{}{}{}", prefix, val_str, suffix);
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

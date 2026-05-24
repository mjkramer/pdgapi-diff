#pragma once

#include "util.hh"

#include <format>
#include <map>
#include <ranges>
#include <string>
#include <variant>
#include <vector>

namespace sql {

using tblname_t = std::string;
using colname_t = std::string;
using ident_t = std::string;
using primkey_t = long;
using null_t = void*;

using Val = std::variant<null_t, long, double, std::string>;
bool operator==(const Val&, const Val&);

using Row = std::vector<Val>;

using RowStream = std::vector<sql::Row>;
using Rows = std::map<ident_t, Row>;

using ColSet = std::vector<colname_t>;

using RowMap = std::map<tblname_t, Rows>;
using ColMap = std::map<tblname_t, ColSet>;
using IdMap = std::map<primkey_t, ident_t>;
using IdMapMap = std::map<tblname_t, IdMap>;

struct Insert {
    Row row;
};
struct Delete {
    Row row;
};
struct Update {
    Row row, new_row;
};
using Delta = std::variant<Insert, Delete, Update>;

class Ident {
public:
    Ident() = default;
    Ident(const std::vector<std::string>&);
    Ident(const std::string&);

    primkey_t id_at(size_t idx) const;
    std::vector<std::string>& keys() { return m_keys; }
    const std::vector<std::string>& keys() const { return m_keys; }

    const std::string& operator[](size_t idx) const;
    std::string& operator[](size_t idx);

private:
    std::vector<std::string> m_keys;
};

} // namespace sql

inline std::string joined(const std::vector<std::string>& v, const std::string& delim)
{
    return v | std::views::join_with(delim) | std::ranges::to<std::string>();
}

template <> struct std::formatter<sql::Val> {
    bool quote = false;
    constexpr auto parse(std::format_parse_context& ctx)
    {
        auto it = ctx.begin();
        if (it != ctx.end() && *it == 'q') {
            quote = true;
            ++it;
        }
        if (it != ctx.end() && *it != '}')
            throw std::format_error("invalid format spec for sql::Val");
        return it;
    }
    auto format(const sql::Val& v, std::format_context& ctx) const
    {
        auto fmt = [&](auto&& v) {
            using T = decay_t<decltype(v)>;
            if constexpr (is_same_v<T, string>)
                if (quote)
                    return std::format_to(ctx.out(), "\"{}\"", v);
                else
                    return std::format_to(ctx.out(), "{}", v);
            else if constexpr (is_same_v<T, sql::null_t>)
                return std::format_to(ctx.out(), "NULL");
            else
                return std::format_to(ctx.out(), "{}", v);
        };
        return visit(fmt, v);
    }
};

template <> struct std::formatter<sql::Ident> : std::formatter<std::string> {
    auto format(const sql::Ident& ident, std::format_context& ctx) const
    {
        auto s = joined(ident.keys(), "::");
        return std::format_to(ctx.out(), "{}", s);
    }
};

template <> struct std::formatter<sql::ColSet> : std::formatter<std::string> {
    auto format(const sql::ColSet& cols, std::format_context& ctx) const
    {
        auto s = joined(cols, ", ");
        return std::format_to(ctx.out(), "{}", s);
    }
};

std::string format_val(const sql::Val& val, const sql::Val* other = nullptr,
                       const std::string* diff_hl_color = nullptr);
std::string format_row(const sql::Row& row, const sql::Row* other = nullptr,
                       const std::string* diff_hl_color = nullptr);

constexpr std::string ANSI_RESET = "\033[0m";
constexpr std::string ANSI_RED   = "\033[31m";
constexpr std::string ANSI_GREEN = "\033[32m";
constexpr std::string ANSI_CYAN  = "\033[36m";

template <> struct std::formatter<sql::Delta> : std::formatter<std::string> {
    auto format(const sql::Delta& delta, std::format_context& ctx) const
    {
        auto c = util::cases{
            [&](const sql::Insert& ins) {
                const string row_str = format_row(ins.row);
                return format_to(ctx.out(), "{}INSERT:{} {}", ANSI_GREEN, ANSI_RESET, row_str);
            },
            [&](const sql::Delete& del) {
                const string row_str = format_row(del.row);
                return format_to(ctx.out(), "{}DELETE:{} {}", ANSI_RED, ANSI_RESET, row_str);
            },
            [&](const sql::Update& upd) {
                const string row1_str = format_row(upd.row, &upd.new_row, &ANSI_RED);
                const string row2_str = format_row(upd.new_row, &upd.row, &ANSI_GREEN);
                return format_to(ctx.out(), "{}UPDATE-:{} {}\n{}UPDATE+:{} {}",
                                ANSI_RED, ANSI_RESET, row1_str, ANSI_GREEN, ANSI_RESET, row2_str);
            }
        };
        return visit(c, delta);
    }
};

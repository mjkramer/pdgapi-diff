#pragma once

#include <format>
#include <map>
#include <set>
#include <string>
#include <variant>
#include <vector>

namespace sql {

using tblname_t = std::string;
using colname_t = std::string;
using ident_t = std::string;
using primkey_t = long;
using null_t = void*;

using TableVec = std::vector<tblname_t>;

using Val = std::variant<null_t, long, double, std::string>;
bool operator==(const Val&, const Val&);

using Row = std::vector<Val>;
bool operator==(const Row&, const Row&);

using RowStream = std::vector<sql::Row>;
using Rows = std::map<ident_t, Row>;

using ColVec = std::vector<colname_t>;
using ColSet = std::set<colname_t>;

using RowMap = std::map<tblname_t, Rows>;
using ColMap = std::map<tblname_t, ColVec>;
using ColSetMap = std::map<tblname_t, ColSet>;
using IdMap = std::map<primkey_t, ident_t>;
using IdMapMap = std::map<tblname_t, IdMap>;
using InvIdMap = std::map<ident_t, primkey_t>;
using InvIdMapMap = std::map<tblname_t, InvIdMap>;

using IdentSet = std::set<ident_t>;
using IdentSetMap = std::map<tblname_t, IdentSet>;

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
    std::format_context::iterator format(const sql::Val& v,
                                         std::format_context& ctx) const;
};

template <> struct std::formatter<sql::Ident> : std::formatter<std::string> {
    std::format_context::iterator format(const sql::Ident& ident,
                                         std::format_context& ctx) const;
};

template <> struct std::formatter<sql::ColVec> : std::formatter<std::string> {
    std::format_context::iterator format(const sql::ColVec& cols,
                                         std::format_context& ctx) const;
};

template <> struct std::formatter<sql::Delta> : std::formatter<std::string> {
    std::format_context::iterator format(const sql::Delta& delta,
                                         std::format_context& ctx) const;
};

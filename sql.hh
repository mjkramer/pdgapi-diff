#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace sql {

using tblname_t = std::string;
using colname_t = std::string;
using ident_t = std::string;
using primkey_t = long;
using null_t = void*;

using Val = std::variant<null_t, long, double, std::string>;
std::ostream& operator<<(std::ostream&, Val);

using Row = std::vector<Val>;
using Rows = std::unordered_map<ident_t, Row>;

using RowMap = std::unordered_map<tblname_t, Rows>;
using ColMap = std::unordered_map<tblname_t, std::vector<colname_t>>;
using IdMap = std::map<primkey_t, ident_t>;
using IdMapMap = std::unordered_map<tblname_t, IdMap>;

struct Insert {Row row;};
struct Delete {Row row;};
struct Update {Row row, new_row;};
using Delta = std::variant<Insert, Delete, Update>;

struct Ident : std::vector<Val> {
    Ident(const std::string&);
    long foreign_key(size_t idx);
    std::string str();
};

} // namespace sql

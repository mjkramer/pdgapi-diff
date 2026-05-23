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

// void* represents a NULL
struct Val : std::variant<void*, long, double, std::string> {
    std::string str() const;
};

using Row = std::vector<Val>;
using Rows = std::unordered_map<ident_t, Row>;

using RowMap = std::unordered_map<tblname_t, Rows>;
using ColMap = std::unordered_map<tblname_t, std::vector<colname_t>>;
using IdMap = std::map<primkey_t, ident_t>;
using IdMapMap = std::unordered_map<tblname_t, IdMap>;

} // namespace sql

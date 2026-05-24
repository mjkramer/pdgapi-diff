#pragma once

#include <map>
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
std::string to_str(const Val&);

using Row = std::vector<Val>;
using RowStream = std::vector<sql::Row>;
using Rows = std::map<ident_t, Row>;

using RowMap = std::map<tblname_t, Rows>;
using ColMap = std::map<tblname_t, std::vector<colname_t>>;
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

    std::string str() const;
    primkey_t id_at(size_t idx) const;
    std::vector<std::string>& keys() { return m_keys; }

    const std::string& operator[](size_t idx) const;
    std::string& operator[](size_t idx);

private:
    std::vector<std::string> m_keys;
};

} // namespace sql

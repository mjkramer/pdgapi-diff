#pragma once

#include "sql.hh"

#include <sqlite3.h>

#include <map>
#include <string>
#include <set>

class SqliteConn {
public:
    SqliteConn(const std::string& path);
    std::vector<std::string> col_names(const std::string& table);
    sql::RowStream all_rows(const std::string& table);
    void set_exclude_cols(const std::map<std::string, std::set<std::string>>&);

private:
    std::string table_query(const std::string& table);

    sqlite3* m_db;
    std::vector<std::string> m_colNames;
    std::map<std::string, std::set<std::string>> m_excludeCols;
};

#pragma once

#include "sql.hh"

#include <sqlite3.h>

#include <string>

class SqliteConn {
public:
    SqliteConn(const std::string& path);
    std::vector<std::string> col_names(const std::string& table);
    sql::RowStream all_rows(const std::string& table);

private:
    std::string table_query(const std::string& table);

    sqlite3* m_db;
    std::vector<std::string> m_colNames;
};

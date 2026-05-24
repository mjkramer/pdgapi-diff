#include "SqliteConn.hh"

#include <format>
#include <ranges>

using namespace std;
using namespace sql;

SqliteConn::SqliteConn(const std::string& path)
{
    sqlite3_open_v2(path.c_str(), &m_db, SQLITE_OPEN_READONLY, nullptr);
}

vector<string> SqliteConn::col_names(const std::string& table)
{
    std::vector<std::string> ret;

    string sql = format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        std::string name = (const char*)sqlite3_column_text(stmt, 1);
        ret.push_back(std::move(name));
    }

    sqlite3_finalize(stmt);
    return ret;
}

string SqliteConn::table_query(const std::string& table)
{
    const auto cnames = col_names(table);
    string_view delim(", ");
    auto cols = cnames | views::join_with(delim) | ranges::to<string>();
    return std::format("SELECT {} FROM {}", cols, table);
}

RowStream SqliteConn::all_rows(const string& table)
{
    std::string sql = table_query(table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);
    const size_t ncol = sqlite3_column_count(stmt);

    RowStream ret;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        Row row;
        for (size_t i = 1; i < ncol; ++i) {
            switch (sqlite3_column_type(stmt, i)) {
            case SQLITE_NULL:
                row.emplace_back(nullptr);
                break;
            case SQLITE_INTEGER:
                row.emplace_back(sqlite3_column_int64(stmt, i));
                break;
            case SQLITE_FLOAT:
                row.emplace_back(sqlite3_column_double(stmt, i));
                break;
            case SQLITE_BLOB:
            case SQLITE_TEXT:
                row.emplace_back((const char*)sqlite3_column_text(stmt, i));
                break;
            default:
                throw;
            }
        }
        ret.push_back(std::move(row));
    }

    sqlite3_finalize(stmt);
    return ret;
}

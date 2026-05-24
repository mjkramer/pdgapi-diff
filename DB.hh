#pragma once

#include <sqlite3.h>

#include "SqliteConn.hh"
#include "sql.hh"

class DB {
public:
    DB(const std::string& path);
    const sql::Rows& rows(const std::string& table) const;
    const sql::ColSet& cols(const std::string& table) const;

    static const std::vector<std::string> TABLES;

private:
    void patch_all_refs();
    void read_table(const std::string& table);
    void patch_ident_refs(const std::string& src_table, const std::string& column,
                          const std::string& dest_table);
    void patch_refs(const std::string& src_table, const std::string& column,
                    const std::string& dest_table);
    void patch_id(const std::string& table);
    sql::IdMap& get_id_map(const std::string& table);

    SqliteConn m_db;

    sql::RowMap m_rowMap;
    sql::ColMap m_colMap;
    sql::IdMapMap m_idMaps;

    static const std::map<std::string, std::vector<std::string>> IDENT_COLS;
};

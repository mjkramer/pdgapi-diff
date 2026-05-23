#pragma once

#include <sqlite3.h>

#include "sql.hh"

class DB {
public:
    DB(const char* path);
    const sql::Rows& get_rows(const char* table) const;

private:
    void patch_all_refs();
    void read_table(const std::string& table);
    void patch_ident_refs(const char* src_table, const char* column,
                          const char* dest_table);
    void patch_refs(const char* src_table, const char* column, const char* dest_table);
    sql::IdMap& get_id_map(const char* table);

    sqlite3* m_db;

    sql::RowMap m_rowMap;
    sql::ColMap m_colMap;
    sql::IdMapMap m_idMaps;

    static const std::vector<std::string> TABLES;
    static const std::unordered_map<std::string, std::vector<std::string>> IDENT_COLS;
};


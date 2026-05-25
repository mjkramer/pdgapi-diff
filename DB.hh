#pragma once

#include "SqliteConn.hh"
#include "sql.hh"

#include <set>

class DB {
public:
    DB(const std::string& path);
    const sql::Rows& rows(const std::string& table) const;
    const sql::ColVec& cols(const std::string& table) const;

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
    sql::InvIdMapMap m_invIdMaps;

    sql::IdentSetMap m_ambigIdents;

    static const sql::ColMap IDENT_COLS;
    static const sql::ColMap EXTRA_IDENT_COLS;
    static const sql::ColSetMap EXCLUDE_COLS;
};

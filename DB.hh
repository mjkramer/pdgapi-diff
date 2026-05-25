#pragma once

#include "SqliteConn.hh"
#include "sql.hh"

class DB {
public:
    DB(const std::string& path);
    const sql::Rows& rows(const sql::tblname_t&) const;
    const sql::ColVec& cols(const sql::tblname_t&) const;

    static const sql::TableVec TABLES;

    static const sql::ColMap IDENT_COLS;
    static const sql::ColMap EXTRA_IDENT_COLS;
    static const sql::ColMap FUZZY_COLS;
    static const sql::ColSetMap EXCLUDE_COLS;

private:
    void patch_all_refs();
    void read_table(const sql::tblname_t&);
    void patch_ident_refs(const sql::tblname_t& src_table, const sql::colname_t& column,
                          const sql::tblname_t& dest_table);
    void patch_refs(const sql::tblname_t& src_table, const sql::colname_t& column,
                    const sql::tblname_t& dest_table);
    void patch_id(const sql::tblname_t&);
    sql::IdMap& get_id_map(const sql::tblname_t&);

    SqliteConn m_db;

    sql::RowMap m_rowMap;
    sql::ColMap m_colMap;
    sql::IdMapMap m_idMaps;
    sql::InvIdMapMap m_invIdMaps;

    sql::IdentSetMap m_ambigIdents;
};

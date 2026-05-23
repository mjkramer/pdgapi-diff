#include "DB.hh"

using namespace sql;
using namespace std;


const std::vector<std::string> DB::TABLES{"pdgdata",        "pdgdecay",
                                          "pdgfootnote",    "pdgid_map",
                                          "pdgid",          "pdgitem_map",
                                          "pdgitem",        "pdgmeasurement_footnote",
                                          "pdgmeasurement", "pdgmeasurement_values",
                                          "pdgparticle",    "pdgreference",
                                          "pdgtext"};

const std::unordered_map<std::string, std::vector<std::string>> DB::IDENT_COLS{
  {"pdgdata", {"pdgid", "value_type", "sort"}},
  {"pdgdecay", {"pdgid"}},
  {"pdgfootnote", {"pdgid", "footnote_index"}},
  {"pdgid_map", {"source", "target"}},
  {"pdgid", {"pdgid"}},
  {"pdgitem_map", {"name", "target_id"}},
  {"pdgitem", {"name"}},
  {"pdgmeasurement_footnote", {"pdgmeasurement_id", "pdgfootnote_id"}},
  {"pdgmeasurement", {"pdgid", "pdgreference_id", "sort"}},
  {"pdgmeasurement_values", {"pdgmeasurement_id", "column_name", "sort"}},
  {"pdgparticle", {"pdgid", "name"}},
  {"pdgreference", {"document_id"}},
  {"pdgtext", {"pdgid"}}};


DB::DB(const char* path)
{
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);

    for (const auto& table : TABLES) {
        read_table(table);
    }

    patch_all_refs();
}

void DB::patch_all_refs()
{
    patch_ident_refs("pdgitem_map", "target_id", "pdgitem");
    patch_ident_refs("pdgmeasurement_footnote", "pdgmeasurement_id", "pdgmeasurement");
    patch_ident_refs("pdgmeasurement_footnote", "pdgfootnote_id", "pdgfootnote");
    patch_ident_refs("pdgmeasurement", "pdgreference_id", "pdgreference");
    patch_ident_refs("pdgmeasurement_values", "pdgmeasurement_id", "pdgmeasurement");

    patch_refs("pdgdata", "pdgid_id", "pdgid");
    patch_refs("pdgdecay", "pdgid_id", "pdgid");
    patch_refs("pdgdecay", "pdgitem_id", "pdgitem");
    patch_refs("pdgid", "parent_id", "pdgid");
    patch_refs("pdgid_map", "source_id", "pdgid");
    patch_refs("pdgid_map", "target_id", "pdgid");
    patch_refs("pdgitem_map", "pdgitem_id", "pdgitem");
    patch_refs("pdgmeasurement", "pdgid_id", "pdgid");
    patch_refs("pdgparticle", "pdgid_id", "pdgid");
    patch_refs("pdgparticle", "pdgitem_id", "pdgitem");
    patch_refs("pdgtext", "pdgid_id", "pdgid");
}

sql::IdMap& DB::get_id_map(const char* table)
{
    if (m_idMaps.count(table))
        return m_idMaps[table];

    m_idMaps[table] = {};
    auto& id_map = m_idMaps[table];

    const auto& cols = m_colMap[table];
    const int id_idx = std::ranges::find(cols, "id") - cols.begin();

    for (const auto& [ident_str, row] : m_rowMap[table]) {
        const long id = std::get<long>(row[id_idx]);
        id_map[id] = ident_str;
    }

    return id_map;
}

void DB::patch_ident_refs(const char* src_table, const char* column,
                          const char* dest_table)
{
    patch_refs(src_table, column, dest_table);

    const auto& ident_cols = IDENT_COLS.at(src_table);
    const int ident_idx = std::ranges::find(ident_cols, column) - ident_cols.begin();

    const auto& id_map = get_id_map(dest_table);

    Rows new_rows;

    for (auto& [ident_str, row] : m_rowMap[src_table]) {
        Ident ident{ident_str};
        const std::string dest_ident = id_map.at(ident.get_int(ident_idx));
        ident.set(ident_idx, dest_ident);
        new_rows[ident.str()] = std::move(row);
    }

    m_rowMap[src_table] = std::move(new_rows);
}

void DB::patch_refs(const char* src_table, const char* column, const char* dest_table)
{
    const auto& cols = m_colMap[src_table];
    const int idx = std::ranges::find(cols, column) - cols.begin();

    const auto& id_map = get_id_map(dest_table);

    for (auto& [ident_str, row] : m_rowMap[src_table]) {
        const long id = std::get<long>(row[idx]);
        row[idx] = Val{id_map.at(id)};
    }
}

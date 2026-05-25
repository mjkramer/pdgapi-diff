#include "DB.hh"
#include "sql.hh"
#include "util.hh"

#include <format>
#include <iostream>
#include <set>
#include <stdexcept>
#include <variant>

using namespace sql;
using namespace std;

const vector<string> DB::TABLES{"pdgdata",        "pdgdecay",
                                "pdgfootnote",    "pdgid_map",
                                "pdgid",          "pdgitem_map",
                                "pdgitem",        "pdgmeasurement_footnote",
                                "pdgmeasurement", "pdgmeasurement_values",
                                "pdgparticle",    "pdgreference",
                                "pdgtext"};

const map<string, vector<string>> DB::IDENT_COLS{
  {"pdgdata", {"pdgid", "value_type"}},
  {"pdgdecay", {"pdgid", "sort"}},
  {"pdgfootnote", {"pdgid", "footnote_index"}},
  {"pdgid_map", {"source", "target"}},
  {"pdgid", {"pdgid"}},
  {"pdgitem_map", {"name", "target_id"}},
  {"pdgitem", {"name"}},
  {"pdgmeasurement_footnote", {"pdgmeasurement_id", "pdgfootnote_id"}},
  {"pdgmeasurement", {"pdgid", "pdgreference_id"}},
  {"pdgmeasurement_values", {"pdgmeasurement_id", "column_name"}},
  {"pdgparticle", {"pdgid", "name"}},
  {"pdgreference", {"document_id"}},
  {"pdgtext", {"pdgid"}}};

const map<string, vector<string>> DB::EXTRA_IDENT_COLS{
  {"pdgdata", {"sort"}},
  {"pdgmeasurement", {"sort"}},
  {"pdgmeasurement_values", {"sort"}}};

const map<string, set<string>> DB::EXCLUDE_COLS{
  {"pdgdata", {"edition"}}, {"pdgid", {"parent_id", "mode_number", "sort"}}};

DB::DB(const string& path) : m_db(path)
{
    m_db.set_exclude_cols(DB::EXCLUDE_COLS);

    for (const auto& table : TABLES) {
        m_colMap[table] = m_db.col_names(table); // cache column names
        read_table(table);
    }

    patch_all_refs();
}

void DB::read_table(const string& table)
{
    auto& row_map = m_rowMap[table] = {};

    vector<size_t> ident_idcs;
    for (const auto& cname : IDENT_COLS.at(table)) {
        const size_t idx =
          ranges::find(m_colMap[table], cname) - m_colMap[table].begin();
        ident_idcs.push_back(idx);
    }

    vector<size_t> extra_ident_idcs;
    if (EXTRA_IDENT_COLS.contains(table)) {
        for (const auto& cname : EXTRA_IDENT_COLS.at(table)) {
            const size_t idx =
              ranges::find(m_colMap[table], cname) - m_colMap[table].begin();
            extra_ident_idcs.push_back(idx);
        }
    }

    auto rows = m_db.all_rows(table);

    for (const auto& row : rows) {
        Ident ident;
        for (size_t idx : ident_idcs) {
            ident.keys().push_back(format("{}", row[idx]));
        }
        const auto ident_str = format("{}", ident);
        if (row_map.contains(ident_str)) {
            m_ambigIdents[table].insert(ident_str);
            Row& other_row = row_map[ident_str];
            Ident other_ident{ident_str};
            for (size_t idx : extra_ident_idcs) {
                other_ident.keys().push_back(format("{}", other_row[idx]));
            }
            const auto other_ident_str = format("{}", other_ident);
            if (row_map.contains((other_ident_str))) {
                throw std::runtime_error{format("AMBIGUOUS1: {}", other_ident_str)};
            }
            row_map[other_ident_str] = std::move(other_row);
            row_map.erase(ident_str);
        }
        if (m_ambigIdents[table].contains(ident_str)) {
            for (size_t idx : extra_ident_idcs) {
                ident.keys().push_back(format("{}", row[idx]));
            }
            const auto new_ident_str = format("{}", ident);
            if (row_map.contains((new_ident_str))) {
                throw std::runtime_error{format("AMBIGUOUS2: {}", new_ident_str)};
            }
            row_map[new_ident_str] = row;
        } else
            row_map[ident_str] = row;
    }
}

const Rows& DB::rows(const string& table) const { return m_rowMap.at(table); }
const ColVec& DB::cols(const string& table) const { return m_colMap.at(table); }

void DB::patch_all_refs()
{
    for (const auto& table : TABLES) {
        get_id_map(table);
        // patch_id(table);
    }

    patch_ident_refs("pdgitem_map", "target_id", "pdgitem");
    patch_id("pdgitem_map");
    patch_ident_refs("pdgmeasurement", "pdgreference_id", "pdgreference");
    patch_id("pdgmeasurement");
    patch_ident_refs("pdgmeasurement_values", "pdgmeasurement_id", "pdgmeasurement");
    patch_id("pdgmeasurement_values");
    patch_ident_refs("pdgmeasurement_footnote", "pdgmeasurement_id", "pdgmeasurement");
    patch_ident_refs("pdgmeasurement_footnote", "pdgfootnote_id", "pdgfootnote");
    patch_id("pdgmeasurement_footnote");

    patch_refs("pdgdata", "pdgid_id", "pdgid");
    patch_refs("pdgdecay", "pdgid_id", "pdgid");
    patch_refs("pdgdecay", "pdgitem_id", "pdgitem");
    // patch_refs("pdgid", "parent_id", "pdgid");
    patch_refs("pdgid_map", "source_id", "pdgid");
    patch_refs("pdgid_map", "target_id", "pdgid");
    patch_refs("pdgitem_map", "pdgitem_id", "pdgitem");
    patch_refs("pdgmeasurement", "pdgid_id", "pdgid");
    patch_refs("pdgparticle", "pdgid_id", "pdgid");
    patch_refs("pdgparticle", "pdgitem_id", "pdgitem");
    patch_refs("pdgtext", "pdgid_id", "pdgid");

    for (const auto& table : TABLES) {
        patch_id(table);
    }
}

void DB::patch_ident_refs(const string& src_table, const string& column,
                          const string& dest_table)
{
    patch_refs(src_table, column, dest_table);

    const auto& ident_cols = IDENT_COLS.at(src_table);
    const int ident_idx = ranges::find(ident_cols, column) - ident_cols.begin();
    const size_t src_id_idx =
      ranges::find(m_colMap[src_table], "id") - m_colMap[src_table].begin();

    auto& src_id_map = m_idMaps[src_table];
    const auto& dest_id_map = m_idMaps[dest_table];

    Rows new_rows;
    src_id_map.clear();
    InvIdMap new_inv;

    for (auto& [ident_str, row] : m_rowMap[src_table]) {
        Ident ident{ident_str};
        const auto src_id = m_invIdMaps[src_table][ident_str];
        const size_t dest_id = ident.id_at(ident_idx);
        const string dest_ident = dest_id_map.at(dest_id);
        ident[ident_idx] =
          format("({})", util::replace_all_copy(dest_ident, "::", "@@"));

        src_id_map[src_id] = format("{}", ident);
        new_rows[format("{}", ident)] = std::move(row);
        new_inv[format("{}", ident)] = src_id;
    }

    m_rowMap[src_table] = std::move(new_rows);
    m_invIdMaps[src_table] = std::move(new_inv);
    // patch_id(src_table);
}

void DB::patch_refs(const string& src_table, const string& column,
                    const string& dest_table)
{
    const auto& cols = m_colMap[src_table];
    const int idx = ranges::find(cols, column) - cols.begin();

    const auto& id_map = m_idMaps[dest_table];

    for (auto& [ident_str, row] : m_rowMap[src_table]) {
        if (std::holds_alternative<long>(row[idx])) {
            const auto id = get<long>(row[idx]);
            if (not id_map.contains(id)) {
                cerr << format("WARNING1: {} {} {} {}", src_table, column, dest_table,
                               id)
                     << endl;
                row[idx] = "BORK";
            } else
                row[idx] = id_map.at(id);
        } else if (std::holds_alternative<null_t>(row[idx])) {
            row[idx] = "NULL";
        } else
            throw;
    }
}

IdMap& DB::get_id_map(const string& table)
{
    m_idMaps[table] = {};
    m_invIdMaps[table] = {};
    auto& id_map = m_idMaps[table];
    auto& inv_id_map = m_invIdMaps[table];

    const auto& cols = m_colMap[table];
    const int id_idx = ranges::find(cols, "id") - cols.begin();

    for (const auto& [ident_str, row] : m_rowMap[table]) {
        const long id = get<long>(row[id_idx]);
        id_map[id] = ident_str;
        inv_id_map[ident_str] = id;
    }

    return id_map;
}

void DB::patch_id(const string& table)
{
    const auto& cols = m_colMap[table];
    const int idx = ranges::find(cols, "id") - cols.begin();

    for (auto& [ident_str, row] : m_rowMap[table]) {
        row[idx] = ident_str;
    }
}

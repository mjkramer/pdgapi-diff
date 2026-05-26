#include "DB.hh"
#include "sql.hh"
#include "util.hh"

#include <format>
#include <iostream>
#include <ranges>
#include <stdexcept>
#include <variant>

using namespace sql;
using namespace util;

using namespace std;

const TableVec DB::TABLES{"pdgdata",        "pdgdecay",
                          "pdgfootnote",    "pdgid_map",
                          "pdgid",          "pdgitem_map",
                          "pdgitem",        "pdgmeasurement_footnote",
                          "pdgmeasurement", "pdgmeasurement_values",
                          "pdgparticle",    "pdgreference",
                          "pdgtext"};

const ColMap DB::IDENT_COLS{
  {"pdgdata", {"pdgid", "value_type"}},
  {"pdgdecay", {"pdgid", "sort"}},
  {"pdgfootnote", {"pdgid"}},
  {"pdgid_map", {"source", "target"}},
  {"pdgid", {"pdgid"}},
  {"pdgitem_map", {"name", "target_id"}},
  {"pdgitem", {"name"}},
  {"pdgmeasurement_footnote", {"pdgmeasurement_id", "pdgfootnote_id"}},
  {"pdgmeasurement", {"pdgid", "pdgreference_id", "comment"}},
  {"pdgmeasurement_values", {"pdgmeasurement_id", "column_name"}},
  {"pdgparticle", {"pdgid", "name"}},
  {"pdgreference", {"document_id"}},
  {"pdgtext", {"pdgid"}}};

const ColMap DB::EXTRA_IDENT_COLS{{"pdgdata", {"sort"}},
                                  {"pdgitem_map", {"sort"}},
                                  {"pdgmeasurement", {"sort"}},
                                  {"pdgmeasurement_values", {"sort"}}};

const ColMap DB::FUZZY_COLS{{"pdgfootnote", {"text"}}};

const ColSetMap DB::EXCLUDE_COLS{{"pdgdata", {"edition"}},
                                 {"pdgfootnote", {"footnote_index", "changebar"}},
                                 {"pdgid", {"parent_id", "mode_number", "sort"}},
                                 {"pdgmeasurement", {"changebar"}}};

const ColSetMap DB::POST_EXCLUDE_COLS{{"pdgmeasurement", {"sort"}}};

DB::DB(const string& path) : m_db(path)
{
    m_db.set_exclude_cols(DB::EXCLUDE_COLS);

    for (const auto& table : TABLES) {
        m_colMap[table] = m_db.col_names(table); // cache column names
        read_table(table);
    }

    patch_all_refs();
    do_post_exclude();
}

void DB::do_post_exclude()
{
    for (const auto& [table, cols] : POST_EXCLUDE_COLS) {
        auto& colmap = m_colMap[table];
        for (const auto& col : cols) {
            const auto idx = index_of(colmap, col);
            colmap.erase(colmap.begin() + idx);
            for (auto& [ident, rows] : m_rowMap[table])
                for (auto& row : rows)
                    row.erase(row.begin() + idx);
        }
    }
}

static inline vector<long> to_indices(const ColVec& cols, const ColVec& all_cols)
{
    auto to_index = [&](string_view name) { return util::index_of(all_cols, name); };
    return cols | ranges::views::transform(to_index) | ranges::to<vector>();
}

static inline vector<string> format_cols(const Row& row, const vector<long>& idcs)
{
    return idcs |
           ranges::views::transform([&](size_t i) { return format("{}", row[i]); }) |
           ranges::to<vector>();
}

void DB::read_table(const tblname_t& table)
{
    auto& row_map = m_rowMap[table] = {};

    const auto ident_idcs = to_indices(IDENT_COLS.at(table), m_colMap[table]);
    const auto extra_ident_idcs =
      EXTRA_IDENT_COLS.contains(table)
        ? to_indices(EXTRA_IDENT_COLS.at(table), m_colMap[table])
        : vector<long>{};

    auto rows = m_db.all_rows(table);

    for (const auto& row : rows) {
        const auto ident_keys = format_cols(row, ident_idcs);
        const auto ident_str = format("{}", Ident{ident_keys});
        const auto n_base_keys = IDENT_COLS.at(table).size();
        const auto max_keys = ident_keys.size() + extra_ident_idcs.size();

        auto extended_ident = [&](const Row& row) {
            Ident ident{ident_str};
            auto& keys = ident.keys();
            while (keys.size() < max_keys) {
                const auto j = keys.size() - n_base_keys;
                const auto new_key = row[extra_ident_idcs[j]];
                keys.push_back(format("{}", new_key));
                const auto str = format("{}", ident);

                if ((not row_map.contains(str)) and (not m_ambigIdents.contains(str)))
                    break;

                m_ambigIdents[table].insert(format("{}", ident));
            }
            return format("{}", ident);
        };

        if (row_map.contains(ident_str) and not extra_ident_idcs.empty()) {
            m_ambigIdents[table].insert(ident_str);
            RowVec& other_rows = row_map[ident_str];
            const auto other_new_ident_str = extended_ident(other_rows[0]);
            if (row_map.contains(other_new_ident_str)) {
                throw std::runtime_error{format("AMBIGUOUS1: {}", other_new_ident_str)};
            }
            row_map[other_new_ident_str] = std::move(other_rows);
            row_map.erase(ident_str);
        }

        if (m_ambigIdents[table].contains(ident_str)) {
            const auto new_ident_str = extended_ident(row);
            if (row_map.contains(new_ident_str)) {
                throw std::runtime_error{format("AMBIGUOUS2: {}", new_ident_str)};
            }
            row_map[new_ident_str] = {row};
        } else
            row_map[ident_str].push_back(row);
    }
}

const Rows& DB::rows(const tblname_t& table) const { return m_rowMap.at(table); }
const ColVec& DB::cols(const tblname_t& table) const { return m_colMap.at(table); }

void DB::patch_all_refs()
{
    for (const auto& table : TABLES) {
        get_id_map(table);
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

void DB::patch_ident_refs(const tblname_t& src_table, const colname_t& column,
                          const tblname_t& dest_table)
{
    patch_refs(src_table, column, dest_table);

    const auto& ident_cols = IDENT_COLS.at(src_table);
    const int ident_idx = util::index_of(ident_cols, column);
    const size_t src_id_idx = util::index_of(m_colMap[src_table], "id");

    auto& src_id_map = m_idMaps[src_table];
    const auto& dest_id_map = m_idMaps[dest_table];

    Rows new_rows;
    src_id_map.clear();
    InvIdMap new_inv;

    for (auto& [ident, rows] : m_rowMap[src_table]) {
        Ident ident_obj{ident};
        const size_t dest_id = ident_obj.id_at(ident_idx);
        const string dest_ident = dest_id_map.at(dest_id);
        ident_obj[ident_idx] =
          format("({})", util::replace_all(dest_ident, "::", "@@"));

        const auto new_ident = format("{}", ident_obj);
        new_rows[new_ident] = std::move(rows);

        for (const auto src_id : m_invIdMaps[src_table][ident]) {
            src_id_map[src_id] = new_ident;
            new_inv[new_ident].push_back(src_id);
        }
    }

    m_rowMap[src_table] = std::move(new_rows);
    m_invIdMaps[src_table] = std::move(new_inv);
}

void DB::patch_refs(const tblname_t& src_table, const colname_t& column,
                    const tblname_t& dest_table)
{
    const auto& cols = m_colMap[src_table];
    const size_t idx = util::index_of(cols, column);

    const auto& id_map = m_idMaps[dest_table];

    for (auto& [ident_str, rows] : m_rowMap[src_table]) {
        for (auto& row : rows) {
            if (std::holds_alternative<long>(row[idx])) {
                const auto id = get<long>(row[idx]);
                if (not id_map.contains(id)) {
                    cerr << format("WARNING1: {} {} {} {}", src_table, column,
                                   dest_table, id)
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
}

IdMap& DB::get_id_map(const tblname_t& table)
{
    m_idMaps[table] = {};
    m_invIdMaps[table] = {};
    auto& id_map = m_idMaps[table];
    auto& inv_id_map = m_invIdMaps[table];

    const auto& cols = m_colMap[table];
    const size_t id_idx = util::index_of(cols, "id");

    for (const auto& [ident_str, rows] : m_rowMap[table]) {
        for (const auto& row : rows) {
            const long id = get<long>(row[id_idx]);
            id_map[id] = ident_str;
            inv_id_map[ident_str].push_back(id);
        }
    }

    return id_map;
}

void DB::patch_id(const tblname_t& table)
{
    const auto& cols = m_colMap[table];
    const size_t idx = util::index_of(cols, "id");

    for (auto& [ident_str, rows] : m_rowMap[table]) {
        for (auto& row : rows)
            row[idx] = ident_str;
    }
}

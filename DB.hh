struct SqlRow : std::vector<SqlVal> {
    Ident ident;
}; 

struct SqlRows : std::unordered_map<Ident, std::vector<SqlRow>> {
    std::vector<std::string> ident_cols;
};

class DB {
public:
    DB(const char* path);
    const SqlRows& get_rows(const char* table);

private:
    sqlite3* m_db;

    std::unordered_map<std::string, SqlRows> row_map;
    std::unordered_map<std::string, std::vector<std::string>> col_map;

    inline static const std::vector<std::string> TABLES
        {"pdgdata", "pdgdecay", "pdgfootnote", "pdgid_map", "pdgid",
         "pdgitem_map", "pdgitem", "pdgmeasurement_footnote", "pdgmeasurement",
         "pdgmeasurement_values", "pdgparticle", "pdgreference", "pdgtext"};

    inline static const std::vector<std::string> TABLES_REFERENCED
        {"pdgfootnote", "pdgitem", "pdgmeasurement", "pdgreference"};

    inline static const
    std::unordered_map<std::string, std::vector<std::string>> IDENT_COLS {
        {"pdgdata",                 {"pdgid", "value_type", "sort"}},
        {"pdgdecay",                {"pdgid"}},
        {"pdgfootnote",             {"pdgid", "footnote_index"}},
        {"pdgid_map",               {"source", "target"}},
        {"pdgid",                   {"pdgid"}},
        {"pdgitem_map",             {"name", "target_id"}},
        {"pdgitem",                 {"name"}},
        {"pdgmeasurement_footnote", {"pdgmeasurement_id", "pdgfootnote_id"}},
        {"pdgmeasurement",          {"pdgid", "pdgreference_id", "sort"}},
        {"pdgmeasurement_values",   {"pdgmeasurement_id", "column_name", "sort"}},
        {"pdgparticle",             {"pdgid", "name"}},
        {"pdgreference",            {"document_id"}},
        {"pdgtext",                 {"pdgid"}}
    };
};

DB::DB(const char* path)
{
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);

    for (const auto& table : TABLES_ALL) {
        read_table(table);
    }

    // for (const auto& table : TABLES_REFERENCED) {
    //     gen_id2ident(table);
    // }

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

void
DB::patch_ident_refs(const char* src_table, const char* column, const char* dest_table)
{
    patch_refs(src_table, column, dest_table, true);

    const auto& cols = IDENT_COLS[src_table];
    const size_t idx = std::ranges::find(cols, column) - cols.begin();
}

std::vector<Delta> compare(const DB& db1, const DB& db2, const char* table)
{
    std::vector<Delta> ret;

    const SqlRows& rows1 = db1.get_rows(table);
    const SqlRows& rows2 = db2.get_rows(table);
    SqlRows rows2_new{rows2};

    for (const auto& [ident, row1] : rows1) {
        std::optional<SqlRow> row2_opt = rows2.find(ident);
        if (not row2_opt.has_value()) {
            ret.push_back(Delete(row1));
        } else {
            const auto& row2 = row2_opt.value();
            if (row1 != row2) {
                ret.push_back(Update(row1, row2));
            }
            rows2_new.remove(ident);
        }
    }

    for (const auto& [ident, row2] : rows2_new) {
        ret.push_back(Insert(row2));
    }

    return ret;
}

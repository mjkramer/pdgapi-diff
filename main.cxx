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

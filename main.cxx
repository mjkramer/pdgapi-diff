#include "DB.hh"
#include "sql.hh"

using namespace std;
using namespace sql;

vector<Delta> compare(const DB& db1, const DB& db2, const char* table)
{
    std::vector<Delta> ret;

    const Rows& rows1 = db1.get_rows(table);
    const Rows& rows2 = db2.get_rows(table);
    Rows rows2_new{rows2};

    for (const auto& [ident, row1] : rows1) {
        if (not rows2.count(ident)) {
            ret.push_back(Delete(row1));
        } else {
            const auto& row2 = rows2.at(ident);
            if (row1 != row2) {
                ret.push_back(Update(row1, row2));
            }
            rows2_new.erase(ident);
        }
    }

    for (const auto& [ident, row2] : rows2_new) {
        ret.push_back(Insert(row2));
    }

    return ret;
}

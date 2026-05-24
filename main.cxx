#include "DB.hh"
#include "sql.hh"

#include <cxxopts.hpp>

#include <iostream>
#include <filesystem>

using namespace std;
using namespace sql;

vector<Delta> compare(const DB& db1, const DB& db2, const char* table)
{
    vector<Delta> ret;

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

void run(string_view db1_path, string_view db2_path)
{
    DB db1( )
}

int main(int argc, char** argv)
{
    const string progname = filesystem::path(argv[0]).filename().string();
    cxxopts::Options options(progname.c_str(), "PDG API diff tool");
    options.add_options(
        "", {{"h,help", "Print usage"},
            {"db1", "First DB file", cxxopts::value<std::string>()},
            {"db2", "Second DB file", cxxopts::value<std::string>()}});
    options.parse_positional({"db1", "db2"});
    options.positional_help("db1 db2");
    cxxopts::ParseResult result = options.parse(argc, argv);
    
    try {
        const auto db1 = result["db1"].as<std::string>();
        const auto db2 = result["db2"].as<std::string>();

        run(db1, db2);
    } catch (cxxopts::exceptions::option_has_no_value) {
        cout << options.help() << std::endl;
        return 1;
    }
}

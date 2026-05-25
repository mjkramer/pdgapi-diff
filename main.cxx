#include "DB.hh"
#include "sql.hh"

#include <cxxopts.hpp>

#include <filesystem>
#include <iostream>

using namespace std;
using namespace sql;

vector<Delta> match_updates(const RowVec& rows1, const RowVec& rows2)
{
    if (not(rows1.size() == 1 and rows2.size() == 1))
        throw std::runtime_error{"FIXME"};

    if (rows1[0] != rows2[0])
        return {Update(rows1[0], rows2[0])};
    else
        return {};
}

vector<Delta> compare(const DB& db1, const DB& db2, const std::string& table)
{
    vector<Delta> ret;

    const Rows& all_rows1 = db1.rows(table);
    const Rows& all_rows2 = db2.rows(table);
    Rows all_rows2_new{all_rows2};

    for (const auto& [ident, rows1] : all_rows1) {
        if (not all_rows2.contains(ident)) {
            for (const auto& row : rows1)
                // NB fold into trivial case of match_updates?
                ret.push_back(Delete(row));
        } else {
            const auto& rows2 = all_rows2.at(ident);
            for (const auto& delta : match_updates(rows1, rows2))
                ret.push_back(delta);
            all_rows2_new.erase(ident);
        }
    }

    for (const auto& [ident, rows2] : all_rows2_new) {
        for (const auto& row : rows2)
            ret.push_back(Insert(row));
    }

    return ret;
}

void run(const string& db1_path, const string& db2_path,
         const optional<string>& a_table)
{
    DB db1(db1_path);
    DB db2(db2_path);

    auto tables = a_table ? vector{*a_table} : DB::TABLES;

    for (const auto& table : tables) {
        cout << format("### {}\n\n", table);
        cout << format("{}\n\n", db1.cols(table));

        const auto deltas = compare(db1, db2, table);
        for (const auto& delta : deltas) {
            cout << format("{}\n\n", delta);
        }
    }
}

int main(int argc, char** argv)
{
    const string progname = filesystem::path(argv[0]).filename().string();
    cxxopts::Options options(progname.c_str(), "PDG API diff tool");
    options.add_options(
      "", {{"h,help", "Print usage"},
           {"db1", "First DB file", cxxopts::value<std::string>()},
           {"db2", "Second DB file", cxxopts::value<std::string>()},
           {"t,table", "Table to compare", cxxopts::value<std::string>()}});
    options.parse_positional({"db1", "db2"});
    options.positional_help("db1 db2");
    cxxopts::ParseResult result = options.parse(argc, argv);

    try {
        const auto db1_path = result["db1"].as<std::string>();
        const auto db2_path = result["db2"].as<std::string>();
        const auto table = result["table"].as_optional<std::string>();

        run(db1_path, db2_path, table);
    } catch (cxxopts::exceptions::option_has_no_value) {
        cout << options.help() << std::endl;
        return 1;
    }

    return 0;
}

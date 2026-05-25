#include "DB.hh"
#include "sql.hh"
#include "util.hh"

#include <cxxopts.hpp>

#include <iostream>

using namespace sql;
using namespace util;

using namespace std;
using namespace std::ranges;

vector<Delta> match_updates(const RowVec& rows1, const RowVec& rows2)
{
    if (not(rows1.size() <= 1 and rows2.size() <= 1))
        throw std::runtime_error{"FIXME"};

    if (rows1.size() == 0)
        return rows2 | views::transform(util::construct<Insert>{}) |
               to<vector<Delta>>();

    if (rows2.size() == 0)
        return rows1 | views::transform(util::construct<Delete>{}) |
               to<vector<Delta>>();

    if (rows1[0] != rows2[0])
        return {Update(rows1[0], rows2[0])};
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
            const auto deletes = match_updates(rows1, {});
            ret.append_range(deletes);
        } else {
            all_rows2_new.erase(ident);
            const auto& rows2 = all_rows2.at(ident);
            const auto deltas = match_updates(rows1, rows2);
            ret.append_range(deltas);
        }
    }

    for (const auto& [ident, rows2] : all_rows2_new) {
        const auto inserts = match_updates({}, rows2);
        ret.append_range(inserts);
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

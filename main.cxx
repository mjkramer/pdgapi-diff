#include <cxxopts.hpp>
#include <sqlite3.h>

#include <format>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

typedef std::variant<long, double, std::string> SqlVal;
typedef std::vector<SqlVal> SqlRow;
typedef std::vector<SqlRow> SqlTable;

struct Delta {
  SqlRow row;
};

struct Insert : Delta {};
struct Delete : Delta {};
struct Update : Delta {
  SqlRow new_row;
};

struct DB {
  DB(const char* path)
  {
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);
  }

  ~DB()
  {
    sqlite3_close(m_db);
  }

  SqlTable get_all(const char* table,
                   std::set<std::string> exclude_cols = {})
  {
    return {};
  }

  std::vector<std::string>
  get_col_names(const char* table, std::set<std::string> exclude_cols = {})
  {
    std::vector<std::string> ret;

    std::string sql = std::format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
      std::string name = (const char*) sqlite3_column_text(stmt, 1);
      if (exclude_cols.count(name) == 0)
        ret.push_back(std::move(name));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  sqlite3* m_db;
};

void run(const cxxopts::ParseResult& result)
{
  DB db(result["db1"].as<std::string>().c_str());
  auto v = db.get_col_names("pdgdoc", {"value", "indicator"});
  for (auto& c : v) std::cout << c << std::endl;
}

int main(int argc, char** argv)
{
  cxxopts::Options options("pdgapi_diff_pp", "PDG API diff tool");
  options.add_options()
    ("h,help", "Print usage")
    ("max-dist", "Maximum distance", cxxopts::value<int>()->default_value("3"))
    ("db1", "First DB file", cxxopts::value<std::string>())
    ("db2", "Second DB file", cxxopts::value<std::string>())
    ("table", "Table to compare", cxxopts::value<std::string>());
  options.parse_positional({"db1", "db2", "table"});
  options.positional_help("db1 db2 table");
  cxxopts::ParseResult result = options.parse(argc, argv);

  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  try {
    run(result);
  } catch (cxxopts::exceptions::option_has_no_value) {
    std::cout << options.help() << std::endl;
    return 1;
  }

  return 0;
}
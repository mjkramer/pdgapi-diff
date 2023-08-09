#include <cxxopts.hpp>
#include <sqlite3.h>

#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/view/intersperse.hpp>

#include <format>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

struct SqlVal : std::variant<long, double, std::string> {
  std::string as_str()
  {
    if (std::holds_alternative<long>(*this))
      return std::format("{}", std::get<long>(*this));
    if (std::holds_alternative<double>(*this))
      return std::format("{}", std::get<double>(*this));
    if (std::holds_alternative<std::string>(*this))
      return std::get<std::string>(*this);
    throw;
  }
};

using SqlRow = std::vector<SqlVal>;
using SqlTable = std::vector<SqlRow>;

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
    std::vector<std::string> col_names = get_col_names(table, exclude_cols);
    const size_t ncol = col_names.size();
    auto r = col_names | ranges::views::intersperse(", ");
    std::string joined_cols = ranges::accumulate(r, std::string());
    // std::cout << joined_cols << std::endl;
    std::string sql = std::format("SELECT {} FROM {}", joined_cols, table);
    std::cout << sql << std::endl;
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    SqlTable ret;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      SqlRow row;
      for (size_t i = 0; i < ncol; ++i) {
        switch (sqlite3_column_type(stmt, i)) {
          case SQLITE_NULL:
          case SQLITE_INTEGER:
            row.emplace_back(sqlite3_column_int64(stmt, i));
            break;
          case SQLITE_FLOAT:
            row.emplace_back(sqlite3_column_double(stmt, i));
            break;
          case SQLITE_BLOB:
          case SQLITE_TEXT:
            row.emplace_back((const char*)sqlite3_column_text(stmt, i));
            break;
          default:
            throw;
        }
      }
      ret.push_back(std::move(row));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  std::vector<std::string>
  get_col_names(const char* table, std::set<std::string> exclude_cols = {})
  {
    std::string sql = std::format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    std::vector<std::string> ret;
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
  auto table = db.get_all("pdgdoc", {"id"});
  for (auto& row : table) {
    for (auto& val : row) {
      std::cout << val.as_str() << " ";
    }
    std::cout << std::endl;
  }
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
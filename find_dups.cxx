#include <cxxopts.hpp>
#include <sqlite3.h>

#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/view/indices.hpp>
#include <range/v3/view/intersperse.hpp>
#include <range/v3/view/iota.hpp>

#include <format>
#include <iomanip>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

using ranges::accumulate;
using ranges::view::indices;
using ranges::view::intersperse;

struct SqlVal : std::variant<long, double, std::string> {};

std::ostream& operator<<(std::ostream& os, const SqlVal& val)
{
  auto write = [&](auto&& v) {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<T, std::string>)
      os << std::quoted(v);
    else
      os << v;
  };
  std::visit(write, val);
  return os;
}

using PdgId = std::string;

struct SqlRow : std::vector<SqlVal> {
  PdgId pdgid;

  size_t distance(const SqlRow& other) const
  {
    if (pdgid != other.pdgid)
      return 10000;

    size_t ret = 0;
    for (auto i : indices(size())) {
      if ((*this)[i] != other[i])
        ++ret;
    }

    return ret;
  }

  size_t distance_clipped(const SqlRow& other, size_t max_dist) const
  {
    if (pdgid != other.pdgid)
      return 10000;

    size_t ret = 0;
    for (auto i : indices(size())) {
      if ((*this)[i] != other[i])
        ++ret;
      if (ret == max_dist + 1)
        break;
    }

    return ret;
  }
};

using SqlMap = std::unordered_map<PdgId, std::vector<SqlRow>>;

std::ostream& operator<<(std::ostream& os, const SqlRow& row)
{
  os << std::quoted(row.pdgid) << ", ";
  for (auto i : indices(row.size())) {
    if (i > 0)
      os << ", ";
    os << row[i];
  }
  return os;
}

struct DB {
  DB(const char* path)
  {
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);
  }

  ~DB() { sqlite3_close(m_db); }

  SqlMap get_all(const char* table, std::set<std::string> exclude_cols = {})
  {
    std::vector<std::string> col_names = get_col_names(table, exclude_cols);
    const size_t ncol = col_names.size();
    auto r = col_names | intersperse(", ");
    std::string joined_cols = accumulate(r, std::string());
    // std::cout << joined_cols << std::endl;
    std::string sql = std::format("SELECT {} FROM {}", joined_cols, table);
    std::cout << sql << std::endl << std::endl;
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    SqlMap ret;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      SqlRow row;
      // Assume that the pdgid is the first column
      row.pdgid = (const char*)sqlite3_column_text(stmt, 0);
      for (size_t i = 1; i < ncol; ++i) {
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
      ret[row.pdgid].push_back(std::move(row));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  std::vector<std::string> get_col_names(
    const char* table, std::set<std::string> exclude_cols = {})
  {
    std::string sql = std::format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    // We never want the id(?)
    // exclude_cols.insert("id");
    // exclude_cols.insert("parent_id");

    std::vector<std::string> ret;
    // Ensure that pdgid is always the first column
    ret.push_back("pdgid");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      std::string name = (const char*)sqlite3_column_text(stmt, 1);
      if (exclude_cols.count(name) == 0 && name != "pdgid")
        ret.push_back(std::move(name));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  sqlite3* m_db;
};

void run(const cxxopts::ParseResult& result)
{
  DB db(result["db"].as<std::string>().c_str());

  const auto table = result["table"].as<std::string>();
  const auto exclude_cols_v =
    result["exclude-cols"].as<std::vector<std::string>>();
  std::set<std::string> exclude_cols(exclude_cols_v.begin(),
                                     exclude_cols_v.end());
  const SqlMap data = db.get_all(table.c_str(), exclude_cols);
  const int max_dist = result["max-dist"].as<int>();

  for (const auto& [pdgid, rows] : data) {
    if (rows.size() > 1) {
      for (size_t i = 1; i < rows.size(); ++i) {
        for (size_t j = 0; j < i; ++j) {
          if (rows[i].distance(rows[j]) <= max_dist) {
            std::cout << rows[i] << std::endl;
            std::cout << rows[j] << std::endl;
            std::cout << std::endl;
          }
        }
      }
    }
  }
}

int main(int argc, char** argv)
{
  cxxopts::Options options("find_dups", "PDG API duplicate finder");
  options.add_options(
    "", {{"h,help", "Print usage"},
         {"max-dist", "Maximum distance",
          cxxopts::value<int>()->default_value("3")},
         {"exclude-cols", "Columns to exclude",
          cxxopts::value<std::vector<std::string>>()->default_value("")},
         {"db", "DB file", cxxopts::value<std::string>()},
         {"table", "Table to examine", cxxopts::value<std::string>()}});
  options.parse_positional({"db", "table"});
  options.positional_help("db table");
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

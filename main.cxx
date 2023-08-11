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

struct SqlRow : std::vector<SqlVal> {
  size_t distance(const SqlRow& other) const
  {
    size_t ret = 0;
    for (size_t i = 0; i < size(); ++i) {
      if ((*this)[i] != other[i])
        ++ret;
    }
    return ret;
  }
};

using SqlTable = std::vector<SqlRow>;

struct Insert {
  SqlRow row;
};
struct Delete {
  SqlRow row;
};
struct Update {
  SqlRow row, new_row;
};

using Delta = std::variant<Insert, Delete, Update>;

struct DB {
  DB(const char* path)
  {
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);
  }

  ~DB() { sqlite3_close(m_db); }

  SqlTable get_all(const char* table, std::set<std::string> exclude_cols = {})
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

  std::vector<std::string> get_col_names(
    const char* table, std::set<std::string> exclude_cols = {})
  {
    std::string sql = std::format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    std::vector<std::string> ret;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      std::string name = (const char*)sqlite3_column_text(stmt, 1);
      if (exclude_cols.count(name) == 0)
        ret.push_back(std::move(name));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  sqlite3* m_db;
};

std::optional<SqlRow> find_nearest(const SqlRow& needle,
                                   const SqlTable& haystack, size_t max_dist)
{
  size_t min_dist = 100000;
  std::vector<const SqlRow*> matches;

  for (const auto& straw : haystack) {
    size_t dist = needle.distance(straw);
    if (dist < min_dist) {
      dist = min_dist;
      matches.clear();
    }
    if (dist == min_dist) {
      matches.push_back(&straw);
    }
  }

  if (min_dist > max_dist) {
    return std::nullopt;
  }

  if (matches.size() != 1) {
    throw std::format("Ambiguous match!");
  }

  return *matches[0];
}

std::vector<Delta> compare(const SqlTable& rows1, const SqlTable& rows2,
                           size_t max_dist)
{
  std::vector<Delta> ret;
  std::set<SqlRow> rows2_new(rows2.begin(), rows2.end());

  for (const auto& row : rows1) {
    std::optional<SqlRow> nearest = find_nearest(row, rows2, max_dist);
    if (not nearest.has_value()) {
      ret.push_back(Delete(row));
    } else {
      std::optional<SqlRow> reverse_nearest =
        find_nearest(nearest.value(), rows1, max_dist);
      if ((not reverse_nearest.has_value()) or
          (reverse_nearest.value() != row)) {
        throw std::format("Asymmetric match!");
      }
      rows2_new.erase(nearest.value());
      if (nearest.value() != row) {
        ret.push_back(Update(row, nearest.value()));
      }
    }
  }

  for (const auto& row : rows2_new) {
    ret.push_back(Insert(row));
  }

  return ret;
}

void run(const cxxopts::ParseResult& result)
{
  DB db(result["db1"].as<std::string>().c_str());
  auto v = db.get_col_names("pdgdoc", {"value", "indicator"});
  for (auto& c : v)
    std::cout << c << std::endl;
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
  options.add_options()("h,help", "Print usage")(
    "max-dist", "Maximum distance", cxxopts::value<int>()->default_value("3"))(
    "db1", "First DB file", cxxopts::value<std::string>())(
    "db2", "Second DB file", cxxopts::value<std::string>())(
    "table", "Table to compare", cxxopts::value<std::string>());
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
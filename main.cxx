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

struct SqlRow : std::vector<SqlVal> {
  std::string pdgid;

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
// struct Delta : public std::variant<Insert, Delete, Update> {};

std::ostream& operator<<(std::ostream& os, const Delta& delta)
{
  if (std::holds_alternative<Insert>(delta)) {
    os << "INSERT: " << std::get<Insert>(delta).row << "\n";
  } else if (std::holds_alternative<Delete>(delta)) {
    os << "DELETE: " << std::get<Delete>(delta).row << "\n";
  } else if (std::holds_alternative<Update>(delta)) {
    os << "UPDATE-:" << std::get<Update>(delta).row << "\n";
    os << "UPDATE+:" << std::get<Update>(delta).new_row << "\n";
  }
  return os;
}

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
    auto r = col_names | intersperse(", ");
    std::string joined_cols = accumulate(r, std::string());
    // std::cout << joined_cols << std::endl;
    std::string sql = std::format("SELECT {} FROM {}", joined_cols, table);
    std::cout << sql << std::endl;
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    SqlTable ret;
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

    // We never want the id(?)
    exclude_cols.insert("id");
    exclude_cols.insert("parent_id");

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

std::optional<SqlRow> find_nearest(const SqlRow& needle,
                                   const SqlTable& haystack, size_t max_dist)
{
  size_t min_dist = 100000;
  std::vector<const SqlRow*> matches;

  for (const auto& straw : haystack) {
    size_t dist = needle.distance_clipped(straw, max_dist);
    if (dist < min_dist) {
      min_dist = dist;
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
                           size_t max_dist, bool pedantic = false)
{
  std::vector<Delta> ret;
  std::set<SqlRow> rows2_new(rows2.begin(), rows2.end());

  for (auto& row : rows1) {
    std::optional<SqlRow> nearest = find_nearest(row, rows2, max_dist);
    if (not nearest.has_value()) {
      ret.push_back(Delete(row));
    } else {
      if (pedantic) {
        std::optional<SqlRow> reverse_nearest =
          find_nearest(nearest.value(), rows1, max_dist);
        if ((not reverse_nearest.has_value()) or
            (reverse_nearest.value() != row)) {
          throw std::format("Asymmetric match!");
        }
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

void test_run(const cxxopts::ParseResult& result)
{
  DB db(result["db1"].as<std::string>().c_str());
  auto v = db.get_col_names("pdgdoc", {"value", "indicator"});
  for (auto& c : v)
    std::cout << c << std::endl;
  auto table = db.get_all("pdgdoc", {"id"});
  for (auto& row : table) {
    for (auto& val : row) {
      std::cout << val << " ";
    }
    std::cout << std::endl;
  }
}

void run(const cxxopts::ParseResult& result)
{
  DB db1(result["db1"].as<std::string>().c_str());
  DB db2(result["db2"].as<std::string>().c_str());

  const auto table = result["table"].as<std::string>();
  const auto exclude_cols_v =
    result["exclude-cols"].as<std::vector<std::string>>();
  std::set<std::string> exclude_cols(exclude_cols_v.begin(),
                                     exclude_cols_v.end());
  const SqlTable rows1 = db1.get_all(table.c_str(), exclude_cols);
  const SqlTable rows2 = db2.get_all(table.c_str(), exclude_cols);
  const int max_dist = result["max-dist"].as<int>();
  const bool pedantic = result["pedantic"].as<bool>();

  std::vector<Delta> deltas = compare(rows1, rows2, max_dist, pedantic);
  for (const auto& delta : deltas) {
    std::cout << delta << std::endl;
  }
}

int main(int argc, char** argv)
{
  cxxopts::Options options("pdgapi_diff_pp", "PDG API diff tool");
  options.add_options(
    "", {{"h,help", "Print usage"},
         {"max-dist", "Maximum distance",
          cxxopts::value<int>()->default_value("3")},
         {"pedantic", "Pedantic mode"},
         {"exclude-cols", "Columns to exclude",
          cxxopts::value<std::vector<std::string>>()->default_value("")},
         {"db1", "First DB file", cxxopts::value<std::string>()},
         {"db2", "Second DB file", cxxopts::value<std::string>()},
         {"table", "Table to compare", cxxopts::value<std::string>()}});
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

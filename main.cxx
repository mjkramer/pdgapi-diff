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

namespace settings {
  bool pedantic;
  size_t max_dist;
  std::set<std::string> exclude_cols;
}

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

  size_t distance_clipped(const SqlRow& other) const
  {
    if (pdgid != other.pdgid)
      return 10000;

    size_t ret = 0;
    for (auto i : indices(size())) {
      if ((*this)[i] != other[i])
        ++ret;
      if (ret == settings::max_dist + 1)
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
    os << "UPDATE-: " << std::get<Update>(delta).row << "\n";
    os << "UPDATE+: " << std::get<Update>(delta).new_row << "\n";
  }
  return os;
}

struct DB {
  DB(const char* path)
  {
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);
  }

  ~DB() { sqlite3_close(m_db); }

  SqlMap get_all(const char* table)
  {
    std::vector<std::string> col_names = get_col_names(table);
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
    const char* table)
  {
    std::string sql = std::format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    std::vector<std::string> ret;
    // Ensure that pdgid is always the first column
    ret.push_back("pdgid");
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      std::string name = (const char*)sqlite3_column_text(stmt, 1);
      if (settings::exclude_cols.count(name) == 0 && name != "pdgid")
        ret.push_back(std::move(name));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  sqlite3* m_db;
};

std::optional<SqlRow> find_nearest(const SqlRow& needle, const SqlMap& haystack)
{
  size_t min_dist = 100000;
  std::vector<const SqlRow*> matches;

  if (haystack.count(needle.pdgid) == 0)
    return std::nullopt;

  for (const auto& straw : haystack.at(needle.pdgid)) {
    size_t dist = needle.distance_clipped(straw);
    if (dist < min_dist) {
      min_dist = dist;
      matches.clear();
    }
    if (dist == min_dist) {
      matches.push_back(&straw);
    }
  }

  if (min_dist > settings::max_dist) {
    return std::nullopt;
  }

  if (matches.size() != 1) {
    std::cerr << "Ambiguous match!" << std::endl;
    std::cerr << "FROM: " << needle << std::endl;
    for (const auto match : matches) {
      std::cerr << "TO:   " << *match << std::endl;
    }
    std::cerr << std::endl;
    // throw std::format("Ambiguous match!");
  }

  return *matches[0];
}

std::vector<Delta> compare(const SqlMap& map1, const SqlMap& map2)
{
  std::vector<Delta> ret;
  // std::set<SqlRow> rows2_new(rows2.begin(), rows2.end());      // !
  std::set<SqlRow> rows2_new;
  for (const auto& [pdgid2, rows2] : map2) {
    for (const auto& row : rows2) {
      rows2_new.insert(row);
    }
  }

  for (const auto& [pdgid, rows1] : map1) {
    for (const auto& row : rows1) {
      std::optional<SqlRow> nearest = find_nearest(row, map2);
      if (not nearest.has_value()) {
        ret.push_back(Delete(row));
      } else {
        if (settings::pedantic) {
          std::optional<SqlRow> reverse_nearest =
            find_nearest(nearest.value(), map1);
          if ((not reverse_nearest.has_value()) or
              (reverse_nearest.value() != row)) {
            std::cerr << "Asymmetric match!" << std::endl;
            std::cerr << "FROM: " << nearest.value() << std::endl;
            std::cerr << "TO:   " << reverse_nearest.value() << std::endl;
            std::cerr << std::endl;
            // throw std::format("Asymmetric match!");
          }
        }
        rows2_new.erase(nearest.value());
        if (nearest.value() != row) {
          ret.push_back(Update(row, nearest.value()));
        }
      }
    }
  }

  for (const auto& row : rows2_new) {
    ret.push_back(Insert(row));
  }

  return ret;
}

void run(const char* db1_path, const char* db2_path, const char* table)
{
  DB db1(db1_path);
  DB db2(db2_path);

  const SqlMap rows1 = db1.get_all(table);
  const SqlMap rows2 = db2.get_all(table);

  std::vector<Delta> deltas = compare(rows1, rows2);
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

  settings::max_dist = result["max-dist"].as<int>();
  settings::pedantic = result["pedantic"].as<bool>();

  const auto exclude_cols_v =
    result["exclude-cols"].as<std::vector<std::string>>();
  settings::exclude_cols = std::set<std::string>(exclude_cols_v.begin(),
                                                 exclude_cols_v.end());
  // We never want the id(?)
  settings::exclude_cols.insert("id");
  settings::exclude_cols.insert("parent_id");
  settings::exclude_cols.insert("pdgid_id");

  try {
    const char* db1 = result["db1"].as<std::string>().c_str();
    const char* db2 = result["db2"].as<std::string>().c_str();
    const char* table = result["table"].as<std::string>().c_str();

    run(db1, db2, table);
  } catch (cxxopts::exceptions::option_has_no_value) {
    std::cout << options.help() << std::endl;
    return 1;
  }

  return 0;
}

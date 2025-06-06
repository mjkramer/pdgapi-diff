#include <cxxopts.hpp>
#include <sqlite3.h>

#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/view/indices.hpp>
#include <range/v3/view/intersperse.hpp>
#include <range/v3/view/iota.hpp>

#include <algorithm>
#include <cmath>
#include <filesystem>
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

#define ANSI_RESET "\033[0m"
#define ANSI_RED   "\033[31m"
#define ANSI_GREEN "\033[32m"
#define ANSI_CYAN  "\033[36m"

namespace settings {
bool pedantic;
bool only_updates;
bool no_updates;
size_t max_dist;
std::set<std::string> exclude_cols;
std::string align;
bool no_color;
}

namespace constants {
const std::set<std::string> strict_cols = { "value_type" };
}

// void* represents a NULL
struct SqlVal : std::variant<void*, long, double, std::string> {
  std::string str() const
  {
    std::ostringstream os;
    auto write = [&](auto&& v) {
      using T = std::decay_t<decltype(v)>;
      if constexpr (std::is_same_v<T, std::string>)
        os << std::quoted(v);
      else if constexpr (std::is_same_v<T, void*>)
        os << "NULL";
      else
        os << v;
    };
    std::visit(write, *this);
    return os.str();
  }
};

std::ostream& operator<<(std::ostream& os, const SqlVal& val)
{
  os << val.str();
  return os;
}

bool isclose(double a, double b, double rel_tol = 1e-6, double abs_tol = 0.0)
{
  const auto max_abs = std::max(std::fabs(a), std::fabs(b));
  return std::fabs(a - b) <= std::max(rel_tol * max_abs, abs_tol);
}

bool operator==(const SqlVal& lhs, const SqlVal& rhs)
{
  auto compare = [&](auto&& l) {
    using T = std::decay_t<decltype(l)>;
    if (not std::holds_alternative<T>(rhs))
      return false;
    const auto r = std::get<T>(rhs);

    if constexpr (std::is_same_v<T, double>)
      return isclose(l, r);
    else
      return l == r;
  };

  return std::visit(compare, lhs);
}

using Ident = std::string;

// forward declarations
struct SqlRow;
std::ostream& operator<<(std::ostream& os, const SqlRow& row);

struct SqlRow : std::vector<SqlVal> {
  Ident ident;

  // HACK. Should be const ref, but would need to customize move ctor.
  // (The move ctor is called during std::vector::erase)
  std::vector<std::string> col_names;

  SqlRow(const std::vector<std::string>& col_names) :
    col_names(col_names) {}

  // NOTE: SqlRow inherits operator== from vector so the ident is not compared
  // unless we override the operator. (But do we need an equality operator?)
  bool operator==(const SqlRow& other) const
  {
    return ident == other.ident &&
      *static_cast<const std::vector<SqlVal>*>(this)
      == *static_cast<const std::vector<SqlVal>*>(&other);
  }

  size_t distance(const SqlRow& other) const
  {
    if (ident != other.ident)
      return 10000;

    size_t ret = 0;
    for (auto i : indices(size())) {
      // This calls "isclose" when the column is floating-point
      if ((*this)[i] != other[i]) {
        ++ret;
        if (constants::strict_cols.count(col_names[i])) {
          return 5000;
        }
      }
      if (ret == settings::max_dist + 1)
        break;
    }

    return ret;
  }

  std::string str(const SqlRow* other = nullptr,
                  const char* hl_ansi_color = nullptr) const
  {
    std::ostringstream os;

    os << std::quoted(ident) << ", ";
    for (auto i : indices(size())) {
      if (i > 0)
        os << ", ";
      if (other) {
        const bool highlight = (*this)[i] != (*other)[i];
        if (highlight and not settings::no_color) os << hl_ansi_color;

        if (settings::align == "none")
          os << (*this)[i];
        else {
          const size_t width = std::max((*this)[i].str().size(),
                                        (*other)[i].str().size());
          const auto align = settings::align == "left" ? std::left : std::right;
          os << align << std::setw(width) << (*this)[i] << std::setw(0);
        }

        if (highlight and not settings::no_color) os << ANSI_RESET;
      } else
        os << (*this)[i];
    }
    return os.str();
  }

  std::string hl_diffs_to(const SqlRow& other) const
  {
    return str(&other, ANSI_RED);
  }

  std::string hl_diffs_from(const SqlRow& other) const
  {
    return str(&other, ANSI_GREEN);
  }
};

struct SqlMap : std::unordered_map<Ident, std::vector<SqlRow>> {
  std::vector<std::string> col_names;
};

std::ostream& operator<<(std::ostream& os, const SqlRow& row)
{
  os << row.str();
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
  auto color = [&](const char* str, const char* ansi) {
    if (not settings::no_color)
      return std::string(ansi) + str + ANSI_RESET;
    else
      return std::string(str);
  };

  auto red = [&](const char* str) {
    return color(str, ANSI_RED);
  };

  auto green = [&](const char* str) {
    return color(str, ANSI_GREEN);
  };

  auto cyan = [&](const char* str) {
    return color(str, ANSI_CYAN);
  };

  if (std::holds_alternative<Insert>(delta)) {
    os << green("INSERT: ") << std::get<Insert>(delta).row << "\n";
  } else if (std::holds_alternative<Delete>(delta)) {
    os << red("DELETE: ") << std::get<Delete>(delta).row << "\n";
  } else if (std::holds_alternative<Update>(delta)) {
    // os << std::setprecision(30);
    const auto update = std::get<Update>(delta);
    os << cyan("UPDATE-: ") << update.row.hl_diffs_to(update.new_row) << "\n";
    os << cyan("UPDATE+: ") << update.new_row.hl_diffs_from(update.row) << "\n";
  }
  return os;
}

const char* get_ident_col(const char* table);

struct DB {
  // The index is currently just used to avoid printing the same thing multiple
  // times (see get_all)
  static inline size_t next_db_index = 0;
  size_t db_index;

  DB(const char* path)
  {
    db_index = next_db_index++;
    sqlite3_open_v2(path, &m_db, SQLITE_OPEN_READONLY, nullptr);
  }

  ~DB() { sqlite3_close(m_db); }

  SqlMap get_all(const char* table)
  {
    std::string sql = get_table_query(table);
    if (db_index == 0)
      std::cout << sql << std::endl << std::endl;
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);
    const size_t ncol = sqlite3_column_count(stmt);

    SqlMap ret;

    // We start at i = 1 to exclude the "ident" column
    for (size_t i = 1; i < ncol; ++i) {
      ret.col_names.push_back(sqlite3_column_name(stmt, i));
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
      SqlRow row(ret.col_names);
      // get_col_names ensures that the ident is the first column
      row.ident = (const char*)sqlite3_column_text(stmt, 0);
      for (size_t i = 1; i < ncol; ++i) {
        switch (sqlite3_column_type(stmt, i)) {
        case SQLITE_NULL:
          row.emplace_back(nullptr);
          break;
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
      ret[row.ident].push_back(std::move(row));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  std::string get_table_query(const char* table)
  {
    // Special case for pdgitem_map
    if (std::string_view(table) == "pdgitem_map") {
      return "SELECT pdgitem_map.name AS name, pdgitem.name AS target_name, sort "
        "FROM pdgitem_map JOIN pdgitem ON target_id == pdgitem.id";
    }

    std::vector<std::string> col_names = get_col_names(table);
    auto r = col_names | intersperse(", ");
    std::string joined_cols = accumulate(r, std::string());
    return std::format("SELECT {} FROM {}", joined_cols, table);
  }

  std::vector<std::string> get_col_names(
    const char* table)
  {
    std::string sql = std::format("PRAGMA table_info({})", table);
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(m_db, sql.c_str(), -1, &stmt, nullptr);

    std::vector<std::string> ret;
    // Ensure that the ident is always the first column
    const char* ident_col = get_ident_col(table);
    ret.push_back(ident_col);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      std::string name = (const char*)sqlite3_column_text(stmt, 1);
      if (settings::exclude_cols.count(name) == 0 && name != ident_col)
        ret.push_back(std::move(name));
    }

    sqlite3_finalize(stmt);
    return ret;
  }

  sqlite3* m_db;
};

std::optional<SqlRow> find_nearest(const SqlRow& needle, const SqlMap& haystack)
{
  size_t min_dist = 1000;
  std::vector<const SqlRow*> matches;

  if (haystack.count(needle.ident) == 0)
    return std::nullopt;

  for (const auto& straw : haystack.at(needle.ident)) {
    size_t dist = needle.distance(straw);
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
    // skip duplicates (unless in pedantic mode)
    bool seen_multiple = false;
    for (size_t i = 1; i < matches.size(); ++i) {
      if (matches[i]->distance(*matches[0]) > 0) {
        seen_multiple = true;
        break;
      }
    }

    if (seen_multiple or settings::pedantic) {
      std::cerr << "Ambiguous match!" << std::endl;
      std::cerr << "FROM: " << needle << std::endl;
      for (const auto match : matches) {
        std::cerr << "TO:   " << *match << std::endl;
      }
      std::cerr << std::endl;
    }
  }

  return *matches[0];
}

std::vector<Delta> compare(const SqlMap& map1, const SqlMap& map2)
{
  std::vector<Delta> ret;
  SqlMap map2_inserted = map2;
  for (const auto& [ident, rows1] : map1) {
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
        auto& v = map2_inserted[ident];
        const auto it = std::find(v.begin(), v.end(), nearest.value());
        if (it != v.end()) v.erase(it);
        if (nearest.value() != row) {
          ret.push_back(Update(row, nearest.value()));
        }
      }
    }
  }

  for (const auto& [ident, rows] : map2_inserted) {
    for (const auto& row : rows) {
      ret.push_back(Insert(row));
    }
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
    const bool is_update = std::holds_alternative<Update>(delta);
    if (settings::only_updates and not is_update)
      continue;
    if (settings::no_updates and is_update)
      continue;
    std::cout << delta << std::endl;
  }
}

const char* get_ident_col(const char* table)
{
  std::string_view t(table);

  if (t == "pdgid" ||
      t == "pdgparticle" ||
      t == "pdgdata" ||
      t == "pdgdecay" ||
      t == "pdgmeasurement" ||
      t == "pdgtext" ||
      t == "pdgfootnote")
    return "pdgid";

  if (t == "pdgitem" ||
      t == "pdgitem_map")
    return "name";

  if (t == "pdgmeasurement_footnote" ||
      t == "pdgmeasurement_values")
    return "pdgmeasurement_id";

  if (t == "pdgreference")
    return "document_id";

  if (t == "pdgid_map")
    return "source";

  throw;
}

int main(int argc, char** argv)
{
  const std::string progname =
    std::filesystem::path(argv[0]).filename().string();

  cxxopts::Options options(progname.c_str(), "PDG API diff tool");
  options.add_options(
    "", {{"h,help", "Print usage"},
         {"max-dist", "Maximum distance",
          cxxopts::value<int>()->default_value("3")},
         {"pedantic", "Pedantic mode"},
         {"include-primary-keys", "Show differences between primary keys"},
         {"only-updates", "Show only UPDATES, not INSERTS or DELETES"},
         {"no-updates", "Don't show UPDATES, just INSERTS and DELETES"},
         {"align", "Alignment of columns (left, right, or none)",
          cxxopts::value<std::string>()->default_value("right")},
         {"no-color", "Disable color"},
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
  settings::only_updates = result["only-updates"].as<bool>();
  settings::no_updates = result["no-updates"].as<bool>();
  settings::align = result["align"].as<std::string>();
  settings::no_color = result["no-color"].as<bool>();

  if (settings::only_updates and settings::no_updates) {
    std::cerr << "--only-updates and --no-updates are mutually exclusive" << std::endl;
    return 1;
  }

  if (settings::align != "left" &&
      settings::align != "right" &&
      settings::align != "none") {
    std::cerr << "--align must be left, right, or none" << std::endl;
    return 1;
  }

  const auto exclude_cols_v =
    result["exclude-cols"].as<std::vector<std::string>>();
  settings::exclude_cols = std::set<std::string>(exclude_cols_v.begin(),
                                                 exclude_cols_v.end());

  if (not result["include-primary-keys"].as<bool>()) {
    settings::exclude_cols.insert("id");
    settings::exclude_cols.insert("parent_id");
    settings::exclude_cols.insert("pdgid_id");
    settings::exclude_cols.insert("pdgitem_id");
  }

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

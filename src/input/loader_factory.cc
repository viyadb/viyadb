#include <stdexcept>
#include "db/column.h"
#include "db/database.h"
#include "codegen/db/upsert.h"
#include "input/loader.h"
#include "input/loader_factory.h"
#include "input/file.h"
#include "util/config.h"

namespace viya {
namespace input {

namespace db = viya::db;

std::vector<int> LoaderFactory::CreateTupleIdxMapping(const util::Config& config, const db::Table& table) {
  auto columns = table.columns();

  auto has_field_mapping = std::find_if(columns.begin(), columns.end(), [](auto col) {
    return !col->input_field().empty();
  }) != columns.end();

  std::vector<int> tuple_idx_map;

  auto input_cols = Loader::GetInputColumns(table);
  tuple_idx_map.resize(input_cols.size());

  if (config.exists("columns")) {
    auto load_cols = config.strlist("columns");
    size_t idx = 0;
    for (auto col : input_cols) {
      const std::string& col_name = col->input_field().empty() ? col->name() : col->input_field();
      auto it = std::find(load_cols.begin(), load_cols.end(), col_name);
      if (it == load_cols.end()) {
        throw std::runtime_error("Column name '" + col_name + "' is not specified in load spec");
      }
      tuple_idx_map[idx++] = std::distance(load_cols.begin(), it);
    }
  } else {
    if (has_field_mapping) {
      throw std::runtime_error(
        "Column names must be specified, because one or more columns define field name mapping");
    }
    for (size_t idx = 0; idx < input_cols.size(); ++idx) {
      tuple_idx_map[idx] = idx;
    }
  }

  return std::move(tuple_idx_map);
}

Loader* LoaderFactory::Create(const util::Config& config, db::Database& database) {
  auto table = database.GetTable(config.str("table"));

  auto tuple_idx_map = CreateTupleIdxMapping(config, const_cast<const db::Table&>(*table));

  std::string type = config.str("type");
  if (type == "file") {
    return new FileLoader(*table, config, tuple_idx_map);
  }
  throw std::invalid_argument("Unsupported input type: " + type);
}

}}

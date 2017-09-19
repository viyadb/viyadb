#include "db/column.h"
#include "input/loader.h"
#include "input/file.h"

namespace viya {
namespace input {

namespace db = viya::db;

Loader::Format parse_format(const std::string& format) {
  if (format == "tsv") {
    return Loader::Format::TSV;
  }
  throw std::invalid_argument("Unsupported input format: " + format);
}

Loader* LoaderFactory::Create(const util::Config& config, db::Database& database) {
  auto table = database.GetTable(config.str("table"));
  Loader::Format format = parse_format(config.str("format"));

  std::vector<const db::Column*> table_cols;
  for (auto dimension : table->dimensions()) {
    table_cols.push_back(dimension);
  }
  for (auto metric : table->metrics()) {
    if (metric->agg_type() != db::Metric::AggregationType::COUNT) {
      table_cols.push_back(metric);
    }
  }

  std::vector<int> tuple_idx_map;
  if (config.exists("columns")) {
    auto file_cols = config.strlist("columns");
    tuple_idx_map.resize(file_cols.size());
    for (size_t fc_idx = 0; fc_idx < file_cols.size(); ++fc_idx) {
      int target_idx = -1;
      for (int tc_idx = 0; tc_idx < (int)table_cols.size(); ++tc_idx) {
        if (table_cols[tc_idx]->name() == file_cols[fc_idx]) {
          target_idx = tc_idx;
          break;
        }
      }
      tuple_idx_map[fc_idx] = target_idx;
    }
  } else {
    tuple_idx_map.resize(table_cols.size());
    for (int tc_idx = 0; tc_idx < (int)table_cols.size(); ++tc_idx) {
      tuple_idx_map[tc_idx] = tc_idx;
    } 
  }

  std::string type = config.str("type");
  if (type == "file") {
    return new FileLoader(*table, format, config.str("file"), tuple_idx_map);
  }
  throw std::invalid_argument("Unsupported input type: " + type);
}

}}

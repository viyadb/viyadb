#include "db/column.h"
#include "db/table.h"
#include "db/database.h"
#include "db/stats.h"
#include "input/loader.h"

namespace viya {
namespace input {

namespace db = viya::db;

Loader::Loader(db::Table& table, const std::vector<int>& tuple_idx_map):
  table_(table),tuple_idx_map_(tuple_idx_map),
  stats_(table.database().statsd(), table.name()) {

  cg::UpsertGenerator upsert_gen(table.database().compiler(), table_, tuple_idx_map_);

  before_upsert_ = upsert_gen.BeforeFunction();
  after_upsert_ = upsert_gen.AfterFunction();
  upsert_ = upsert_gen.Function();
  upsert_gen.SetupFunction()(table_);
}

void Loader::BeforeLoad() {
  before_upsert_();
}

db::UpsertStats Loader::AfterLoad() {
  return after_upsert_();
}

std::vector<const db::Column*> Loader::GetInputColumns(const db::Table& table) {
  std::vector<const db::Column*> input_cols;
  for (auto dimension : table.dimensions()) {
    input_cols.push_back(dimension);
  }
  for (auto metric : table.metrics()) {
    if (metric->agg_type() != db::Metric::AggregationType::COUNT) {
      input_cols.push_back(metric);
    }
  }
  return std::move(input_cols);
}

}}

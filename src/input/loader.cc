#include "db/table.h"
#include "db/database.h"
#include "input/loader.h"

namespace viya {
namespace input {

Loader::Loader(const util::Config& config, const db::Table& table):
  desc_(config, table),
  stats_(table.database().statsd(), table.name()) {

  cg::UpsertGenerator upsert_gen(desc_);

  before_upsert_ = upsert_gen.BeforeFunction();
  after_upsert_ = upsert_gen.AfterFunction();
  upsert_ = upsert_gen.Function();
  upsert_gen.SetupFunction()(table);
}

void Loader::BeforeLoad() {
  before_upsert_();
}

db::UpsertStats Loader::AfterLoad() {
  return after_upsert_();
}

}}

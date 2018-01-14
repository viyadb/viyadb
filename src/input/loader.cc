/*
 * Copyright (c) 2017-present ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "input/loader.h"
#include "db/database.h"
#include "db/table.h"

namespace viya {
namespace input {

Loader::Loader(const util::Config &config, db::Table &table)
    : desc_(config, table), table_(table),
      stats_(table.database().statsd(), table.name()) {

  cg::UpsertGenerator upsert_gen(desc_);

  before_upsert_ = upsert_gen.BeforeFunction();
  after_upsert_ = upsert_gen.AfterFunction();
  upsert_ = upsert_gen.Function();

  void *upsert_ctx = upsert_gen.SetupFunction()(desc_);
  table.set_upsert_ctx(upsert_ctx);
}

void Loader::BeforeLoad() { before_upsert_(table_.upsert_ctx()); }

db::UpsertStats Loader::AfterLoad() {
  return after_upsert_(table_.upsert_ctx());
}
} // namespace input
} // namespace viya

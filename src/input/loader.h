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

#ifndef VIYA_INPUT_LOADER_H_
#define VIYA_INPUT_LOADER_H_

#include "codegen/db/upsert.h"
#include "db/table.h"
#include "input/loader_desc.h"
#include "input/stats.h"

namespace viya {
namespace input {

namespace cg = viya::codegen;

class Loader {
public:
  Loader(const util::Config &config, db::Table &table);
  Loader(const Loader &other) = delete;
  virtual ~Loader() = default;

  virtual void LoadData() = 0;
  const LoaderDesc &desc() const { return desc_; }

protected:
  void BeforeLoad();
  void Load(std::vector<std::string> &values) { upsert_(loader_ctx_, values); }
  db::UpsertStats AfterLoad();

protected:
  const LoaderDesc desc_;
  db::Table &table_;
  LoaderStats stats_;
  cg::BeforeUpsertFn before_upsert_;
  cg::AfterUpsertFn after_upsert_;
  cg::UpsertFn upsert_;
  void *loader_ctx_;
};

} // namespace input
} // namespace viya

#endif // VIYA_INPUT_LOADER_H_

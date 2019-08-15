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

#include "db/database.h"
#include "db/table.h"
#include "input/loader_factory.h"
#include "query/runner.h"
#include <glog/logging.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace viya {
namespace db {

Database::Database(const util::Config &config)
    : Database(config, 1, config.num("query_threads", 1)) {}

Database::Database(const util::Config &config, size_t write_threads,
                   size_t read_threads)
    : compiler_(config), write_pool_(write_threads), read_pool_(read_threads),
      watcher_(*this), last_batch_id_(0L) {

  if (config.exists("tables")) {
    for (const util::Config &table_conf : config.sublist("tables")) {
      CreateTable(table_conf);
    }
  }

  if (config.exists("statsd")) {
    statsd_.Connect(config.sub("statsd"));
  }
}

Database::~Database() {
  for (auto &it : tables_) {
    delete it.second;
  }
}

void Database::CreateTable(const util::Config &table_conf) {
  CreateTable(table_conf.str("name"), table_conf);
}

void Database::CreateTable(const std::string &name,
                           const util::Config &table_conf) {
  folly::RWSpinLock::WriteHolder guard(lock_);
  LOG(INFO) << "Creating table: " << name;
  auto it = tables_.find(name);
  if (it != tables_.end()) {
    throw std::runtime_error("Table already exists: " + name);
  }
  tables_.insert(std::make_pair(name, new Table(table_conf, *this)));
}

void Database::DropTable(const std::string &name) {
  LOG(INFO) << "Dropping table: " << name;
  folly::RWSpinLock::WriteHolder guard(lock_);
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    throw std::invalid_argument("No such table: " + name);
  }
  auto table = it->second;
  tables_.erase(it);
  delete table;
}

Table *Database::GetTable(const std::string &name) {
  folly::RWSpinLock::ReadHolder guard(lock_);
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    throw std::invalid_argument("No such table: " + name);
  }
  return it->second;
}

void Database::PrintMetadata(std::string &metadata) {
  nlohmann::json meta;
  meta["tables"] = nlohmann::json::array();
  {
    folly::RWSpinLock::ReadHolder guard(lock_);
    for (auto &it : tables_) {
      auto table = it.second;
      nlohmann::json table_meta;
      table_meta["name"] = table->name();
      meta["tables"].push_back(table_meta);
    }
  }
  metadata = meta.dump();
}

query::QueryStats Database::Query(const util::Config &query_conf,
                                  query::RowOutput &output) {
  query::QueryFactory query_factory;
  std::unique_ptr<query::Query> q(query_factory.Create(query_conf, *this));
  query::QueryRunner query_runner(*this, output);
  q->Accept(query_runner);
  return query_runner.stats();
}

void Database::Load(const util::Config &load_conf) {
  input::LoaderFactory loader_factory;
  std::unique_ptr<input::Loader> loader(
      loader_factory.Create(load_conf, *this));
  loader->LoadData();

  if (load_conf.exists("batch_id")) {
    auto id = load_conf.num("batch_id");
    if (id > last_batch_id_) {
      last_batch_id_ = id;
    }
  }
}
} // namespace db
} // namespace viya

/*
 * Copyright (c) 2017 ViyaDB Group
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

#include <json.hpp>
#include <glog/logging.h>
#include "db/database.h"
#include "db/table.h"
#include "query/runner.h"
#include "input/loader_factory.h"

namespace viya {
namespace db {

Database::Database(const util::Config& config)
  :compiler_(config),
  write_pool_(1),
  read_pool_(config.num("query_threads", 1)),
  watcher_(*this) {

  if (config.exists("tables")) {
    for (const util::Config& table_conf : config.sublist("tables")) {
      CreateTable(table_conf);
    }
  }

  if (config.exists("statsd")) {
    statsd_.Connect(config.sub("statsd"));
  }
}

Database::~Database() {
  for (auto& it : tables_) {
    delete it.second;
  }
}

void Database::CreateTable(const util::Config& table_conf) {
  folly::RWSpinLock::WriteHolder guard(lock_);
  std::string name = table_conf.str("name");
  LOG(INFO)<<"Creating table: "<<name;
  auto it = tables_.find(name);
  if (it != tables_.end()) {
    throw std::runtime_error("Table already exists: " + name);
  }
  tables_.insert(std::make_pair(name, new Table(table_conf, *this)));
}

void Database::DropTable(const std::string& name) {
  LOG(INFO)<<"Dropping table: "<<name;
  folly::RWSpinLock::WriteHolder guard(lock_);
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    throw std::invalid_argument("No such table: " + name);
  }
  auto table = it->second;
  tables_.erase(it);
  delete table;
}

Table* Database::GetTable(const std::string& name) {
  folly::RWSpinLock::ReadHolder guard(lock_);
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    throw std::invalid_argument("No such table: " + name);
  }
  return it->second;
}

void Database::PrintMetadata(std::string& metadata) {
  using json = nlohmann::json;
  json meta;
  meta["tables"] = json::array();
  {
    folly::RWSpinLock::ReadHolder guard(lock_);
    for (auto& it : tables_) {
      auto table = it.second;
      json table_meta;
      table_meta["name"] = table->name();
      meta["tables"].push_back(table_meta);
    }
  }
  metadata = meta.dump();
}

query::QueryStats Database::Query(const util::Config& query_conf, query::RowOutput& output) {
  query::QueryFactory query_factory;
  auto* q = query_factory.Create(query_conf, *this);
  query::QueryRunner query_runner(*this, output);
  q->Accept(query_runner);
  delete q;
  return query_runner.stats();
}

void Database::Load(const util::Config& load_conf) {
  input::LoaderFactory loader_factory;
  auto loader = loader_factory.Create(load_conf, *this);
  loader->LoadData();
  delete loader;
}

}}

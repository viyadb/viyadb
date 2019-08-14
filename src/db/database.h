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

#ifndef VIYA_DB_DATABASE_H_
#define VIYA_DB_DATABASE_H_

#include "codegen/compiler.h"
#include "db/dictionary.h"
#include "input/watcher.h"
#include "query/output.h"
#include "query/stats.h"
#include "util/config.h"
#include "util/macros.h"
#include "util/rwlock.h"
#include "util/statsd.h"
#include <ThreadPool/ThreadPool.h>
#include <unordered_map>

namespace viya {
namespace db {

namespace cg = viya::codegen;
namespace query = viya::query;
namespace input = viya::input;

class Table;

class Database {
public:
  Database(const util::Config &config);
  Database(const util::Config &config, size_t write_threads,
           size_t read_threads);
  DISALLOW_COPY_AND_MOVE(Database);
  ~Database();

  void CreateTable(const util::Config &table_conf);
  void CreateTable(const std::string &name, const util::Config &table_conf);
  void DropTable(const std::string &name);
  Table *GetTable(const std::string &name);

  void PrintMetadata(std::string &);

  cg::Compiler &compiler() { return compiler_; }
  Dictionaries &dicts() { return dicts_; }
  ThreadPool &read_pool() { return read_pool_; }
  ThreadPool &write_pool() { return write_pool_; }
  input::Watcher &watcher() { return watcher_; }
  const util::Statsd &statsd() const { return statsd_; }
  const std::unordered_map<std::string, Table *> &tables() const {
    return tables_;
  }
  long last_batch_id() const { return last_batch_id_; }

  query::QueryStats Query(const util::Config &query_conf,
                          query::RowOutput &output);
  void Load(const util::Config &load_conf);

private:
  cg::Compiler compiler_;
  std::unordered_map<std::string, Table *> tables_;
  folly::RWSpinLock lock_;
  Dictionaries dicts_;

  ThreadPool write_pool_;
  ThreadPool read_pool_;

  input::Watcher watcher_;
  util::Statsd statsd_;

  long last_batch_id_;
};
} // namespace db
} // namespace viya

#endif // VIYA_DB_DATABASE_H_

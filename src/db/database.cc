#include <json.hpp>
#include <glog/logging.h>
#include "db/database.h"
#include "query/runner.h"
#include "input/loader.h"
#include "codegen/db/metadata.h"

namespace viya {
namespace db {

Database::Database(const util::Config& config)
  :compiler_(config.sub("compiler")),
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
  lock_.lock();
  std::string name = table_conf.str("name");
  LOG(INFO)<<"Creating table: "<<name;
  auto it = tables_.find(name);
  if (it != tables_.end()) {
    lock_.unlock();
    throw std::runtime_error("Table already exists: " + name);
  }
  tables_.insert(std::make_pair(name, new Table(table_conf, *this)));
  lock_.unlock();
}

void Database::DropTable(const std::string& name) {
  LOG(INFO)<<"Dropping table: "<<name;
  lock_.lock();
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    lock_.unlock();
    throw std::invalid_argument("No such table: " + name);
  }
  auto table = it->second;
  tables_.erase(it);
  delete table;
  lock_.unlock();
}

Table* Database::GetTable(const std::string& name) {
  lock_.lock_shared();
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    lock_.unlock_shared();
    throw std::invalid_argument("No such table: " + name);
  }
  auto table = it->second;
  lock_.unlock_shared();
  return table;
}

void Database::PrintMetadata(std::string& metadata) {
  using json = nlohmann::json;
  json meta;
  meta["tables"] = json::array();
  lock_.lock_shared();
  for (auto& it : tables_) {
    auto table = it.second;
    json table_meta;
    table_meta["name"] = table->name();
    meta["tables"].push_back(table_meta);
  }
  lock_.unlock_shared();
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

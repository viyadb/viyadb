#ifndef VIYA_DB_DATABASE_H_
#define VIYA_DB_DATABASE_H_

#include <unordered_map>
#include <ThreadPool/ThreadPool.h>
#include "db/dictionary.h"
#include "query/output.h"
#include "query/stats.h"
#include "codegen/compiler.h"
#include "input/watcher.h"
#include "util/config.h"
#include "util/rwlock.h"
#include "util/statsd.h"

namespace viya {
namespace db {

namespace cg = viya::codegen;
namespace query = viya::query;
namespace input = viya::input;

class Table;

class Database {
  public:
    Database(const util::Config& config);
    Database(const Database& other) = delete;
    ~Database();

    void CreateTable(const util::Config& table_conf);
    void DropTable(const std::string& name);
    Table* GetTable(const std::string& name);

    void PrintMetadata(std::string&);

    cg::Compiler& compiler() { return compiler_; }
    Dictionaries& dicts() { return dicts_; }
    ThreadPool& read_pool() { return read_pool_; }
    ThreadPool& write_pool() { return write_pool_; }
    input::Watcher& watcher() { return watcher_; }
    const util::Statsd& statsd() const { return statsd_; }

    query::QueryStats Query(const util::Config& query_conf, query::RowOutput& output);
    void Load(const util::Config& load_conf);

  private:
    cg::Compiler compiler_;
    std::unordered_map<std::string,Table*> tables_;
    folly::RWSpinLock lock_;
    Dictionaries dicts_;

    ThreadPool write_pool_;
    ThreadPool read_pool_;

    input::Watcher watcher_;
    util::Statsd statsd_;
};

}}

#endif // VIYA_DB_DATABASE_H_

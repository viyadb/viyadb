#ifndef VIYA_INPUT_WATCHER_H_
#define VIYA_INPUT_WATCHER_H_

#include <vector>
#include <thread>
#include <mutex>
#include "db/table.h"
#include "util/config.h"

namespace viya {
namespace input {

namespace db = viya::db;
namespace util = viya::util;

struct Watch {
  Watch(db::Table* table, std::string dir, std::vector<std::string> exts, int wd):
    table(table),dir(dir),exts(exts),wd(wd) {}

  db::Table* table;
  std::string dir;
  std::vector<std::string> exts;
  int wd;
  std::string last_file;
};

class Watcher {
  public:
    Watcher(db::Database& db);
    ~Watcher();

    void AddWatch(const util::Config& config, db::Table* table);
    void RemoveWatch(db::Table* table);

  private:
    std::vector<std::string> ScanFiles(Watch& watch);
    void ProcessEvent(Watch& watch);
    void Run();

  private:
    db::Database& db_;
    int fd_;
    std::thread thread_;
    std::mutex mutex_;
    std::vector<Watch> watches_;
};

}}

#endif // VIYA_INPUT_WATCHER_H_

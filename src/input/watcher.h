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

#ifndef VIYA_INPUT_WATCHER_H_
#define VIYA_INPUT_WATCHER_H_

#include "util/config.h"
#include <mutex>
#include <thread>
#include <vector>

namespace viya {
namespace db {

class Database;
class Table;
}
}

namespace viya {
namespace input {

namespace db = viya::db;
namespace util = viya::util;

struct Watch {
  Watch(db::Table *table, std::string dir, std::vector<std::string> exts,
        int wd)
      : table(table), dir(dir), exts(exts), wd(wd) {}

  db::Table *table;
  std::string dir;
  std::vector<std::string> exts;
  int wd;
  std::string last_file;
};

class Watcher {
public:
  Watcher(db::Database &db);
  ~Watcher();

  void AddWatch(const util::Config &config, db::Table *table);
  void RemoveWatch(db::Table *table);

private:
  std::vector<std::string> ScanFiles(Watch &watch);
  void ProcessEvent(Watch &watch);
  void Run();

private:
  db::Database &db_;
  int fd_;
  std::thread thread_;
  std::mutex mutex_;
  std::vector<Watch> watches_;
};
}
}

#endif // VIYA_INPUT_WATCHER_H_

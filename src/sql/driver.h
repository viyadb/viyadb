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

#ifndef VIYA_SQL_DRIVER_H_
#define VIYA_SQL_DRIVER_H_

#include "util/config.h"
#include <istream>
#include <vector>

namespace viya {
namespace db {
class Database;
}
}
namespace viya {
namespace query {
class RowOutput;
}
}

namespace viya {
namespace sql {

namespace query = viya::query;
namespace util = viya::util;

class Parser;
class Scanner;
class location;
class Statement;

class Driver {
public:
  Driver(db::Database &db);
  ~Driver();

  void Run(std::istream &stream, query::RowOutput *output = nullptr,
           bool header = false);
  std::vector<util::Config> ParseQueries(std::istream &stream);
  void Reset();

  const db::Database &db() const { return db_; }

private:
  void Parse(std::istream &stream);
  void AddError(const std::string &error);
  void AddStatement(Statement *stmt);

private:
  db::Database &db_;
  Scanner *scanner_;
  Parser *parser_;
  location *location_;
  // int error_;
  std::vector<std::string> errors_;
  std::vector<Statement *> stmts_;

  friend class Parser;
  friend class Scanner;
};
}
}

#endif // VIYA_SQL_DRIVER_H_

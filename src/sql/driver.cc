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

#include "sql/driver.h"
#include "db/database.h"
#include "query/output.h"
#include "sql/parser.hh"
#include "sql/scanner.h"
#include "sql/statement.h"
#include <sstream>

namespace viya {
namespace sql {

namespace util = viya::util;

Driver::Driver(db::Database &db, bool add_header)
    : db_(db), add_header_(add_header), scanner_(new Scanner()),
      parser_(new Parser(*this)), location_(new location()) {}

Driver::~Driver() {
  for (auto *stmt : stmts_) {
    delete stmt;
  }
  delete parser_;
  delete scanner_;
  delete location_;
}

void Driver::Reset() {
  delete location_;
  location_ = new location();

  for (auto *stmt : stmts_) {
    delete stmt;
  }
  stmts_.clear();
}

void Driver::Parse(std::istream &stream) {
  scanner_->switch_streams(&stream, &std::cerr);
  parser_->parse();
  if (!errors_.empty()) {
    std::ostringstream err("Error parsing SQL query: ");
    for (auto &msg : errors_) {
      err << msg << std::endl;
    }
    throw std::runtime_error(err.str());
  }
}

void Driver::Run(std::istream &stream, query::RowOutput *output) {
  Parse(stream);

  for (auto stmt : stmts_) {
    util::Config desc(stmt->descriptor());
    switch (stmt->type()) {
    case Statement::Type::QUERY:
      if (stmts_.size() != 1) {
        throw std::runtime_error(
            "Multiple SQL statements are not supported in a single query");
      }
      if (output == nullptr) {
        throw std::runtime_error("Output handler is not provided");
      }
      db_.Query(desc, *output);
      break;
    case Statement::Type::LOAD:
      db_.Load(desc);
      break;
    default:
      throw std::runtime_error("Wrong query statement");
    }
  }
}

std::vector<Statement> Driver::ParseStatements(std::istream &stream) {
  Parse(stream);
  std::vector<Statement> stmts;
  for (auto stmt : stmts_) {
    stmts.emplace_back(*stmt);
  }
  return std::move(stmts);
}

void Driver::AddStatement(Statement *stmt) { stmts_.push_back(stmt); }

void Driver::AddError(const std::string &error) { errors_.push_back(error); }

} // namespace sql
} // namespace viya

#include <sstream>
#include "db/database.h"
#include "query/output.h"
#include "util/config.h"
#include "sql/driver.h"
#include "sql/parser.hh"
#include "sql/scanner.h"
#include "sql/statement.h"

namespace viya {
namespace sql {

namespace util = viya::util;

Driver::Driver(db::Database& db):
  db_(db),
  scanner_(new Scanner()),
  parser_(new Parser(*this)),
  location_(new location()) {
}

Driver::~Driver() {
  for (auto* stmt: stmts_) {
    delete stmt;
  }
  delete parser_;
  delete scanner_;
  delete location_;
}

void Driver::Reset() {
  delete location_;
  location_ = new location();
}

void Driver::Parse(std::istream& stream) {
  scanner_->switch_streams(&stream, &std::cerr);
  parser_->parse();
  if (!errors_.empty()) {
    std::ostringstream err("Error parsing SQL query: ");
    for (auto& msg : errors_) {
      err<<msg<<std::endl;
    }
    throw std::runtime_error(err.str());
  }
}

void Driver::Run(std::istream& stream, query::RowOutput& output) {
  Parse(stream);

  for (auto stmt : stmts_) {
    util::Config desc(new json(stmt->descriptor()));
    switch (stmt->type()) {
      case Statement::Type::QUERY:
        if (stmts_.size() != 1) {
          throw std::runtime_error("Multiple SQL statements are not supported in a single query");
        }
        db_.Query(desc, output);
        break;
      default:
        throw std::runtime_error("Wrong query statement");
    }
  }
}

void Driver::AddStatement(Statement* stmt) {
  stmts_.push_back(stmt);
}

void Driver::AddError(const std::string& error) {
  errors_.push_back(error);
}

}}

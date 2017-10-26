#ifndef VIYA_SQL_DRIVER_H_
#define VIYA_SQL_DRIVER_H_

#include <istream>
#include <vector>

namespace viya { namespace db { class Database; }}
namespace viya { namespace query { class RowOutput; }}

namespace viya {
namespace sql {

namespace query = viya::query;

class Parser;
class Scanner;
class location;
class Statement;

class Driver {
  public:
    Driver(db::Database& db);
    ~Driver();

    void Run(std::istream& stream, query::RowOutput& output);
    void Reset();

    const db::Database& db() const { return db_; }

  private:
    void Parse(std::istream& stream);
    void AddError(const std::string& error);
    void AddStatement(Statement* stmt);

  private:
    db::Database &db_;
    Scanner* scanner_;
    Parser* parser_;
    location* location_;
    //int error_;
    std::vector<std::string> errors_;
    std::vector<Statement*> stmts_;

    friend class Parser;
    friend class Scanner;
};

}}

#endif // VIYA_SQL_DRIVER_H_

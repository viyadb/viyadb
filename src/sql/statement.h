#ifndef VIYA_SQL_STATEMENT_H_
#define VIYA_SQL_STATEMENT_H_

#include <json.hpp>

namespace viya {
namespace sql {

using json = nlohmann::json;

class Statement {
  public:
    enum Type { QUERY };

    Statement(Type type):type_(type) {}

    Type type() const { return type_; }
    json& descriptor() { return descriptor_; }

  private:
    Type type_;
    json descriptor_;
};

}}

#endif // VIYA_SQL_DRIVER_H_

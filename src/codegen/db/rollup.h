#ifndef VIYA_CODEGEN_DB_ROLLUP_H_
#define VIYA_CODEGEN_DB_ROLLUP_H_

#include "db/rollup.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

class RollupDefs: public CodeGenerator {
  public:
    RollupDefs(const std::vector<const db::Dimension*>& dimensions):
      dimensions_(dimensions) {}

    RollupDefs(const RollupDefs& other) = delete;

    Code GenerateCode() const;

  private:
    const std::vector<const db::Dimension*>& dimensions_;
};

class RollupReset: public CodeGenerator {
  public:
    RollupReset(const std::vector<const db::Dimension*>& dimensions):
      dimensions_(dimensions) {}

    RollupReset(const RollupReset& other) = delete;

    Code GenerateCode() const;

  private:
    const std::vector<const db::Dimension*>& dimensions_;
};

class TimestampRollup: public CodeGenerator {
  public:
    TimestampRollup(const db::TimeDimension* dimension, const std::string& var_name)
      :dimension_(dimension),var_name_(var_name) {}

    TimestampRollup(const TimestampRollup& other) = delete;

    Code GenerateCode() const;

  private:
    const db::TimeDimension* dimension_;
    std::string var_name_;
};

}}

#endif // VIYA_CODEGEN_DB_ROLLUP_H_

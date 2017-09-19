#include "db/column.h"
#include "codegen/db/rollup.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

Code RollupDefs::GenerateCode() const {
  Code code;
  code.AddHeaders({"util/time.h"});
  code<<" namespace util = viya::util;\n";
  for (auto dimension : dimensions_) {
    if (dimension->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dimension->index());
      code<<" util::Time"<<std::to_string(dimension->num_type().size() * 8)<<" time"<<dim_idx<<";\n";
      auto& rollup_rules = static_cast<const db::TimeDimension*>(dimension)->rollup_rules();
      for (size_t rule_idx = 0; rule_idx < rollup_rules.size(); ++rule_idx) {
        code<<dimension->num_type().cpp_type()<<" rollup_b"<<dim_idx<<"_"<<std::to_string(rule_idx)<<";\n";
      }
    }
  }
  return code;
}

Code RollupReset::GenerateCode() const {
  Code code;

  char* test_ts = getenv("VIYA_TEST_ROLLUP_TS");
  std::string ts_value = test_ts != nullptr ? std::string(test_ts) : "std::time(nullptr)";

  for (auto dimension : dimensions_) {
    if (dimension->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dimension->index());
      auto time_dim = static_cast<const db::TimeDimension*>(dimension);
      auto& rollup_rules = time_dim->rollup_rules();

      for (size_t rule_idx = 0; rule_idx < rollup_rules.size(); ++rule_idx) {
        auto& rollup_rule = rollup_rules[rule_idx];
        auto after = rollup_rule.after();

        code<<" rollup_b"<<dim_idx<<"_"<<std::to_string(rule_idx)
          <<" = util::Duration(static_cast<util::TimeUnit>("
          <<std::to_string(static_cast<int>(after.time_unit()))<<"), "
          <<std::to_string(after.count())<<").add_to((uint32_t) "<<ts_value<<", -1)";

        if (time_dim->micro_precision()) {
          code<<" * 1000000L";
        }
        code<<";\n";
      }
    }
  }
  return code;
}

Code TimestampRollup::GenerateCode() const {
  Code code;
  auto dim_idx = std::to_string(dimension_->index());
  auto& rollup_rules = dimension_->rollup_rules();
  for (size_t rule_idx = 0; rule_idx < rollup_rules.size(); ++rule_idx) {
    auto& rollup_rule = rollup_rules[rule_idx];
    if (rule_idx > 0) {
      code<<" else ";
    }
    code<<" if ("<<var_name_<<" < rollup_b"<<dim_idx<<"_"<<rule_idx<<") {\n";
    code<<"  time"<<dim_idx<<".trunc<static_cast<util::TimeUnit>("
      <<static_cast<int>(rollup_rule.granularity().time_unit())<<")>();\n";
    code<<" }\n";
  }
  return code;
}

}}

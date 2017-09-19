#include "db/rollup.h"

namespace viya {
namespace db {

Granularity::Granularity(const std::string& name):
  time_unit_(util::time_unit_by_name(name)) {
}

RollupRule::RollupRule(const util::Config& config):
  granularity_(config.str("granularity")),
  after_(config.str("after")) {
}

}}

#ifndef  VIYA_DB_ROLLUP_H_
#define  VIYA_DB_ROLLUP_H_

#include <string>
#include "util/config.h"
#include "util/time.h"

namespace viya {
namespace db {

namespace util = viya::util;

class Granularity {
  public:
    Granularity():time_unit_(util::TimeUnit::_UNDEFINED) {}
    Granularity(const std::string& name);

    bool empty() const { return time_unit_ == util::TimeUnit::_UNDEFINED; }
    util::TimeUnit time_unit() const { return time_unit_; }

  private:
    util::TimeUnit time_unit_;
};

class RollupRule {
  public:
    RollupRule(const util::Config& conf);

    const util::Duration& after() const { return after_; }
    const Granularity& granularity() const { return granularity_; }
    
  private:
    Granularity granularity_;
    util::Duration after_;
};

}}

#endif // VIYA_DB_ROLLUP_H_

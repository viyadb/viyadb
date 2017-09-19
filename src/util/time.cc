#include <sstream>
#include <array>
#include <stdexcept>
#include "util/time.h"

namespace viya {
namespace util {

static const std::array<std::string, 7> tu_names = {{
  "year", "month", "week", "day", "hour", "minute", "second"
}};

TimeUnit time_unit_by_name(const std::string& name) {
  for (size_t i = 0; i < tu_names.size(); ++i) {
    if (name == tu_names[i]) {
      return static_cast<TimeUnit>(i);
    }
  }
  throw std::invalid_argument("Unsupported time unit: " + name);
}

Duration::Duration(const std::string& desc) {
  std::istringstream ss(desc);
  std::string tu_name;
  int n;
  if (!(ss>>n) || !(ss>>tu_name) || n<=0) {
    throw std::invalid_argument("Wrong duration description: " + desc);
  }
  count_ = n;
  tu_name.pop_back();
  time_unit_ = time_unit_by_name(tu_name);
}

uint32_t Duration::add_to(uint32_t timestamp, int sign) const {
  time_t t = (time_t) timestamp;
  std::tm tm;
  gmtime_r(&t, &tm);
  switch (time_unit_) {
    case TimeUnit::YEAR:   tm.tm_year += sign * count_; break;
    case TimeUnit::MONTH:  tm.tm_mon  += sign * count_; break;
    case TimeUnit::WEEK:   tm.tm_mday += 7 * sign * count_; break;
    case TimeUnit::DAY:    tm.tm_mday += sign * count_; break;
    case TimeUnit::HOUR:   tm.tm_hour += sign * count_; break;
    case TimeUnit::MINUTE: tm.tm_min  += sign * count_; break;
    case TimeUnit::SECOND: tm.tm_sec  += sign * count_; break;
    default:
      throw std::runtime_error("Unsupported duration");
  }
  return (uint32_t) timegm(&tm);
}

uint64_t Duration::add_to(uint64_t timestamp, int sign) const {
  return add_to((uint32_t) (timestamp / 1000000L), sign) * 1000000L; 
}

}}

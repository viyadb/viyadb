#ifndef VIYA_UTIL_TIME_H_
#define VIYA_UTIL_TIME_H_

#include <string>
#include <ctime>
#include <type_traits>

namespace viya {
namespace util {

enum TimeUnit { YEAR=0, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, _UNDEFINED };

TimeUnit time_unit_by_name(const std::string& name);

class Duration {
  public:
    Duration(const std::string& desc);
    Duration(TimeUnit time_unit, size_t count):time_unit_(time_unit),count_(count) {}

    TimeUnit time_unit() const { return time_unit_; }
    size_t count() const { return count_; }

    uint32_t add_to(uint32_t timestamp, int sign) const;
    uint64_t add_to(uint64_t timestamp, int sign) const;

    bool operator> (const Duration &d) const {
      return add_to((uint32_t) 0L, 1) > d.add_to((uint32_t) 0L, 1);
    }

  private:
    TimeUnit time_unit_;
    size_t count_;
};

class Truncator {
  public:
    template<TimeUnit U>
    inline static void trunc(std::tm& tm);
};

template <>
inline void Truncator::trunc<TimeUnit::YEAR>(std::tm& tm) {
  tm.tm_sec = 0;
  tm.tm_min = 0;
  tm.tm_hour = 0;
  tm.tm_mday = 1;
  tm.tm_mon = 1;
}

template <>
inline void Truncator::trunc<TimeUnit::MONTH>(std::tm& tm) {
  tm.tm_sec = 0;
  tm.tm_min = 0;
  tm.tm_hour = 0;
  tm.tm_mday = 1;
}

template <>
inline void Truncator::trunc<TimeUnit::DAY>(std::tm& tm) {
  tm.tm_sec = 0;
  tm.tm_min = 0;
  tm.tm_hour = 0;
}

template <>
inline void Truncator::trunc<TimeUnit::HOUR>(std::tm& tm) {
  tm.tm_sec = 0;
  tm.tm_min = 0;
}

template <>
inline void Truncator::trunc<TimeUnit::MINUTE>(std::tm& tm) {
  tm.tm_sec = 0;
}

template <>
inline void Truncator::trunc<TimeUnit::SECOND>(std::tm& tm __attribute__((unused))) {
}

class Time32 {
  public:
    Time32():tm_ {} {
    }

    void parse(const char* format, const std::string& value) {
      strptime(value.c_str(), format, &tm_);
    }

    void set_ts(uint32_t timestamp) {
      time_t t = (time_t) timestamp;
      gmtime_r(&t, &tm_);
    }

    uint32_t get_ts() {
      return timegm(&tm_);
    }

    template<TimeUnit U>
    void trunc() {
      Truncator::trunc<U>(tm_);
    }

  protected:
    std::tm tm_;
};

class Time64 {
  public:
    Time64():micros_(0),tm_ {} {
    }

    void parse(const char* format, const std::string& value) {
      strptime(value.c_str(), format, &tm_);
      micros_ = 0;
    }

    void set_ts(uint64_t timestamp) {
      micros_ = timestamp % 1000000L;
      time_t t = (time_t) (timestamp / 1000000L);
      gmtime_r(&t, &tm_);
    }

    uint64_t get_ts() {
      return timegm(&tm_) * 1000000L + micros_;
    }

    template<TimeUnit U>
    void trunc() {
      Truncator::trunc<U>(tm_);
      micros_ = 0;
    }

  protected:
    uint32_t micros_;
    std::tm tm_;
};

}}

#endif // VIYA_UTIL_TIME_H_

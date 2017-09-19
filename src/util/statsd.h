#ifndef VIYA_UTIL_STATSD_H_
#define VIYA_UTIL_STATSD_H_

#include <netinet/in.h>
#include "util/config.h"

namespace viya {
namespace util {

class Statsd {
  public:
    Statsd();
    Statsd(Statsd& other) = delete;
    ~Statsd();

    void Connect(const Config& config);
    void Timing(const std::string& key, int64_t value, float rate = 1.0) const;
    void Increment(const std::string& key, float rate = 1.0) const;
    void Decrement(const std::string& key, float rate = 1.0) const;
    void Count(const std::string& key, int64_t value, float rate = 1.0) const;
    void Gauge(const std::string& key, int64_t value, float rate = 1.0) const;
    void Set(const std::string& key, int64_t value, float rate = 1.0) const;

  protected:
    void Send(const std::string& key, int64_t value, float rate, const std::string& unit) const;

  private:
    std::string prefix_;
    struct sockaddr_in server_;
    int socket_;
};

}}

#endif // VIYA_UTIL_STATSD_H_

#ifndef VIYA_QUERY_STATS_H_
#define VIYA_QUERY_STATS_H_

#include <chrono>
#include "util/statsd.h"

namespace viya {
namespace query {

namespace cr = std::chrono;
namespace util = viya::util;

class QueryStats {
  public:
    QueryStats(const util::Statsd& statsd):
      statsd_(statsd),scanned_segments(0),scanned_recs(0),
      aggregated_recs(0),output_recs(0) {}

    void OnBegin(const std::string& query_type, const std::string& table);
    void OnCompile();
    void OnEnd();

  public:
    const util::Statsd& statsd_;
    std::string query_type_;
    std::string table_;
    size_t scanned_segments;
    size_t scanned_recs;
    size_t aggregated_recs;
    size_t output_recs;
    cr::duration<float> compile_time;
    cr::duration<float> whole_time;

  private:
    cr::steady_clock::time_point begin_work_;
};

}}

#endif // VIYA_QUERY_STATS_H_

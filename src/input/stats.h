#ifndef VIYA_INPUT_STATS_H_
#define VIYA_INPUT_STATS_H_

#include <chrono>
#include <string>
#include "db/stats.h"

namespace viya { namespace util { class Statsd; } }

namespace viya {
namespace input {

namespace db = viya::db;
namespace cr = std::chrono;
namespace util = viya::util;

class LoaderStats {
  public:
    LoaderStats(const util::Statsd& statsd, const std::string& table):
      statsd_(statsd),table_(table),total_recs(0),failed_recs(0) {}

    void OnBegin();
    void OnEnd();

  public:
    const util::Statsd& statsd_;
    std::string table_;
    size_t total_recs;
    size_t failed_recs;
    cr::duration<float> whole_time;
    db::UpsertStats upsert_stats;

  private:
    cr::steady_clock::time_point begin_work_;
};

}}

#endif // VIYA_INPUT_STATS_H_

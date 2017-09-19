#include <glog/logging.h>
#include <sstream>
#include "input/stats.h"

namespace viya {
namespace input {

void LoaderStats::OnBegin() {
  begin_work_ = cr::steady_clock::now();
}

void LoaderStats::OnEnd() {
  whole_time = cr::steady_clock::now() - begin_work_;
  auto load_time = cr::duration_cast<cr::milliseconds>(whole_time).count();

  LOG(INFO)<<"Load time "<<load_time<<" ms ("
    <<"tr="<<std::to_string(total_recs)
    <<",fr="<<std::to_string(failed_recs)
    <<",nr="<<std::to_string(upsert_stats.new_recs)
    <<")"<<std::endl;

  std::ostringstream prefix;
  prefix<<"loader."<<table_<<".";
  statsd_.Timing(prefix.str() + "time", load_time);
  statsd_.Count(prefix.str() + "rows", total_recs);
}

}}


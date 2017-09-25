#include "cluster/worker.h"
#include "util/hostname.h"

namespace viya {
namespace cluster {

Worker::Worker(const util::Config& config):config_(config),consul_(config) {

  if (consul_.Enabled()) {
    session_ = consul_.CreateSession(std::string("viyadb-worker"));

    std::ostringstream key;
    key<<config_.str("cluster_id")<<"/nodes/workers/"<<util::get_hostname()
      <<":"<<std::to_string(config_.num("http_port"));

    json data = json({});
    if (config_.exists("rack_id")) {
      data["rack_id"] = config_.str("rack_id");
    }
    if (config_.exists("cpu_list")) {
      data["cpu_list"] = config_.numlist("cpu_list");
    }
    session_->EphemeralKey(key.str(), data.dump());
  }
}

}}

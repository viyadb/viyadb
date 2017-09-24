#include <sched.h>
#include <sstream>
#include <json.hpp>
#include "server/viyad.h"
#include "util/hostname.h"

namespace viya {
namespace server {

namespace db = viya::db;
namespace coord = viya::coord;
namespace util = viya::util;

using json = nlohmann::json;

Viyad::Viyad(const util::Config& config):config_(config),consul_(config) {
}

void Viyad::Start() {
#ifdef __linux__
  SetCpuAffinity();
#endif

  db::Database database(config_);
  Http http_service(config_, database);
  
  RegisterNode(http_service);

  http_service.Start();
}

void Viyad::RegisterNode(const Http& http_service) {
  if (consul_.Enabled()) {
    session_ = consul_.CreateSession(std::string("viyadb-worker"));

    std::ostringstream key;
    key<<config_.str("cluster_id")
      <<"/nodes/workers/"<<util::get_hostname()<<":"<<std::to_string(http_service.port());

    json data = json({});
    session_->EphemeralKey(key.str(), data.dump());
  }
}

#ifdef __linux__
void Viyad::SetCpuAffinity() {
  if (config_.exists("cpu_list")) {
    pid_t pid= getpid();
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    for (auto cpu : config_.numlist("cpu_list")) {
      CPU_SET(cpu, &cpu_set);
    }
    sched_setaffinity(pid, sizeof(cpu_set), &cpu_set);
  }
}
#endif

}}

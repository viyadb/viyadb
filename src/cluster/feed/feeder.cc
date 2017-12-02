#include <glog/logging.h>
#include "util/config.h"
#include "cluster/consul/consul.h"
#include "cluster/feed/feeder.h"
#include "cluster/feed/notifier.h"

namespace viya {
namespace cluster {
namespace feed {

Feeder::Feeder(const consul::Consul& consul,
               const util::Config& cluster_config,
               const std::unordered_map<std::string, util::Config>& table_configs)
  :consul_(consul),cluster_config_(cluster_config),table_configs_(table_configs) {

  Start();
}

Feeder::~Feeder() {
  for (auto l : notifiers_) delete l;
  notifiers_.clear();
}

void Feeder::Start() {
  LOG(INFO)<<"Reading indexers configurations";
  for (auto& indexer_id : cluster_config_.strlist("indexers", {})) {
    auto indexer_conf = util::Config(consul_.GetKey("indexers/" + indexer_id + "/config"));
    auto notifier = NotifierFactory::Create(indexer_conf.sub("realTime").sub("indexer"));
    notifier->Listen([this](const json& info) {
      ProcessMicroBatch(info);
    });
    notifiers_.push_back(notifier);
  }
}

void Feeder::ProcessMicroBatch(const json& info) {
  auto tables = info["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    auto table = it.key();
    auto table_info = it.value().get<json>();
    auto paths = table_info["paths"].get<std::vector<std::string>>();
  }
}

}}}

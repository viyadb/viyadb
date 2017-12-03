#include <glog/logging.h>
#include "util/config.h"
#include "cluster/controller.h"
#include "cluster/feeder.h"
#include "cluster/notifier.h"

namespace viya {
namespace cluster {

Feeder::Feeder(cluster::Controller& controller):controller_(controller) {
  Start();
}

Feeder::~Feeder() {
  for (auto notifier : notifiers_) {
    delete notifier;
  }
  notifiers_.clear();
}

void Feeder::Start() {
  for (auto& it : controller_.indexers_configs()) {
    auto& indexer_conf = it.second;
    auto notifier = NotifierFactory::Create(indexer_conf.sub("realTime").sub("notifier"));
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

}}

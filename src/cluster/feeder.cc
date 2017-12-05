#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include "util/config.h"
#include "cluster/controller.h"
#include "cluster/feeder.h"
#include "cluster/notifier.h"
#include "cluster/downloader.h"

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;

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
    auto& indexer_id = it.first;
    auto& indexer_conf = it.second;

    auto notifier = NotifierFactory::Create(indexer_conf.sub("realTime").sub("notifier"), IndexerType::REALTIME);
    notifier->Listen([this, &indexer_id](const Info& info) {
      ProcessMicroBatch(indexer_id, static_cast<const MicroBatchInfo&>(info));
    });
    notifiers_.push_back(notifier);
  }
}

void Feeder::ProcessMicroBatch(const std::string& indexer_id, const MicroBatchInfo& info) {
  if (info.id() <= controller_.batches().at(indexer_id)->last_microbatch()) {
    LOG(WARNING)<<"Skipping already processed micro batch: "<<info.id();
  } else {
    for (auto& it : info.tables_paths()) {
      auto table = it.first;
      auto paths = it.second;
      
      for (auto& path : paths) {
        std::string target_path = Downloader::Instance().Download(path);
        fs::remove_all(target_path);
      }
    }
  }
}

}}

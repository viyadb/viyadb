#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include "util/config.h"
#include "util/scope_guard.h"
#include "cluster/controller.h"
#include "cluster/feeder.h"
#include "cluster/notifier.h"
#include "cluster/downloader.h"

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;

Feeder::Feeder(const Controller& controller, const std::string& load_prefix):
  controller_(controller),
  loader_(controller, load_prefix) {

  Start();
}

Feeder::~Feeder() {
  for (auto notifier : notifiers_) {
    delete notifier;
  }
  notifiers_.clear();
}

void Feeder::Start() {
  LoadHistoricalData();

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

void Feeder::LoadHistoricalData() {
  for (auto& it : controller_.batches()) {
    for (auto& tit : it.second->tables_info()) {
      auto& table_name = tit.first;
      auto& table_info = tit.second;
      auto partitions = controller_.tables_plans().at(table_name).workers_partitions();

      for (auto& path : table_info.paths()) {
        auto prefix = boost::trim_right_copy_if(path, boost::is_any_of("/"));

        for (auto& pit : partitions) {
          std::string target_path = Downloader::Fetch(prefix + "/part=" + std::to_string(pit.second));
          util::ScopeGuard delete_tmpdir = [&]() {
            if (target_path != path) {
              fs::remove_all(target_path);
            }
          };

          loader_.LoadFolder(target_path, table_name, pit.first);
        }
      }
    }
  }
}

void Feeder::ProcessMicroBatch(const std::string& indexer_id, const MicroBatchInfo& info) {
  if (info.id() <= controller_.batches().at(indexer_id)->last_microbatch()) {
    LOG(WARNING)<<"Skipping already processed micro batch: "<<info.id();
  } else {
    for (auto& it : info.tables_info()) {
      auto& table_info = it.second;

      for (auto& path : table_info.paths()) {
        std::string target_path = Downloader::Fetch(path);
        util::ScopeGuard delete_tmpdir = [&]() {
          if (target_path != path) {
            fs::remove_all(target_path);
          }
        };
      }
    }
  }
}

}}

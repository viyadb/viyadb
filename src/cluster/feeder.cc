/*
 * Copyright (c) 2017-present ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    auto notifier = NotifierFactory::Create(
      it.first, it.second.sub("realTime").sub("notifier"), IndexerType::REALTIME);
    notifier->Listen(*this);
    notifiers_.push_back(notifier);
  }
}

void Feeder::LoadHistoricalData(const std::string& target_worker) {
  std::vector<std::string> delete_paths;
  util::ScopeGuard cleanup = [&delete_paths]() { for (auto& path : delete_paths) fs::remove_all(path); };

  for (auto& batches_it : controller_.indexers_batches()) {
    for (auto& tables_it : batches_it.second->tables_info()) {
      auto& table_name = tables_it.first;
      auto& table_info = tables_it.second;
      auto partitions = controller_.tables_plans().at(table_name).workers_partitions();

      for (auto& path : table_info.paths()) {
        auto prefix = boost::trim_right_copy_if(path, boost::is_any_of("/"));

        for (auto& part_it : partitions) {
          auto& worker_id = part_it.first;
          if (!target_worker.empty() && worker_id != target_worker) {
            continue;
          }
          auto& partition = part_it.second;

          std::string target_path = Downloader::Fetch(prefix + "/part=" + std::to_string(partition));
          if (target_path != path) {
            delete_paths.push_back(target_path);
          }
          loader_.LoadFiles(target_path, table_name, table_info, worker_id);
        }
      }
    }
  }
}

void Feeder::LoadMicroBatch(const MicroBatchInfo& mb_info, const std::string& target_worker) {
  std::vector<std::string> delete_paths;
  util::ScopeGuard cleanup = [&delete_paths]() { for (auto& path : delete_paths) fs::remove_all(path); };

  LOG(INFO)<<"Processing micro batch: "<<mb_info.id();
  for (auto& it : mb_info.tables_info()) {
    auto& table_name = it.first;
    auto& table_info = it.second;

    for (auto& path : table_info.paths()) {
      std::string target_path = Downloader::Fetch(path);
      if (target_path != path) {
        delete_paths.push_back(target_path);
      }

      if (target_worker.empty()) {
        loader_.LoadFilesToAll(target_path, table_name, table_info);
      } else {
        loader_.LoadFiles(target_path, table_name, table_info, target_worker);
      }
    }
  }
}

bool Feeder::IsNewMicroBatch(const std::string& indexer_id, const MicroBatchInfo& mb_info) {
  uint32_t last_microbatch = 0L;
  auto& indexers_batches = controller_.indexers_batches();
  if (indexers_batches.size() > 0) {
    last_microbatch = controller_.indexers_batches().at(indexer_id)->last_microbatch();
  }
  return mb_info.id() > last_microbatch;
}

bool Feeder::ProcessMessage(const std::string& indexer_id, const Message& message) {
  auto& mb_info = static_cast<const MicroBatchInfo&>(message);

  if (IsNewMicroBatch(indexer_id, mb_info)) {
    LoadMicroBatch(mb_info);
    return false;
  }
  LOG(WARNING)<<"Skipping already processed micro batch: "<<mb_info.id();
  return true;
}

void Feeder::ReloadWorker(const std::string& worker_id) {
  LoadHistoricalData(worker_id);

  for (auto notifier : notifiers_) {
    for (auto& message : notifier->GetAllMessages()) {
      auto& mb_info = static_cast<const MicroBatchInfo&>(*message);

      if (IsNewMicroBatch(notifier->indexer_id(), mb_info)) {
        LoadMicroBatch(mb_info, worker_id);
      }
    }
  }
}

}}

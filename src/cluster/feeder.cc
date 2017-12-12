/*
 * Copyright (c) 2017 ViyaDB Group
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
          auto& partition = part_it.second;

          std::string target_path = Downloader::Fetch(prefix + "/part=" + std::to_string(partition));
          if (target_path != path) {
            delete_paths.push_back(target_path);
          }
          loader_.LoadFiles(target_path, table_name, worker_id);
        }
      }
    }
  }
}

void Feeder::ProcessMicroBatch(const std::string& indexer_id, const MicroBatchInfo& info) {
  std::vector<std::string> delete_paths;
  util::ScopeGuard cleanup = [&delete_paths]() { for (auto& path : delete_paths) fs::remove_all(path); };
  auto& indexer_batch = controller_.indexers_batches().at(indexer_id);

  if (info.id() <= indexer_batch->last_microbatch()) {
    LOG(WARNING)<<"Skipping already processed micro batch: "<<info.id();
  } else {
    for (auto& it : info.tables_info()) {
      auto& table_name = it.first;
      auto& table_info = it.second;

      for (auto& path : table_info.paths()) {
        std::string target_path = Downloader::Fetch(path);
        if (target_path != path) {
          delete_paths.push_back(target_path);
        }
        loader_.LoadFilesToAll(target_path, table_name);
      }
    }
  }
}

}}

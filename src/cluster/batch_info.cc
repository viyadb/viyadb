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

#include <json.hpp>
#include <set>
#include "cluster/batch_info.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

Info::Info(const json& info):id_(info["id"]) {
}

BatchTableInfo::BatchTableInfo(const json& info):
  paths_(info["paths"].get<std::vector<std::string>>()) {

  if (info.find("partitioning") != info.end()) {
    json partition_conf = info["partitionConf"];
    partitioning_ = std::make_unique<Partitioning>(
      info["partitioning"].get<std::vector<uint32_t>>(),
      partition_conf["partitions"],
      partition_conf["columns"].get<std::vector<std::string>>()
    );
  }
}

BatchInfo::BatchInfo(const json& info):
  Info(info),last_microbatch_(0L) {

  auto micro_batches = info["microBatches"].get<std::vector<long>>();
  if (!micro_batches.empty()) {
    last_microbatch_ = *std::max_element(micro_batches.begin(), micro_batches.end());
  }

  auto tables = info["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    tables_info_.emplace(it.key(), it.value().get<json>());
  }
}

MicroBatchTableInfo::MicroBatchTableInfo(const json& info):
  paths_(info["paths"].get<std::vector<std::string>>()) {
}

MicroBatchInfo::MicroBatchInfo(const json& info):Info(info) {
  auto tables = info["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    tables_info_.emplace(it.key(), it.value().get<json>());
  }
}

}}

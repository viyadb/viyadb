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

#include "cluster/batch_info.h"
#include <nlohmann/json.hpp>
#include <set>

namespace viya {
namespace cluster {

using json = nlohmann::json;

Message::Message(const json &message) : id_(message["id"]) {}

TableInfo::TableInfo(const json &message)
    : paths_(message["paths"].get<std::vector<std::string>>()),
      columns_(message["columns"].get<std::vector<std::string>>()) {}

BatchTableInfo::BatchTableInfo(const json &message) : TableInfo(message) {
  if (message.find("partitioning") != message.end()) {
    json partition_conf = message["partitionConf"];
    partitioning_ = std::make_unique<Partitioning>(
        message["partitioning"].get<std::vector<uint32_t>>(),
        partition_conf["partitions"],
        partition_conf["columns"].get<std::vector<std::string>>());
  }
}

BatchInfo::BatchInfo(const json &message)
    : Message(message), last_microbatch_(0L) {

  auto micro_batches = message["microBatches"].get<std::vector<long>>();
  if (!micro_batches.empty()) {
    last_microbatch_ =
        *std::max_element(micro_batches.begin(), micro_batches.end());
  }

  auto tables = message["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    tables_info_.emplace(it.key(), it.value().get<json>());
  }
}

MicroBatchTableInfo::MicroBatchTableInfo(const json &message)
    : TableInfo(message) {}

MicroBatchInfo::MicroBatchInfo(const json &message) : Message(message) {
  auto tables = message["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    tables_info_.emplace(it.key(), it.value().get<json>());
  }
}

} // namespace cluster
} // namespace viya

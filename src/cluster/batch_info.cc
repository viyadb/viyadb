#include <json.hpp>
#include <set>
#include "cluster/batch_info.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

Partitions::Partitions(const std::map<std::string, uint32_t>& partitions):
  partitions_(partitions) {
  std::set<uint32_t> d;
  for (auto& it : partitions) {
    d.insert(it.second);
  }
  partitions_num_ = d.size();
}

BatchInfo::BatchInfo(const std::string& info):last_microbatch_(0L) {
  json j = json::parse(info);
  id_ = j["id"];

  auto micro_batches = j["micro_batches"].get<std::vector<long>>();
  if (!micro_batches.empty()) {
    last_microbatch_ = *std::max_element(micro_batches.begin(), micro_batches.end());
  }

  auto tables = j["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    auto table = it.key();
    auto table_info = it.value().get<json>();

    json partitions = table_info["partitions"];
    std::map<std::string, uint32_t> table_partitions;
    for (auto pit = partitions.begin(); pit != partitions.end(); ++pit) {
      uint32_t partition = it.value(); 
      table_partitions.emplace(it.key(), partition);
    }
    tables_partitions_.emplace(table, table_partitions);
  }

}

MicroBatchInfo::MicroBatchInfo(const std::string& info) {
  json j = json::parse(info);
  id_ = j["id"];

  auto tables = j["tables"].get<json>();
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    auto table = it.key();
    auto table_info = it.value().get<json>();
    tables_paths_[table] = table_info["paths"].get<std::vector<std::string>>();
  }
}

}}

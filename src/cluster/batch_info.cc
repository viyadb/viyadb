#include <json.hpp>
#include <set>
#include "cluster/batch_info.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

Info::Info(const json& info):id_(info["id"]) {
}

Partitioning::Partitioning(const json& info) {
  std::set<uint32_t> d;
  for (auto it = info.begin(); it != info.end(); ++it) {
    uint32_t partition = it.value().get<uint32_t>();
    partitioning_.emplace(it.key(), partition);
    d.insert(partition);
  }
  total_ = d.size();
}

PartitionConf::PartitionConf(const json& info):
  columns_(info["columns"].get<std::vector<std::string>>()) {
  if (info.find("hash") != info.end()) {
    hashed_ = info["hash"].get<bool>();
  }
}

BatchTableInfo::BatchTableInfo(const json& info):
  paths_(info["paths"].get<std::vector<std::string>>()),
  partitioning_(info["partitioning"]),
  partition_conf_(info["partitionConf"]) {
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

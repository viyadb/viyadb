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

#include "cluster/plan.h"
#include <map>
#include <nlohmann/json.hpp>
#include <sstream>

namespace viya {
namespace cluster {

Plan::Plan(const Partitions &partitions) : partitions_(partitions) {
  AssignPartitionsToWorkers();
}

Plan::Plan(const json &plan) {
  for (auto plan_it = plan.begin(); plan_it != plan.end(); ++plan_it) {
    Replicas replicas;
    for (auto placement_it = plan_it->begin(); placement_it != plan_it->end();
         ++placement_it) {
      json placement = *placement_it;
      replicas.emplace_back(placement["hostname"].get<std::string>(),
                            (uint16_t)placement["port"].get<int>());
    }
    partitions_.emplace_back(std::move(replicas));
  }

  AssignPartitionsToWorkers();
}

json Plan::ToJson() const {
  json plan = json::array();
  for (auto &replicas : partitions_) {
    json partitions = json::array();
    for (auto &p : replicas) {
      partitions.push_back({{"hostname", p.hostname()}, {"port", p.port()}});
    }
    std::sort(partitions.begin(), partitions.end());
    plan.push_back(partitions);
  }
  std::sort(plan.begin(), plan.end());
  return plan;
}

void Plan::AssignPartitionsToWorkers() {
  workers_partitions_.clear();
  partitions_workers_.resize(partitions_.size());

  std::vector<uint32_t> partitions;
  uint32_t partition = 0;
  for (auto &replicas : partitions_) {
    for (auto &replica : replicas) {
      std::string worker_id =
          replica.hostname() + ":" + std::to_string(replica.port());
      workers_partitions_.emplace(worker_id, partition);
      partitions_workers_[partition].push_back(worker_id);
    }
    ++partition;
  }
}

Plan PlanGenerator::Generate(
    size_t partitions_num, size_t replication_factor,
    const std::map<std::string, util::Config> &workers_configs) {

  typedef std::vector<util::Config> workers;
  typedef std::map<std::string, workers> workers_by_host;
  typedef std::map<std::string, workers_by_host> hosts_by_rack;

  hosts_by_rack hbr;
  size_t total_workers = 0;

  for (auto &it : workers_configs) {
    auto &worker_conf = it.second;
    std::string rack_id = worker_conf.str("rack_id", "");
    std::string hostname = worker_conf.str("hostname");

    auto hbr_it = hbr.find(rack_id);
    if (hbr_it == hbr.end()) {
      hbr_it = hbr.emplace(rack_id, workers_by_host{}).first;
    }
    auto wbh_it = hbr_it->second.find(hostname);
    if (wbh_it == hbr_it->second.end()) {
      wbh_it = hbr_it->second.emplace(hostname, workers{}).first;
    }
    wbh_it->second.push_back(worker_conf);
    ++total_workers;
  }

  // Validate configuration:
  if (partitions_num * replication_factor > total_workers) {
    std::ostringstream s;
    s << "Can't place " << std::to_string(replication_factor) << " copies of "
      << std::to_string(partitions_num) << " partitions on "
      << std::to_string(total_workers) << " workers";
    throw std::runtime_error(s.str());
  }
  if (replication_factor > hbr.size()) {
    std::ostringstream s;
    s << "Replication factor of " << std::to_string(replication_factor)
      << " is smaller than the number of racks: " << std::to_string(hbr.size());
    throw std::runtime_error(s.str());
  }

  // Create easily iterable collections for racks, hosts and workers:
  typedef std::vector<workers> host_workers;
  typedef std::vector<host_workers> rack_hosts;

  rack_hosts rh;
  for (auto r : hbr) {
    host_workers hw;
    for (auto h : r.second) {
      hw.push_back(h.second);
    }
    rh.push_back(hw);
  }

  // Store indices that will be used for iteration:
  std::vector<std::vector<size_t>> worker_idxs;
  std::vector<size_t> host_idxs;

  host_idxs.resize(rh.size(), 0);
  worker_idxs.resize(rh.size());
  for (size_t i = 0; i < rh.size(); ++i) {
    worker_idxs[i].resize(rh[i].size(), 0);
  }

  Partitions partitions(partitions_num, Replicas{});
  size_t partition = 0;
  while (partition < partitions_num) {
    // Iterate on every rack and on every host in cycles.
    // Let's say we have two racks, two hosts under each rack and two workers
    // under each host:
    // r1:h1:w1 -> r2:h1:w1 -> r1:h2:w1 -> r2:h2:w1 -> r1:h1:w2 -> r2:h1:w2 ->
    // r1:h2:w2 -> r2:h2:w2
    for (size_t ri = 0, w = 0, p = 0;
         p < replication_factor && w < total_workers; ++w) {
      size_t hi = host_idxs[ri];
      size_t wi = worker_idxs[ri][hi];

      // Try to place partition replica on next worker
      if (wi < rh[ri][hi].size()) {
        auto next_worker = rh[ri][hi][wi];
        worker_idxs[ri][hi] = wi + 1;
        partitions[partition].emplace_back(next_worker.str("hostname"),
                                           next_worker.num("http_port"));
        ++p;
      }
      host_idxs[ri] = (hi + 1) % rh[ri].size();
      ri = (ri + 1) % rh.size();
    }
    ++partition;
  }

  return {partitions};
}

} // namespace cluster
} // namespace viya

#include <sstream>
#include <unordered_map>
#include "cluster/plan.h"

namespace viya {
namespace cluster {

json Plan::ToJson() const {
  json plan = json::array();
  for (auto& v : placements_) {
    json placements = json::array();
    for (auto p : v) {
      placements.push_back({{"hostname", p.hostname()}, {"port", p.port()}});
    } 
    std::sort(placements.begin(), placements.end());
    plan.push_back(placements);
  }
  std::sort(plan.begin(), plan.end());
  return std::move(plan);
}

PlanGenerator::PlanGenerator(const util::Config& cluster_config)
  :cluster_config_(cluster_config) {
}

Plan PlanGenerator::Generate(
  size_t partitions_num, const std::vector<util::Config>& worker_configs) {

  typedef std::vector<util::Config> workers;
  typedef std::unordered_map<std::string, workers> workers_by_host;
  typedef std::unordered_map<std::string, workers_by_host> hosts_by_rack;

  hosts_by_rack hbr;
  size_t total_workers = 0;

  for (auto& worker_conf : worker_configs) {
    std::string rack_id = worker_conf.str("rack_id", "");
    std::string hostname = worker_conf.str("hostname");

    auto hbr_it = hbr.find(rack_id);
    if (hbr_it == hbr.end()) {
      hbr_it = hbr.emplace(rack_id, workers_by_host {}).first;
    }
    auto wbh_it = hbr_it->second.find(hostname);
    if (wbh_it == hbr_it->second.end()) {
      wbh_it = hbr_it->second.emplace(hostname, workers {}).first;
    }
    wbh_it->second.push_back(worker_conf);
    ++total_workers;
  }

  size_t replicas_num = cluster_config_.num("replication_factor", 3);

  // Validate configuration:
  if (partitions_num * replicas_num > total_workers) {
    std::ostringstream s;
    s<<"Can't place "<<std::to_string(replicas_num)
      <<" copies of "<<std::to_string(partitions_num)
      <<" partitions on "<<std::to_string(total_workers)<<" workers";
    throw std::runtime_error(s.str());
  }
  if (replicas_num > hbr.size()) {
    std::ostringstream s;
    s<<"Replication factor of "<<std::to_string(replicas_num)
      <<" is smaller than the number of racks: "<<std::to_string(hbr.size());
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

  Plan table_plan(partitions_num);
  size_t partition = 0;

  while (partition < partitions_num) {
    // Iterate on every rack and on every host in cycles.
    // Let's say we have two racks, two hosts under each rack and two workers under each host:
    // r1:h1:w1 -> r2:h1:w1 -> r1:h2:w1 -> r2:h2:w1 -> r1:h1:w2 -> r2:h1:w2 -> r1:h2:w2 -> r2:h2:w2
    for (size_t ri = 0, w = 0, p = 0; p < replicas_num && w < total_workers; ++w) {
      size_t hi = host_idxs[ri];
      size_t wi = worker_idxs[ri][hi];

      // Try to place partition replica on next worker
      if (wi < rh[ri][hi].size()) {
        auto next_worker = rh[ri][hi][wi];
        worker_idxs[ri][hi] = wi + 1;
        table_plan.AddPlacement(partition, Placement(
            next_worker.str("hostname"), next_worker.num("http_port")
        ));
        ++p;
      }
      host_idxs[ri] = (hi + 1) % rh[ri].size();
      ri = (ri + 1) % rh.size();
    }
    ++partition;
  }

  return table_plan;
}

}}

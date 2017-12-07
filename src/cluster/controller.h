#ifndef VIYA_CLUSTER_CONTROLLER_H_
#define VIYA_CLUSTER_CONTROLLER_H_

#include <memory>
#include <map>
#include "util/config.h"
#include "util/schedule.h"
#include "cluster/consul/consul.h"
#include "cluster/plan.h"
#include "cluster/feeder.h"
#include "cluster/batch_info.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

class Controller {
  public:
    Controller(const util::Config& config);
    Controller(const Controller& other) = delete;

    const consul::Consul& consul() const { return consul_; }
    const util::Config& cluster_config() const { return cluster_config_; }
    const std::map<std::string, util::Config>& tables_configs() const { return tables_configs_; }
    const std::map<std::string, util::Config>& workers_configs() const { return workers_configs_; }
    const std::map<std::string, util::Config>& indexers_configs() const { return indexers_configs_; }
    const std::map<std::string, std::unique_ptr<BatchInfo>>& batches() const { return batches_; }
    const std::map<std::string, Plan>& tables_plans() const { return tables_plans_; }

  private:
    void ReadClusterConfig();
    bool ReadWorkersConfigs();
    void FetchLatestBatchInfo();
    void Initialize();
    void AssignPartitionsToWorkers();
    void InitializePlan();
    bool ReadPlan();
    bool GeneratePlan();

  private:
    util::Config config_;
    const std::string cluster_id_;
    const consul::Consul consul_;
    util::Config cluster_config_;
    std::unique_ptr<consul::Session> session_;
    std::unique_ptr<consul::LeaderElector> le_;
    std::unique_ptr<util::Later> initializer_;
    std::map<std::string, util::Config> tables_configs_;
    std::map<std::string, util::Config> workers_configs_;
    std::map<std::string, util::Config> indexers_configs_;
    std::map<std::string, std::unique_ptr<BatchInfo>> batches_;
    std::map<std::string, Plan> tables_plans_;
    std::unique_ptr<Feeder> feeder_;
};

}}

#endif // VIYA_CLUSTER_CONTROLLER_H_

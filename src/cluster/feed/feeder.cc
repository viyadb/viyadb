#include "cluster/feed/feeder.h"

namespace viya {
namespace cluster {
namespace feed {

Feeder::Feeder(const consul::Consul& consul, const util::Config& cluster_config,
           const std::unordered_map<std::string, util::Config>& table_configs):
  consul_(consul),cluster_config_(cluster_config),table_configs_(table_configs) {
};

}}}

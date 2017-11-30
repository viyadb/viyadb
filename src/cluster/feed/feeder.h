#ifndef VIYA_CLUSTER_CONSUL_FEEDER_H_
#define VIYA_CLUSTER_CONSUL_FEEDER_H_

#include <unordered_map>

namespace viya { namespace util { class Config; }}
namespace viya { namespace cluster { namespace consul { class Consul; }}}

namespace viya {
namespace cluster {
namespace feed {

class Feeder {
  public:
    Feeder(const consul::Consul& consul, const util::Config& cluster_config,
           const std::unordered_map<std::string, util::Config>& table_configs);

    Feeder(const Feeder& other) = delete;

  private:
    const consul::Consul& consul_;
    const util::Config& cluster_config_;
    const std::unordered_map<std::string, util::Config>& table_configs_;
};

}}}

#endif // VIYA_CLUSTER_CONSUL_FEEDER_H_

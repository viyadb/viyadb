#ifndef VIYA_CLUSTER_PARTITIONS_H_
#define VIYA_CLUSTER_PARTITIONS_H_

#include <map>
#include <json.hpp>

namespace viya {
namespace cluster {

using json = nlohmann::json;

class Partitions {
  public:
    Partitions(const json& table_info);
    Partitions(const Partitions& other) = delete;

    uint32_t ByKey(const std::string& key) { return partitions_[key]; }
    size_t partitions_num() const { return partitions_num_; }

  private:
    std::map<std::string, uint32_t> partitions_;
    size_t partitions_num_;
};

}}

#endif // VIYA_CLUSTER_PARTITIONS_H_

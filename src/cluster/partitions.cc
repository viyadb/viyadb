#include <set>
#include "cluster/partitions.h"

namespace viya {
namespace cluster {

Partitions::Partitions(const json& table_info) {
  json p = table_info["partitions"];
  std::set<uint32_t> d;
  for (auto it = p.begin(); it != p.end(); ++it) {
    uint32_t partition = it.value(); 
    partitions_.emplace(it.key(), partition);
    d.insert(partition);
  }
  partitions_num_ = d.size();
}

}}

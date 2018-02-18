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

#ifndef VIYA_CLUSTER_PARTITIONING_H_
#define VIYA_CLUSTER_PARTITIONING_H_

#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

namespace viya {
namespace cluster {

using json = nlohmann::json;

class Partitioning {
public:
  Partitioning(const json &json);
  Partitioning(const std::vector<uint32_t> &mapping, size_t total,
               const std::vector<std::string> &columns);

  const std::vector<uint32_t> &mapping() const { return mapping_; }
  size_t total() const { return total_; }
  const std::vector<std::string> &columns() const { return columns_; };

  json ToJson() const;

private:
  std::vector<uint32_t> mapping_;
  size_t total_;
  std::vector<std::string> columns_;
};

} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_PARTITIONING_H_

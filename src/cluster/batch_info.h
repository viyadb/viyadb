/*
 * Copyright (c) 2017 ViyaDB Group
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

#ifndef VIYA_CLUSTER_INFO_H_
#define VIYA_CLUSTER_INFO_H_

#include <map>
#include <json.hpp>

namespace viya {
namespace cluster {

using json = nlohmann::json;

class Info {
  public:
    Info(const json& info);
    virtual ~Info() = default;

    long id() const { return id_; }

  private:
    const long id_;
};

class MicroBatchTableInfo {
  public:
    MicroBatchTableInfo(const json& info);
    MicroBatchTableInfo(const MicroBatchTableInfo& other) = delete;

    const std::vector<std::string>& paths() const { return paths_; }

  private:
    const std::vector<std::string> paths_;
};

class MicroBatchInfo: public Info {
  public:
    MicroBatchInfo(const json& info);
    MicroBatchInfo(const MicroBatchInfo& other) = delete;

    const std::map<std::string, MicroBatchTableInfo>& tables_info() const {
      return tables_info_;
    }

  private:
    std::map<std::string, MicroBatchTableInfo> tables_info_;
};

class BatchTableInfo {
  public:
    BatchTableInfo(const json& info);

    const std::vector<std::string>& paths() const { return paths_; }
    const std::vector<int>& partitioning() const { return partitioning_; }
    size_t total_partitions() const { return total_partitions_; }
    const std::vector<std::string>& partition_columns() const { return partition_columns_; };

  private:
    const std::vector<std::string> paths_;
    std::vector<int> partitioning_;
    std::vector<std::string> partition_columns_;
    size_t total_partitions_;
};

class BatchInfo: public Info {
  public:
    BatchInfo(const json& info);

    long last_microbatch() const { return last_microbatch_; }

    const std::map<std::string, BatchTableInfo>& tables_info() const {
      return tables_info_;
    }

  private:
    long last_microbatch_;
    std::map<std::string, BatchTableInfo> tables_info_;
};

}}

#endif // VIYA_CLUSTER_INFO_H_

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

#ifndef VIYA_CLUSTER_BATCH_INFO_H_
#define VIYA_CLUSTER_BATCH_INFO_H_

#include <map>
#include <memory>
#include <json.hpp>
#include "cluster/partitioning.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

class Message {
  public:
    Message(const json& message);
    virtual ~Message() = default;

    long id() const { return id_; }

  private:
    const long id_;
};

class TableInfo {
  public:
    TableInfo(const json& message);
    TableInfo(const TableInfo& other) = delete;

    const std::vector<std::string>& paths() const { return paths_; }
    const std::vector<std::string>& columns() const { return columns_; }

  private:
    const std::vector<std::string> paths_;
    const std::vector<std::string> columns_;
};

class MicroBatchTableInfo : public TableInfo {
  public:
    MicroBatchTableInfo(const json& message);
    MicroBatchTableInfo(const MicroBatchTableInfo& other) = delete;
};

class MicroBatchInfo: public Message {
  public:
    MicroBatchInfo(const json& message);
    MicroBatchInfo(const MicroBatchInfo& other) = delete;

    const std::map<std::string, MicroBatchTableInfo>& tables_info() const {
      return tables_info_;
    }

  private:
    std::map<std::string, MicroBatchTableInfo> tables_info_;
};

class BatchTableInfo : public TableInfo {
  public:
    BatchTableInfo(const json& message);

    const Partitioning& partitioning() const { return *partitioning_; }
    bool has_partitioning() const { return (bool)partitioning_; }

  private:
    std::unique_ptr<Partitioning> partitioning_;
};

class BatchInfo: public Message {
  public:
    BatchInfo(const json& message);

    long last_microbatch() const { return last_microbatch_; }

    const std::map<std::string, BatchTableInfo>& tables_info() const {
      return tables_info_;
    }

  private:
    long last_microbatch_;
    std::map<std::string, BatchTableInfo> tables_info_;
};

}}

#endif // VIYA_CLUSTER_BATCH_INFO_H_

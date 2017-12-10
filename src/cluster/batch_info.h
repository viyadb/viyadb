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

class Partitioning {
  public:
    Partitioning(const json& info);
    Partitioning(const Partitioning& other) = delete;

    uint32_t Get(const std::string& key) { return partitioning_.at(key); }
    size_t TotalPartitions() const { return total_; }

  private:
    std::map<std::string, uint32_t> partitioning_;
    size_t total_;
};

class PartitionConf {
  public:
    PartitionConf(const json& info);
    PartitionConf(const PartitionConf& other) = delete;

    const std::vector<std::string>& columns() const { return columns_; };
    bool hashed() const { return hashed_; }

  private:
    const std::vector<std::string> columns_;
    bool hashed_;
};

class BatchTableInfo {
  public:
    BatchTableInfo(const json& info);

    const std::vector<std::string>& paths() const { return paths_; }
    const Partitioning& partitioning() const { return partitioning_; }
    const PartitionConf& partition_conf() const { return partition_conf_; }

  private:
    const std::vector<std::string> paths_;
    const Partitioning partitioning_;
    const PartitionConf partition_conf_;
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

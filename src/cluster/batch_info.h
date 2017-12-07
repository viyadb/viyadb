#ifndef VIYA_CLUSTER_INFO_H_
#define VIYA_CLUSTER_INFO_H_

#include <map>

namespace viya {
namespace cluster {

class Info {
  public:
    virtual ~Info() = default;
};

class MicroBatchTableInfo {
  public:
    MicroBatchTableInfo(const std::vector<std::string>& paths):paths_(paths) {}
    MicroBatchTableInfo(const MicroBatchTableInfo& other) = delete;

    const std::vector<std::string>& paths() const { return paths_; }

  private:
    const std::vector<std::string> paths_;
};

class MicroBatchInfo: public Info {
  public:
    MicroBatchInfo(const std::string& info);
    MicroBatchInfo(const MicroBatchInfo& other) = delete;

    long id() const { return id_; }

    const std::map<std::string, MicroBatchTableInfo>& tables_info() const {
      return tables_info_;
    }

  private:
    long id_;
    std::map<std::string, MicroBatchTableInfo> tables_info_;
};

class Partitioning {
  public:
    Partitioning(const std::map<std::string, uint32_t>& partitioning);
    Partitioning(const Partitioning& other) = delete;

    uint32_t Get(const std::string& key) { return partitioning_[key]; }
    size_t TotalPartitions() const { return total_; }

  private:
    std::map<std::string, uint32_t> partitioning_;
    size_t total_;
};

class BatchTableInfo {
  public:
    BatchTableInfo(const std::vector<std::string>& paths,
                   const std::map<std::string, uint32_t>& partitioning)
      :paths_(paths),partitioning_(partitioning) {}

    const std::vector<std::string>& paths() const { return paths_; }
    const Partitioning& partitioning() const { return partitioning_; }

  private:
    const std::vector<std::string> paths_;
    const Partitioning partitioning_;
};

class BatchInfo: public Info {
  public:
    BatchInfo(const std::string& info);

    long id() const { return id_; }
    long last_microbatch() const { return last_microbatch_; }

    const std::map<std::string, BatchTableInfo>& tables_info() const {
      return tables_info_;
    }

  private:
    long id_;
    long last_microbatch_;
    std::map<std::string, BatchTableInfo> tables_info_;
};

}}

#endif // VIYA_CLUSTER_INFO_H_

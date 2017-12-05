#ifndef VIYA_CLUSTER_INFO_H_
#define VIYA_CLUSTER_INFO_H_

#include <map>

namespace viya {
namespace cluster {

class Partitions {
  public:
    Partitions(const std::map<std::string, uint32_t>& partitions);
    Partitions(const Partitions& other) = delete;

    uint32_t Get(const std::string& key) { return partitions_[key]; }
    size_t total() const { return total_; }

  private:
    std::map<std::string, uint32_t> partitions_;
    size_t total_;
};

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

class BatchTableInfo {
  public:
    BatchTableInfo(const std::vector<std::string>& paths,
                   const std::map<std::string, uint32_t>& partitions)
      :paths_(paths),partitions_(partitions) {}

    const std::vector<std::string>& paths() const { return paths_; }
    const Partitions& partitions() const { return partitions_; }

  private:
    const std::vector<std::string> paths_;
    const Partitions partitions_;
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

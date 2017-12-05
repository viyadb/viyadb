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
    size_t partitions_num() const { return partitions_num_; }

  private:
    std::map<std::string, uint32_t> partitions_;
    size_t partitions_num_;
};

class Info {
  public:
    virtual ~Info() = default;
};

class MicroBatchInfo: public Info {
  public:
    MicroBatchInfo(const std::string& info);

    long id() const { return id_; }

    const std::map<std::string, std::vector<std::string>>& tables_paths() const {
      return tables_paths_;
    }

  private:
    long id_;
    std::map<std::string, std::vector<std::string>> tables_paths_;
};

class BatchInfo: public Info {
  public:
    BatchInfo(const std::string& info);

    long id() const { return id_; }
    long last_microbatch() const { return last_microbatch_; }

    const std::map<std::string, Partitions>& tables_partitions() const {
      return tables_partitions_;
    }

  private:
    long id_;
    long last_microbatch_;
    std::map<std::string, Partitions> tables_partitions_;
};

}}

#endif // VIYA_CLUSTER_INFO_H_

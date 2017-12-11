#ifndef VIYA_INPUT_LOAD_DESC_H_
#define VIYA_INPUT_LOAD_DESC_H_

#include <vector>
#include <memory>

namespace viya { namespace db { class Table; class Column; }}
namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace db = viya::db;

class PartitionFilter {
  public:
    PartitionFilter(const util::Config& config);
    PartitionFilter(const PartitionFilter& other) = delete;

    const std::vector<std::string>& columns() const { return columns_; }
    size_t partitions_num() const { return partitions_num_; }
    uint32_t partition() const { return partition_; }

  private:
    const std::vector<std::string> columns_;
    const size_t partitions_num_;
    const uint32_t partition_;
};

class LoaderDesc {
  public:
    enum Format { TSV, UNKNOWN };

    LoaderDesc(const util::Config& config, const db::Table& table);
    LoaderDesc(const LoaderDesc& other) = delete;
    virtual ~LoaderDesc() = default;

    const util::Config& config() const { return config_; }
    const db::Table& table() const { return table_; }
    Format format() const { return format_; }
    const std::string& fname() const { return fname_; }
    const std::vector<int>& tuple_idx_map() const { return tuple_idx_map_; }
    const PartitionFilter& partition_filter() const { return *partition_filter_; }
    bool has_partition_filter() const { return (bool) partition_filter_; }

  private:
    void InitTupleIdxMap();

  private:
    const util::Config& config_;
    const db::Table& table_;
    Format format_;
    std::string fname_;
    std::vector<int> tuple_idx_map_;
    std::unique_ptr<PartitionFilter> partition_filter_;
};

}}

#endif // VIYA_INPUT_LOAD_DESC_H_

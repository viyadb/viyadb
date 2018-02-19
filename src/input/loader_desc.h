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

#ifndef VIYA_INPUT_LOAD_DESC_H_
#define VIYA_INPUT_LOAD_DESC_H_

#include <memory>
#include <vector>

namespace viya {
namespace db {
class Table;
class Column;
} // namespace db
} // namespace viya
namespace viya {
namespace util {
class Config;
}
} // namespace viya

namespace viya {
namespace input {

namespace db = viya::db;

class PartitionFilter {
public:
  PartitionFilter(const util::Config &config);
  PartitionFilter(const PartitionFilter &other) = delete;

  const std::vector<std::string> &columns() const { return columns_; }
  size_t total_partitions() const { return total_partitions_; }
  const std::vector<uint32_t> &values() const { return values_; }

private:
  const std::vector<std::string> columns_;
  const size_t total_partitions_;
  const std::vector<uint32_t> values_;
};

class LoaderDesc {
public:
  enum Format { TSV, UNKNOWN };

  LoaderDesc(const util::Config &config, const db::Table &table);
  LoaderDesc(const LoaderDesc &other) = delete;
  virtual ~LoaderDesc() = default;

  const util::Config &config() const { return config_; }
  const db::Table &table() const { return table_; }
  Format format() const { return format_; }
  const std::string &fname() const { return fname_; }
  const std::vector<int> &tuple_idx_map() const { return tuple_idx_map_; }
  size_t columns_num() const { return columns_num_; }
  const PartitionFilter &partition_filter() const { return *partition_filter_; }
  bool has_partition_filter() const { return (bool)partition_filter_; }

private:
  void InitTupleIdxMap();

private:
  const util::Config &config_;
  const db::Table &table_;
  Format format_;
  std::string fname_;
  std::vector<int> tuple_idx_map_;
  size_t columns_num_;
  std::unique_ptr<PartitionFilter> partition_filter_;
};
} // namespace input
} // namespace viya

#endif // VIYA_INPUT_LOAD_DESC_H_

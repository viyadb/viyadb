#ifndef VIYA_DB_TABLE_H_
#define VIYA_DB_TABLE_H_

#include <string>
#include <vector>
#include "util/config.h"
#include "db/stats.h"

namespace viya {
namespace db {

namespace util = viya::util;

class Column;
class Table;
class Database;
class Dimension;
class Metric;
class SegmentStore;

class CardinalityGuard {
  public:
    CardinalityGuard(const util::Config& config, const Dimension* dim, const Table& table);

    const Dimension* dim() const { return dim_; }
    const std::vector<const Dimension*>& dimensions() const { return dimensions_; }
    size_t limit() const { return limit_; }

  private:
    const Dimension* dim_;
    std::vector<const Dimension*> dimensions_;
    size_t limit_;
};

class Table {
  public:
    Table(const util::Config& config, class Database& database);
    Table(const Table& other) = delete;
    ~Table();

    const std::string& name() const { return name_; }
    const Database& database() const { return database_; }

    const Column* column(const std::string& name) const;
    const std::vector<const Column*> columns() const;
    const std::vector<const Dimension*>& dimensions() const { return dimensions_; }
    const std::vector<const Metric*>& metrics() const { return metrics_; }
    const Dimension* dimension(const std::string& name) const;
    const Dimension* dimension(size_t index) const { return dimensions_[index]; }
    const Metric* metric(const std::string& name) const;
    const Metric* metric(size_t index) const { return metrics_[index]; }
    const SegmentStore* store() const { return store_; }
    SegmentStore* store() { return store_; }
    size_t segment_size() const { return segment_size_; }
    const std::vector<CardinalityGuard>& cardinality_guards() const { return cardinality_guards_; } 

    void PrintMetadata(std::string&);

  private:
    class Database& database_;
    std::string name_;
    std::vector<const Dimension*> dimensions_;
    std::vector<const Metric*> metrics_;
    SegmentStore* store_;
    size_t segment_size_;
    std::vector<CardinalityGuard> cardinality_guards_;
};

}}

#endif // VIYA_DB_TABLE_H_

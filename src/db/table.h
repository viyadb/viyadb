#ifndef VIYA_DB_TABLE_H_
#define VIYA_DB_TABLE_H_

#include <string>
#include <vector>
#include "util/config.h"
#include "db/column.h"
#include "db/store.h"
#include "db/stats.h"

namespace viya {
namespace db {

namespace util = viya::util;

class Table;

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

using TableMetadataFn = void (*)(Table&, std::string&);
using UpsertSetupFn = void (*)(Table&);
using BeforeUpsertFn = void (*)();
using AfterUpsertFn = UpsertStats (*)();
using UpsertFn = void (*)(std::vector<std::string>&);

class Table {
  public:
    Table(const util::Config& config, class Database& database);
    Table(const Table& other) = delete;
    ~Table();

    const std::string& name() const { return name_; }
    const Database& database() const { return database_; }

    const Column* column(const std::string& name) const;
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

    void BeforeLoad();
    UpsertStats AfterLoad();
    void Load(std::vector<std::string>& values) { upsert_(values); }
    void Load(std::initializer_list<std::vector<std::string>> rows);
    void PrintMetadata(std::string&);

  private:
    void GenerateFunctions();

  private:
    class Database& database_;
    std::string name_;
    std::vector<const Dimension*> dimensions_;
    std::vector<const Metric*> metrics_;
    SegmentStore* store_;
    size_t segment_size_;
    std::vector<CardinalityGuard> cardinality_guards_;

    BeforeUpsertFn before_upsert_;
    AfterUpsertFn after_upsert_;
    UpsertFn upsert_;
};

}}

#endif // VIYA_DB_TABLE_H_

#ifndef VIYA_CODEGEN_DB_UPSERT_H_
#define VIYA_CODEGEN_DB_UPSERT_H_

#include "db/column.h"
#include "db/stats.h"
#include "codegen/generator.h"

namespace viya { namespace input { class LoaderDesc; }}

namespace viya {
namespace codegen {

namespace db = viya::db;
namespace input = viya::input;

class ValueParser: public db::ColumnVisitor {
  public:
    ValueParser(Code& code, const std::vector<int>& tuple_idx_map, size_t& value_idx):
      code_(code),tuple_idx_map_(tuple_idx_map),value_idx_(value_idx) {}

    void Visit(const db::StrDimension* dimension);
    void Visit(const db::NumDimension* dimension);
    void Visit(const db::TimeDimension* dimension);
    void Visit(const db::BoolDimension* dimension);
    void Visit(const db::ValueMetric* metric);
    void Visit(const db::BitsetMetric* metric);

  private:
    Code& code_;
    const std::vector<int>& tuple_idx_map_;
    size_t& value_idx_;
};

using UpsertSetupFn = void (*)(const db::Table&);
using BeforeUpsertFn = void (*)();
using AfterUpsertFn = db::UpsertStats (*)();
using UpsertFn = void (*)(std::vector<std::string>&);

class UpsertGenerator: public FunctionGenerator {
  public:
    UpsertGenerator(const input::LoaderDesc& desc);
    UpsertGenerator(const UpsertGenerator& other) = delete;

    Code GenerateCode() const;

    UpsertSetupFn SetupFunction();
    BeforeUpsertFn BeforeFunction();
    AfterUpsertFn AfterFunction();
    UpsertFn Function();

  private:
    Code SetupFunctionCode() const;
    Code CardinalityProtection() const;
    Code PartitionFilter() const;
    bool AddOptimize() const;
    Code OptimizeFunctionCode() const;

  private:
    const input::LoaderDesc& desc_;
};

}}

#endif // VIYA_CODEGEN_DB_UPSERT_H_

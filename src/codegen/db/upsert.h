#ifndef VIYA_CODEGEN_DB_UPSERT_H_
#define VIYA_CODEGEN_DB_UPSERT_H_

#include "db/table.h"
#include "db/column.h"
#include "db/stats.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

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

using UpsertSetupFn = void (*)(db::Table&);
using BeforeUpsertFn = void (*)();
using AfterUpsertFn = db::UpsertStats (*)();
using UpsertFn = void (*)(std::vector<std::string>&);

class UpsertGenerator: public FunctionGenerator {
  public:
    UpsertGenerator(Compiler& compiler, const db::Table& table, const std::vector<int>& tuple_idx_map)
      :FunctionGenerator(compiler),table_(table),tuple_idx_map_(tuple_idx_map) {}

    UpsertGenerator(const UpsertGenerator& other) = delete;

    Code GenerateCode() const;

    UpsertSetupFn SetupFunction();
    BeforeUpsertFn BeforeFunction();
    AfterUpsertFn AfterFunction();
    UpsertFn Function();

  private:
    Code SetupFunctionCode() const;
    Code CardinalityProtection() const;
    bool AddOptimize() const;
    Code OptimizeFunctionCode() const;

  private:
    const db::Table& table_;
    const std::vector<int>& tuple_idx_map_;
};

}}

#endif // VIYA_CODEGEN_DB_UPSERT_H_

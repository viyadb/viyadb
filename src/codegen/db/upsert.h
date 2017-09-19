#ifndef VIYA_CODEGEN_DB_UPSERT_H_
#define VIYA_CODEGEN_DB_UPSERT_H_

#include "db/table.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

class ValueParser: public db::ColumnVisitor {
  public:
    ValueParser(Code& code, size_t& value_idx):
      code_(code),value_idx_(value_idx) {}

    void Visit(const db::StrDimension* dimension);
    void Visit(const db::NumDimension* dimension);
    void Visit(const db::TimeDimension* dimension);
    void Visit(const db::BoolDimension* dimension);
    void Visit(const db::ValueMetric* metric);
    void Visit(const db::BitsetMetric* metric);

  private:
    Code& code_;
    size_t& value_idx_;
};

class UpsertGenerator: public FunctionGenerator {
  public:
    UpsertGenerator(Compiler& compiler, const db::Table& table)
      :FunctionGenerator(compiler),table_(table) {}

    UpsertGenerator(const UpsertGenerator& other) = delete;

    Code GenerateCode() const;

    db::UpsertSetupFn SetupFunction();
    db::BeforeUpsertFn BeforeFunction();
    db::AfterUpsertFn AfterFunction();
    db::UpsertFn Function();

  private:
    Code SetupFunctionCode() const;
    Code CardinalityProtection() const;
    bool AddOptimize() const;
    Code OptimizeFunctionCode() const;

  private:
    const db::Table& table_;
};

}}

#endif // VIYA_CODEGEN_DB_UPSERT_H_

#ifndef VIYA_CODEGEN_DB_METADATA_H_
#define VIYA_CODEGEN_DB_METADATA_H_

#include "db/table.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

using TableMetadataFn = void (*)(db::Table&, std::string&);

class TableMetadata: public FunctionGenerator {
  public:
    TableMetadata(Compiler& compiler, const db::Table& table)
      :FunctionGenerator(compiler),table_(table) {}

    TableMetadata(const TableMetadata& other) = delete;

    Code GenerateCode() const;
    TableMetadataFn Function();

  private:
    const db::Table& table_;
};

}}

#endif // VIYA_CODEGEN_DB_METADATA_H_

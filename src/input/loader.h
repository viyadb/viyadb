#ifndef VIYA_INPUT_LOADER_H_
#define VIYA_INPUT_LOADER_H_

#include "input/stats.h"
#include "codegen/db/upsert.h"

namespace viya { namespace db { class Table; class Column; }}

namespace viya {
namespace input {

namespace db = viya::db;
namespace cg = viya::codegen;

class Loader {
  public:
    Loader(db::Table& table, const std::vector<int>& tuple_idx_map);
    Loader(const Loader& other) = delete;
    virtual ~Loader() {}

    virtual void LoadData() = 0;
    static std::vector<const db::Column*> GetInputColumns(const db::Table& table);

  protected:
    void BeforeLoad();
    void Load(std::vector<std::string>& values) { upsert_(values); }
    db::UpsertStats AfterLoad();

  protected:
    db::Table& table_;
    const std::vector<int> tuple_idx_map_;
    LoaderStats stats_;
    cg::BeforeUpsertFn before_upsert_;
    cg::AfterUpsertFn after_upsert_;
    cg::UpsertFn upsert_;
};

}}

#endif // VIYA_INPUT_LOADER_H_

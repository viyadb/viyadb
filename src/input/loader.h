#ifndef VIYA_INPUT_LOADER_H_
#define VIYA_INPUT_LOADER_H_

#include "input/loader_desc.h"
#include "input/stats.h"
#include "codegen/db/upsert.h"

namespace viya {
namespace input {

namespace cg = viya::codegen;

class Loader {
  public:
    Loader(const util::Config& config, const db::Table& table);
    Loader(const Loader& other) = delete;
    virtual ~Loader() = default;

    virtual void LoadData() = 0;
    const LoaderDesc& desc() const { return desc_; }

  protected:
    void BeforeLoad();
    void Load(std::vector<std::string>& values) { upsert_(values); }
    db::UpsertStats AfterLoad();

  protected:
    const LoaderDesc desc_;
    LoaderStats stats_;
    cg::BeforeUpsertFn before_upsert_;
    cg::AfterUpsertFn after_upsert_;
    cg::UpsertFn upsert_;
};

}}

#endif // VIYA_INPUT_LOADER_H_

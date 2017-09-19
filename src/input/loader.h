#ifndef VIYA_INPUT_LOADER_H_
#define VIYA_INPUT_LOADER_H_

#include "db/table.h"
#include "db/database.h"
#include "input/stats.h"
#include "util/config.h"

namespace viya {
namespace input {

namespace db = viya::db;
namespace util = viya::util;

class Loader {
  public:
    enum Format { TSV };

    Loader(db::Table& table, Format format):
      table_(table),stats_(table.database().statsd(), table.name()),
      format_(format) {}

    Loader(const Loader& other) = delete;
    virtual ~Loader() {}

    virtual void LoadData() = 0;

  protected:
    db::Table& table_;
    LoaderStats stats_;
    const Format format_;
};

class LoaderFactory {
  public:
    Loader* Create(const util::Config& config, db::Database& database);
};

}}

#endif // VIYA_INPUT_LOADER_H_

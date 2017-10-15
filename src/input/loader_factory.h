#ifndef VIYA_INPUT_LOADER_FACTORY_H_
#define VIYA_INPUT_LOADER_FACTORY_H_

#include "input/loader.h"

namespace viya { namespace db { class Database; class Table; }}
namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace db = viya::db;
namespace util = viya::util;

class LoaderFactory {
  public:
    Loader* Create(const util::Config& config, db::Database& database);

  private:
    std::vector<int> CreateTupleIdxMapping(const util::Config& config,
                                           const db::Table& table);
};

}}

#endif // VIYA_INPUT_LOADER_FACTORY_H_

#ifndef VIYA_INPUT_SIMPLE_H_
#define VIYA_INPUT_SIMPLE_H_

#include "input/loader.h"
#include "input/loader_desc.h"

namespace viya { namespace db { class Table; }}
namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace input = viya::input;
namespace db = viya::db;
namespace util = viya::util;

class SimpleLoader : public Loader {
  public:
    SimpleLoader(const db::Table& table);
    SimpleLoader(const util::Config& config, const db::Table& table);
    SimpleLoader(const SimpleLoader& other) = delete;

    void Load(std::initializer_list<std::vector<std::string>> rows);
    void Load(std::vector<std::string>& values) { Loader::Load(values); }
    void LoadData() {}
};

}}

#endif // VIYA_INPUT_SIMPLE_H_

#ifndef VIYA_INPUT_SIMPLE_H_
#define VIYA_INPUT_SIMPLE_H_

#include "db/table.h"
#include "input/loader.h"

namespace viya {
namespace input {

namespace input = viya::input;
namespace db = viya::db;

class SimpleLoader : public Loader {
  public:
    SimpleLoader(db::Table& table);

    void Load(std::initializer_list<std::vector<std::string>> rows);
    void Load(std::vector<std::string>& values) { Loader::Load(values); }
    void LoadData() {}
};

}}

#endif // VIYA_INPUT_SIMPLE_H_

#include <vector>
#include "input/simple.h"

namespace viya {
namespace input {

std::vector<int> get_tuple_idx_map(db::Table& table) {
  auto input_cols = Loader::GetInputColumns(table);
  std::vector<int> tuple_idx_map(input_cols.size());
  for (size_t i = 0; i < input_cols.size(); ++i) {
    tuple_idx_map[i] = i;
  }
  return std::move(tuple_idx_map);
}

SimpleLoader::SimpleLoader(db::Table& table):Loader(table, get_tuple_idx_map(table)) {
}

void SimpleLoader::Load(std::initializer_list<std::vector<std::string>> rows) {
  BeforeLoad();
  for (auto row : rows) {
    Loader::Load(row);
  }
  AfterLoad();
}

}}


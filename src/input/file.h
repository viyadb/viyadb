#ifndef VIYA_INPUT_FILE_H_
#define VIYA_INPUT_FILE_H_

#include "input/loader.h"

namespace viya {
namespace input {

class FileLoader: public Loader {
  public:
    FileLoader(db::Table& table, Format format, const std::string& fname,
               std::vector<int>& tuple_idx_map);
    FileLoader(const FileLoader&) = delete;
    ~FileLoader();

    void LoadData();

  protected:
    void LoadTsv();

  private:
    std::string fname_;
    int fd_;
    const std::vector<int> tuple_idx_map_;
};

}}

#endif // VIYA_INPUT_FILE_H_

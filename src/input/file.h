#ifndef VIYA_INPUT_FILE_H_
#define VIYA_INPUT_FILE_H_

#include "input/stats.h"
#include "input/loader_desc.h"
#include "input/loader.h"

namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace util = viya::util;

class FileLoader: public Loader {
  public:
    FileLoader(const util::Config& config, const db::Table& table);
    FileLoader(const FileLoader&) = delete;
    ~FileLoader();

    void LoadData();

  protected:
    void LoadTsv();

  private:
    int fd_;
    char* buf_;
    size_t buf_size_;
};

}}

#endif // VIYA_INPUT_FILE_H_

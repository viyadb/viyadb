#ifndef VIYA_INPUT_FILE_H_
#define VIYA_INPUT_FILE_H_

#include "input/loader.h"

namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace util = viya::util;

class FileLoader: public Loader {
  public:
    enum Format { TSV };

    FileLoader(db::Table& table, const util::Config& config, std::vector<int>& tuple_idx_map);
    FileLoader(const FileLoader&) = delete;
    ~FileLoader();

    void LoadData();

  protected:
    void LoadTsv();

  private:
    const Format format_;
    std::string fname_;
    int fd_;
    char* buf_;
    size_t buf_size_;
};

}}

#endif // VIYA_INPUT_FILE_H_

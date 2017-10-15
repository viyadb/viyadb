#include <fcntl.h>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <algorithm>
#include <glog/logging.h>
#include "input/file.h"
#include "util/config.h"

namespace viya {
namespace input {

FileLoader::Format parse_format(const std::string& format) {
  if (format == "tsv") {
    return FileLoader::Format::TSV;
  }
  throw std::invalid_argument("Unsupported input format: " + format);
}

FileLoader::FileLoader(db::Table& table, const util::Config& config, std::vector<int>& tuple_idx_map)
  :Loader(table, tuple_idx_map),format_(parse_format(config.str("format"))), fname_(config.str("file")) {

  fd_ = open(fname_.c_str(), O_RDONLY);
  if (fd_ == -1) {
    throw std::runtime_error("File not accessible: " + fname_);
  }
#ifdef __linux__
  posix_fadvise(fd_, 0, 0, 1); // FDADVICE_SEQUENTIAL
#endif
}

FileLoader::~FileLoader() {
  if (fd_ != -1) {
    close(fd_);
  }
}

void FileLoader::LoadTsv() {
  LOG(INFO)<<"Loading "<<fname_<<" into table: "<<table_.name();

  static const size_t BUFFER_SIZE = 16384;
  static const size_t MAX_TUPLE_SIZE = 1024000;

  static char buf[BUFFER_SIZE + 1];
  static char line[MAX_TUPLE_SIZE];

  size_t cols_num = *std::max_element(tuple_idx_map_.begin(), tuple_idx_map_.end()) + 1;

  std::vector<std::string> tuple(cols_num);
  for (auto& s: tuple) {
    s.reserve(MAX_TUPLE_SIZE / cols_num);
  }

  size_t line_num = 1;
  size_t remaining = 0, start = 0;
  char* f;

  while (size_t bytes_read = read(fd_, buf, BUFFER_SIZE)) {
    if (bytes_read == (size_t) - 1) {
      throw std::runtime_error("I/O error reading from: " + fname_);
    }
    if (!bytes_read) {
      break;
    }

    // Read line:
    for(char *p = buf; (f = (char*) memchr(p, '\n', (buf + bytes_read) - p)); ++p) {
      assert(f - p + 1 + start < MAX_TUPLE_SIZE);
      memcpy(line + start, p, f - p + 1);
      line[f - p + start] = '\0';

      // Parse tuples:
      size_t tuple_idx = 0;
      char *tp = line;
      char *tp_start = line;
      while (*tp) {
        if (*tp == '\t') {
          *tp = '\0';
          if (tuple_idx >= cols_num) {
            throw std::runtime_error("number of input columns is too big");
          }
          tuple[tuple_idx++].assign(tp_start, tp - tp_start);
          tp_start = tp + 1;
        }
        tp++;
      }
      if (tp > tp_start) {
        if (tuple_idx >= cols_num) {
          throw std::runtime_error("number of input columns is too big");
        }
        tuple[tuple_idx++].assign(tp_start, tp - tp_start);
      }

      try {
        Load(tuple);
        ++line_num;
      } catch (std::exception& e) {
        throw std::runtime_error(
          "reading TSV file at line " + std::to_string(line_num) +
          " (" + std::string(e.what()) + ")");
      }
      ++stats_.total_recs;

      p = f;
      start = 0;
      remaining = p - buf + 1;
    }
    if (remaining < bytes_read) {
      memcpy(line, buf + remaining, bytes_read - remaining);
      start = bytes_read - remaining;
    }
  }
}

void FileLoader::LoadData() {
  stats_.OnBegin();
  BeforeLoad();

  if (format_ == Format::TSV) {
    LoadTsv();
  }

  stats_.upsert_stats = AfterLoad();
  stats_.OnEnd();
}

}}


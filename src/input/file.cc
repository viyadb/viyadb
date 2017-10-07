#include <fcntl.h>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <glog/logging.h>
#include "input/file.h"

namespace viya {
namespace input {

namespace util = viya::util;

FileLoader::FileLoader(db::Table& table, Format format, const std::string& fname,
                       std::vector<int>& tuple_idx_map)
  :Loader(table, format),fname_(fname),tuple_idx_map_(tuple_idx_map) {

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

  size_t cols_num = table_.dimensions().size();
  for (auto metric : table_.metrics()) {
    if (metric->agg_type() != db::Metric::AggregationType::COUNT) {
      ++cols_num;
    }
  }

  static const size_t BUFFER_SIZE = 16384;
  static const size_t MAX_TUPLE_SIZE = 1024000;

  static char buf[BUFFER_SIZE + 1];
  static char line[MAX_TUPLE_SIZE];

  size_t file_cols_num = tuple_idx_map_.size();

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
          if (tuple_idx >= file_cols_num) {
            throw std::runtime_error("number of input columns is too big");
          }
          auto target_idx = tuple_idx_map_[tuple_idx];
          if (target_idx != -1) {
            tuple[target_idx].assign(tp_start, tp - tp_start);
          }
          tp_start = tp + 1;
          tuple_idx++;
        }
        tp++;
      }
      if (tp > tp_start) {
        if (tuple_idx >= file_cols_num) {
          throw std::runtime_error("number of input columns is too big");
        }
        auto target_idx = tuple_idx_map_[tuple_idx];
        if (target_idx != -1) {
          tuple[target_idx].assign(tp_start, tp - tp_start);
        }
      }

      try {
        table_.Load(tuple);
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
  table_.BeforeLoad();

  if (format_ == Format::TSV) {
    LoadTsv();
  }

  stats_.upsert_stats = table_.AfterLoad();
  stats_.OnEnd();
}

}}


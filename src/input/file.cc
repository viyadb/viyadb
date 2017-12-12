/*
 * Copyright (c) 2017 ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <glog/logging.h>
#include "db/table.h"
#include "db/database.h"
#include "input/file.h"
#include "util/config.h"

namespace viya {
namespace input {

FileLoader::FileLoader(const util::Config& config, const db::Table& table):
  Loader(config, table),
  fd_(-1),buf_(nullptr),buf_size_(0L) {

  auto& fname = desc_.fname();
  fd_ = open(fname.c_str(), O_RDONLY);
  if (fd_ == -1) {
    throw std::runtime_error("File not accessible: " + fname);
  }

  struct stat fs;
  if (fstat(fd_, &fs) == -1) {
    throw std::runtime_error("Can't get file status: " + fname);
  }
  buf_size_ = fs.st_size;

  buf_ = static_cast<char*>(mmap(NULL, buf_size_, PROT_READ, MAP_SHARED, fd_, 0));
  if (buf_ == MAP_FAILED) {
    throw std::runtime_error("Can't mmap() on file: " + fname);
  }

  if (madvise(buf_, buf_size_, MADV_WILLNEED | MADV_SEQUENTIAL) == -1) {
    LOG(WARNING)<<"Can't madvise() on file: "<<fname;
  }
}

FileLoader::~FileLoader() {
  if (fd_ != -1) {
    if (close(fd_) == -1) {
      LOG(WARNING)<<"Can't close() input file: "<<desc_.fname();
    }
  }
  if (buf_ != nullptr) {
    if (munmap(buf_, buf_size_) == -1) {
      LOG(WARNING)<<"Can't munmap() input file: "<<desc_.fname();
    }
  }
}

void FileLoader::LoadTsv() {
  LOG(INFO)<<"Loading "<<desc_.fname()<<" into table: "<<desc_.table().name();

  static const size_t MAX_TUPLE_SIZE = 1024000;
  static char line[MAX_TUPLE_SIZE];

  auto& tuple_idx_map = desc_.tuple_idx_map();
  size_t cols_num = *std::max_element(tuple_idx_map.begin(), tuple_idx_map.end()) + 1;

  std::vector<std::string> tuple(cols_num);
  for (auto& s: tuple) {
    s.reserve(MAX_TUPLE_SIZE / cols_num);
  }

  size_t line_num = 1;
  char* buf_end = buf_ + buf_size_;

  // Read lines:
  for(char* lstart = buf_; lstart < buf_end; ) {
    char* lend = (char*) memchr(lstart, '\n', buf_end - lstart);
    if (lend == NULL) {
      lend = buf_end;
    }
    assert((size_t)(lend - lstart + 1) < MAX_TUPLE_SIZE);
    memcpy(line, lstart, lend - lstart + 1);
    line[lend - lstart] = '\0';

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
    lstart = lend + 1;
  }
}

void FileLoader::LoadData() {
  stats_.OnBegin();
  BeforeLoad();

  if (desc_.format() == LoaderDesc::Format::TSV) {
    LoadTsv();
  } else {
    throw std::runtime_error("Unknown input file format");
  }

  stats_.upsert_stats = AfterLoad();
  stats_.OnEnd();
}

}}


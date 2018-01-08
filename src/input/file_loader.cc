/*
 * Copyright (c) 2017-present ViyaDB Group
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
#include "util/config.h"
#include "input/file_loader.h"

namespace viya {
namespace input {

FileLoader::FileLoader(const util::Config& config, db::Table& table):
  BufferLoader(config, table),fd_(-1) {

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

  buf_ = static_cast<const char*>(mmap(NULL, buf_size_, PROT_READ, MAP_SHARED, fd_, 0));
  if (buf_ == MAP_FAILED) {
    throw std::runtime_error("Can't mmap() on file: " + fname);
  }

  if (madvise(const_cast<char*>(buf_), buf_size_, MADV_WILLNEED | MADV_SEQUENTIAL) == -1) {
    LOG(WARNING)<<"Can't madvise() on file: "<<fname;
  }

  LOG(INFO)<<"Loading "<<desc_.fname()<<" into table: "<<desc_.table().name();
}

FileLoader::~FileLoader() {
  if (fd_ != -1) {
    if (close(fd_) == -1) {
      LOG(WARNING)<<"Can't close() input file: "<<desc_.fname();
    }
  }
  if (buf_ != nullptr) {
    if (munmap(const_cast<char*>(buf_), buf_size_) == -1) {
      LOG(WARNING)<<"Can't munmap() input file: "<<desc_.fname();
    }
  }
}

}}


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
#include <cassert>
#include <cstring>
#include "db/table.h"
#include "db/database.h"
#include "util/config.h"
#include "input/buffer_loader.h"

namespace viya {
namespace input {

BufferLoader::BufferLoader(const util::Config& config, db::Table& table):
  Loader(config, table),buf_(nullptr),buf_size_(0L) {
}

BufferLoader::BufferLoader(const util::Config& config, db::Table& table,
                           const char* buf, size_t buf_size):
  Loader(config, table),buf_(buf),buf_size_(buf_size) {
}

void BufferLoader::LoadTsv() {
  static const size_t MAX_TUPLE_SIZE = 1024000;
  static char line[MAX_TUPLE_SIZE];

  auto& tuple_idx_map = desc_.tuple_idx_map();
  size_t cols_num = *std::max_element(tuple_idx_map.begin(), tuple_idx_map.end()) + 1;

  std::vector<std::string> tuple(cols_num);
  for (auto& s: tuple) {
    s.reserve(MAX_TUPLE_SIZE / cols_num);
  }

  size_t line_num = 1;
  const char* buf_end = buf_ + buf_size_;

  // Read lines:
  for(const char* lstart = buf_; lstart < buf_end; ) {
    const char* lend = (const char*) memchr(lstart, '\n', buf_end - lstart);
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
        "at line " + std::to_string(line_num) + " (" + std::string(e.what()) + ")");
    }
    ++stats_.total_recs;
    lstart = lend + 1;
  }
}

void BufferLoader::LoadData() {
  stats_.OnBegin();
  BeforeLoad();

  if (desc_.format() == LoaderDesc::Format::TSV) {
    LoadTsv();
  } else {
    throw std::runtime_error("unknown input format (supported formats: TSV)");
  }

  stats_.upsert_stats = AfterLoad();
  stats_.OnEnd();
}

}}


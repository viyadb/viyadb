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

#ifndef VIYA_INPUT_BUFFER_LOADER_H_
#define VIYA_INPUT_BUFFER_LOADER_H_

#include "input/stats.h"
#include "input/loader_desc.h"
#include "input/loader.h"

namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace util = viya::util;

class BufferLoader: public Loader {
  public:
    BufferLoader(const util::Config&, db::Table&, const char*, size_t);
    BufferLoader(const BufferLoader&) = delete;
    virtual ~BufferLoader() {}

    void LoadData();

  protected:
    BufferLoader(const util::Config&, db::Table&);

  private:
    void LoadTsv();

  protected:
    const char* buf_;
    size_t buf_size_;
};

}}

#endif // VIYA_INPUT_BUFFER_LOADER_H_

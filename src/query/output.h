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

#ifndef VIYA_QUERY_OUTPUT_H_
#define VIYA_QUERY_OUTPUT_H_

#include <vector>
#include <string>

namespace viya {
namespace query {

class RowOutput {
  public:
    using Row = std::vector<std::string>;

    RowOutput() {}
    virtual ~RowOutput() {}

    virtual void Start() {};
    virtual void Send(const Row& row) = 0;
    virtual void SendAsCol(const Row& col) = 0;
    virtual void Flush() {};
};

class MemoryRowOutput: public RowOutput {
  public:
    void Send(const Row& row) { rows_.push_back(row); }
    void SendAsCol(const Row& row) { rows_.push_back(row); }

    const std::vector<Row>& rows() const { return rows_; }

  private:
    std::vector<Row> rows_;
};

}}

#endif // VIYA_QUERY_OUTPUT_H_

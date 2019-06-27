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

#ifndef VIYA_DB_SEGMENT_H_
#define VIYA_DB_SEGMENT_H_

#include "util/macros.h"
#include "util/rwlock.h"

namespace viya {
namespace db {

class SegmentBase {
public:
  SegmentBase(size_t capacity) : size_(0), capacity_(capacity){};
  DISALLOW_COPY_AND_MOVE(SegmentBase);
  virtual ~SegmentBase() {}

  bool full() const { return size_ == capacity_; }

  size_t size() {
    lock_.lock_shared();
    auto size = size_;
    lock_.unlock_shared();
    return size;
  }

  size_t capacity() const { return capacity_; }

protected:
  size_t size_;
  size_t capacity_;
  folly::RWSpinLock lock_;
};
} // namespace db
} // namespace viya

#endif // VIYA_DB_SEGMENT_H_

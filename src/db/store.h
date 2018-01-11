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

#ifndef VIYA_DB_STORE_H_
#define VIYA_DB_STORE_H_

#include "db/segment.h"
#include "util/rwlock.h"
#include <vector>

namespace viya {
namespace db {

class Table;

using CreateSegmentFn = SegmentBase *(*)();

class SegmentStore {
public:
  SegmentStore(class Database &database, class Table &table);
  SegmentStore(const SegmentStore &other) = delete;
  ~SegmentStore();

  std::vector<SegmentBase *> &segments() { return segments_; }

  const std::vector<SegmentBase *> segments_copy() {
    folly::RWSpinLock::ReadHolder guard(lock_);
    std::vector<SegmentBase *> copy = segments_;
    return std::move(copy);
  }

  SegmentBase *last() {
    if (segments_.empty() || segments_.back()->full()) {
      folly::RWSpinLock::WriteHolder guard(lock_);
      segments_.push_back(create_segment_());
    }
    return segments_.back();
  }

private:
  std::vector<SegmentBase *> segments_;
  folly::RWSpinLock lock_;
  CreateSegmentFn create_segment_;
};
}
}

#endif // VIYA_DB_STORE_H_

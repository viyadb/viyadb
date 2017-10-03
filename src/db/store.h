#ifndef VIYA_DB_STORE_H_
#define VIYA_DB_STORE_H_

#include <vector>
#include "db/segment.h"
#include "util/rwlock.h"

namespace viya {
namespace db {

class Table;

using CreateSegmentFn = SegmentBase* (*)();

class SegmentStore {
  public:
    SegmentStore(class Database& database, class Table& table);
    SegmentStore(const SegmentStore& other) = delete;
    ~SegmentStore();

    std::vector<SegmentBase*>& segments() { return segments_; }

    const std::vector<SegmentBase*> segments_copy() {
      folly::RWSpinLock::ReadHolder guard(lock_);
      std::vector<SegmentBase*> copy = segments_;
      return std::move(copy);
    }

    SegmentBase* last() {
      if (segments_.empty() || segments_.back()->full()) {
        folly::RWSpinLock::WriteHolder guard(lock_);
        segments_.push_back(create_segment_());
      }
      return segments_.back();
    }

  private:
    std::vector<SegmentBase*> segments_;
    folly::RWSpinLock lock_;
    CreateSegmentFn create_segment_;
};

}}

#endif // VIYA_DB_STORE_H_

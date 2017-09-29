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
      lock_.lock_shared();
      std::vector<SegmentBase*> copy = segments_;
      lock_.unlock_shared();
      return copy;
    }

    SegmentBase* last() {
      if (segments_.empty() || segments_.back()->full()) {
        lock_.lock();
        segments_.push_back(create_segment_());
        lock_.unlock();
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

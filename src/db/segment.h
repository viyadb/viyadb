#ifndef VIYA_DB_SEGMENT_H_
#define VIYA_DB_SEGMENT_H_

#include <cstdio>
#include "util/rwlock.h"

namespace viya {
namespace db {

class SegmentBase {
  public:
    SegmentBase(size_t capacity):size_(0),capacity_(capacity) {};

    SegmentBase(const SegmentBase& other) = delete;
    virtual ~SegmentBase() {}

    bool full() const { return size_ == capacity_; }

    size_t size() {
      lock_.lock_shared(); 
      auto size = size_;
      lock_.unlock_shared();
      return size;
    }

    size_t capacity() const  { return capacity_; }

#if ENABLE_PERSISTENCE
    virtual size_t save(FILE* fp) = 0;
    virtual size_t load(FILE* fp) = 0;
#endif

  protected:
    size_t size_;
    size_t capacity_;
    folly::RWSpinLock lock_;
};

}}

#endif // VIYA_DB_SEGMENT_H_

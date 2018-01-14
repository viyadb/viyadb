#ifndef VIYA_UTIL_RWLOCK_H_
#define VIYA_UTIL_RWLOCK_H_

/*
 * Copyright 2017 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define RW_SPINLOCK_USE_X86_INTRINSIC_
#include <x86intrin.h>
#define RW_SPINLOCK_USE_SSE_INSTRUCTIONS_

#include "util/likely.h"
#include <algorithm>
#include <atomic>
#include <string>
#include <thread>

namespace folly {

inline void asm_volatile_memory() { asm volatile("" : : : "memory"); }

inline void asm_volatile_pause() { asm volatile("pause"); }

/*
 * A simple, small (4-bytes), but unfair rwlock.  Use it when you want
 * a nice writer and don't expect a lot of write/read contention, or
 * when you need small rwlocks since you are creating a large number
 * of them.
 *
 * Note that the unfairness here is extreme: if the lock is
 * continually accessed for read, writers will never get a chance.  If
 * the lock can be that highly contended this class is probably not an
 * ideal choice anyway.
 *
 * It currently implements most of the Lockable, SharedLockable and
 * UpgradeLockable concepts except the TimedLockable related locking/unlocking
 * interfaces.
 */
class RWSpinLock {
  enum : int32_t { READER = 4, UPGRADED = 2, WRITER = 1 };

public:
  constexpr RWSpinLock() : bits_(0) {}

  RWSpinLock(RWSpinLock const &) = delete;
  RWSpinLock &operator=(RWSpinLock const &) = delete;

  // Lockable Concept
  void lock() {
    int count = 0;
    while (!LIKELY(try_lock())) {
      if (++count > 1000)
        std::this_thread::yield();
    }
  }

  // Writer is responsible for clearing up both the UPGRADED and WRITER bits.
  void unlock() {
    static_assert(READER > WRITER + UPGRADED, "wrong bits!");
    bits_.fetch_and(~(WRITER | UPGRADED), std::memory_order_release);
  }

  // SharedLockable Concept
  void lock_shared() {
    int count = 0;
    while (!LIKELY(try_lock_shared())) {
      if (++count > 1000)
        std::this_thread::yield();
    }
  }

  void unlock_shared() { bits_.fetch_add(-READER, std::memory_order_release); }

  // Downgrade the lock from writer status to reader status.
  void unlock_and_lock_shared() {
    bits_.fetch_add(READER, std::memory_order_acquire);
    unlock();
  }

  // UpgradeLockable Concept
  void lock_upgrade() {
    int count = 0;
    while (!try_lock_upgrade()) {
      if (++count > 1000)
        std::this_thread::yield();
    }
  }

  void unlock_upgrade() {
    bits_.fetch_add(-UPGRADED, std::memory_order_acq_rel);
  }

  // unlock upgrade and try to acquire write lock
  void unlock_upgrade_and_lock() {
    int64_t count = 0;
    while (!try_unlock_upgrade_and_lock()) {
      if (++count > 1000)
        std::this_thread::yield();
    }
  }

  // unlock upgrade and read lock atomically
  void unlock_upgrade_and_lock_shared() {
    bits_.fetch_add(READER - UPGRADED, std::memory_order_acq_rel);
  }

  // write unlock and upgrade lock atomically
  void unlock_and_lock_upgrade() {
    // need to do it in two steps here -- as the UPGRADED bit might be OR-ed at
    // the same time when other threads are trying do try_lock_upgrade().
    bits_.fetch_or(UPGRADED, std::memory_order_acquire);
    bits_.fetch_add(-WRITER, std::memory_order_release);
  }

  // Attempt to acquire writer permission. Return false if we didn't get it.
  bool try_lock() {
    int32_t expect = 0;
    return bits_.compare_exchange_strong(expect, WRITER,
                                         std::memory_order_acq_rel);
  }

  // Try to get reader permission on the lock. This can fail if we
  // find out someone is a writer or upgrader.
  // Setting the UPGRADED bit would allow a writer-to-be to indicate
  // its intention to write and block any new readers while waiting
  // for existing readers to finish and release their read locks. This
  // helps avoid starving writers (promoted from upgraders).
  bool try_lock_shared() {
    // fetch_add is considerably (100%) faster than compare_exchange,
    // so here we are optimizing for the common (lock success) case.
    int32_t value = bits_.fetch_add(READER, std::memory_order_acquire);
    if (UNLIKELY(value & (WRITER | UPGRADED))) {
      bits_.fetch_add(-READER, std::memory_order_release);
      return false;
    }
    return true;
  }

  // try to unlock upgrade and write lock atomically
  bool try_unlock_upgrade_and_lock() {
    int32_t expect = UPGRADED;
    return bits_.compare_exchange_strong(expect, WRITER,
                                         std::memory_order_acq_rel);
  }

  // try to acquire an upgradable lock.
  bool try_lock_upgrade() {
    int32_t value = bits_.fetch_or(UPGRADED, std::memory_order_acquire);

    // Note: when failed, we cannot flip the UPGRADED bit back,
    // as in this case there is either another upgrade lock or a write lock.
    // If it's a write lock, the bit will get cleared up when that lock's done
    // with unlock().
    return ((value & (UPGRADED | WRITER)) == 0);
  }

  // mainly for debugging purposes.
  int32_t bits() const { return bits_.load(std::memory_order_acquire); }

  class ReadHolder;
  class UpgradedHolder;
  class WriteHolder;

  class ReadHolder {
  public:
    explicit ReadHolder(RWSpinLock *lock) : lock_(lock) {
      if (lock_)
        lock_->lock_shared();
    }

    explicit ReadHolder(RWSpinLock &lock) : lock_(&lock) {
      lock_->lock_shared();
    }

    ReadHolder(ReadHolder &&other) noexcept : lock_(other.lock_) {
      other.lock_ = nullptr;
    }

    // down-grade
    explicit ReadHolder(UpgradedHolder &&upgraded) : lock_(upgraded.lock_) {
      upgraded.lock_ = nullptr;
      if (lock_)
        lock_->unlock_upgrade_and_lock_shared();
    }

    explicit ReadHolder(WriteHolder &&writer) : lock_(writer.lock_) {
      writer.lock_ = nullptr;
      if (lock_)
        lock_->unlock_and_lock_shared();
    }

    ReadHolder &operator=(ReadHolder &&other) {
      using std::swap;
      swap(lock_, other.lock_);
      return *this;
    }

    ReadHolder(const ReadHolder &other) = delete;
    ReadHolder &operator=(const ReadHolder &other) = delete;

    ~ReadHolder() {
      if (lock_)
        lock_->unlock_shared();
    }

    void reset(RWSpinLock *lock = nullptr) {
      if (lock == lock_)
        return;
      if (lock_)
        lock_->unlock_shared();
      lock_ = lock;
      if (lock_)
        lock_->lock_shared();
    }

    void swap(ReadHolder *other) { std::swap(lock_, other->lock_); }

  private:
    friend class UpgradedHolder;
    friend class WriteHolder;
    RWSpinLock *lock_;
  };

  class UpgradedHolder {
  public:
    explicit UpgradedHolder(RWSpinLock *lock) : lock_(lock) {
      if (lock_)
        lock_->lock_upgrade();
    }

    explicit UpgradedHolder(RWSpinLock &lock) : lock_(&lock) {
      lock_->lock_upgrade();
    }

    explicit UpgradedHolder(WriteHolder &&writer) {
      lock_ = writer.lock_;
      writer.lock_ = nullptr;
      if (lock_)
        lock_->unlock_and_lock_upgrade();
    }

    UpgradedHolder(UpgradedHolder &&other) noexcept : lock_(other.lock_) {
      other.lock_ = nullptr;
    }

    UpgradedHolder &operator=(UpgradedHolder &&other) {
      using std::swap;
      swap(lock_, other.lock_);
      return *this;
    }

    UpgradedHolder(const UpgradedHolder &other) = delete;
    UpgradedHolder &operator=(const UpgradedHolder &other) = delete;

    ~UpgradedHolder() {
      if (lock_)
        lock_->unlock_upgrade();
    }

    void reset(RWSpinLock *lock = nullptr) {
      if (lock == lock_)
        return;
      if (lock_)
        lock_->unlock_upgrade();
      lock_ = lock;
      if (lock_)
        lock_->lock_upgrade();
    }

    void swap(UpgradedHolder *other) {
      using std::swap;
      swap(lock_, other->lock_);
    }

  private:
    friend class WriteHolder;
    friend class ReadHolder;
    RWSpinLock *lock_;
  };

  class WriteHolder {
  public:
    explicit WriteHolder(RWSpinLock *lock) : lock_(lock) {
      if (lock_)
        lock_->lock();
    }

    explicit WriteHolder(RWSpinLock &lock) : lock_(&lock) { lock_->lock(); }

    // promoted from an upgrade lock holder
    explicit WriteHolder(UpgradedHolder &&upgraded) {
      lock_ = upgraded.lock_;
      upgraded.lock_ = nullptr;
      if (lock_)
        lock_->unlock_upgrade_and_lock();
    }

    WriteHolder(WriteHolder &&other) noexcept : lock_(other.lock_) {
      other.lock_ = nullptr;
    }

    WriteHolder &operator=(WriteHolder &&other) {
      using std::swap;
      swap(lock_, other.lock_);
      return *this;
    }

    WriteHolder(const WriteHolder &other) = delete;
    WriteHolder &operator=(const WriteHolder &other) = delete;

    ~WriteHolder() {
      if (lock_)
        lock_->unlock();
    }

    void reset(RWSpinLock *lock = nullptr) {
      if (lock == lock_)
        return;
      if (lock_)
        lock_->unlock();
      lock_ = lock;
      if (lock_)
        lock_->lock();
    }

    void swap(WriteHolder *other) {
      using std::swap;
      swap(lock_, other->lock_);
    }

  private:
    friend class ReadHolder;
    friend class UpgradedHolder;
    RWSpinLock *lock_;
  };

private:
  std::atomic<int32_t> bits_;
};

#ifdef RW_SPINLOCK_USE_X86_INTRINSIC_
// A more balanced Read-Write spin lock implemented based on GCC intrinsics.

namespace detail {
template <size_t kBitWidth> struct RWTicketIntTrait {
  static_assert(kBitWidth == 32 || kBitWidth == 64,
                "bit width has to be either 32 or 64 ");
};

template <> struct RWTicketIntTrait<64> {
  typedef uint64_t FullInt;
  typedef uint32_t HalfInt;
  typedef uint16_t QuarterInt;

#ifdef RW_SPINLOCK_USE_SSE_INSTRUCTIONS_
  static __m128i make128(const uint16_t v[4]) {
    return _mm_set_epi16(0, 0, 0, 0, short(v[3]), short(v[2]), short(v[1]),
                         short(v[0]));
  }
  static inline __m128i fromInteger(uint64_t from) {
    return _mm_cvtsi64_si128(int64_t(from));
  }
  static inline uint64_t toInteger(__m128i in) {
    return uint64_t(_mm_cvtsi128_si64(in));
  }
  static inline uint64_t addParallel(__m128i in, __m128i kDelta) {
    return toInteger(_mm_add_epi16(in, kDelta));
  }
#endif
};

template <> struct RWTicketIntTrait<32> {
  typedef uint32_t FullInt;
  typedef uint16_t HalfInt;
  typedef uint8_t QuarterInt;

#ifdef RW_SPINLOCK_USE_SSE_INSTRUCTIONS_
  static __m128i make128(const uint8_t v[4]) {
    return _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, char(v[3]),
                        char(v[2]), char(v[1]), char(v[0]));
  }
  static inline __m128i fromInteger(uint32_t from) {
    return _mm_cvtsi32_si128(int32_t(from));
  }
  static inline uint32_t toInteger(__m128i in) {
    return uint32_t(_mm_cvtsi128_si32(in));
  }
  static inline uint32_t addParallel(__m128i in, __m128i kDelta) {
    return toInteger(_mm_add_epi8(in, kDelta));
  }
#endif
};
} // namespace detail

template <size_t kBitWidth, bool kFavorWriter = false> class RWTicketSpinLockT {
  typedef detail::RWTicketIntTrait<kBitWidth> IntTraitType;
  typedef typename detail::RWTicketIntTrait<kBitWidth>::FullInt FullInt;
  typedef typename detail::RWTicketIntTrait<kBitWidth>::HalfInt HalfInt;
  typedef typename detail::RWTicketIntTrait<kBitWidth>::QuarterInt QuarterInt;

  union RWTicket {
    constexpr RWTicket() : whole(0) {}
    FullInt whole;
    HalfInt readWrite;
    __extension__ struct {
      QuarterInt write;
      QuarterInt read;
      QuarterInt users;
    };
  } ticket;

private: // Some x64-specific utilities for atomic access to ticket.
  template <class T> static T load_acquire(T *addr) {
    T t = *addr; // acquire barrier
    asm_volatile_memory();
    return t;
  }

  template <class T> static void store_release(T *addr, T v) {
    asm_volatile_memory();
    *addr = v; // release barrier
  }

public:
  constexpr RWTicketSpinLockT() {}

  RWTicketSpinLockT(RWTicketSpinLockT const &) = delete;
  RWTicketSpinLockT &operator=(RWTicketSpinLockT const &) = delete;

  void lock() {
    if (kFavorWriter) {
      writeLockAggressive();
    } else {
      writeLockNice();
    }
  }

  /*
   * Both try_lock and try_lock_shared diverge in our implementation from the
   * lock algorithm described in the link above.
   *
   * In the read case, it is undesirable that the readers could wait
   * for another reader (before increasing ticket.read in the other
   * implementation).  Our approach gives up on
   * first-come-first-serve, but our benchmarks showed improve
   * performance for both readers and writers under heavily contended
   * cases, particularly when the number of threads exceeds the number
   * of logical CPUs.
   *
   * We have writeLockAggressive() using the original implementation
   * for a writer, which gives some advantage to the writer over the
   * readers---for that path it is guaranteed that the writer will
   * acquire the lock after all the existing readers exit.
   */
  bool try_lock() {
    RWTicket t;
    FullInt old = t.whole = load_acquire(&ticket.whole);
    if (t.users != t.write)
      return false;
    ++t.users;
    return __sync_bool_compare_and_swap(&ticket.whole, old, t.whole);
  }

  /*
   * Call this if you want to prioritize writer to avoid starvation.
   * Unlike writeLockNice, immediately acquires the write lock when
   * the existing readers (arriving before the writer) finish their
   * turns.
   */
  void writeLockAggressive() {
    // std::this_thread::yield() is needed here to avoid a pathology if the
    // number
    // of threads attempting concurrent writes is >= the number of real
    // cores allocated to this process. This is less likely than the
    // corresponding situation in lock_shared(), but we still want to
    // avoid it
    int count = 0;
    QuarterInt val = __sync_fetch_and_add(&ticket.users, 1);
    while (val != load_acquire(&ticket.write)) {
      asm_volatile_pause();
      if (UNLIKELY(++count > 1000))
        std::this_thread::yield();
    }
  }

  // Call this when the writer should be nicer to the readers.
  void writeLockNice() {
    // Here it doesn't cpu-relax the writer.
    //
    // This is because usually we have many more readers than the
    // writers, so the writer has less chance to get the lock when
    // there are a lot of competing readers.  The aggressive spinning
    // can help to avoid starving writers.
    //
    // We don't worry about std::this_thread::yield() here because the caller
    // has already explicitly abandoned fairness.
    while (!try_lock()) {
    }
  }

  // Atomically unlock the write-lock from writer and acquire the read-lock.
  void unlock_and_lock_shared() {
    QuarterInt val = __sync_fetch_and_add(&ticket.read, 1);
  }

  // Release writer permission on the lock.
  void unlock() {
    RWTicket t;
    t.whole = load_acquire(&ticket.whole);
    FullInt old = t.whole;

#ifdef RW_SPINLOCK_USE_SSE_INSTRUCTIONS_
    // SSE2 can reduce the lock and unlock overhead by 10%
    static const QuarterInt kDeltaBuf[4] = {1, 1, 0, 0}; // write/read/user
    static const __m128i kDelta = IntTraitType::make128(kDeltaBuf);
    __m128i m = IntTraitType::fromInteger(old);
    t.whole = IntTraitType::addParallel(m, kDelta);
#else
    ++t.read;
    ++t.write;
#endif
    store_release(&ticket.readWrite, t.readWrite);
  }

  void lock_shared() {
    // std::this_thread::yield() is important here because we can't grab the
    // shared lock if there is a pending writeLockAggressive, so we
    // need to let threads that already have a shared lock complete
    int count = 0;
    while (!LIKELY(try_lock_shared())) {
      asm_volatile_pause();
      if (UNLIKELY((++count & 1023) == 0))
        std::this_thread::yield();
    }
  }

  bool try_lock_shared() {
    RWTicket t, old;
    old.whole = t.whole = load_acquire(&ticket.whole);
    old.users = old.read;
#ifdef RW_SPINLOCK_USE_SSE_INSTRUCTIONS_
    // SSE2 may reduce the total lock and unlock overhead by 10%
    static const QuarterInt kDeltaBuf[4] = {0, 1, 1, 0}; // write/read/user
    static const __m128i kDelta = IntTraitType::make128(kDeltaBuf);
    __m128i m = IntTraitType::fromInteger(old.whole);
    t.whole = IntTraitType::addParallel(m, kDelta);
#else
    ++t.read;
    ++t.users;
#endif
    return __sync_bool_compare_and_swap(&ticket.whole, old.whole, t.whole);
  }

  void unlock_shared() {
    QuarterInt val = __sync_fetch_and_add(&ticket.write, 1);
  }

  class WriteHolder;

  typedef RWTicketSpinLockT<kBitWidth, kFavorWriter> RWSpinLock;
  class ReadHolder {
  public:
    ReadHolder(ReadHolder const &) = delete;
    ReadHolder &operator=(ReadHolder const &) = delete;

    explicit ReadHolder(RWSpinLock *lock) : lock_(lock) {
      if (lock_)
        lock_->lock_shared();
    }

    explicit ReadHolder(RWSpinLock &lock) : lock_(&lock) {
      if (lock_)
        lock_->lock_shared();
    }

    // atomically unlock the write-lock from writer and acquire the read-lock
    explicit ReadHolder(WriteHolder *writer) : lock_(nullptr) {
      std::swap(this->lock_, writer->lock_);
      if (lock_) {
        lock_->unlock_and_lock_shared();
      }
    }

    ~ReadHolder() {
      if (lock_)
        lock_->unlock_shared();
    }

    void reset(RWSpinLock *lock = nullptr) {
      if (lock_)
        lock_->unlock_shared();
      lock_ = lock;
      if (lock_)
        lock_->lock_shared();
    }

    void swap(ReadHolder *other) { std::swap(this->lock_, other->lock_); }

  private:
    RWSpinLock *lock_;
  };

  class WriteHolder {
  public:
    WriteHolder(WriteHolder const &) = delete;
    WriteHolder &operator=(WriteHolder const &) = delete;

    explicit WriteHolder(RWSpinLock *lock) : lock_(lock) {
      if (lock_)
        lock_->lock();
    }
    explicit WriteHolder(RWSpinLock &lock) : lock_(&lock) {
      if (lock_)
        lock_->lock();
    }

    ~WriteHolder() {
      if (lock_)
        lock_->unlock();
    }

    void reset(RWSpinLock *lock = nullptr) {
      if (lock == lock_)
        return;
      if (lock_)
        lock_->unlock();
      lock_ = lock;
      if (lock_)
        lock_->lock();
    }

    void swap(WriteHolder *other) { std::swap(this->lock_, other->lock_); }

  private:
    friend class ReadHolder;
    RWSpinLock *lock_;
  };
};

typedef RWTicketSpinLockT<32> RWTicketSpinLock32;
typedef RWTicketSpinLockT<64> RWTicketSpinLock64;

#endif // RW_SPINLOCK_USE_X86_INTRINSIC_

} // namespace folly

#endif // VIYA_UTIL_RWLOCK_H_

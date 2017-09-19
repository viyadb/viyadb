#ifndef VIYA_UTIL_BITSET_H_
#define VIYA_UTIL_BITSET_H_

#include <type_traits>
#include <roaring64map.hh>

template<int SizeBytes>
class Bitset {
  using RoaringType = typename std::conditional<SizeBytes == 8, Roaring64Map, Roaring>::type;
  using NumType = typename std::conditional<SizeBytes == 8, uint64_t, uint32_t>::type;

  public:
    Bitset():cardinality_(0L) {}

    /**
     * Add element if it doesn't exist yet. Otherwise, cached cardinality will be wrong!
     */
    void add(NumType num) {
      roaring_.add(num);
      if (cardinality_ != 0) {
        cardinality_++;
      }
    }

    NumType cardinality() {
      if (cardinality_ == 0L) {
        cardinality_ = roaring_.cardinality();
      }
      return cardinality_;
    }

    bool contains(NumType num) {
      return roaring_.contains(num);
    }

    Bitset& operator|=(const Bitset& other) {
      roaring_ |= other.roaring_;
      cardinality_ = 0L;
      return *this;
    }

    void optimize() {
      roaring_.runOptimize();
    }

  private:
    NumType cardinality_;
    RoaringType roaring_;
};

#endif // VIYA_UTIL_BITSET_H_

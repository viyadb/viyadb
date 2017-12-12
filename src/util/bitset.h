/*
 * Copyright (c) 2017 ViyaDB Group
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

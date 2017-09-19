#ifndef VIYA_DB_DICTIONARY_H_
#define VIYA_DB_DICTIONARY_H_

#include <vector>
#include <unordered_map>
#include "util/rwlock.h"
#include "db/column.h"

namespace viya {
namespace db {

template<typename V>
class DictImpl: public std::unordered_map<std::string, V> {
  public:
    DictImpl(const std::string& exceeded_value) {
      this->insert(std::make_pair(exceeded_value, 0));
    }
};

class DimensionDict {
  public:
    DimensionDict(const NumericType& code_type);
    ~DimensionDict();

    void* v2c() const { return v2c_; }
    folly::RWSpinLock& lock() { return lock_; }
    std::vector<std::string>& c2v() { return c2v_; }

    AnyNum Decode(const std::string& value);

  private:
    const NumericType& code_type_;
    folly::RWSpinLock lock_;
    std::vector<std::string> c2v_; // code to value
    void* v2c_;                    // value to code
};

class Dictionaries {
  public:
    Dictionaries() {}
    ~Dictionaries();

    DimensionDict* GetOrCreate(const std::string& dim_name, const NumericType& code_type);

  private:
    std::unordered_map<std::string, DimensionDict*> dicts_;
    folly::RWSpinLock lock_;
};

}}

#endif // VIYA_DB_DICTIONARY_H_

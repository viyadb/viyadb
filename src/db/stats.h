#ifndef VIYA_DB_STATS_H_
#define VIYA_DB_STATS_H_

namespace viya {
namespace db {

class UpsertStats {
  public:
    UpsertStats():new_recs(0) {}

    size_t new_recs;
};

}}

#endif // VIYA_DB_STATS_H_

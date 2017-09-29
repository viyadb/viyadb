#include "codegen/db/store.h"
#include "db/database.h"
#include "db/table.h"
#include "db/store.h"

namespace viya {
namespace db {

namespace cg = viya::codegen;

SegmentStore::SegmentStore(Database& database, Table& table) {
  create_segment_ = cg::CreateSegment(database.compiler(), table).Function();
}

SegmentStore::~SegmentStore() {
  for (auto s : segments_) {
    delete s;
  }
}

}}

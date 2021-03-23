/// Copyright 2016 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.


#include "common/segment_utils.h"

#include <string>

#include "folly/String.h"

const uint32_t kShardLength = 5;

namespace common {

std::string SegmentToDbName(const std::string& segment,
                            const int shard_id) {
  return folly::stringPrintf("%s%05d", segment.c_str(), shard_id);
}

std::string DbNameToSegment(const std::string& db_name) {
  std::string segment;
  std::string version;
  DbNameToSegmentAndVersion(db_name, &segment, &version, "");
  return segment;
}

void DbNameToSegmentAndVersion(const std::string& db_name, std::string* seg,
                               std::string* v, const std::string v_deli) {
  if (db_name.size() <= kShardLength) {
    *seg = db_name;
    return;
  }
  if (!v_deli.empty()) {
    auto iter = db_name.rfind(v_deli);
    if (iter != std::string::npos) {
      *seg = db_name.substr(0, iter);
      *v = db_name.substr(
          iter + v_deli.size(),
          db_name.size() - seg->size() - v_deli.size() - kShardLength);
      return;
    }
  }
  *seg = db_name.substr(0, db_name.size() - kShardLength);
}

int ExtractShardId(const std::string& db_name) {
  if (UNLIKELY(db_name.size() < kShardLength)) {
    return -1;
  }

  try {
    return folly::to<int>(db_name.substr(db_name.size() - kShardLength));
  } catch (...) {
    return -1;
  }
}

std::string DbNameToHelixPartitionName(const std::string& db_name) {
  return DbNameToSegment(db_name) + '_' + std::to_string(ExtractShardId(db_name));
}

}  // namespace common

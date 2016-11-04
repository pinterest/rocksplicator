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


#include "rocksdb_admin/utils.h"

#include <string>

#include "folly/String.h"

namespace admin {

std::string SegmentToDbName(const std::string& segment,
                            const int shard_id) {
  return folly::stringPrintf("%s%05d", segment.c_str(), shard_id);
}

std::string DbNameToSegment(const std::string& db_name) {
  if (db_name.size() <= 5) {
    return db_name;
  }
  return db_name.substr(0, db_name.size() - 5);
}

}  // namespace admin

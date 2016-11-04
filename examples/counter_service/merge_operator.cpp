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

//
// @author bol (bol@pinterest.com)
//

#include "examples/counter_service/merge_operator.h"

namespace counter {

bool CounterMergeOperator::Merge(const rocksdb::Slice& key,
                                  const rocksdb::Slice* existing_value,
                                  const rocksdb::Slice& value,
                                  std::string* new_value,
                                  rocksdb::Logger* logger) const {
  if (existing_value == nullptr) {
    *new_value = value.ToString();
    return true;
  }

  if (existing_value->size() != sizeof(int64_t) ||
      value.size() != sizeof(int64_t)) {
    return false;
  }

  int64_t old_counter;
  int64_t counter;
  memcpy(&old_counter, existing_value->data(), sizeof(old_counter));
  memcpy(&counter, value.data(), sizeof(counter));
  counter += old_counter;
  new_value->assign(reinterpret_cast<const char*>(&counter), sizeof(counter));
  return true;
}

}  // namespace counter

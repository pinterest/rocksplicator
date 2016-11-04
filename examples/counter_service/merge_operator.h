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

#pragma once

#include <string>

#include "rocksdb/merge_operator.h"

namespace counter {

// Gives the client a way to express the read -> modify -> write semantics
// key:           (IN) The key that's associated with this merge operation.
// existing_value:(IN) null indicates the key does not exist before this op
// value:         (IN) the value to update/merge the existing_value with
// new_value:    (OUT) Client is responsible for filling the merge result
// here. The string that new_value is pointing to will be empty.
// logger:        (IN) Client could use this to log errors during merge.
//
// Return true on success.
// All values passed in will be client-specific values. So if this method
// returns false, it is because client specified bad data or there was
// internal corruption. The client should assume that this will be treated
// as an error by the library.
class CounterMergeOperator : public rocksdb::AssociativeMergeOperator {
 public:
  bool Merge(const rocksdb::Slice& key,
             const rocksdb::Slice* existing_value,
             const rocksdb::Slice& value,
             std::string* new_value,
             rocksdb::Logger* logger) const override;

  const char* Name() const override {
    return "Counter merge operator";
  }
};

}  // namespace counter

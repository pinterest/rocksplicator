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

#include "rocksdb_replicator/fast_read_map.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

namespace replicator {

class ReplicatorHandler : public ReplicatorSvIf {
 public:
  using DBMapType = detail::FastReadMap<std::string,
    std::shared_ptr<RocksDBReplicator::ReplicatedDB>>;

  explicit ReplicatorHandler(DBMapType* db_map) : db_map_(db_map) {}

  void async_tm_replicate(
      std::unique_ptr<apache::thrift::HandlerCallback<
        std::unique_ptr<ReplicateResponse>>> callback,
      std::unique_ptr<ReplicateRequest> request) override;

 private:
  DBMapType* db_map_;
};

}  // namespace replicator

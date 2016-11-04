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

#include "rocksdb_replicator/replicator_handler.h"

namespace replicator {

void ReplicatorHandler::async_eb_replicate(
    std::unique_ptr<apache::thrift::HandlerCallback<
      std::unique_ptr<ReplicateResponse>>> callback,
    std::unique_ptr<ReplicateRequest> request) {
  std::shared_ptr<RocksDBReplicator::ReplicatedDB> db;
  if (!db_map_->get(request->db_name, &db)) {
    ReplicateException e;
    e.code = ErrorCode::SOURCE_NOT_FOUND;
    e.msg = "could not find " + request->db_name;
    callback->exception(e);
    return;
  }

  db->handleReplicateRequest(std::move(callback), std::move(request));
}

}  // namespace replicator

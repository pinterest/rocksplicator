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

#pragma once

#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"
#include "rocksdb_replicator/rocksdb_replicator.h"

namespace replicator {  // namespace replicator

const char* ReplicaRoleString(ReplicaRole role) {
  switch(role) {
    case ReplicaRole::NOOP:
        return "NOOP";
    case ReplicaRole::FOLLOWER:
        return "FOLLOWER";
    case ReplicaRole::LEADER:
        return "LEADER" ;

    default:
        return "__unknown_role__";
  }
}

const char* ReturnCodeString(ReturnCode code) {
  switch(code) {
    case ReturnCode::OK:
        return "OK";
    case ReturnCode::DB_NOT_FOUND:
        return "DB_NOT_FOUND";
    case ReturnCode::DB_PRE_EXIST:
        return "DB_PRE_EXIST" ;
    case ReturnCode::WRITE_TO_SLAVE:
        return "WRITE_TO_SLAVE" ;
    case ReturnCode::WRITE_ERROR:
        return "WRITE_ERROR" ;
    case ReturnCode::WAIT_SLAVE_TIMEOUT:
        return "WAIT_SLAVE_TIMEOUT" ;

    default:
        return "__unknown_code__";
  }
}

}  // namespace replicator

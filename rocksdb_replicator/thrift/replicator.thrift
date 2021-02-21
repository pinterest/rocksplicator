# Copyright 2016 Pinterest Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is the interface of a thrift server internally used by the RocksDB
# replication library. The library will solely use it to pull RocksDB updates
# from upstream. The interface is not exposed to any clients.

namespace cpp2 replicator

struct ReplicateRequest {
  # The largest sequence number currently in the local DB. Request for updates
  # of sequence (seq_no + 1) and larger
  1: required i64 seq_no,

  # The name of the db replicating from
  2: required binary db_name,

  # If the requested data is available, the server will reply immediately.
  # Otherwise, it will wait for this amount of time before replying with an
  # empty response.
  # A value of 0 means no wait at all.
  3: required i32 max_wait_ms,

  # A server may return more than one update for a request. max_updates is the
  # uppper limit set by client side.
  # A value of 0 means no limit
  4: required i32 max_updates,
}

typedef binary (cpp.type = "folly::IOBuf") IOBuf
struct Update {
  # The raw data for this update, which may be applied to slaves.
  # Use folly::IOBuf to ensure zero copy for raw_data during serialization and
  # deserialization
  1: required IOBuf raw_data,

  # When this update was first applied to the Master.
  # A value of 0 means it is unavilable
  2: required i64 timestamp,
}

struct ReplicateResponse {
  # updates is an ordered continuous range of updates starting from the seq_no
  # specified in ReplicateRequest.
  1: required list<Update> updates,
}

enum ErrorCode {
  OTHER = 0,
  SOURCE_NOT_FOUND = 1, # could not find the upstream db
  SOURCE_READ_ERROR = 2,
  SOURCE_REMOVED = 3, # leader db has moved to a different instance
}

exception ReplicateException {
  1: required string msg,
  2: required ErrorCode code,
}

service Replicator {
  ReplicateResponse replicate(1:ReplicateRequest request)
      throws (1:ReplicateException e)
}

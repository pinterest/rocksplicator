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

# This is the interface for a real time counter service

include "rocksdb_admin/rocksdb_admin.thrift"

namespace cpp2 counter

enum ErrorCode {
  OTHER = 0,
  DB_NOT_FOUND = 1,
  ROCKSDB_ERROR = 2,
  CORRUPTED_DATA = 3,
  SERVER_NOT_FOUND = 4,
}

exception CounterException {
  1: required string msg,
  2: required ErrorCode code,
}

struct GetRequest {
  1: required string counter_name,
  2: optional string segment = "default",
  3: optional bool need_routing = 1,
}

struct GetResponse {
  1: required i64 counter_value,
}

struct SetRequest {
  1: required string counter_name,
  2: required i64 counter_value,
  3: optional string segment = "default",
  4: optional bool need_routing = 1,
}

struct SetResponse {
  # for future use
}

struct BumpRequest {
  1: required string counter_name,
  2: required i64 counter_delta,
  3: optional string segment = "default",
  4: optional bool need_routing = 1,
}

struct BumpResponse {
  # for future use
}


service Counter extends rocksdb_admin.Admin {
  GetResponse getCounter(1: GetRequest request)
      throws (1: CounterException e)

  SetResponse setCounter(1: SetRequest request)
      throws (1: CounterException e)

  BumpResponse bumpCounter(1: BumpRequest request)
      throws (1: CounterException e)
}

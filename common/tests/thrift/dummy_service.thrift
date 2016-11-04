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

namespace cpp2 dummy_service.thrift

enum DummyServiceErrorCode {
  DUMMY_ERROR = 1
}

exception DummyServiceException {
  1: required string message,
  2: required DummyServiceErrorCode errorCode
}

service DummyService {
  void ping()
  i64 getSomething(1:i64 input) throws (1:DummyServiceException e)
}

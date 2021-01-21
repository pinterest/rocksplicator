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

namespace java com.pinterest.rocksplicator.thrift.commons.io

enum CompressionAlgorithm {
  UNCOMPRESSED = 0,
  SNAPPY = 1,
  GZIP = 2
}

enum SerializationProtocol {
  BINARY = 1
  COMPACT = 2
}

struct WrappedData {
  1: required binary data,
  2: required binary serialization_protocol,
  3: required binary compression_algorithm,
}
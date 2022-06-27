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

#include <string>

#include "folly/ExceptionWrapper.h"
#include "folly/Range.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace admin {

template <typename T>
bool DecodeThriftStruct(const std::string& data, T* obj) {
  auto ex = folly::try_and_catch<std::exception>([data, obj]() {
    apache::thrift::CompactSerializer::deserialize(folly::StringPiece(data.data(), data.size()),
                                                   *obj);
  });
  ex.with_exception([](std::exception& e) { LOG(ERROR) << e.what(); });
  return !ex;
}

template <typename T>
bool EncodeThriftStruct(const T& obj, std::string* data) {
  data->clear();
  auto ex = folly::try_and_catch<std::exception>(
      [obj, data]() { apache::thrift::CompactSerializer::serialize(obj, data); });
  ex.with_exception([](std::exception& e) { LOG(ERROR) << e.what(); });
  return !ex;
}

// Will remove and create a directory
// returns true if any of these operations failed.
bool ClearAndCreateDir(std::string dir_path, std::string* err);


}  // namespace admin

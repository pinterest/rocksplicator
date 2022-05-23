/// Copyright 2019 Pinterest Inc.
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
#include <thread>

#include "folly/executors/thread_factory/ThreadFactory.h"
#include "folly/system/ThreadName.h"

namespace common {

class IdenticalNameThreadFactory : public folly::ThreadFactory {
 public:
  explicit IdenticalNameThreadFactory(const std::string& name)
    : name_(name) {}

  std::thread newThread(folly::Func&& func) override {
    auto thread = std::thread(std::move(func));
    folly::setThreadName(thread.native_handle(), name_);

    return thread;
  }

 private:
  const std::string name_;
};

}  // namespace common

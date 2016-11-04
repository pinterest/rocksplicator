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

#include "examples/counter_service/thrift/gen-cpp2/Counter.h"
#include "common/thrift_router.h"

namespace counter {

class CounterRouter {
 public:
  explicit CounterRouter(
      const std::string& local_az, const std::string& filename) :
    router_(local_az, filename, common::parseConfig) {}

  std::string GetDBName(const std::string& segment, const std::string& key);

  void GetClientsFor(const std::string& segment,
                     const std::string& key,
                     const bool for_read,
                     std::vector<std::shared_ptr<CounterAsyncClient>>* clients);

 private:
  common::ThriftRouter<CounterAsyncClient> router_;
};

}  // namespace counter

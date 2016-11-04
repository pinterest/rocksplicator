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
// @author shu (shu@pinterest.com)
//

#include "examples/counter_service/counter_router.h"

namespace {

uint32_t ShardId(const std::string& key, const uint32_t num_shards) {
  int hash_code = 0;
  for (size_t i = 0; i < key.size(); i++) {
    hash_code = 31 * hash_code + key[i];
  }

  return abs(hash_code % num_shards);
}

}  // anonymous namespace

namespace counter {

std::string CounterRouter::GetDBName(const std::string& segment,
                                     const std::string& key) {
  auto num_shards = router_.getShardNumberFor(segment);
  if (num_shards <= 0) {
    return "";
  }

  return folly::stringPrintf("%s%05d", segment.c_str(),
                             ShardId(key, num_shards));
}

void CounterRouter::GetClientsFor(
    const std::string& segment,
    const std::string& key,
    const bool for_read,
    std::vector<std::shared_ptr<CounterAsyncClient>>* clients) {
  clients->clear();

  auto num_shards = router_.getShardNumberFor(segment);
  if (num_shards <= 0) {
    return;
  }

  using RouterType = common::ThriftRouter<CounterAsyncClient>;
  router_.getClientsFor(
    segment,
    for_read ? RouterType::Role::ANY : RouterType::Role::MASTER,
    for_read ? RouterType::Quantity::ONE : RouterType::Quantity::ALL,
    ShardId(key, num_shards),
    clients);
}

}  // namespace counter


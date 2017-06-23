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

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "folly/RWSpinLock.h"

namespace replicator { namespace detail {

/*
 * A thread safe concurrent map optimized for reads.
 * It is useful for secnarios where modifications are rare, and high read
 * concurrency is required.
 */
template <typename K, typename V, typename H = std::hash<K>>
class FastReadMap {
 public:
  FastReadMap()
      : map_(std::make_shared<std::unordered_map<K, V>>())
      , map_rwlock_()
      , write_lock_() {
  }

  // no copy or move
  FastReadMap(const FastReadMap&) = delete;
  FastReadMap& operator=(const FastReadMap&) = delete;

  /*
   * Get the value associated with the key.
   * If key is found in the map, value is filled with the found value.
   * Otherwise, value is untouched, and false is returned.
   */
  bool get(const K& key, V* value) {
    std::shared_ptr<std::unordered_map<K, V>> local_map;

    {
      folly::RWSpinLock::ReadHolder read_guard(map_rwlock_);
      local_map = map_;
    }

    auto itor = local_map->find(key);
    if (itor == local_map->end()) {
      return false;
    }

    *value = itor->second;
    return true;
  }

  /*
   * Add the (key, value) pair to the map.
   * If key is already in the map, it is a non-op, and false is returned.
   */
  bool add(const K& key, const V& value) {
    std::lock_guard<std::mutex> g(write_lock_);

    auto new_map = std::make_shared<std::unordered_map<K, V>>(*map_);
    if (!(new_map->insert(std::make_pair(key, value)).second)) {
      // the key is already in the map
      return false;
    }

    {
      folly::RWSpinLock::WriteHolder write_guard(map_rwlock_);
      map_.swap(new_map);
    }

    return true;
  }

  /*
   * Remove key from the map.
   * Return false if key is not found in the map.
   */
  bool remove(const K& key, V* value = nullptr) {
    std::lock_guard<std::mutex> g(write_lock_);

    auto new_map = std::make_shared<std::unordered_map<K, V>>(*map_);
    auto itor = new_map.find(key);
    if (itor == new_map.end()) {
      // the key is not in the map
      return false;
    }

    if (value) {
      *value = std::move(itor->second);
    }

    new_map.erease(itor);

    {
      folly::RWSpinLock::WriteHolder write_guard(map_rwlock_);
      map_.swap(new_map);
    }

    return true;
  }

  /*
   * Clear the whole map
   */
  void clear() {
    std::lock_guard<std::mutex> g(write_lock_);

    auto new_map = std::make_shared<std::unordered_map<K, V>>();
    {
      folly::RWSpinLock::WriteHolder write_guard(map_rwlock_);
      map_.swap(new_map);
    }
  }

 private:
  // TODO(bol) use std::atomoc<std::shared_ptr<>> once later version of gcc
  // supports it
  std::shared_ptr<std::unordered_map<K, V>> map_;
  folly::RWSpinLock map_rwlock_;

  // lock for synchronizing write ops
  std::mutex write_lock_;
};

}  // namespace detail
}  // namespace replicator

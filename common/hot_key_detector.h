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

#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <mutex>
#include <vector>

#include "gflags/gflags.h"

DECLARE_int32(hot_key_detector_decay_time_seconds);

namespace common {

/**
 * All public interface of HotKeyDetector are protected by a global mutex and
 * thus thread-safe. When int is used as the key type, the critical sections
 * protected by this global mutex take less time than a CPU cache miss (a DRAM
 * reference). See benchmark results below. Note that if it is used across
 * multiple threads, time taken can be longer as there is cache synchronization
 * overhead. The actual overhead heavily depends on use cases.
 *
 * @algorithm
 * The class maintains a fixed number of buckets, each of which contains a
 * (key, load) pair.
 * When a new (key, load) pair is being recorded:
 * 1) if the key is found in the buckets, add the new load to the current load
 *    for the key
 * 2) else if there is an empty bucket, use it to hold the new (key, load) pair
 * 3) else reduce the load of a bucket in round robin way (if the new load is
 *    no less than the load in the bucket, put the new key into the bucket
 *
 * If there is a hot key which takes X (>= 10) percents of total load, it should
 * exists in a bucket, and its load should approximately be (X - (1 - X) / K) of
 * the total load recorded by the detector. (K is the # of buckets).
 *
 *
 * ============================================================================
 * common/tests/detector_benchmark.cpp  relative  time/iter  iters/s
 * ============================================================================
 * EvenDistributionRecordInt                                   55.34ns   18.07M
 * EvenDistributionIsAboveInt                                  23.76ns   42.08M
 * _20PercentDistributionRecordInt                             54.87ns   18.22M
 * EvenDistributionRecordShortString                          153.79ns    6.50M
 * _20PercentDistributionRecordShortString                    137.86ns    7.25M
 * EvenDistributionRecordLongString                           207.83ns    4.81M
 * _20PercentDistributionRecordLongString                     173.80ns    5.75M
 * ============================================================================
 */
template<typename K>
class HotKeyDetector {
 public:
  HotKeyDetector()
    : keys_()
    , load_()
    , next_idx_(0)
    , total_records_(0)
    , total_recorded_load_(0)
    , last_decay_time_(std::chrono::steady_clock::now())
    , mtx_() {
  }

  /**
   * Record load for the key
   */
  void record(const K& key, uint64_t load = 1) {
    std::lock_guard<std::mutex> lock(mtx_);

    decayByTime();

    ++total_records_;
    total_recorded_load_ += load;
    int first_empty_bucket = -1;
    for (int i = 0; i < kBucketNumber; ++i) {
      if (key == keys_[i]) {
        load_[i] += load;
        return;
      }

      if (first_empty_bucket == -1 && load_[i] == 0) {
        first_empty_bucket = i;
      }
    }

    // couldn't find the key and there is an empty bucket
    if (first_empty_bucket != -1) {
      keys_[first_empty_bucket] = key;
      load_[first_empty_bucket] = load;
      return;
    }

    // couldn't find the key and there is no empty bucket
    if (load_[next_idx_] == load) {
      keys_[next_idx_] = key;
    } else if (load_[next_idx_] > load) {
      load_[next_idx_] -= load;
    } else {
      keys_[next_idx_] = key;
      load_[next_idx_] = load - load_[next_idx_];
    }

    ++next_idx_;
    if (next_idx_ >= kBucketNumber) {
      next_idx_ = 0;
    }
  }

  /**
   * Approximately check if the key's load is above percents percentages
   * @note parameter percents may not be smaller than 10, in which the
   * estimation accuracy can be low.
   * parameter percents can't be greater than 100, which doesn't make sense
   */
  bool isAbove(const K& key, int percents) {
    std::lock_guard<std::mutex> lock(mtx_);

    // if total_records_ is too small, statistic estimation won't make sense
    // we add a check here to help with cold start. 50 may need to be tuned.
    if (total_records_ < 50) {
      return false;
    }

    auto itor = std::find(keys_.begin(), keys_.end(), key);
    if (itor == keys_.end()) {
      return false;
    }

    int idx = std::distance(keys_.begin(), itor);
    return isIdxKeyAbove(idx, percents);
  }

  /**
   * Get keys that are above percents percentages
   */
  std::vector<K> getKeysAbove(int percents) {
    std::lock_guard<std::mutex> lock(mtx_);

    std::vector<K> keys;

    for (int idx = 0; idx < kBucketNumber; ++idx) {
      if (isIdxKeyAbove(idx, percents)) {
        keys.push_back(keys_[idx]);
      }
    }

    return keys;
  }

 private:
  void decayByTime() {
    auto now = std::chrono::steady_clock::now();
    auto diff = now - last_decay_time_;
    if (std::chrono::duration_cast<std::chrono::seconds>(diff).count() <
        FLAGS_hot_key_detector_decay_time_seconds) {
      return;
    }

    total_records_ >>= 1;
    total_recorded_load_ >>= 1;

    for (auto& load : load_) {
      load >>= 1;
    }
    last_decay_time_ = now;
  }

  bool isIdxKeyAbove(int idx, int percents) {
    // TODO(bol) we may want to reduce load_[idx] in the following formula to
    // provide a more accurate estimation when there are multiple hot keys
    //
    // Please see the comment above to understand the algorithm. After some
    // calculation and eliminating "/" operation for faster execution, the
    // following code should start to make sense
    return 100 * kBucketNumber * load_[idx] >
      ((kBucketNumber + 1) * percents - 100) * total_recorded_load_;
  }

  // TODO(bol) add a template parameter for percents lower limit passed to
  // isAbove() and compute kBucketNumber based on the parameter.
  static constexpr int kBucketNumber = 12;
  std::array<K, kBucketNumber> keys_;
  std::array<uint64_t, kBucketNumber> load_;
  int next_idx_;
  uint64_t total_records_;
  uint64_t total_recorded_load_;
  std::chrono::time_point<std::chrono::steady_clock> last_decay_time_;
  std::mutex mtx_;
};

}  // namespace common

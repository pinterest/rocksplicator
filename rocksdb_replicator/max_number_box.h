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

#include <condition_variable>
#include <mutex>
#include <vector>

#include "glog/logging.h"

namespace replicator { namespace detail {

/*
 * MaxNumberBox is a data structure for maintaining the max number it receives
 * through its post() API.
 *
 * It also provides a wait() API, which will block caller until the max number
 * is no less than the number parameter, or timeout_ms has passed.
 *
 * @note All public interface of MaxNumberBox are thread safe.
 */
class MaxNumberBox {
 public:
  /*
   * init_num is the initial number in the box.
   */
  MaxNumberBox(const uint64_t init_num = 0)
    : mtx_()
    , max_number_(init_num)
    , waiters_() {}

  ~MaxNumberBox() {
    // it's client's responsibility to ensure there is no pending calls when the
    // destructor is called.
    CHECK(waiters_.empty());
  }

  // no copy or move
  MaxNumberBox(const MaxNumberBox&) = delete;
  MaxNumberBox& operator=(const MaxNumberBox&) = delete;

  /*
   * Post a number to the box.
   */
  void post(const uint64_t num);

  /*
   * Wait until (num <= the max number *this received) or timeout_ms
   * timeout_ms == 0 means no timeout
   *
   * @return true if (num <= the max number *this received), false if timeout.
   */
  bool wait(const uint64_t num, const uint64_t timeout_ms);

 private:
  struct Waiter {
    uint64_t num_to_wait;
    std::condition_variable cv;
  };

  // mtx_ protects max_number_ and waiters_
  std::mutex mtx_;
  uint64_t max_number_;
  std::vector<Waiter*> waiters_;
};

}  // namespace detail
}  // namespace replicator

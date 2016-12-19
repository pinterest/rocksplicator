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

#include "rocksdb_replicator/max_number_box.h"

#include <algorithm>

#include "folly/Likely.h"

namespace replicator { namespace detail {

void MaxNumberBox::post(const uint64_t num) {
  std::vector<Waiter*> waiters_to_notify;
  std::vector<Waiter*> waiters_to_wait;

  {
    std::unique_lock<std::mutex> lk(mtx_);
    if (num <= max_number_) {
      return;
    }

    max_number_ = num;

    if (waiters_.empty()) {
      return;
    }

    std::partition_copy(waiters_.begin(), waiters_.end(),
                        std::back_inserter(waiters_to_notify),
                        std::back_inserter(waiters_to_wait),
                        [this] (Waiter* w) {
                          return w->num_to_wait <= this->max_number_;
                        });

    waiters_.swap(waiters_to_wait);
  }

  // We don't want to hold mtx_ when calling notify_one(). Because that could
  // make pending threads wake up and go to sleep immediately due to we are
  // still holding mtx_.
  // This could make a pending thread return true from wait() or wait_for() due
  // to spurious wakeup or timeout. This is benign. Because we use the pred versions
  // of wait() and wait_for(). Also concurrent calling notify_one() is safe.
  std::for_each(waiters_to_notify.begin(), waiters_to_notify.end(),
                [] (Waiter* w) { w->cv.notify_one(); });
}

bool MaxNumberBox::wait(const uint64_t num, const uint64_t timeout_ms) {
  // We use thread local waiter because 1) avoid constructor and destructor
  // overhead of Waiter::cv; 2) It is possible that the thread calling wait()
  // returns from wait() before the thread calling post() calls notify_one()
  // on Waiter::cv.
  thread_local Waiter me;

  std::unique_lock<std::mutex> lk(mtx_);
  if (UNLIKELY(num <= max_number_)) {
    return true;
  }

  me.num_to_wait = num;
  waiters_.push_back(&me);

  if (timeout_ms == 0) {
    me.cv.wait(lk, [this, num] { return num <= this->max_number_; });
    return true;
  }


  if (me.cv.wait_for(lk,
                     std::chrono::milliseconds(timeout_ms),
                     [this, num] { return num <= this->max_number_; })) {
    return true;
  }

  // We are timeouted if we are here.
  // If it is timeout. wait_for() will acquire the lock and recheck pred. So
  // it is safe to assert that we must still be in waiters_, and that
  // num > max_number_.
  // waiters_ usually has a few to at most a few hundreds elements. So we use
  // std::vector instead of list to make code simpler and achieve better cache
  // locality. Also timeout shouldn't be on the performance critical path.
  auto new_end = std::remove(waiters_.begin(), waiters_.end(), &me);
  // TODO (bol) change it to DCHECK/assert() once it's stable.
  CHECK(num > max_number_ && std::distance(new_end, waiters_.end()) == 1);
  waiters_.erase(new_end, waiters_.end());
  return false;
}

}  // namespace detail
}  // namespace replicator


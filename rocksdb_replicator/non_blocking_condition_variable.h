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
#include <functional>
#include <memory>
#include <mutex>

#include "folly/Executor.h"
#include "folly/futures/Future.h"

namespace replicator { namespace detail {

/*
 * NonBlockingConditionVariable is designed for the scenario that a large number
 * of tasks need to run on some conditions. Semantically we can use one thread
 * for each task, and let every thread wait on the condition variable for the
 * event it waits. Since the # of tasks can be too big, we can't afford that
 * many of threads. NonBlockingConditionVariable allows tasks to be run on
 * a executor passing in through its constructor. So the # of threads can be
 * much smaller than the # of tasks.
 */
class NonBlockingConditionVariable {
 private:
  struct Task {
    template <typename Func>
    explicit Task(Func&& f)
        : func(std::move(f))
        , next()
        , has_done(false) {
    }

    bool should_i_run() {
      return !has_done.exchange(true);
    }

    std::function<void()> func;
    std::shared_ptr<Task> next;
    std::atomic<bool> has_done;
  };

 public:
  // executor must outlive *this and the time of the last call of
  // runIfConditionOrWaitForNotify + the corresponding timeout_ms
  // This shouldn't be that inconvenient, because we expect executor
  // is long living.
  explicit NonBlockingConditionVariable(folly::Executor* executor)
      : tasks_()
      , tasks_mutex_()
      , executor_(executor) {
  }

  // no copy or move
  NonBlockingConditionVariable(const NonBlockingConditionVariable&) = delete;
  NonBlockingConditionVariable& operator=(
      const NonBlockingConditionVariable&) = delete;

  // run f() in the executor if p() is true. Otherwise, put f into the pending
  // task list, which will be scheduled to run later.
  //
  // f() will run in the executor exactly once in one of the four possible
  // conditions.
  // 1. If p() is true, run it immediately.
  // 2. If p() is false, and notifyAll() is called later before timeout_ms.
  // 3. If timeout_ms has passed.
  // 4. If none of aboves happen before the destructor of *this is called.
  template <typename Func, typename Predicate>
  void runIfConditionOrWaitForNotify(Func f, Predicate p, uint64_t timeout_ms) {
    if (p()) {
      executor_->add(std::move(f));
      return;
    }

    auto task = std::make_shared<Task>(std::move(f));

    // add task to the pending task list
    {
      // TODO(bol) use CAS operation of std::atomic<std::shared_ptr<>> when
      // move to gcc 5.1
      std::lock_guard<std::mutex> g(tasks_mutex_);
      task->next = std::move(tasks_);
      tasks_ = task;
    }

    // we need to recheck the the condition in case missing a notification
    if (p() && task->should_i_run()) {
      executor_->add(std::move(task->func));
      return;
    }

    if (timeout_ms > 0) {
      // Put a weak_ptr instead of shared_ptr in the timeout lambda. Otherwise,
      // we may accumulate some unnecessary tasks if they have run before the
      // timeout.
      std::weak_ptr<Task> weak_task(task);
#if __GNUC__ >= 8
      auto future = folly::futures::sleepUnsafe(std::chrono::milliseconds(timeout_ms));
#else
      auto future = folly::futures::sleep(std::chrono::milliseconds(timeout_ms));
#endif
      std::move(future).then([weak_task = std::move(weak_task), executor = executor_] (folly::Try<folly::Unit>&& t) {
          auto task = weak_task.lock();
          if (task && task->should_i_run()) {
            executor->add(std::move(task->func));
          }
        });
    }
  }

  // put all pending tasks to be run in the executor.
  //
  void notifyAll() {
    std::shared_ptr<Task> local_tasks;

    {
      // TODO(bol) use atomic_exchange() once we move to gcc 5.1
      std::lock_guard<std::mutex> g(tasks_mutex_);
      tasks_.swap(local_tasks);
    }

    runATaskList(std::move(local_tasks));
  }

  ~NonBlockingConditionVariable() {
    // no need to do any synchronizations, because we are in the destructor,
    // and thus no others are working on *this. Otherwise, there is a bug in
    // the client side code.
    runATaskList(std::move(tasks_));
  }

 private:
  void runATaskList(std::shared_ptr<Task> tasks) {
    while (tasks) {
      if (tasks->should_i_run()) {
        executor_->add(std::move(tasks->func));
      }

      tasks = std::move(tasks->next);
    }
  }

  // TODO(bol) replacing them with std::atomic<std::shared_ptr<>> once we
  // upgrade to gcc 5.1
  std::shared_ptr<Task> tasks_;
  std::mutex tasks_mutex_;

  folly::Executor* const executor_;
};

}  // namespace detail
}  // namespace replicator

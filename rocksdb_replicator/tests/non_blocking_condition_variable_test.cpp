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

#include <atomic>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "rocksdb_replicator/non_blocking_condition_variable.h"
#include "wangle/concurrent/CPUThreadPoolExecutor.h"

using replicator::detail::NonBlockingConditionVariable;
using std::atomic;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::thread;
using std::this_thread::sleep_for;
using std::unique_ptr;
using std::vector;
using wangle::BlockingQueue;
using wangle::LifoSemMPMCQueue;
using CPUTask = wangle::CPUThreadPoolExecutor::CPUTask;

unique_ptr<BlockingQueue<CPUTask>> queue =
  std::make_unique<LifoSemMPMCQueue<CPUTask>>(1 << 18);
wangle::CPUThreadPoolExecutor g_executor(8, std::move(queue));
const int kPauseTimeMs = 250;

TEST(NonBlockingConditionVariableTest, Basics) {
  NonBlockingConditionVariable variable(&g_executor);
  bool flag = false;
  atomic<int> counter(0);
  auto predicate = [&flag] {
    return flag;
  };
  auto func = [&counter] {
    ++counter;
  };

  // trigger by notifyAll()
  variable.runIfConditionOrWaitForNotify(func, predicate, 0);
  EXPECT_EQ(counter, 0);
  variable.notifyAll();
  sleep_for(milliseconds(kPauseTimeMs));
  EXPECT_EQ(counter, 1);

  // trigger by timeout
  variable.runIfConditionOrWaitForNotify(func, predicate, kPauseTimeMs / 2);
  EXPECT_EQ(counter, 1);
  sleep_for(milliseconds(kPauseTimeMs));
  EXPECT_EQ(counter, 2);
  variable.notifyAll();

  // trigger immediately by predicator
  flag = true;
  variable.runIfConditionOrWaitForNotify(func, predicate, 0);
  sleep_for(milliseconds(kPauseTimeMs));
  EXPECT_EQ(counter, 3);

  // multiple tasks
  flag = false;
  variable.runIfConditionOrWaitForNotify(func, predicate, 0);
  variable.runIfConditionOrWaitForNotify(func, predicate, kPauseTimeMs / 2);
  variable.runIfConditionOrWaitForNotify(func, predicate, kPauseTimeMs / 2);
  EXPECT_EQ(counter, 3);
  flag = true;
  variable.runIfConditionOrWaitForNotify(func, predicate, 0);
  sleep_for(milliseconds(kPauseTimeMs / 4));
  EXPECT_EQ(counter, 4);
  sleep_for(milliseconds(kPauseTimeMs));
  EXPECT_EQ(counter, 6);
  sleep_for(milliseconds(kPauseTimeMs));
  EXPECT_EQ(counter, 6);

  // trigger by destructor
  variable.~NonBlockingConditionVariable();
  sleep_for(milliseconds(kPauseTimeMs));
  EXPECT_EQ(counter, 7);
}

void stress(int n_notify_threads, int n_add_threads,
            int notify_interval_ms, int n_tasks_per_thread) {
  NonBlockingConditionVariable cond_var(&g_executor);
  atomic<int> counter(0);
  atomic<bool> keep_notify(true);

  vector<thread> notify_threads(n_notify_threads);
  for (int i = 0; i < n_notify_threads; ++i) {
    notify_threads[i] = thread([&cond_var, &keep_notify, notify_interval_ms] {
        while (keep_notify.load()) {
          cond_var.notifyAll();
          sleep_for(milliseconds(notify_interval_ms));
        }
      });
  }

  vector<thread> add_threads(n_add_threads);
  for (int i = 0; i < n_add_threads; ++i) {
    add_threads[i] = thread([&cond_var, &counter, n_tasks_per_thread,
                             notify_interval_ms] {
        bool flag = false;
        auto predicate = [&flag] {
          return flag;
        };

        for (int i = 0; i < n_tasks_per_thread; ++i) {
          cond_var.runIfConditionOrWaitForNotify(
              [&counter] { counter.fetch_add(1); },
              predicate,
              i & 1 ? notify_interval_ms / 2 : 0);
          flag = !flag;
        }
      });
  }

  for (auto& t : add_threads) {
    t.join();
  }

  keep_notify.store(false);

  for (auto& t : notify_threads) {
    t.join();
  }

  cond_var.~NonBlockingConditionVariable();
  sleep_for(seconds(1));
  EXPECT_EQ(counter.load(), n_tasks_per_thread * n_add_threads);
}

TEST(NonBlockingConditionVariableTest, Stress) {
  stress(1, 1, 4, 100000);
  stress(10, 2, 4, 100000);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


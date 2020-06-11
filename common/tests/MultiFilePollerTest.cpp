/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/MultiFilePoller.h"
#include <future>
#include <mutex>

#include <folly/futures/Future.h>
#include <folly/FileUtil.h>
#include <folly/MapUtil.h>
#include <folly/String.h>
#include <folly/experimental/TestUtil.h>
#include <folly/portability/GTest.h>

using namespace common;
using namespace folly::test;

// Fine to set below 1_s because we have a padding of kWriteWaitMs.
static const std::chrono::milliseconds kPollIntervalMs{200};

// FilePoller's min granularity.
static const std::chrono::milliseconds kWriteWaitMs{500};

// Should be a few times larger than kPollInterval.
static const std::chrono::milliseconds kMaxSemaphoreWaitMs{1000};

class MultiFilePollerTest : public testing::Test {
 public:
  void SetUp() override {
    updater_.reset(new MultiFilePoller(kPollIntervalMs));
    tmpdirPath_ = folly::fs::canonical(tmpdir_.path()).string();
  }

  void delayedWrite(const std::string& path, const std::string& data) {
    // The delay makes sure mtime (in granularity of sec) of the modified
    // file is increased by at least 1. Otherwise common::FilePoller may not
    // detect the change.
    folly::makeFuture().delayed(kWriteWaitMs).wait();
    ASSERT_TRUE(folly::writeFile(data, path.c_str()));
  }

 protected:
  std::unique_ptr<MultiFilePoller> updater_;
  std::string tmpdirPath_;
  TemporaryDirectory tmpdir_{"MultiFilePollerTest"};
};

/**
 * BasicTest
 * This test checks basic usage scenario of MultiFilePoller:
 *  (1) registers a callback on one file.
 *  (2) writes data to the file, and expects the callback to be triggered.
 *  (3) cancels the callback, and expects the callback to not run.
 */
TEST_F(MultiFilePollerTest, BasicTest) {
  const std::string f(tmpdirPath_ + "/Basic1"), d1("a"), d2("b"), d3("c");
  size_t count = 0;

  std::promise<bool> promise;
  std::promise<bool> promiseAfterCancellation;

  // Write initial data.
  ASSERT_TRUE(folly::writeFile(d1, f.c_str()));

  // Register the callback.
  auto cbId = updater_->registerFile(
      f, [&](const MultiFilePoller::CallbackArg& newData) {
        auto& content = folly::get_or_throw(newData, f);
        EXPECT_EQ(d2, content);
        EXPECT_EQ(1, ++count);
        if (!promise.get_future().valid()) {
          promise.set_value(true);
        } else if (!promiseAfterCancellation.get_future().valid()) {
          promiseAfterCancellation.set_value(false);
        }
      });

  delayedWrite(f, d2);

  // Check whether the callback is triggered by acquiring the semaphore.
  ASSERT_TRUE(promise.get_future().get());
  ASSERT_EQ(1, count);

  // Cancel the callback.
  updater_->cancelCallback(cbId);

  // Write to the file again. The callback should not run.
  delayedWrite(f, d3);

  ASSERT_TRUE(promiseAfterCancellation.get_future().wait_for(
    kMaxSemaphoreWaitMs) == std::future_status::timeout);

  // If the callback runs, the assertion inside callback will also fail.
}

/**
 * CancellationTest
 * This test tests the basic functionality of callback cancellation.
 */
TEST_F(MultiFilePollerTest, CancellationTest) {
  const std::string f(tmpdirPath_ + "/Cancel1"), d("111");

  auto cb = updater_->registerFile(
      f, [&](const MultiFilePoller::CallbackArg& /* unused */) { FAIL(); });

  // Proper cancellation.
  updater_->cancelCallback(cb);

  // Refuse double cancellation.
  EXPECT_THROW(updater_->cancelCallback(cb), std::out_of_range);

  delayedWrite(f, d);
  // Test will also fail if the callback runs.
}

/**
 * ComplexTest
 * This test constructs a more complex use scenario of MultiFilePoller:
 *  (1) There are three files: f1, f2, and f3.
 *  (2) There are four callbacks:
 *        cb1 = {f1}
 *        cb2 = {f2}
 *        cb3 = {f3, f1} // So f1 is used by cb1 and cb3, but cb3 also use f3.
 *        cb4 = {f2}     // So cb2 and cb4 use and only use f2.
 */
TEST_F(MultiFilePollerTest, ComplexTest) {
  const std::string f1(tmpdirPath_ + "/Complex1"), d1("a"), d11("AA");
  const std::string f2(tmpdirPath_ + "/Complex2"), d2("b"), d21("B"), d22("X");
  const std::string f3(tmpdirPath_ + "/Complex3"), d3("c");

  std::string data2, data4;
  std::vector<std::string> data3;
  size_t count1 = 0, count2 = 0, count3 = 0, count4 = 0;

  std::promise<bool> promise1;
  std::promise<bool> promise1_cancelled;
  std::promise<bool> promise21;
  std::promise<bool> promise22;
  std::promise<bool> promise2_cancelled;
  std::promise<bool> promise31;
  std::promise<bool> promise32;
  std::promise<bool> promise33;
  std::promise<bool> promise41;
  std::promise<bool> promise42;
  std::promise<bool> promise4_cancelled;

  // cb1 is only triggered once. It expects the content to equal d1, which is
  // written to the file to trigger the callback.
  auto cb1 = updater_->registerFile(
      f1, [&](const MultiFilePoller::CallbackArg& newData) {
        auto& content = folly::get_or_throw(newData, f1);
        EXPECT_EQ(d1, content);
        EXPECT_EQ(1, ++count1); // Fail if run more than once.
      promise1.set_value(true);
      if (!promise1.get_future().valid()) {
        promise1.set_value(true);
      } else if (!promise1_cancelled.get_future().valid()) {
        // This should never get called
        promise1_cancelled.set_value(false);
      }
    });

  // cb2 copies content of f2 from arg to data2, and increments count2.
  auto cb2 = updater_->registerFile(
      f2, [&](const MultiFilePoller::CallbackArg& newData) {
        data2 = folly::get_or_throw(newData, f2);
        count2++;
        if (!promise21.get_future().valid()) {
          promise21.set_value(true);
        } else if (!promise22.get_future().valid()) {
          promise22.set_value(true);
        } else if (!promise2_cancelled.get_future().valid()) {
          // This should never get called
          promise2_cancelled.set_value(false);
        }
      });

  // cb3 concatenates the data of f3 and f3 and writes the value to data3.
  /* cb3 */ updater_->registerFiles(
      {f3, f1}, [&](const MultiFilePoller::CallbackArg& newData) {
        if (count3 == 0) {
          // When we write to f3 to trigger cb3, f1 does not exist yet.
          // Check that f1 is not in the map.
          ASSERT_EQ(newData.end(), newData.find(f1));
          promise31.set_value(true);
        } else {
          // Otherwise f1 must exist in the map.
          ASSERT_NE(newData.end(), newData.find(f1));
          if (!promise32.get_future().valid()) {
            promise32.set_value(true);
          } else if (!promise33.get_future().valid()) {
            promise33.set_value(true);
          }
        }
        const auto& f3Data = folly::get_or_throw(newData, f3);
        const auto& f1Data = folly::get_default(newData, f1, "<NODATA>");
        data3 = {f3Data, f1Data};
        count3++;
      });

  // Create f2 to trigger cb2.
  ASSERT_TRUE(folly::writeFile(d2, f2.c_str()));
  ASSERT_TRUE(promise21.get_future().get());

  EXPECT_EQ(1, count2); // +1.
  EXPECT_EQ(0, count1); // No change.
  EXPECT_EQ(0, count3); // No change.
  EXPECT_EQ(d2, data2); // The data obtained by cb should equal what's written.

  // Create f3 to trigger cb3. Note that cb1 should not run.
  ASSERT_TRUE(folly::writeFile(d3, f3.c_str()));
  ASSERT_TRUE(promise31.get_future().get());
  EXPECT_EQ(1, count3); // +1.
  EXPECT_EQ(0, count1); // No change.
  EXPECT_EQ(1, count2); // No change.
  EXPECT_EQ(std::vector<std::string>({d3, "<NODATA>"}), data3);

  // Create f1 to trigger cb3 and cb1. Order doesn't matter.
  ASSERT_TRUE(folly::writeFile(d1, f1.c_str()));
  ASSERT_TRUE(promise1.get_future().get());

  EXPECT_EQ(1, count1); // +1.
  EXPECT_EQ(1, count2); // No change.
  ASSERT_TRUE(promise32.get_future().get());
  EXPECT_EQ(2, count3);              // +1.
  EXPECT_EQ(std::vector<std::string>({d3, d1}), data3);

  // cb4 is the same as cb2 except that it's another callback.
  auto cb4 = updater_->registerFile(
      f2, [&](const MultiFilePoller::CallbackArg& newData) {
        data4 = folly::get_or_throw(newData, f2);
        count4++;
        if (!promise41.get_future().valid()) {
          promise41.set_value(true);
        } else if (!promise42.get_future().valid()) {
          promise42.set_value(true);
        } else if (!promise4_cancelled.get_future().valid()) {
          promise4_cancelled.set_value(false);
        }
      });

  // Write to f2 to trigger second callback.
  delayedWrite(f2, d21);

  ASSERT_TRUE(promise22.get_future().get());
  ASSERT_TRUE(promise41.get_future().get());

  EXPECT_EQ(2, count2);  // +1.
  EXPECT_EQ(1, count4);  // +1.
  EXPECT_EQ(1, count1);  // No change.
  EXPECT_EQ(2, count3);  // No change.
  EXPECT_EQ(d21, data2); // Both data2 and data4 got what was written.
  EXPECT_EQ(d21, data4);

  // f1 is in two different callbacks. Cancel cb1 should not affect cb3.
  updater_->cancelCallback(cb1);
  ASSERT_TRUE(folly::writeFile(d11, f1.c_str())); // Last write to f1 > 1s ago.
  ASSERT_TRUE(promise33.get_future().get());
  ASSERT_TRUE(promise1_cancelled.get_future().wait_for(
    kMaxSemaphoreWaitMs) == std::future_status::timeout);

  EXPECT_EQ(3, count3); // +1.
  EXPECT_EQ(1, count1); // No change.
  EXPECT_EQ(2, count2); // No change.
  EXPECT_EQ(1, count4); // No change.
  EXPECT_EQ(std::vector<std::string>({d3, d11}), data3);

  // cb2 and cb4 use and only use f2. Cancel cb2 should not affect cb4.
  updater_->cancelCallback(cb2);
  ASSERT_TRUE(folly::writeFile(d22, f2.c_str())); // Last write to f2 > 1s ago.
  ASSERT_TRUE(promise42.get_future().get());
  ASSERT_TRUE(promise2_cancelled.get_future().wait_for(
    kMaxSemaphoreWaitMs) == std::future_status::timeout);

  EXPECT_EQ(d21, data2); // cb2 should not run, so not updated.
  EXPECT_EQ(d22, data4); // cb4 should run, so updated to d22.
  EXPECT_EQ(2, count2);  // No change.
  EXPECT_EQ(2, count4);  // +1

  // Now we cancel cb4. Record of f2 should be cleaned up.
  updater_->cancelCallback(cb4);
  ASSERT_TRUE(folly::writeFile(d1, f2.c_str()));
  ASSERT_TRUE(promise4_cancelled.get_future().wait_for(
    kMaxSemaphoreWaitMs) == std::future_status::timeout);

  EXPECT_EQ(d21, data2); // cb2 should not run, so not updated to d1.
  EXPECT_EQ(d22, data4); // cb4 should not run, so not updated to d1.
}

int main(int argc, char** argv) {
  // FLAGS_recheck_removed_file_interval_ms = 500;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
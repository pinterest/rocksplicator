/// Copyright 2018 Pinterest Inc.
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

#include "common/future_util.h"

#include <atomic>
#include <chrono>
#include <functional>

#include "gtest/gtest.h"

using folly::Duration;
using folly::Future;
using folly::makeFuture;
using common::GetSpeculativeFuture;
using std::atomic;
using std::chrono::milliseconds;
using std::function;
using std::runtime_error;
using std::this_thread::sleep_for;

namespace {

class FutureUtilTest : public ::testing::Test {
 public:
  function<Future<int>()> getBackupFutureFuncSuccess(int value,
                                                     Duration delay) {
    return [this, value, delay] { ++this->backup_fired_;
      return makeFuture(value).delayed(delay);
    };
  }

  function<Future<int>()> getBackupFutureFuncFailure(Duration delay) {
    return [this, delay] { ++this->backup_fired_;
      return makeFuture<int>(runtime_error("backup_error")).delayed(delay);
    };
  }

 protected:
  void SetUp() { backup_fired_.store(0); }

  static void TearDownTestCase() {
    // Sleep a little to make sure all outstanding futures are timed out.
    // Otherwise, prematurely destroying the HHWheelTimer thread causes an
    // exception.
    sleep_for(milliseconds(1000));
  }

  atomic<int> backup_fired_;
};

TEST_F(FutureUtilTest, NoBackupFiredTest) {
  // No backup fired since primary finishes before speculative timeout.
  EXPECT_EQ(1, GetSpeculativeFuture(
                   makeFuture(1).delayed(milliseconds(20)),
                   getBackupFutureFuncSuccess(2, milliseconds(30)),
                   milliseconds(50)).get());
  EXPECT_EQ(0, backup_fired_.load());
}

TEST_F(FutureUtilTest, BackupFiredPrimaryWins) {
  // Both futures succeed with primary future finishing first.
  EXPECT_EQ(1, GetSpeculativeFuture(
                   makeFuture(1).delayed(milliseconds(100)),
                   getBackupFutureFuncSuccess(2, milliseconds(100)),
                   milliseconds(50)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, BackupFiredBackupWins) {
  // Both futures succeed with backup future finishing first.
  EXPECT_EQ(2, GetSpeculativeFuture(
                   makeFuture(1).delayed(milliseconds(1000)),
                   getBackupFutureFuncSuccess(2, milliseconds(30)),
                   milliseconds(50)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, PrimaryFailedBeforeSpeculativeTimeout) {
  // Primary future fails before speculative timeout is hit.
  EXPECT_EQ(2, GetSpeculativeFuture(
                   makeFuture<int>(runtime_error("primary_error"))
                       .delayed(milliseconds(20)),
                   getBackupFutureFuncSuccess(2, milliseconds(40)),
                   milliseconds(50)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest,
       PrimaryFailedAfterSpeculativeTimeoutBeforeBackupSuccess) {
  // Primary future fails after speculative timeout is hit and backup future
  // finishes after the primary fails.
  EXPECT_EQ(2, GetSpeculativeFuture(
                   makeFuture<int>(runtime_error("primary_error"))
                       .delayed(milliseconds(70)),
                   getBackupFutureFuncSuccess(2, milliseconds(50)),
                   milliseconds(50)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, PrimaryFailedAfterSpeculativeTimeoutAfterBackupSuccess) {
  // Primary future fails after speculative timeout is hit and backup future
  // finishes before the primary fails.
  EXPECT_EQ(2, GetSpeculativeFuture(
                   makeFuture<int>(runtime_error("primary_error"))
                       .delayed(milliseconds(80)),
                   getBackupFutureFuncSuccess(2, milliseconds(50)),
                   milliseconds(10)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, BackupFailedAfterPrimarySuccess) {
  // Backup future fails after primary succeeds.
  EXPECT_EQ(1, GetSpeculativeFuture(
                   makeFuture(1).delayed(milliseconds(70)),
                   getBackupFutureFuncFailure(milliseconds(50)),
                   milliseconds(50)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, BackupFailedBeforePrimarySuccess) {
  // Backup future fails before primary succeeds.
  EXPECT_EQ(1, GetSpeculativeFuture(
                   makeFuture(1).delayed(milliseconds(100)),
                   getBackupFutureFuncFailure(milliseconds(20)),
                   milliseconds(50)).get());
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, BothFailedWithPrimaryFirst) {
  // Both futures fail but primary fails first. The backup future is fired
  // before the primary future fails.
  bool error = false;
  try {
    GetSpeculativeFuture(makeFuture<int>(runtime_error("primary_error"))
                             .delayed(milliseconds(70)),
                         getBackupFutureFuncFailure(milliseconds(50)),
                         milliseconds(50)).get();
  }
  catch (runtime_error const & e) {
    ASSERT_STREQ("primary_error", e.what());
    error = true;
  }
  EXPECT_TRUE(error);
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, BothFailedWithBackupFirst) {
  // The backup future fails before the primary future fails. Backup future
  // is fired before primary fails.
  bool error = false;
  try {
    GetSpeculativeFuture(makeFuture<int>(runtime_error("primary_error"))
                             .delayed(milliseconds(1000)),
                         getBackupFutureFuncFailure(milliseconds(50)),
                         milliseconds(10)).get();
  }
  catch (runtime_error const & e) {
    ASSERT_STREQ("primary_error", e.what());
    error = true;
  }
  EXPECT_TRUE(error);
  EXPECT_EQ(1, backup_fired_.load());
}

TEST_F(FutureUtilTest, BothFailedWithPrimaryBeforeSpeculativeCall) {
  // Primary future fails first before speculative exec is kicked in.
  // The backup is fired and also fails.
  bool error = false;
  try {
    GetSpeculativeFuture(makeFuture<int>(runtime_error("primary_error"))
                             .delayed(milliseconds(20)),
                         getBackupFutureFuncFailure(milliseconds(50)),
                         milliseconds(50)).get();
  }
  catch (runtime_error const & e) {
    ASSERT_STREQ("primary_error", e.what());
    error = true;
  }
  EXPECT_TRUE(error);
  EXPECT_EQ(1, backup_fired_.load());
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

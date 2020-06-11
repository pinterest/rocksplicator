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
 *
 * Changes and modifications in this file
 * Copyright 2020 Pinterest Inc.
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
 *
 * This file is a copied and subsequently modified version of file from
 * Facebook's wangle library from gitsha: 639938b92547030232c42ceef3eba70d027de3c0
 * hosted at: https://github.com/facebook/wangle
 *
 * Details of modification (very minor modification to make it work in current
 * versions of wangle/folly/rocksplicator dependencies)
 * 1. Reason for copying it from wangle, rather then use if from wangle directly
 * is because in rocksplicator, currently we are pinned to wangle and folly
 * versions which are quite old and these files are only available in newer
 * version of wangle.
 *
 * 2. The file has been migrated to namespace common, instead of it's original
 * wangle namespace, since other applications that uses rocksplicator could
 * potentially be on newer version of wangle/folly and this might result
 * in linking error.
 *
 * 3. When we are ready to upgrade rocksplicator to use newer version of wangle
 * /folly, we can keep these file and make migration over to wangle's version
 * of file in separate changes.
 *
 * 4. Some of the tests were modified to remove usage of functionality not available
 * in current version of folly/wangle as used by rocksplicator, without removing
 * any functional test.
 */

#include "common/FilePoller.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <future>

#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Singleton.h>
#include <folly/experimental/TestUtil.h>
#include <folly/portability/GTest.h>
#include <folly/portability/SysStat.h>
#include <glog/logging.h>

using namespace common;
using namespace folly;
using namespace folly::test;
using namespace std::chrono;

using FileTime = FilePoller::FileTime;

class FilePollerTest : public testing::Test {
 public:
  void createFile() { File(tmpFile, O_CREAT); }

  TemporaryDirectory tmpDir;
  fs::path tmpFilePath{tmpDir.path() / "file-poller"};
  std::string tmpFile{tmpFilePath.string()};
};

void updateModifiedTime(
    const std::string& path,
    bool forward = true,
    nanoseconds timeDiffNano = seconds(10)) {
  struct stat currentFileStat;
  std::array<struct timespec, 2> newTimes;

  if (stat(path.c_str(), &currentFileStat) < 0) {
    throw std::runtime_error("Failed to stat file: " + path);
  }

  newTimes[0] = currentFileStat.st_atim;
  newTimes[1] = currentFileStat.st_mtim;

  auto secVal = duration_cast<seconds>(timeDiffNano).count();
  auto nsecVal = timeDiffNano.count();
  if (forward) {
    newTimes[1].tv_sec += secVal;
    newTimes[1].tv_nsec += nsecVal;
  } else {
    newTimes[1].tv_sec -= secVal;
    newTimes[1].tv_nsec -= nsecVal;
  }
  // 0 <= tv_nsec < 1e9
  newTimes[1].tv_nsec %= (long)1e9;
  if (newTimes[1].tv_nsec < 0) {
    newTimes[1].tv_nsec *= -1;
  }

  if (utimensat(AT_FDCWD, path.c_str(), newTimes.data(), 0) < 0) {
    throw std::runtime_error("Failed to set time for file: " + path);
  }
}

TEST_F(FilePollerTest, TestUpdateFile) {
  createFile();
  std::promise<bool> promise;
  FilePoller poller(milliseconds(1));
  poller.addFileToTrack(tmpFile, [&]() {
    promise.set_value(true);
  });
  updateModifiedTime(tmpFile);
  ASSERT_TRUE(promise.get_future().get());
}

TEST_F(FilePollerTest, TestUpdateFileSubSecond) {
  createFile();
  std::promise<bool> promise;
  FilePoller poller(milliseconds(1));
  poller.addFileToTrack(tmpFile, [&]() {
    promise.set_value(true);
  });
  updateModifiedTime(tmpFile, true, milliseconds(10));
  ASSERT_TRUE(promise.get_future().get());
}

TEST_F(FilePollerTest, TestUpdateFileBackwards) {
  createFile();
  std::promise<bool> promise;
  FilePoller poller(milliseconds(1));
  poller.addFileToTrack(tmpFile, [&]() {
    promise.set_value(true);
  });
  updateModifiedTime(tmpFile, false);
  ASSERT_TRUE(promise.get_future().get());
}

TEST_F(FilePollerTest, TestCreateFile) {
  std::promise<bool> promise;
  createFile();
  PCHECK(remove(tmpFile.c_str()) == 0);
  FilePoller poller(milliseconds(1));
  poller.addFileToTrack(tmpFile, [&]() {
    promise.set_value(true);
  });
  File(creat(tmpFile.c_str(), O_RDONLY));
  ASSERT_TRUE(promise.get_future().get());
}

TEST_F(FilePollerTest, TestDeleteFile) {
  std::promise<bool> promise;
  createFile();
  FilePoller poller(milliseconds(1));
  poller.addFileToTrack(tmpFile, [&]() {
    promise.set_value(true);
  });
  PCHECK(remove(tmpFile.c_str()) == 0);

  ASSERT_TRUE(
    promise.get_future().wait_for(seconds(1)) == std::future_status::timeout);
}

struct UpdateSyncState {
  std::mutex m;
  std::condition_variable cv;
  bool updated{false};

  void updateTriggered() {
    std::unique_lock<std::mutex> lk(m);
    updated = true;
    cv.notify_one();
  }

  void waitForUpdate(bool expect = true) {
    std::unique_lock<std::mutex> lk(m);
    cv.wait_for(lk, seconds(1), [&] { return updated; });
    ASSERT_EQ(updated, expect);
    updated = false;
  }
};

class TestFile {
 public:
  TestFile(bool exists, FileTime modTime)
      : exists_(exists), modTime_(modTime) {}

  void update(bool e, FileTime t) {
    std::unique_lock<std::mutex> lk(m);
    exists_ = e;
    modTime_ = t;
  }

  FilePoller::FileModificationData toFileModData() {
    std::unique_lock<std::mutex> lk(m);
    return FilePoller::FileModificationData(exists_, modTime_);
  }

  const std::string name{"fakeFile"};
 private:
  bool exists_{false};
  FileTime modTime_;
  std::mutex m;

};

class NoDiskPoller : public FilePoller {
 public:
  explicit NoDiskPoller(TestFile& testFile)
      : FilePoller(milliseconds(10)), testFile_(testFile) {}

 protected:
  FilePoller::FileModificationData
  getFileModData(const std::string& path) noexcept override {
    EXPECT_EQ(path, testFile_.name);
    return testFile_.toFileModData();
  }

 private:
  TestFile& testFile_;
};

struct PollerWithState {
  explicit PollerWithState(TestFile& testFile) {
    poller = std::make_unique<NoDiskPoller>(testFile);
    poller->addFileToTrack(testFile.name, [&] {
      state.updateTriggered();
    });
  }

  void waitForUpdate(bool expect = true) {
    ASSERT_NO_FATAL_FAILURE(state.waitForUpdate(expect));
  }

  std::unique_ptr<FilePoller> poller;
  UpdateSyncState state;
};

TEST_F(FilePollerTest, TestTwoUpdatesAndDelete) {
  TestFile testFile(true, FileTime(seconds(1)));
  PollerWithState poller(testFile);

  testFile.update(true, FileTime(seconds(2)));
  ASSERT_NO_FATAL_FAILURE(poller.waitForUpdate());

  testFile.update(true, FileTime(seconds(3)));
  ASSERT_NO_FATAL_FAILURE(poller.waitForUpdate());

  testFile.update(false, FileTime(seconds(0)));
  ASSERT_NO_FATAL_FAILURE(poller.waitForUpdate(false));
}

TEST_F(FilePollerTest, TestFileCreatedLate) {
  TestFile testFile(false,
                    FileTime(seconds(0))); // not created yet
  PollerWithState poller(testFile);
  ASSERT_NO_FATAL_FAILURE(poller.waitForUpdate(false));

  testFile.update(true, FileTime(seconds(1)));
  ASSERT_NO_FATAL_FAILURE(poller.waitForUpdate());
}

TEST_F(FilePollerTest, TestMultiplePollers) {
  TestFile testFile(true, FileTime(seconds(1)));
  PollerWithState p1(testFile);
  PollerWithState p2(testFile);

  testFile.update(true, FileTime(seconds(2)));
  ASSERT_NO_FATAL_FAILURE(p1.waitForUpdate());
  ASSERT_NO_FATAL_FAILURE(p2.waitForUpdate());

  testFile.update(true, FileTime(seconds(1)));
  ASSERT_NO_FATAL_FAILURE(p1.waitForUpdate());
  ASSERT_NO_FATAL_FAILURE(p2.waitForUpdate());

  // clear one of the pollers and make sure the other is still
  // getting them
  p2.poller.reset();
  testFile.update(true, FileTime(seconds(3)));
  ASSERT_NO_FATAL_FAILURE(p1.waitForUpdate());
  ASSERT_NO_FATAL_FAILURE(p2.waitForUpdate(false));
}

TEST(FilePoller, TestFork) {
  TestFile testFile(true, FileTime(seconds(1)));
  PollerWithState p1(testFile);
  testFile.update(true, FileTime(seconds(2)));
  ASSERT_NO_FATAL_FAILURE(p1.waitForUpdate());
  // nuke singleton
  folly::SingletonVault::singleton()->destroyInstances();
  testFile.update(true, FileTime(seconds(3)));
  ASSERT_NO_FATAL_FAILURE(p1.waitForUpdate());
}

int main(int argc, char** argv) {
  // FLAGS_recheck_removed_file_interval_ms = 500;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

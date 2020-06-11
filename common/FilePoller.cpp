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

#include <atomic>

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/Singleton.h>
#include <sys/stat.h>


namespace common {

namespace {
// Class that manages poller Ids and a function scheduler.  The function
// scheduler will keep running if a FilePoller is hanging on to it, even
// if this context is destroyed.
class PollerContext {
 public:
  PollerContext() : nextPollerId(1) {
    scheduler = std::make_shared<folly::FunctionScheduler>();
    scheduler->setThreadName("file-poller");
    scheduler->start();
  }

  const std::shared_ptr<folly::FunctionScheduler>& getScheduler() {
    return scheduler;
  }

  uint64_t getNextId() {
    return nextPollerId++;
  }

 private:
  std::shared_ptr<folly::FunctionScheduler> scheduler;
  std::atomic<uint64_t> nextPollerId;
};

folly::Singleton<PollerContext> contextSingleton([] {
  return new PollerContext();
});
}

bool* FilePoller::ThreadProtector::polling() {
  static thread_local bool sPolling{false};
  return &sPolling;
}


constexpr std::chrono::milliseconds FilePoller::kDefaultPollInterval;

FilePoller::FilePoller(std::chrono::milliseconds pollInterval) {
  init(pollInterval);
}

FilePoller::~FilePoller() { stop(); }

void FilePoller::init(std::chrono::milliseconds pollInterval) {
  auto context = contextSingleton.try_get();
  if (!context) {
    LOG(ERROR) << "Poller context requested after destruction.";
    return;
  }
  pollerId_ = context->getNextId();
  scheduler_ = context->getScheduler();
  scheduler_->addFunction(
      [this] { this->checkFiles(); },
      pollInterval,
      folly::to<std::string>(pollerId_));
}

void FilePoller::stop() {
  if (scheduler_) {
    scheduler_->cancelFunction(
        folly::to<std::string>(pollerId_));
  }
}

void FilePoller::checkFiles() noexcept {
  std::lock_guard<std::mutex> lg(filesMutex_);
  ThreadProtector tp;
  for (auto& fData : fileDatum_) {
    auto modData = getFileModData(fData.first);
    auto& fileData = fData.second;
    if (fileData.condition(fileData.modData, modData) && fileData.yCob) {
      fileData.yCob();
    } else if (fileData.nCob) {
      fileData.nCob();
    }
    fileData.modData = modData;
  }
}

void FilePoller::initFileData(
    const std::string& fName,
    FileData& fData) noexcept {
  auto modData = getFileModData(fName);
  fData.modData.exists = modData.exists;
  fData.modData.modTime = modData.modTime;
}

void FilePoller::addFileToTrack(
    const std::string& fileName,
    Cob yCob,
    Cob nCob,
    Condition condition) {
  if (fileName.empty()) {
    // ignore empty file paths
    return;
  }
  if (ThreadProtector::inPollerThread()) {
    LOG(ERROR) << "Adding files from a callback is disallowed";
    return;
  }
  std::lock_guard<std::mutex> lg(filesMutex_);
  fileDatum_[fileName] = FileData(yCob, nCob, condition);
  initFileData(fileName, fileDatum_[fileName]);
}

void FilePoller::removeFileToTrack(const std::string& fileName) {
  if (fileName.empty()) {
    // ignore
    return;
  }
  if (ThreadProtector::inPollerThread()) {
    LOG(ERROR) << "Adding files from a callback is disallowed";
    return;
  }
  std::lock_guard<std::mutex> lg(filesMutex_);
  fileDatum_.erase(fileName);
}

FilePoller::FileModificationData FilePoller::getFileModData(
    const std::string& path) noexcept {
  struct stat info;
  int ret = stat(path.c_str(), &info);
  if (ret != 0) {
    return FileModificationData{false, std::chrono::system_clock::time_point()};
  }

  auto system_time = std::chrono::system_clock::from_time_t(info.st_mtime);

  // On posix systems we can improve granularity by adding in the
  // nanoseconds portion of the mtime
#ifndef _WIN32
  auto& mtim =
#if defined(__APPLE__) || defined(__FreeBSD__) \
 || (defined(__NetBSD__) && (__NetBSD_Version__ < 6099000000))
      info.st_mtimespec
#else
      info.st_mtim
#endif
      ;

  system_time +=
      std::chrono::duration_cast<std::chrono::system_clock::duration>(
                         std::chrono::nanoseconds(mtim.tv_nsec));
#endif

  return FileModificationData{true, system_time};
}
}

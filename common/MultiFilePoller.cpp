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

#include "common/MultiFilePoller.h"

#include <algorithm>
#include <folly/FileUtil.h>
#include <folly/String.h>

using namespace folly;

namespace common {

MultiFilePoller::MultiFilePoller(std::chrono::milliseconds pollInterval)
    : poller_(pollInterval) {}

size_t MultiFilePoller::getNextCallbackId() {
  size_t ret = lastCallbackId_;
  // Order of callback cancellation is arbitrary, so the next available
  // callbackId might not be lastCallbackId_+1.
  while (++ret != lastCallbackId_) {
    if (idsToCallbacks_.find(ret) == idsToCallbacks_.end()) {
      lastCallbackId_ = ret; // Assume write lock is acquired.
      return ret;
    }
  }
  throw std::runtime_error("Run out of callback ID.");
}

MultiFilePoller::CallbackId MultiFilePoller::registerFile(
    std::string path,
    Callback cb) {
  return registerFiles({std::move(path)}, std::move(cb));
}

MultiFilePoller::CallbackId MultiFilePoller::registerFiles(
    const std::vector<std::string>& paths,
    Callback cb) {
  VLOG(4) << "registerFiles({" << join(", ", paths) << "}, cb=" << &cb << ")";
  if (paths.empty()) {
    throw std::invalid_argument("Argument paths must be non-empty.");
  }
  StringReferences cbPaths;
  SharedMutex::WriteHolder wh(rwlock_);
  auto cbId = getNextCallbackId();
  // Create the bi-directional relation between path and callback.
  for (const auto& path : paths) {
    pathsToCallbackIds_[path].push_back(cbId);
    // Use reference to key of pathsToCallbackIds_ map to avoid duplicates.
    const auto& key = pathsToCallbackIds_.find(path)->first;
    cbPaths.push_back(key);
    poller_.addFileToTrack(key, [=] { onFileUpdated(key); });
  }
  idsToCallbacks_.emplace(cbId,
                          CallbackDetail(std::move(cbPaths), std::move(cb)));
  return MultiFilePoller::CallbackId(cbId);
}

void MultiFilePoller::cancelCallback(const CallbackId& cbId) {
  std::vector<std::string> pathsToErase;
  SharedMutex::WriteHolder wh(rwlock_);

  auto pos = idsToCallbacks_.find(cbId.id_);
  if (pos == idsToCallbacks_.end()) {
    throw std::out_of_range(
        to<std::string>("Callback ", cbId.id_, " not found"));
  }

  // Remove the callback ID from its registered paths.
  for (const auto& path : pos->second.files_) {
    auto& callbackIds = pathsToCallbackIds_[path];
    callbackIds.erase(
        std::remove(callbackIds.begin(), callbackIds.end(), cbId.id_));
    // If the path has no more callbacks, erase it from map.
    if (callbackIds.empty()) {
      poller_.removeFileToTrack(path);
      pathsToErase.emplace_back(path);
    }
  }
  // Remove the callback.
  idsToCallbacks_.erase(cbId.id_);
  // Remove callback-less paths from pathsToCallbackIds_, if any, at last.
  for (const auto& path : pathsToErase) {
    pathsToCallbackIds_.erase(path);
  }
}

void MultiFilePoller::onFileUpdated(const std::string& triggeredPath) {
  VLOG(4) << "onFileUpdated(" << triggeredPath << ").";

  // A temporary read cache. Not worth it making it permanent because
  // files do not change frequently.
  std::unordered_map<std::string, std::string> filePathsToFileContents;
  SharedMutex::ReadHolder rh(rwlock_);

  const auto& callbacks = pathsToCallbackIds_.find(triggeredPath);
  if (callbacks == pathsToCallbackIds_.end()) {
    return;
  }

  for (const auto& cbId : callbacks->second) {
    const auto& cbEnt = idsToCallbacks_.find(cbId);
    // Lazily read all files needed by the callback.
    for (const auto& path : cbEnt->second.files_) {
      if (filePathsToFileContents.find(path) == filePathsToFileContents.end()) {
        std::string data;
        if (readFile(path.get().c_str(), data)) {
          filePathsToFileContents.emplace(path, std::move(data));
        } else {
          VLOG(4) << "Failed to read file " << path.get();
        }
      }
    }
    cbEnt->second.cb_(filePathsToFileContents);
  }
}

} // namespace wangle

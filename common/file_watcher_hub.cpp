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

#include "file_watcher.h"
#include "file_watcher_hub.h"
#include "glog/logging.h"

namespace common {

FileWatcherHub * FileWatcherHub::Instance() {
  static FileWatcherHub instance;
  return &instance;
}

typedef std::pair<std::string, EventObserverPtr> ObserverEventCallbackPair;

bool FileWatcherHub::subscribe(
  std::string fileName,
  std::string observerId,
  EventObserverPtr observerPtr) {
  folly::RWSpinLock::WriteHolder write_gaurd(subscription_lock_);
  auto watcherIter = subscriptions_.find(fileName);
  if (watcherIter == subscriptions_.end()) {
    // Add to the subscription
    IdToObserverMapPtr idToObserverMapPtr = std::make_shared<IdToObserverMap>();
    subscriptions_.insert(std::pair(fileName, idToObserverMapPtr));
    watcherIter = subscriptions_.find(fileName);

    auto obsPtr = watcherIter->second->find(observerId);
    if (obsPtr == watcherIter->second->end()) {
      watcherIter->second->insert(ObserverEventCallbackPair(observerId, observerPtr));
    }

    CHECK(common::FileWatcher::Instance()->AddFile(
      fileName,
      [fileName = fileName, this] (std::string content) {
        folly::RWSpinLock::ReadHolder read_gaurd(subscription_lock_);
        auto mapptr = subscriptions_.find(fileName);
        for (auto it = mapptr->second->begin(); it != mapptr->second->end(); ++it) {
          // Call the update method on event observer.
          it->second->update(content);
        }
      })) << "Failed to add file watcher to " << fileName;
  } else {
    // The file is already being watched...
    auto obsPtr = watcherIter->second->find(observerId);
    if (obsPtr == watcherIter->second->end()) {
      watcherIter->second->insert(ObserverEventCallbackPair(observerId, observerPtr));
    } else {
      return false;
    }
  }
  return true;
}

bool FileWatcherHub::unsubscribe(std::string fileName, std::string observerId) {
  folly::RWSpinLock::WriteHolder write_gaurd(subscription_lock_);
  auto watcherIter = subscriptions_.find(fileName);
  if (watcherIter != subscriptions_.end()) {
    auto obsPtr = watcherIter->second->find(observerId);
    if (obsPtr != watcherIter->second->end()) {
      watcherIter->second->erase(obsPtr);
      if (watcherIter->second->empty()) {
        // Remove watch on the given fileName
        subscriptions_.erase(watcherIter);
        common::FileWatcher::Instance()->RemoveFile(fileName);
      }
    } else {
      // This observerId is not subscribed to watching given fileName.
      return false;
    }
  } else {
    // This given file is not being watched... hence nothing to unsubscribe
    return false;
  }
  return true;
}
} // namespace common


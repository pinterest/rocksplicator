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

#pragma once

#include <string>
#include <memory>
#include <map>

#include "folly/RWSpinLock.h"

namespace common {

class EventObserver {
public:
  virtual void update(const std::string& content) = 0;
};

typedef std::shared_ptr<EventObserver> EventObserverPtr;
typedef std::map<std::string, EventObserverPtr> IdToObserverMap;
typedef std::shared_ptr<IdToObserverMap> IdToObserverMapPtr;

/// A Hub for forwarding notifications from FileWatchers.
/// Multiple Observers can subscribe to watching same file and be notified.
/// Notification for each individual observer happens in sequence in sync
/// fashion in same thread from FileWatcher that processes notification.
///
/// This class must be a singleton class
class FileWatcherHub {
private:
  FileWatcherHub();

public :
  /// Retrieve a singleton instance of FileWatcherhub
  static FileWatcherHub *Instance();

  /// Subscribe to notifications when given file changes.
  /// ObserverId is a globally unique string identifying the subscriber. It is
  /// responsibility of subscribers to make sure it is unique within a given process.
  /// EventObserverPtr is a shared ptr to a function that is called when a FileWatcher
  /// triggers an event on the given file.
  bool subscribe(std::string fileName, std::string observerId, EventObserverPtr observerPtr);

  /// Unsubscribe a given  observerId from watching a given file.
  /// Note: The subscriber must call unsubscribe before it's own destruction,
  /// else it will result in memory leak and possibly dangling pointers and
  /// segmentation faults causing the process to terminate with undefined
  /// behaviour.
  bool unsubscribe(std::string fileName, std::string observerId);

private:
  std::map<std::string, IdToObserverMapPtr> subscriptions_;
  mutable folly::RWSpinLock subscription_lock_;
};
}
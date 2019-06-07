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

//
// @author rajathprasad (rajathprasad@pinterest.com)
//

#include "rocksdb_admin/detail/file_watcher_manager.h"

#include "common/kafka/kafka_broker_file_watcher.h"

namespace admin {
namespace detail {

std::shared_ptr<::kafka::KafkaBrokerFileWatcher>
    FileWatcherManager::getFileWatcher(const std::string& file_path) {
  std::shared_ptr<::kafka::KafkaBrokerFileWatcher> kafka_broker_file_watcher;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto map_iter = path_to_watcher_map_.find(file_path);

    if (map_iter == path_to_watcher_map_.end()) {
      // Create a new file watcher.
      kafka_broker_file_watcher =
          std::make_shared<::kafka::KafkaBrokerFileWatcher>(file_path);
      path_to_watcher_map_[file_path] =
          kafka_broker_file_watcher;
    } else {
      kafka_broker_file_watcher = map_iter->second;
    }
  }
  return kafka_broker_file_watcher;
}

FileWatcherManager& FileWatcherManager::getInstance() {
  static FileWatcherManager manager;
  return manager;
}

} //namespace detail
} // namespace admin

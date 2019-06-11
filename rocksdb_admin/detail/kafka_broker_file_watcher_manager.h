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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kafka {
  class KafkaBrokerFileWatcher;
}

namespace admin {
namespace detail {

  // KafkaBrokerFileWatcherManager is a singleton class which manages the kafka
  // broker file watchers for this process.
  class KafkaBrokerFileWatcherManager {
  public:
    std::shared_ptr<::kafka::KafkaBrokerFileWatcher> getFileWatcher(
        const std::string &file_path);

    static KafkaBrokerFileWatcherManager& getInstance();

    KafkaBrokerFileWatcherManager(
        const KafkaBrokerFileWatcherManager&) = delete;
    KafkaBrokerFileWatcherManager& operator=(const
        KafkaBrokerFileWatcherManager&) = delete;
    KafkaBrokerFileWatcherManager(KafkaBrokerFileWatcherManager&&) = delete;
    KafkaBrokerFileWatcherManager& operator=(
        KafkaBrokerFileWatcherManager&&) = delete;

  private:
    KafkaBrokerFileWatcherManager() {};

    std::unordered_map<std::string, std::shared_ptr<
        ::kafka::KafkaBrokerFileWatcher>> path_to_watcher_map_;
    std::mutex mutex_;
  };

} //namespace detail
} // namespace admin

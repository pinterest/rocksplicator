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

#include "common/kafka/kafka_broker_file_watcher.h"

#include <string>
#include <vector>

#include "folly/RWSpinLock.h"
#include "folly/String.h"
#include "glog/logging.h"
#include "common/file_watcher.h"

namespace {

// Return CSV list of broker("host:port") from the broker serverset local file
// content. An empty string is returned if it failed to parse the content.
std::string ParseKafkaBrokerList(const std::string& content) {
  // A CSV list of broker("host:port").
  std::string broker_list;
  // some simple validation on the serverset format
  std::vector<std::string> endpoints;
  folly::split("\n", content, endpoints);
  std::vector<std::string> valid_endpoints;
  for (const auto& endpoint : endpoints) {
    if (endpoint.empty()) {
      continue;
    }
    std::vector<std::string> tokens;
    folly::split(":", endpoint, tokens);
    if (tokens.size() == 2) {
      valid_endpoints.push_back(endpoint);
    } else {
      LOG(ERROR) << "Endpoint is not in the ip:port format: " << endpoint;
    }
  }
  folly::join(",", valid_endpoints, broker_list);
  return broker_list;
}

KafkaBrokerFileWatcher::KafkaBrokerFileWatcher(std::string local_serverset_path)
    : local_serverset_path_(std::move(local_serverset_path)) {
  // Add file watcher for Kafka serverset.
  CHECK(common::FileWatcher::Instance()->AddFile(
      local_serverset_path_, [this](std::string content) {
        const auto new_kafka_broker_list = ParseKafkaBrokerList(content);
        if (new_kafka_broker_list.empty()) {
          LOG(ERROR) << "Failed to find Kafka broker serverset at "
                     << local_serverset_path_;
          return;
        }
        LOG(INFO) << "Found new kafka brokers: " << new_kafka_broker_list
                  << " from serverset: " << local_serverset_path_
                  << ", old serverset: " << kafka_broker_list_;
        {
          folly::RWSpinLock::WriteHolder write_guard(
              kafka_broker_list_rw_lock_);
          kafka_broker_list_ = new_kafka_broker_list;
        }
      })) << "Failed to add file watcher to " << local_serverset_path_;
}

KafkaBrokerFileWatcher::~KafkaBrokerFileWatcher() {
  common::FileWatcher::Instance()->RemoveFile(local_serverset_path_);
}

const std::string& KafkaBrokerFileWatcher::GetKafkaBrokerList() const {
  folly::RWSpinLock::ReadHolder read_guard(kafka_broker_list_rw_lock_);
  return kafka_broker_list_;
}

}  // namespace


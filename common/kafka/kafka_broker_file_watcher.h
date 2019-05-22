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

#include "folly/RWSpinLock.h"


namespace kafka{

class KafkaBrokerFileWatcher {
 public:
  explicit KafkaBrokerFileWatcher(std::string local_serverset_path);

  ~KafkaBrokerFileWatcher();

  const std::string& GetKafkaBrokerList() const;

 private:
  const std::string local_serverset_path_;
  // TODO: We are currently not using the updateable kafka_broker_list_.
  // The kafka consumer pool is set up only once at the beginning now. Consider
  // re-creating kafka consumer pool if the broker changes. But we should be
  // aware that there is a bug with librdkafka client's close() which might
  // hang forever.
  std::string kafka_broker_list_;
  mutable folly::RWSpinLock kafka_broker_list_rw_lock_;
};

}  // kafka namespace

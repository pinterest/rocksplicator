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
#include <vector>
#include "common/stats/stats.h"

const std::string kKafkaConsumerSeek = "kafka_consumer_seek";
const std::string kKafkaConsumerErrorInit = "kafka_consumer_error_init";
const std::string kKafkaConsumerErrorAssign = "kafka_consumer_error_assign";
const std::string kKafkaConsumerErrorSeek = "kafka_consumer_error_seek";
const std::string kKafkaConsumerErrorConsume = "kafka_consumer_error_consume";
const std::string kKafkaConsumerMessageNull = "kKafkaConsumerMessageNull";
const std::string kKafkaWatcherMessageMissing = "kafka_watcher_message_missing";
const std::string kKafkaWatcherMessageDuplicates = "kafka_watcher_message_duplicates";
const std::string kKafkaWatcherNeedReEstablish = "kafka_watcher_need_reestablish";
const std::string kKafkaWatcherBlockingConsumeTimeout =
    "kafka_watcher_blocking_consume_timeout";
const std::string kKafkaWatcherInitMs = "kafka_watcher_init_ms";
const std::string kKafkaMsgTimeDiffFromCurrMs =
    "kafka_msg_time_diff_from_curr_ms";
const std::string kKafkaMsgNumBytes = "kafka_msg_num_bytes";

std::string getFullStatsName(const std::string& metric_name,
    const std::initializer_list<std::string>& tags);

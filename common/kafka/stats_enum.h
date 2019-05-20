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

#define STATS_ADD_METRIC common::Stats::get()->AddMetric
#define STATS_INCR common::Stats::get()->Incr

enum CounterIdx {
  kCounterIdxMin = -1,
#define NEW_COUNTER_STAT(a, b) a,
#define NEW_METRIC_STAT(a, b)
    NEW_COUNTER_STAT(kKafkaConsumerSeek, "kafka_consumer_seek")
    NEW_COUNTER_STAT(kKafkaConsumerErrorInit, "kafka_consumer_error_init")
    NEW_COUNTER_STAT(kKafkaConsumerErrorAssign, "kafka_consumer_error_assign")
    NEW_COUNTER_STAT(kKafkaConsumerErrorSeek, "kafka_consumer_error_seek")
    NEW_COUNTER_STAT(kKafkaConsumerErrorConsume, "kafka_consumer_error_consume")
    NEW_COUNTER_STAT(kKafkaConsumerMessageNull, "kafka_consumer_message_null")
    NEW_COUNTER_STAT(kKafkaWatcherMessageMissing,
        "kafka_watcher_message_missing")
    NEW_COUNTER_STAT(kKafkaWatcherBlockingConsumeTimeout,
        "kafka_watcher_blocking_consume_timeout")
  // Used for kafka
    NEW_METRIC_STAT(kKafkaWatcherInitMs, "kafka_watcher_init_ms")
    NEW_METRIC_STAT(kKafkaMsgTimeDiffFromCurrMs, "kafka_msg_time_diff_from_curr_ms")
    NEW_METRIC_STAT(kKafkaMsgNumBytes, "kafka_msg_num_bytes")
#undef NEW_METRIC_STAT
#undef NEW_COUNTER_STAT
  kCounterIdxMax,
};

enum MetricIdx {
  kMetricIdxMin = -1,
#define NEW_METRIC_STAT(a, b) a,
#define NEW_COUNTER_STAT(a, b)
    NEW_COUNTER_STAT(kKafkaConsumerSeek, "kafka_consumer_seek")
    NEW_COUNTER_STAT(kKafkaConsumerErrorInit, "kafka_consumer_error_init")
    NEW_COUNTER_STAT(kKafkaConsumerErrorAssign, "kafka_consumer_error_assign")
    NEW_COUNTER_STAT(kKafkaConsumerErrorSeek, "kafka_consumer_error_seek")
    NEW_COUNTER_STAT(kKafkaConsumerErrorConsume,
        "kafka_consumer_error_consume")
    NEW_COUNTER_STAT(kKafkaConsumerMessageNull, "kafka_consumer_message_null")
    NEW_COUNTER_STAT(kKafkaWatcherMessageMissing,
        "kafka_watcher_message_missing")
    NEW_COUNTER_STAT(kKafkaWatcherBlockingConsumeTimeout,
        "kafka_watcher_blocking_consume_timeout")
    // Used for kafka
    NEW_METRIC_STAT(kKafkaWatcherInitMs, "kafka_watcher_init_ms")
    NEW_METRIC_STAT(kKafkaMsgTimeDiffFromCurrMs, "kafka_msg_time_diff_from_curr_ms")
    NEW_METRIC_STAT(kKafkaMsgNumBytes, "kafka_msg_num_bytes")
#undef NEW_COUNTER_STAT
#undef NEW_METRIC_STAT
  kMetricIdxMax,
};

std::string getFullMetricName(MetricIdx idx,
    const std::initializer_list<std::string>& tags);
std::string getFullCounterName(CounterIdx idx,
    const std::initializer_list<std::string>& tags);
std::string getFullMetricName(const std::string& metric_name,
                              const std::initializer_list<std::string>& tags);

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

#include "gflags/gflags.h"
#include "glog/logging.h"

DECLARE_int32(kafka_consumer_timeout_ms);
DECLARE_int32(kafka_consumer_num_retries);
DECLARE_int32(kafka_consumer_time_between_retries_ms);
DECLARE_string(kafka_consumer_reset_on_file_change);
DECLARE_string(enable_kafka_auto_offset_store);
DECLARE_string(kafka_client_global_config_file);

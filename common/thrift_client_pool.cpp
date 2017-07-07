/// Copyright 2016 Pinterest Inc.
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
// @author bol (bol@pinterest.com)
//

#include "common/thrift_client_pool.h"

DEFINE_int32(channel_cleanup_min_interval_seconds, 10,
             "The minimum time between two channel cleanups");

DEFINE_int32(channel_max_checking_size, 500,
             "The maximum # of channels checked by one cleanup");

DEFINE_int32(channel_send_timeout_ms, 0,
             "The send timeout for channels");

DEFINE_int32(default_thrift_client_pool_threads, sysconf(_SC_NPROCESSORS_ONLN),
             "The number of threads driving evbs in thrift client pool.");

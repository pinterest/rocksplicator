/// Copyright 2018 Pinterest Inc.
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

#include "common/future_util.h"

#include <gflags/gflags.h>

#include "common/global_cpu_executor.h"

#if __GNUC__ >= 8
#include "folly/executors/CPUThreadPoolExecutor.h"
#else
#include "wangle/concurrent/CPUThreadPoolExecutor.h"
#endif


DEFINE_bool(delayed_future_with_cpu_executor, false,
            "If offload the work from FutureTimekeepr to global cpu pool");

namespace common {

folly::Future<folly::Unit> GenerateDelayedFuture(folly::Duration delay) {
#if __GNUC__ >= 8
  auto future = folly::futures::sleepUnsafe(delay);
#else
  auto future = folly::futures::sleep(delay);
#endif

  if (FLAGS_delayed_future_with_cpu_executor) {
    future = future.via(common::getGlobalCPUExecutor());
  }

  return future;
}

}  // namespace common

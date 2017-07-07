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

#include "common/global_cpu_executor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "wangle/concurrent/CPUThreadPoolExecutor.h"

DEFINE_int32(global_worker_threads, sysconf(_SC_NPROCESSORS_ONLN),
             "The number of threads for global CPU executor.");

namespace {

uint16_t GetThreadsCount() {
  static const auto n_threads =
    static_cast<uint16_t>(FLAGS_global_worker_threads);
  LOG(INFO) << "Running global CPU thread pool with "
            << n_threads << " threads";
  return n_threads;
}

int GetQueueSize() {
  static const auto queue_sz = (1 << 14);
  LOG(INFO) << "Running global CPU thread pool with "
            << queue_sz << " length queue";
  return queue_sz;
}

}  // namespace


namespace common {

wangle::CPUThreadPoolExecutor* getGlobalCPUExecutor() {
  static wangle::CPUThreadPoolExecutor g_executor(
    GetThreadsCount(),
    std::make_unique<
      wangle::LifoSemMPMCQueue<wangle::CPUThreadPoolExecutor::CPUTask,
      wangle::QueueBehaviorIfFull::BLOCK>>(GetQueueSize()));

  return &g_executor;
}

}  // namespace common

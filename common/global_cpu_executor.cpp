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

#include "gflags/gflags.h"
#include "wangle/concurrent/CPUThreadPoolExecutor.h"

DEFINE_int32(cpu_executor_thread_num, sysconf(_SC_NPROCESSORS_ONLN),
             "The number of threads in the global cpu executor");

DEFINE_int64(cpu_executor_queue_size, 1 << 14,
             "The queue size in the global cpu executor");

namespace common {

wangle::CPUThreadPoolExecutor* getGlobalCPUExecutor() {
  static wangle::CPUThreadPoolExecutor g_executor(
    FLAGS_cpu_executor_thread_num,
    std::make_unique<
      wangle::LifoSemMPMCQueue<wangle::CPUThreadPoolExecutor::CPUTask,
      wangle::QueueBehaviorIfFull::BLOCK>>(FLAGS_cpu_executor_queue_size));

  return &g_executor;
}

}  // namespace common

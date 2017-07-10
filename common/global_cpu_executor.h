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

#pragma once

namespace wangle {

class CPUThreadPoolExecutor;

}

namespace common {

/*
 * Return the global CPUThreadPoolExecutor.
 * NOTE: do not call this function before main(). Otherwise, it will cause
 * global variable initialization race between static executor and
 * GFlag of thread count. In case that happens, we will fallback to use the
 * number of cores as the thread pool size.
 */
wangle::CPUThreadPoolExecutor* getGlobalCPUExecutor();

}  // namespace common

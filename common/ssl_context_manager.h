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

//
// @author bol (bol@pinterest.com)
//

#pragma once

#include <memory>

namespace folly {

class SSLContext;

}

namespace common {

// The shared_ptr object pointed by the returned pointer will be refreshed
// periodically with the latest TLS certificates. So we need to use
// std::atomic_load_explicit() to read it.
// If tls certificate gflags are not set or failed to read them, a nullptr will
// be returned.
const std::shared_ptr<folly::SSLContext>* getSSLContext();

}  // namespace common

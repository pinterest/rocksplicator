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

#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "glog/logging.h"

namespace common {

void RocksdbGLogger::Logv(const char* format, va_list ap) {
  static const int kBufSize = 2048;
  static char buf[kBufSize];

  auto ret = vsnprintf(buf, kBufSize, format, ap);
  if (ret < 0) {
    LOG(ERROR) << "Failed to vsnprintf(): " << ret;
    return;
  }

  if (ret >= kBufSize) {
    LOG(ERROR) << "RocksDB log info is of " << ret
               << " bytes, but the buf size is " << kBufSize
               << " bytes only, ignored";
    return;
  }

  if (GetInfoLogLevel() == rocksdb::InfoLogLevel::FATAL_LEVEL) {
    LOG(FATAL) << buf;
  } else if (GetInfoLogLevel() == rocksdb::InfoLogLevel::ERROR_LEVEL) {
    LOG(ERROR) << buf;
  } else if (GetInfoLogLevel() == rocksdb::InfoLogLevel::WARN_LEVEL) {
    LOG(WARNING) << buf;
  } else {
    // everything else
    LOG(INFO) << buf;
  }
}

}  // namespace common

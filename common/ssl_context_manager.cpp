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

#include "common/ssl_context_manager.h"

#include <memory>
#include <string>

#include "common/future_util.h"
#include "common/stats/stats.h"
#include "folly/io/async/SSLContext.h"

DEFINE_string(tls_certfile, "", "Certificate file path location for TLS");

DEFINE_string(tls_trusted_certfile, "",
              "Trusted certificate file path location for TLS");

DEFINE_string(tls_keyfile, "", "Key file path location for TLS");

namespace {

// create a new SSLContext from files, return nullptr on error
std::shared_ptr<folly::SSLContext> loadSSLContext() {
  auto ctx = std::make_shared<folly::SSLContext>();
  errno = 0;
  ctx->loadCertificate(FLAGS_tls_certfile.c_str());
  if (errno) {
    LOG(ERROR) << "Got errors loading certificate : " << ctx->getErrors();
    return nullptr;
  }
  ctx->loadPrivateKey(FLAGS_tls_keyfile.c_str());
  if (errno) {
    LOG(ERROR) << "Got errors loading private key : " << ctx->getErrors();
    return nullptr;
  }
  ctx->loadTrustedCertificates(FLAGS_tls_trusted_certfile.c_str());
  if (errno) {
    LOG(ERROR) << "Got errors loading trusted certificate : "
               << ctx->getErrors();
    return nullptr;
  }

  return ctx;
}

// schedule periodial refresh for cur_ctx
void scheduleRefresh(std::shared_ptr<folly::SSLContext>* cur_ctx) {
  auto delayed_future =
    common::GenerateDelayedFuture(std::chrono::seconds(60 * 60));
  delayed_future.then([cur_ctx] () {
      auto new_ctx = loadSSLContext();
      if (new_ctx) {
        LOG(INFO) << "Got a new SSLContext, swapping";
        static const std::string kSSLContextRefreshTimes =
          "ssl_context_refresh_times";
        common::Stats::get()->Incr(kSSLContextRefreshTimes);
        std::atomic_exchange_explicit(cur_ctx, new_ctx,
                                      std::memory_order_release);
      }

      scheduleRefresh(cur_ctx);
    });
}

std::once_flag schedule_flag;

}  // namespace


namespace common {

const std::shared_ptr<folly::SSLContext>* getSSLContext() {
  static const bool no_tls = FLAGS_tls_certfile.empty() ||
    FLAGS_tls_trusted_certfile.empty() ||
    FLAGS_tls_keyfile.empty();

  // always return nullptr if any of the gflags not set
  if (no_tls) {
    return nullptr;
  }

  static auto ctx = loadSSLContext();


  // always return nullptr if failed to load the context for the first time
  // static object ctx can be accessed from multiple thread, so we have to use
  // atomic functions to read/write it
  if (std::atomic_load_explicit(&ctx, std::memory_order_acquire) == nullptr) {
    return nullptr;
  }

  // succeeded to load the context, schedule for future refresh once
  std::call_once(schedule_flag, scheduleRefresh, &ctx);

  return &ctx;
}

}  // namespace common

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

#pragma once

#include <folly/futures/Future.h>

#include <atomic>
#include <string>
#include <type_traits>

#include "common/stats/stats.h"

namespace common {

/**
 * Get a Unit future that will be fulfilled after the delay.
 */
folly::Future<folly::Unit> GenerateDelayedFuture(folly::Duration delay);

/**
 * Return a derived future composed from primary_future and backup_future_func.
 *
 * If the primary_future succeeds before speculative_timeout, its value is used
 * to fulfill the returned future and no backup future will be fired.
 *
 * If the primary_future gets an exception or doesn't finish before
 * speculative_timeout, a backup future will be fired. The value of the first
 * succeeding future is used to fulfill the returned future. If none of the two
 * futures succeeds, the exception from the primary future is used to fulfill
 * the returned future.
 *
 * @T The value type of the future
 * @F The functor type that returns the backup future
 *
 * @primary_future The primary future
 * @backup_future_func A functor used to build a backup future. It will be
 *                     called at most once
 * @speculative_timeout Timeout
 *
 * @return the derived future
 */
template <typename T, typename F>
folly::Future<T> GetSpeculativeFuture(
    folly::Future<T>&& primary_future,
    F&& backup_future_func,
    folly::Duration speculative_timeout) {
  static_assert(
    std::is_same<decltype(backup_future_func()), folly::Future<T>>::value,
    "backup_future_func and primary_future types mismatch");

  static const std::string kBackupFired = "speculative_failover_fired";
  static const std::string kPrimaryTimeout = "original_request_timeout";

  struct Context {
    explicit Context(F&& func)
      : fulfilled(false)
      , backup_fired(false)
      , promise()
      , backup_future_func(std::forward<F>(func))
      , primary_exception() {}

    ~Context() {
      if (!fulfilled.load()) {
        promise.setTry(std::move(primary_exception));
      }
    }

    void tryToFireBackupFuture(std::shared_ptr<Context> ctx) {
      if (backup_fired.exchange(true)) {
        return;
      }

      auto backup_future = backup_future_func();
      std::move(backup_future).then([ctx = std::move(ctx)] (folly::Try<T>&& t) mutable {
        if (!t.hasException() && ctx->fulfilled.exchange(true) == false) {
          ctx->promise.setTry(std::move(t));
        }
      });

      common::Stats::get()->Incr(kBackupFired);
    }

    std::atomic<bool> fulfilled;
    std::atomic<bool> backup_fired;
    folly::Promise<T> promise;
    F backup_future_func;
    folly::Try<T> primary_exception;
  };

  auto ctx = std::make_shared<Context>(std::forward<F>(backup_future_func));
  auto future = ctx->promise.getFuture();

  std::move(primary_future).then([ctx] (folly::Try<T>&& t) mutable {
    if (t.hasException()) {
      if (!ctx->fulfilled.load()) {
        ctx->tryToFireBackupFuture(ctx);
        ctx->primary_exception = std::move(t);
      }
    } else if (ctx->fulfilled.exchange(true) == false) {
      ctx->promise.setTry(std::move(t));
    }
  });

  auto timeout_future = GenerateDelayedFuture(speculative_timeout);
  std::move(timeout_future).then([ctx] (folly::Try<folly::Unit>&& t) mutable {
    if (!ctx->fulfilled.load()) {
      ctx->tryToFireBackupFuture(ctx);
      common::Stats::get()->Incr(kPrimaryTimeout);
    }
  });

  return future;
}

}  // namespace common

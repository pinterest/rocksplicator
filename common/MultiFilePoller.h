/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Changes and modifications in this file
 * Copyright 2020 Pinterest Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is a copied and subsequently modified version of file from
 * Facebook's wangle library from gitsha: 639938b92547030232c42ceef3eba70d027de3c0
 * hosted at: https://github.com/facebook/wangle
 *
 * Details of modification (very minor modification to make it work in current
 * versions of wangle/folly/rocksplicator dependencies)
 * 1. Reason for copying it from wangle, rather then use if from wangle directly
 * is because in rocksplicator, currently we are pinned to wangle and folly
 * versions which are quite old and these files are only available in newer
 * version of wangle.
 *
 * 2. The file has been migrated to namespace common, instead of it's original
 * wangle namespace, since other applications that uses rocksplicator could
 * potentially be on newer version of wangle/folly and this might result
 * in linking error.
 *
 * 3. When we are ready to upgrade rocksplicator to use newer version of wangle
 * /folly, we can keep these file and make migration over to wangle's version
 * of file in separate changes.
 *
 * 4. Some of the tests were modified to remove usage of functionality not available
 * in current version of folly/wangle as used by rocksplicator, without removing
 * any functional test.
 */

#pragma once

#include "common/FilePoller.h"

#include <chrono>
#include <unordered_map>

#include <folly/Function.h>
#include <folly/SharedMutex.h>

namespace common {

/**
 * An extension to common::FilePoller with the ability to register one or more
 * callback on a file, and to track one or more file in a callback, and to
 * deliver cached file data to callbacks.
 */
class MultiFilePoller {
 public:
  /**
   * A callback:
   *   (1) takes as argument a map from a file path to its latest content.
           Unreadable paths will not show up in the map.
   *   (2) is triggered when any file it registers is changed,
   *       in the context of FilePoller thread.
   *   (3) once registered, cannot have its file list or callback pointer
   *       changed. To make changes, cancel the existing one and register
   *       a new one.
   * Caveat:
   *   (1) If there are N files registered, in worst case the first N-1
   *       callbacks would not get latest content of all N files. It's up to the
   *       callback to determine what to do for those cases.
   *   (2) Once a file, say, F, changes, not only F, but all files needed to
   *       trigger all callbacks that use F will be read. For example,
   *         Callback A needs files {a, b};
   *         Callback B needs files {b, c};
   *         Callback C needs files {d, e};
   *         File {a} changes -> Reads {a, b} -> Calls A(a, b).
   *         File {b} changes -> Reads {a, b, c} -> Calls A(a, b), B(b, c).
   *         File {c} changes -> Reads {b, c} -> Calls B(b, c).
   *         File {d} or {e} changes -> Reads {d, e} -> Calls C(d, e).
   */
  using CallbackArg = std::unordered_map<std::string, std::string>;
  using Callback = folly::Function<void(const CallbackArg& newData) noexcept>;

  /**
   * Type of callback identifier.
   * Use a final class to prevent conversion or modification.
   */
  class CallbackId final {
   private:
    explicit CallbackId(size_t id) : id_(id) {
    }
    friend class MultiFilePoller;
    size_t id_;
  };

  /**
   * @param pollInterval Interval between polls. Setting a value less than 1_s
   *   may cause undesirable behavior because the minimum granularity for
   *   common::FilePoller to detect mtime difference is 1_s.
   */
  explicit MultiFilePoller(std::chrono::milliseconds pollInterval);

  ~MultiFilePoller() = default;

  /**
   * Add a callback to trigger when the specified file changes.
   * @param path The path to monitor.
   * @param cb The callback to trigger.
   * @return An ID of the callback, used for cancellation.
   */
  CallbackId registerFile(std::string path, Callback cb);

  /**
   * Add a callback to trigger when any of the files in paths changes.
   * @param paths The list of paths to monitor. Must be non-empty.
   * @param cb The callback to trigger.
   @ return An ID of the callback, used for cancellation.
   */
  CallbackId registerFiles(const std::vector<std::string>& paths, Callback cb);

  /**
   * Cancel the specified Callback. May throw std::out_of_range if not found.
   * @param cbId ID of the callback returned when registering it.
   */
  void cancelCallback(const CallbackId& cbId);

 private:
  /**
   * The callback dispatcher to be registered to common::FilePoller.
   */
  void onFileUpdated(const std::string& triggeredPath);

  /**
   * Find an unused size_t value as callback Id. Caller must acquire wlock.
   */
  size_t getNextCallbackId();

  using StringReferences =
      std::vector<std::reference_wrapper<const std::string>>;

  struct CallbackDetail {
    CallbackDetail(StringReferences files, Callback cb)
        : files_(std::move(files)), cb_(std::move(cb)) {
    }
    StringReferences files_;
    Callback cb_;
  };

  // The following data structures are protected by the mutex.
  folly::SharedMutex rwlock_;
  size_t lastCallbackId_ = 0;
  std::unordered_map<std::string, std::vector<size_t>> pathsToCallbackIds_;
  std::unordered_map<size_t, CallbackDetail> idsToCallbacks_;

  // The following data structures are set by ctor only.
  common::FilePoller poller_;
};

} // namespace wangle

/// Copyright 2021 Pinterest Inc.
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
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.eventstore;

import java.io.Closeable;

public interface LeaderEventsLogger extends Closeable {
  /**
   * Provides a collector of leader events for a given resource and partition.
   * All logically batched events are collected in collector before committing it to
   * backing store.
   */
  LeaderEventsCollector newEventsCollector(final String resourceName, final String partitionName);

  /**
   * True if logging is generally enabled.
   */
  boolean isLoggingEnabled();

  /**
   * True is logging is enabled for a given resource.
   */
  boolean isLoggingEnabledForResource(final String resourceName);
}

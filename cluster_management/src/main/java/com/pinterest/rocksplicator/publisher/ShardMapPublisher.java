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

package com.pinterest.rocksplicator.publisher;

import org.apache.helix.model.ExternalView;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface ShardMapPublisher<T> extends Closeable {

  /**
   * Publish a shardMap of type T.
   */
  void publish(Set<String> validResources, List<ExternalView> externalViews, T shardMap);

  @Override
  default void close() throws IOException {
  }
}

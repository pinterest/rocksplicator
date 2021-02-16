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

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ShardMapPublisher that takes in shard_map in JSONObject format
 * and dedups the shard_maps published consecutively in sequence, if last published
 * shard_map was same as the current one.
 */
public class DedupingShardMapPublisher implements ShardMapPublisher<JSONObject> {

  private static final Logger LOG = LoggerFactory.getLogger(DedupingShardMapPublisher.class);

  private final ShardMapPublisher<String> delegateShardMapPublisher;
  private String lastPostedContent;

  public DedupingShardMapPublisher(ShardMapPublisher<String> delegateShardMapPublisher) {
    this.delegateShardMapPublisher = delegateShardMapPublisher;
    this.lastPostedContent = null;
  }

  @Override
  public void publish(JSONObject jsonShardMap) {
    String newContent = jsonShardMap.toString();
    if (lastPostedContent != null && lastPostedContent.equals(newContent)) {
      LOG.error("Identical external view observed, skip updating config.");
      return;
    }
    delegateShardMapPublisher.publish(newContent);
  }
}

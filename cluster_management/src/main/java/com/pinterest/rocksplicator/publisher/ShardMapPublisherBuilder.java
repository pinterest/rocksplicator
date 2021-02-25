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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ShardMapPublisherBuilder {

  private static final Logger LOG =
      LoggerFactory.getLogger(ShardMapPublisherBuilder.class);

  private final String clusterName;
  private String postUrl = null;
  private boolean enableLocalDump = false;
  private String zkShardMapConnectString = null;

  private ShardMapPublisherBuilder(String clusterName) {
    this.clusterName = Preconditions.checkNotNull(clusterName);
  }

  public static ShardMapPublisherBuilder create(String clusterName) {
    return new ShardMapPublisherBuilder(clusterName);
  }

  public ShardMapPublisherBuilder withPostUrl(String postUrl) {
    this.postUrl = postUrl;
    return this;
  }

  public ShardMapPublisherBuilder withZkShardMap(String zkShardMapConnectString) {
    this.zkShardMapConnectString = zkShardMapConnectString;
    return this;
  }

  public ShardMapPublisherBuilder withLocalDump() {
    this.enableLocalDump = true;
    return this;
  }

  public ShardMapPublisherBuilder withOutLocalDump() {
    this.enableLocalDump = false;
    return this;
  }

  public ShardMapPublisher<JSONObject> build() {
    List<ShardMapPublisher<String>> publishers = new ArrayList<>();

    if (postUrl != null && !postUrl.isEmpty()) {
      LOG.error(String.format("Publish to Http Url enabled postUrl: %s", postUrl));
      publishers.add(new HttpPostShardMapPublisher(this.postUrl));
    }
    if (enableLocalDump) {
      LOG.error(String.format("Publish to local directory is enabled"));
      publishers.add(new LocalFileShardMapPublisher(enableLocalDump, clusterName));
    }

    ShardMapPublisher<JSONObject> defaultPublisher =
        new DedupingShardMapPublisher(
            new ParallelShardMapPublisher<String>(ImmutableList.copyOf(publishers)));

    if (zkShardMapConnectString == null || zkShardMapConnectString.isEmpty()) {
      return defaultPublisher;
    }

    LOG.error(String.format("Publish to zk server is enabled zkSvr: %s", zkShardMapConnectString));
    ShardMapPublisher<JSONObject> zkShardMapPublisher =
        new ZkBasedPerResourceShardMapPublisher(clusterName, zkShardMapConnectString);

    return new ParallelShardMapPublisher<JSONObject>(
        ImmutableList.of(defaultPublisher, zkShardMapPublisher));
  }
}

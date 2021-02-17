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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class LocalFileShardMapPublisher implements ShardMapPublisher<String> {

  private static final Logger LOG = LoggerFactory.getLogger(DedupingShardMapPublisher.class);

  private final boolean enableDumpToLocal;

  public LocalFileShardMapPublisher(boolean enableDumpToLocal) {
    this.enableDumpToLocal = enableDumpToLocal && new File("/var/log/helixspectator").canWrite();
  }

  @Override
  public void publish(
      final Set<String> validResources,
      final List<ExternalView> externalViews,
      final String jsonStringShardMapNewContent) {
    if (! enableDumpToLocal) {
      // doNothing()
      return;
    }

    // Write the shard config to local
    try {
      FileWriter shard_config_writer = new FileWriter("/var/log/helixspectator/shard_config");
      shard_config_writer.write(jsonStringShardMapNewContent);
      shard_config_writer.close();
      LOG.error("Successfully wrote the shard config to the local.");
    } catch (IOException e) {
      LOG.error("An error occurred when writing shard config to local");
      e.printStackTrace();
    }
  }
}

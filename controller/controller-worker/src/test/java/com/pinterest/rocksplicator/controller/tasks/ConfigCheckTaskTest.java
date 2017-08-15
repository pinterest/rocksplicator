/*
 * Copyright 2017 Pinterest, Inc.
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
 */

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.util.ZKUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Shu Zhang (shu@pinterest.com)
 */
public class ConfigCheckTaskTest extends TaskBaseTest{

  @Test
  public void testSuccessful() throws Exception {
    // use the config from TaskBaseTest
    ConfigCheckTask configCheckTask = new ConfigCheckTask(3);
    injector.injectMembers(configCheckTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    configCheckTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123), "Cluster rocksdb/devtest has good config");
  }

  @Test
  public void testMissingReplicas() throws Exception {
    final String missingReplicaConfig =
            "{" +
            "  \"user_pins\": {" +
            "  \"num_shards\": 3," +
            "  \"127.0.0.1:8090:us-east-1a\": [\"00000:M\", \"00001:S\"]," +
            "  \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"]," +
            "  \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]" +
            "   }" +
            "}";
    zkClient.createContainers(ZKUtil.getClusterConfigZKPath(CLUSTER));
    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER),
        missingReplicaConfig.getBytes());
    ConfigCheckTask configCheckTask = new ConfigCheckTask(3);
    injector.injectMembers(configCheckTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    configCheckTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123),
        "Cluster rocksdb/devtest doesn't have good shard distribution, " +
            "reason = Incorrect number of replicas. Bad shards: {user_pins2=2},");
  }

  @Test
  public void testMissingMasters() throws Exception {
    final String missingReplicaConfig =
        "{" +
            "  \"user_pins\": {" +
            "  \"num_shards\": 3," +
            "  \"127.0.0.1:8090:us-east-1a\": [\"00000:S\", \"00001:S\", \"00002:S\"]," +
            "  \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"]," +
            "  \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]"  +
            "   }" +
            "}";
    zkClient.createContainers(ZKUtil.getClusterConfigZKPath(CLUSTER));
    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER),
        missingReplicaConfig.getBytes());
    ConfigCheckTask configCheckTask = new ConfigCheckTask(3);
    injector.injectMembers(configCheckTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    configCheckTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123),
        "Cluster rocksdb/devtest doesn't have good shard distribution, " +
            "reason = Missing masters for some shards: [user_pins0],");
  }

  @Test
  public void testIncorrectShardNumber() throws Exception {
    final String missingReplicaConfig =
        "{" +
            "  \"user_pins\": {" +
            "  \"num_shards\": 3," +
            "  \"127.0.0.1:8090:us-east-1a\": [\"00008:S\", \"00001:S\", \"00002:S\"]," +
            "  \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"]," +
            "  \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]"  +
            "   }" +
            "}";
    zkClient.createContainers(ZKUtil.getClusterConfigZKPath(CLUSTER));
    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER),
        missingReplicaConfig.getBytes());
    ConfigCheckTask configCheckTask = new ConfigCheckTask(3);
    injector.injectMembers(configCheckTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    configCheckTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123),
        "Cluster rocksdb/devtest doesn't have good shard distribution, " +
            "reason = Incorrect number of shards. Expected 3 but actually 4.,");
  }
}

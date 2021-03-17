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

package com.pinterest.rocksplicator.shardmapagent;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.SimpleJsonObjectByteArrayCodec;
import com.pinterest.rocksplicator.codecs.ZkGZIPCompressedShardMapCodec;
import com.pinterest.rocksplicator.config.ConfigNotifier;
import com.pinterest.rocksplicator.config.FileWatchers;
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ClusterShardMapAgentTest {

  private static final String CLUSTER_NAME = "myCluster";
  private static final String RESOURCE_1 = "myResource_1";
  private static final String RESOURCE_2 = "myResource_2";
  private static final String RESOURCE_3 = "myResource_3";
  private static final Codec<JSONObject, byte[]> codec = new ZkGZIPCompressedShardMapCodec();

  private TestingServer zkTestServer;
  private CuratorFramework zkShardMapClient;


  @Before
  public void setUp() throws Exception {
    zkTestServer = new TestingServer(-1);
    zkShardMapClient =
        CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
    zkShardMapClient.start();
    zkShardMapClient.blockUntilConnected(60, TimeUnit.SECONDS);
  }

  @Test
  public void testClusterShardMapAgent() throws Exception {

    JSONObject topLevel = new JSONObject();
    JSONObject per_resource_shard_map = new JSONObject();

    topLevel.put("meta", new JSONObject());
    topLevel.put("shard_map", per_resource_shard_map);

    per_resource_shard_map.put(RESOURCE_1, new JSONObject());

    /**
     * First write the data to zookeeper for resource_1
     */
    writeDataToZK(RESOURCE_1, topLevel);

    /**
     * Now enable downloading of the data to local file.
     */
    ClusterShardMapAgent clusterShardMapAgent =
        new ClusterShardMapAgent(zkTestServer.getConnectString(), null, CLUSTER_NAME,
            "target/shardmap");

    clusterShardMapAgent.startNotification();

    final AtomicReference<JSONObject> clusterShardMap = new AtomicReference<>();
    final AtomicReference<CountDownLatch>
        countDownLatch =
        new AtomicReference(new CountDownLatch(1));
    // Whenever there is a change this file notifier will notified
    ConfigNotifier<JSONObject> fileNotifier = new ConfigNotifier<JSONObject>(
        new SimpleJsonObjectByteArrayCodec(),
        "target/shardmap/" + CLUSTER_NAME,
        FileWatchers.getHighResolutionExpensiveFileWatcher(),
        new Function<ConfigNotifier.Context<JSONObject>, Void>() {
          @Override
          public Void apply(ConfigNotifier.Context<JSONObject> jsonObjectContext) {
            clusterShardMap.set(jsonObjectContext.getItem());
            countDownLatch.get().countDown();
            return null;
          }
        });
    fileNotifier.start();

    countDownLatch.get().await(10, TimeUnit.SECONDS);

    // We must have the file here with the data uploaded to zookeeper.
    JSONObject shard_map1 = clusterShardMap.get();
    assertTrue(shard_map1.containsKey(RESOURCE_1));

    /**
     * Now write the data for second resource.
     */
    countDownLatch.set(new CountDownLatch(1));
    per_resource_shard_map.clear();
    per_resource_shard_map.put(RESOURCE_2, new JSONObject());
    writeDataToZK(RESOURCE_2, topLevel);
    countDownLatch.get().await(10, TimeUnit.SECONDS);
    JSONObject shard_map2 = clusterShardMap.get();
    assertTrue(shard_map2.containsKey(RESOURCE_1));
    assertTrue(shard_map2.containsKey(RESOURCE_2));

    per_resource_shard_map.clear();
    per_resource_shard_map.put(RESOURCE_3, new JSONObject());
    countDownLatch.set(new CountDownLatch(1));
    writeDataToZK(RESOURCE_3, topLevel);
    countDownLatch.get().await(10, TimeUnit.SECONDS);
    JSONObject shard_map3 = clusterShardMap.get();
    assertTrue(shard_map3.containsKey(RESOURCE_1));
    assertTrue(shard_map3.containsKey(RESOURCE_2));
    assertTrue(shard_map3.containsKey(RESOURCE_3));

    countDownLatch.set(new CountDownLatch(1));
    removeDataFromZK(RESOURCE_1);
    countDownLatch.get().await(10, TimeUnit.SECONDS);
    JSONObject shard_map4 = clusterShardMap.get();
    assertFalse(shard_map4.containsKey(RESOURCE_1));
    assertTrue(shard_map4.containsKey(RESOURCE_2));
    assertTrue(shard_map4.containsKey(RESOURCE_3));

    countDownLatch.set(new CountDownLatch(1));
    removeDataFromZK(RESOURCE_3);
    countDownLatch.get().await(10, TimeUnit.SECONDS);
    JSONObject shard_map5 = clusterShardMap.get();
    assertFalse(shard_map5.containsKey(RESOURCE_1));
    assertTrue(shard_map5.containsKey(RESOURCE_2));
    assertFalse(shard_map5.containsKey(RESOURCE_3));

    fileNotifier.stop();
    fileNotifier.close();
    clusterShardMapAgent.close();
  }

  private void removeDataFromZK(String resource) throws Exception {
    zkShardMapClient.delete()
        .forPath(ZkPathUtils.getClusterResourceShardMapPath(CLUSTER_NAME, resource));
  }

  private void writeDataToZK(String resource, JSONObject topLevel) throws Exception {
    String zkPath = ZkPathUtils.getClusterResourceShardMapPath(CLUSTER_NAME, resource);
    Stat stat = zkShardMapClient.checkExists().creatingParentsIfNeeded().forPath(zkPath);
    if (stat == null) {
      zkShardMapClient.create().creatingParentsIfNeeded()
          .forPath(zkPath, codec.encode(topLevel));
    } else {
      zkShardMapClient.setData().forPath(zkPath, codec.encode(topLevel));
    }
  }

  @After
  public void tearDown() throws IOException {
    zkShardMapClient.close();
    zkTestServer.stop();
  }
}

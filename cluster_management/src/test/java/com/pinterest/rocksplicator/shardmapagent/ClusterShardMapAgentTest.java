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
import com.pinterest.rocksplicator.codecs.JSONObjectCodec;
import com.pinterest.rocksplicator.codecs.ZkGZIPCompressedShardMapCodec;
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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

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
    int TIME_MILLIS_SLEEP = 250;

    JSONObject topLevel = new JSONObject();
    JSONObject shard_map = new JSONObject();

    topLevel.put("meta", new JSONObject());
    topLevel.put("shard_map", shard_map);

    shard_map.put(RESOURCE_1, new JSONObject());

    writeDataToZK(RESOURCE_1, topLevel);
    ClusterShardMapAgent clusterShardMapAgent =
        new ClusterShardMapAgent(zkTestServer.getConnectString(), CLUSTER_NAME, "target/shardmap");

    clusterShardMapAgent.startNotification();

    JSONObject shard_map1 = getDataFromFile("target/shardmap/" + CLUSTER_NAME);
    shard_map.remove(RESOURCE_1);
    Thread.sleep(TIME_MILLIS_SLEEP);
    assertTrue(shard_map1.containsKey(RESOURCE_1));

    shard_map.put(RESOURCE_2, new JSONObject());
    writeDataToZK(RESOURCE_2, topLevel);
    Thread.sleep(TIME_MILLIS_SLEEP);
    JSONObject shard_map2 = getDataFromFile("target/shardmap/" + CLUSTER_NAME);
    shard_map.remove(RESOURCE_2);
    assertTrue(shard_map2.containsKey(RESOURCE_1));
    assertTrue(shard_map2.containsKey(RESOURCE_2));

    shard_map.put(RESOURCE_3, new JSONObject());
    writeDataToZK(RESOURCE_3, topLevel);
    Thread.sleep(TIME_MILLIS_SLEEP);
    JSONObject shard_map3 = getDataFromFile("target/shardmap/" + CLUSTER_NAME);
    shard_map.remove(RESOURCE_3);
    assertTrue(shard_map3.containsKey(RESOURCE_1));
    assertTrue(shard_map3.containsKey(RESOURCE_2));
    assertTrue(shard_map3.containsKey(RESOURCE_3));

    removeDataFromZK(RESOURCE_1);
    Thread.sleep(TIME_MILLIS_SLEEP);
    JSONObject shard_map4 = getDataFromFile("target/shardmap/" + CLUSTER_NAME);
    assertFalse(shard_map4.containsKey(RESOURCE_1));
    assertTrue(shard_map4.containsKey(RESOURCE_2));
    assertTrue(shard_map4.containsKey(RESOURCE_3));

    removeDataFromZK(RESOURCE_3);
    Thread.sleep(TIME_MILLIS_SLEEP);
    JSONObject shard_map5 = getDataFromFile("target/shardmap/" + CLUSTER_NAME);
    assertFalse(shard_map5.containsKey(RESOURCE_1));
    assertTrue(shard_map5.containsKey(RESOURCE_2));
    assertFalse(shard_map5.containsKey(RESOURCE_3));

    clusterShardMapAgent.close();
  }

  private JSONObject getDataFromFile(String filePath) throws Exception {
    InputStream is = new FileInputStream(filePath);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    int read = -1;
    while ((read = is.read()) >= 0) {
      bos.write(read);
    }
    bos.close();
    is.close();

    JSONObjectCodec codec = new JSONObjectCodec();
    return codec.decode(bos.toByteArray());
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

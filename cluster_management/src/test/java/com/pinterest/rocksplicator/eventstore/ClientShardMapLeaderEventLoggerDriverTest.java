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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Optional;

public class ClientShardMapLeaderEventLoggerDriverTest {

  private static final String CLUSTER_NAME = "myCluster";
  private static final String RESOURCE_NAME = "myResource";
  private static final String PARTITION_0_NAME = "myResource_0";
  private static final String PARTITION_1_NAME = "myResource_1";
  private static final String INSTANCE_ID = "10.0.0.2_9090";

  private TestingServer zkTestServer;
  private CuratorFramework zkClient;
  private Codec<LeaderEventsHistory, byte[]>
      codec =
      new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT,
          CompressionAlgorithm.GZIP);

  private File resourceConfigPath;
  private File shardMapPath;

  private LeaderEventsLogger eventsLogger;
  private ClientShardMapLeaderEventLogger clientLogger;
  private ClientShardMapLeaderEventLoggerDriver clientDriver;

  @Before
  public void setUp() throws Exception {
    zkTestServer = new TestingServer(-1);

    System.out.println(zkTestServer.getConnectString());

    zkClient = CuratorFrameworkFactory.newClient(
        zkTestServer.getConnectString(), new RetryOneTime(2000));
    zkClient.start();

    resourceConfigPath =
        File.createTempFile("enabled_resources", ".json");
    resourceConfigPath.deleteOnExit();

    OutputStream os = new FileOutputStream(resourceConfigPath);
    Writer writer = new OutputStreamWriter(os);
    writer.write("[\"" + RESOURCE_NAME + "\"]");
    writer.close();

    shardMapPath = File.createTempFile("shard_map", ".json");
    shardMapPath.deleteOnExit();

    os = new FileOutputStream(shardMapPath);
    writer = new OutputStreamWriter(os);

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("num_shards", 2);
    JSONArray array = new JSONArray();
    array.add("00000:M");
    array.add("00001:S");
    jsonObject.put("10.1.1.1:9090:az_pg", array);
    JSONObject shardMapObj = new JSONObject();
    shardMapObj.put(RESOURCE_NAME, jsonObject);
    writer.write(shardMapObj.toJSONString());
    writer.close();

    /**
     * Create shardMap.
     */
    eventsLogger =
        new LeaderEventsLoggerImpl(INSTANCE_ID, zkTestServer.getConnectString(), CLUSTER_NAME,
            resourceConfigPath.getAbsolutePath(), "JSON_ARRAY",
            Optional.empty());

    assertTrue(eventsLogger.isLoggingEnabled());
    assertTrue(eventsLogger.isLoggingEnabledForResource(RESOURCE_NAME));

    clientLogger = new ClientShardMapLeaderEventLoggerImpl(eventsLogger);
  }

  @Test
  public void testEventDriver() throws Exception {
    clientDriver =
        new ClientShardMapLeaderEventLoggerDriver(CLUSTER_NAME, shardMapPath.getAbsolutePath(),
            eventsLogger, zkTestServer.getConnectString());
    Thread.currentThread().sleep(1000);

    clientDriver.close();
    eventsLogger.close();

    LeaderEventsHistory history = getData(PARTITION_0_NAME);

    assertEquals(1, history.getEventsSize());

    history = getData(PARTITION_1_NAME);

    assertEquals(1, history.getEventsSize());
  }

  private LeaderEventsHistory getData(String partition) throws Exception {
    return codec.decode(zkClient.getData()
        .forPath(ZkMergeableEventStore
            .getLeaderEventHistoryPath(CLUSTER_NAME, RESOURCE_NAME, partition)));
  }

  @After
  public void tearDown() throws Exception {
    zkClient.close();
    zkTestServer.stop();
  }
}

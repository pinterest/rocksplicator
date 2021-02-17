package com.pinterest.rocksplicator.publisher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.pinterest.rocksplicator.codecs.ZkGZIPCompressedShardMapCodec;
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.helix.model.ExternalView;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ZkBasedPerResourceShardMapPublisherTest {
  private static final String CLUSTER_NAME = "myCluster";
  private static final String RESOURCE_1 = "myResource_1";
  private static final String RESOURCE_2 = "myResource_2";
  private static final String RESOURCE_3 = "myResource_3";
  private TestingServer zkTestServer;
  private CuratorFramework zkClient;


  @Before
  public void setUp() throws Exception {
    zkTestServer = new TestingServer(-1);
    zkClient =
        CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(),
            new RetryOneTime(2000));
    zkClient.start();

    zkClient.blockUntilConnected(60, TimeUnit.SECONDS);
  }

  @Test
  public void testUpload() throws Exception {
    Set<String> resources = ImmutableSet.of(RESOURCE_1, RESOURCE_2, RESOURCE_3);

    ExternalView view1 = new ExternalView(RESOURCE_1);
    ExternalView view2 = new ExternalView(RESOURCE_2);
    ExternalView view3 = new ExternalView(RESOURCE_3);

    List<ExternalView> externalViews = ImmutableList.of(view1, view2, view3);

    ZkBasedPerResourceShardMapPublisher publisher = new ZkBasedPerResourceShardMapPublisher(
        CLUSTER_NAME, zkTestServer.getConnectString(), true);

    // First test storing single resource.
    JSONObject shard_map = new JSONObject();
    JSONObject resourceJsonObj1 = new JSONObject();
    JSONObject resourceJsonObj2 = new JSONObject();
    JSONObject resourceJsonObj3 = new JSONObject();

    shard_map.put(RESOURCE_1, resourceJsonObj1);
    publisher.publish(resources, externalViews, shard_map);

    JSONObject retrieved1 = getData(RESOURCE_1);
    JSONObject shard_map_1 = new JSONObject();
    shard_map_1.put(RESOURCE_1, resourceJsonObj1);
    assertEquals(shard_map_1.toJSONString(), ((JSONObject)retrieved1.get("shard_map")).toJSONString());

    shard_map.put(RESOURCE_2, resourceJsonObj2);
    publisher.publish(resources, externalViews, shard_map);
    JSONObject retrieved_1_1 = getData(RESOURCE_1);
    JSONObject retrieved_2_1 = getData(RESOURCE_2);

    assertEquals(shard_map_1.toJSONString(), ((JSONObject)retrieved_1_1.get("shard_map")).toJSONString());

    //Ensure the first one has not changed.
    assertEquals(retrieved_1_1.toJSONString(), retrieved1.toJSONString());

    JSONObject shard_map_2 = new JSONObject();
    shard_map_2.put(RESOURCE_2, resourceJsonObj2);
    assertEquals(shard_map_2.toJSONString(), ((JSONObject)retrieved_2_1.get("shard_map")).toJSONString());


    // Make a change in resource2
    resourceJsonObj2 = new JSONObject();
    resourceJsonObj2.put("new", "value");
    JSONObject shard_map_2_2 = new JSONObject();
    shard_map_2_2.put(RESOURCE_2, resourceJsonObj2);
    shard_map.put(RESOURCE_2, resourceJsonObj2);
    publisher.publish(resources, externalViews, shard_map);
    JSONObject retrieved_2_2 = getData(RESOURCE_2);

    //Ensure that second one is on expected lines
    assertEquals(shard_map_2_2.toJSONString(), ((JSONObject)retrieved_2_2.get("shard_map")).toJSONString());
    // And it is different from posted earlier.
    assertNotEquals(retrieved_2_1.toJSONString(), retrieved_2_2.toJSONString());


    // Now ensure that the resource 2 is removed...
    shard_map.put(RESOURCE_3, resourceJsonObj3);
    shard_map.remove(RESOURCE_2);
    resources = ImmutableSet.of(RESOURCE_1, RESOURCE_3);
    externalViews = ImmutableList.of(view1, view3);
    publisher.publish(resources, externalViews, shard_map);

    try {
      getData(RESOURCE_2);
      fail("Failed to remove the data for RESOURCE_2");
    } catch (Exception e) {
      // Good.. RESOURCE_2 has been successsfully removed from zk.
    }
  }

  private JSONObject getData(String resource) throws Exception {
    String zkPath = ZkPathUtils.getClusterResourceShardMapPath(CLUSTER_NAME, resource);
    byte[] data = zkClient.getData().forPath(zkPath);
    return new ZkGZIPCompressedShardMapCodec().decode(data);
  }

  @After
  public void tearDown() throws IOException {
    zkClient.close();
    zkTestServer.stop();
  }
}

package com.pinterest.rocksplicator.shardmap;

import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.codecs.SimpleJsonObjectDecoder;

import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class JsonShardMapTest {

  @Test
  public void testJsonShardMap() throws Exception {
    JSONObject shardMapJsonObj = new JSONObject();
    List<String> resources = ImmutableList.<String>builder()
        .add("resource0")
        .add("resource1")
        .add("resource2")
        .add("resource3")
        .build();

    List<String> instances = ImmutableList
        .<String>builder()
        .add("host0:9090:az_pg")
        .add("host1:9090:az_pg")
        .add("host2:9090:az_pg")
        .add("host3:9090:az_pg")
        .build();

    int num_shards = 0;
    for (String resource : resources) {
      JSONObject resourceMapJsonObj = new JSONObject();
      ++num_shards;
      resourceMapJsonObj.put("num_shards", num_shards);

      Map<String, JSONArray> instanceToPartitionsMap = new HashMap<>();

      for (int partitionId = 0; partitionId < num_shards; ++partitionId) {
        int instanceId = IntMath.mod(partitionId, instances.size());
        String instance = instances.get(instanceId);
        JSONArray partitionsArray = null;
        if (!instanceToPartitionsMap.containsKey(instance)) {
          instanceToPartitionsMap.put(instance, new JSONArray());
        }
        partitionsArray = instanceToPartitionsMap.get(instance);

        String partitionStr = String.format("%05d", partitionId);
        partitionsArray.add(partitionStr);
      }
      resourceMapJsonObj.putAll(instanceToPartitionsMap);

      shardMapJsonObj.put(resource, resourceMapJsonObj);
    }

    String prettyPrintedJsonStr = "{\n"
        + "  \"resource2\": {\n"
        + "    \"num_shards\": 3,\n"
        + "    \"host0:9090:az_pg\": [\n"
        + "      \"00000\"\n"
        + "    ],\n"
        + "    \"host1:9090:az_pg\": [\n"
        + "      \"00001\"\n"
        + "    ],\n"
        + "    \"host2:9090:az_pg\": [\n"
        + "      \"00002\"\n"
        + "    ]\n"
        + "  },\n"
        + "  \"resource3\": {\n"
        + "    \"num_shards\": 4,\n"
        + "    \"host0:9090:az_pg\": [\n"
        + "      \"00000\"\n"
        + "    ],\n"
        + "    \"host1:9090:az_pg\": [\n"
        + "      \"00001\"\n"
        + "    ],\n"
        + "    \"host3:9090:az_pg\": [\n"
        + "      \"00003\"\n"
        + "    ],\n"
        + "    \"host2:9090:az_pg\": [\n"
        + "      \"00002\"\n"
        + "    ]\n"
        + "  },\n"
        + "  \"resource0\": {\n"
        + "    \"num_shards\": 1,\n"
        + "    \"host0:9090:az_pg\": [\n"
        + "      \"00000\"\n"
        + "    ]\n"
        + "  },\n"
        + "  \"resource1\": {\n"
        + "    \"num_shards\": 2,\n"
        + "    \"host0:9090:az_pg\": [\n"
        + "      \"00000\"\n"
        + "    ],\n"
        + "    \"host1:9090:az_pg\": [\n"
        + "      \"00001\"\n"
        + "    ]\n"
        + "  }\n"
        + "}\n";

    String compactJsonStr = "{\"resource2\":{\"num_shards\":3,\"host0:9090:az_pg\":[\"00000\"],"
        + "\"host1:9090:az_pg\":[\"00001\"],\"host2:9090:az_pg\":[\"00002\"]},"
        + "\"resource3\":{\"num_shards\":4,\"host0:9090:az_pg\":[\"00000\"],"
        + "\"host1:9090:az_pg\":[\"00001\"],\"host3:9090:az_pg\":[\"00003\"],"
        + "\"host2:9090:az_pg\":[\"00002\"]},\"resource0\":{\"num_shards\":1,"
        + "\"host0:9090:az_pg\":[\"00000\"]},\"resource1\":{\"num_shards\":2,"
        + "\"host0:9090:az_pg\":[\"00000\"],\"host1:9090:az_pg\":[\"00001\"]}}\n";

    SimpleJsonObjectDecoder decoder = new SimpleJsonObjectDecoder();
    JSONObject decodedFromCompactJsonStr = decoder.decode(compactJsonStr.getBytes());
    JSONObject decodedFromPrettyPrintedStr = decoder.decode(prettyPrintedJsonStr.getBytes());

    ShardMap firstOne = ShardMaps.fromJson(shardMapJsonObj);
    ShardMap secondOne = ShardMaps.fromJson(decodedFromCompactJsonStr);
    ShardMap thirdOne = ShardMaps.fromJson(decodedFromPrettyPrintedStr);

    assertEquals(4, firstOne.getResources().size());
    assertEquals(4, secondOne.getResources().size());
    assertEquals(4, thirdOne.getResources().size());

    assertEquals(new HashSet<>(resources), firstOne.getResources());
    assertEquals(new HashSet<>(resources), secondOne.getResources());
    assertEquals(new HashSet<>(resources), thirdOne.getResources());

    for (String resource : firstOne.getResources()) {
      ResourceMap resourceMapFirst = firstOne.getResourceMap(resource);
      ResourceMap resourceMapSecond = secondOne.getResourceMap(resource);
      ResourceMap resourceMapThird = thirdOne.getResourceMap(resource);

      assertEquals(resourceMapFirst.getAllMissingPartitions(),
          resourceMapSecond.getAllMissingPartitions());
      assertEquals(resourceMapFirst.getAllMissingPartitions(),
          resourceMapThird.getAllMissingPartitions());

      assertEquals(resourceMapFirst.getAllPartitions(), resourceMapSecond.getAllPartitions());
      assertEquals(resourceMapFirst.getAllPartitions(), resourceMapThird.getAllPartitions());

      assertEquals(resourceMapFirst.getAllKnownPartitions(),
          resourceMapSecond.getAllKnownPartitions());
      assertEquals(resourceMapFirst.getAllKnownPartitions(),
          resourceMapThird.getAllKnownPartitions());

      assertEquals(resourceMapFirst.getInstances(), resourceMapSecond.getInstances());
      assertEquals(resourceMapFirst.getInstances(), resourceMapThird.getInstances());

      for (Instance eachInstance : resourceMapFirst.getInstances()) {
        assertEquals(resourceMapFirst.getAllReplicasOnInstance(eachInstance),
            resourceMapSecond.getAllReplicasOnInstance(eachInstance));
        assertEquals(resourceMapFirst.getAllReplicasOnInstance(eachInstance),
            resourceMapThird.getAllReplicasOnInstance(eachInstance));
      }

      for (Partition partition : resourceMapFirst.getAllKnownPartitions()) {
        assertEquals(resourceMapFirst.getAllReplicasForPartition(partition),
            resourceMapSecond.getAllReplicasForPartition(partition));
        assertEquals(resourceMapFirst.getAllReplicasForPartition(partition),
            resourceMapThird.getAllReplicasForPartition(partition));
      }
    }

    ResourceMap resourceMap = firstOne.getResourceMap("resource3");
    assertEquals(4, resourceMap.getNumShards());
    assertEquals(ImmutableList.of(
            new Replica(
                new Partition("resource3_0"),
                new Instance("host0:9090:az_pg"),
                ReplicaState.ONLINE)),
        resourceMap.getAllReplicasForPartition(new Partition("resource3_0")));

    assertEquals(ImmutableList.of(
        new Replica(
            new Partition("resource3_1"),
            new Instance("host1:9090:az_pg"),
            ReplicaState.ONLINE)),
        resourceMap.getAllReplicasForPartition(new Partition("resource3", "00001")));

    assertEquals(ImmutableList.of(
        new Replica(
            new Partition("resource3_2"),
            new Instance("host2:9090:az_pg"),
            ReplicaState.ONLINE)),
        resourceMap.getAllReplicasForPartition(new Partition("resource3", 2)));
  }
}

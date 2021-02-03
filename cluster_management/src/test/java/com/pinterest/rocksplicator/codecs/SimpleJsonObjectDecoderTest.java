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

package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleJsonObjectDecoderTest {

  @Test
  public void testJsonDecoding() throws Exception {
    JSONObject shardMap = new JSONObject();
    List<String> resources = ImmutableList.<String>builder()
        .add("resource0")
        .add("resource1")
        .add("resource2")
        .add("resource3")
        .build();

    List<String> instances = ImmutableList
        .<String>builder()
        .add("host0:port:az:pg")
        .add("host1:port:az:pg")
        .add("host2:port:az:pg")
        .add("host3:port:az:pg")
        .build();

    int num_shards = 0;
    for (String resource : resources) {
      JSONObject resourceMap = new JSONObject();
      ++num_shards;
      resourceMap.put("num_shards", num_shards);

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
      resourceMap.putAll(instanceToPartitionsMap);

      shardMap.put(resource, resourceMap);
    }

    String prettyPrintedJsonStr = "{\n"
        + "  \"resource2\": {\n"
        + "    \"num_shards\": 3,\n"
        + "    \"host0:port:az:pg\": [\n"
        + "      \"00000\"\n"
        + "    ],\n"
        + "    \"host1:port:az:pg\": [\n"
        + "      \"00001\"\n"
        + "    ],\n"
        + "    \"host2:port:az:pg\": [\n"
        + "      \"00002\"\n"
        + "    ]\n"
        + "  },\n"
        + "  \"resource3\": {\n"
        + "    \"num_shards\": 4,\n"
        + "    \"host0:port:az:pg\": [\n"
        + "      \"00000\"\n"
        + "    ],\n"
        + "    \"host1:port:az:pg\": [\n"
        + "      \"00001\"\n"
        + "    ],\n"
        + "    \"host3:port:az:pg\": [\n"
        + "      \"00003\"\n"
        + "    ],\n"
        + "    \"host2:port:az:pg\": [\n"
        + "      \"00002\"\n"
        + "    ]\n"
        + "  },\n"
        + "  \"resource0\": {\n"
        + "    \"num_shards\": 1,\n"
        + "    \"host0:port:az:pg\": [\n"
        + "      \"00000\"\n"
        + "    ]\n"
        + "  },\n"
        + "  \"resource1\": {\n"
        + "    \"num_shards\": 2,\n"
        + "    \"host0:port:az:pg\": [\n"
        + "      \"00000\"\n"
        + "    ],\n"
        + "    \"host1:port:az:pg\": [\n"
        + "      \"00001\"\n"
        + "    ]\n"
        + "  }\n"
        + "}\n";

    String compactJsonStr = "{\"resource2\":{\"num_shards\":3,\"host0:port:az:pg\":[\"00000\"],"
        + "\"host1:port:az:pg\":[\"00001\"],\"host2:port:az:pg\":[\"00002\"]},"
        + "\"resource3\":{\"num_shards\":4,\"host0:port:az:pg\":[\"00000\"],"
        + "\"host1:port:az:pg\":[\"00001\"],\"host3:port:az:pg\":[\"00003\"],"
        + "\"host2:port:az:pg\":[\"00002\"]},\"resource0\":{\"num_shards\":1,"
        + "\"host0:port:az:pg\":[\"00000\"]},\"resource1\":{\"num_shards\":2,"
        + "\"host0:port:az:pg\":[\"00000\"],\"host1:port:az:pg\":[\"00001\"]}}\n";

    SimpleJsonObjectDecoder decoder = new SimpleJsonObjectDecoder();
    JSONObject decodedFromPrettyPrintedStr = decoder.decode(prettyPrintedJsonStr.getBytes());
    JSONObject decodedFromCompactJsonStr = decoder.decode(compactJsonStr.getBytes());

    /**
     * Note that we can't directly compare JSONObjects since we saw an issue where a numeric
     * value was being returned as Integer or Long object. Due to this, the test for equality
     * directly on the JSONObject is flaky.
     */
    assertEquals(shardMap.toJSONString(), decodedFromPrettyPrintedStr.toJSONString());
    assertEquals(shardMap.toJSONString(), decodedFromCompactJsonStr.toJSONString());
  }
}

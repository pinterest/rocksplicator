/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.config;

import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.bean.ConsistentHashRingBean;
import com.pinterest.rocksplicator.controller.bean.ConsistentHashRingsBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ConsistentHashRingsConfigParserTest {

  @Test
  public void testParseConsistentHashRingsConfig() {
    final String config = "{" +
        "  \"us-east-1a_0\": {" +
        "    \"hosts\": [" +
        "      \"127.1.1.1:8091\"," +
        "      \"127.1.1.2:8092\"" +
        "     ]," +
        "    \"hash_salt\": \"us-east-1a_0\"" +
        "  }," +
        "  \"us-east-1a_1\": {" +
        "    \"hosts\": [" +
        "      \"127.2.2.2:8093\"," +
        "      \"127.2.2.3:8094\"" +
        "     ]," +
        "    \"hash_salt\": \"us-east-1a_1\"" +
        "  }," +
        "  \"us-east-1b_0\": {" +
        "    \"hosts\": [" +
        "      \"127.3.3.3:8091\"," +
        "      \"127.3.3.4:8092\"" +
        "     ]," +
        "    \"hash_salt\": \"us-east-1b_0\"" +
        "  }" +
        "}";
    ConsistentHashRingsBean consistentHashRingsBean =
        ConsistentHashRingsConfigParser.parseConsistentHashRingsConfig(
            new Cluster("namespace", "cluster"), config.getBytes());
    Assert.assertEquals(consistentHashRingsBean.getCluster(), new Cluster("namespace", "cluster"));
    HashMap<String, List<String>> expectedRings = new HashMap<>();
    expectedRings.put("us-east-1a_0", Arrays.asList("127.1.1.1:8091", "127.1.1.2:8092"));
    expectedRings.put("us-east-1a_1", Arrays.asList("127.2.2.2:8093", "127.2.2.3:8094"));
    expectedRings.put("us-east-1b_0", Arrays.asList("127.3.3.3:8091", "127.3.3.4:8092"));
    for (ConsistentHashRingBean consistentHashRingBean :
        consistentHashRingsBean.getConsistentHashRings()) {
      String name = consistentHashRingBean.getName();
      Assert.assertTrue(expectedRings.containsKey(name));
      Assert.assertEquals(consistentHashRingBean.getHashSalt(), name);
      ArrayList<String> hostsList = new ArrayList<>();
      for (HostBean hostBean : consistentHashRingBean.getHosts()) {
        hostsList.add(hostBean.getIp() + ":" + hostBean.getPort());
      }
      Collections.sort(hostsList);
      Assert.assertEquals(hostsList, expectedRings.get(name));
    }
  }
}

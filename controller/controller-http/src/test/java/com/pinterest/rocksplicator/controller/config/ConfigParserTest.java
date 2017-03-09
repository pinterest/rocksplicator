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

import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.Role;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.bean.ShardBean;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;


/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class ConfigParserTest {

  @DataProvider(name = "hostInfo")
  public Object[][] createHostInfo() {
    return new Object[][] {
        {"127.0.0.1:9090:us-east-1a", "127.0.0.1", 9090, "us-east-1a"},
        {"127.0.0.1:65535:zone_b", "127.0.0.1", 65535, "zone_b"},
        {"10.0.0.1:80", "10.0.0.1", 80, "unknown"}
    };
  }

  @DataProvider(name = "badHostInfo")
  public Object[][] createBadHostInfo() {
    return new Object[][] {
        {""},
        {"127.0.0.1"},
        {"127.0.0.1:"},
        {"127.0.0.1:not_an_integer"},
        {"127.0.0.1:2147483648:zone_b"},
        {"127.0.0.1:123:zone_c:zone_d"},
    };
  }

  @DataProvider(name ="shardInfo")
  public Object[][] createShardInfo() {
    return new Object[][] {
        {"00000", 0, Role.MASTER},
        {"00001:M", 1, Role.MASTER},
        {"12345:S", 12345, Role.SLAVE}
    };
  }

  @DataProvider(name = "badShardInfo")
  public Object[][] createBadShardInfo() {
    return new Object[][] {
        {""},
        {"not_an_integer"},
        {"00123:X"},
        {"00123:M:S"},
    };
  }

  @Test(dataProvider = "hostInfo")
  public void testParseHost(String hostInfo, String hostIp, int port, String az) {
    HostBean bean = ConfigParser.parseHost(hostInfo);
    Assert.assertEquals(bean.getIp(), hostIp);
    Assert.assertEquals(bean.getPort(), port);
    Assert.assertEquals(bean.getAvailabilityZone(), az);
  }

  @Test(dataProvider = "badHostInfo", expectedExceptions = IllegalArgumentException.class)
  public void testParseHostBad(String hostInfo) {
    ConfigParser.parseHost(hostInfo);
  }

  @Test(dataProvider = "shardInfo")
  public void testParseShard(String shardInfo, int shardId, Role role) {
    ShardBean bean = ConfigParser.parseShard(shardInfo);
    Assert.assertEquals(bean.getId(), shardId);
    Assert.assertEquals(bean.getRole(), role);
  }

  @Test(dataProvider = "badShardInfo", expectedExceptions = IllegalArgumentException.class)
  public void testParseShardBad(String shardInfo) {
    ConfigParser.parseShard(shardInfo);
  }

  @Test
  public void testParseClusterConfig() {
    final String config =
        "{" +
        "  \"user_pins\": {" +
        "    \"num_shards\": 3," +
        "    \"127.0.0.1:8090\": [\"00000\", \"00001\", \"00002\"]," +
        "    \"127.0.0.1:8091\": [\"00002\"]," +
        "    \"127.0.0.1:8092\": [\"00002\"]" +
        "   }," +
        "  \"interest_pins\": {" +
        "  \"num_shards\": 2," +
        "  \"127.0.0.1:8090\": [\"00000\"]," +
        "  \"127.0.0.1:8091\": [\"00001\"]" +
        "   }" +
        "}";

    ClusterBean bean = ConfigParser.parseClusterConfig("test", config.getBytes());
    Assert.assertNotNull(bean);
    Assert.assertEquals(bean.getName(), "test");
    Assert.assertEquals(bean.getSegments().size(), 2);

    // user_pins
    SegmentBean userPins = findSegment(bean.getSegments(), "user_pins").get();
    Assert.assertEquals(userPins.getNumShards(), 3);
    Assert.assertEquals(userPins.getHosts().size(), 3);
    // 127.0.0.1:8090
    HostBean host = findHost(userPins.getHosts(), "127.0.0.1", 8090).get();
    Assert.assertEquals(host.getAvailabilityZone(), "unknown");
    Assert.assertEquals(host.getShards().size(), 3);
    ShardBean shard = findShard(host.getShards(), 0).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    shard = findShard(host.getShards(), 1).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    shard = findShard(host.getShards(), 2).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    //127.0.1:8091
    host = findHost(userPins.getHosts(), "127.0.0.1", 8091).get();
    Assert.assertEquals(host.getAvailabilityZone(), "unknown");
    Assert.assertEquals(host.getShards().size(), 1);
    shard = findShard(host.getShards(), 2).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    //127.0.1:8092
    host = findHost(userPins.getHosts(), "127.0.0.1", 8092).get();
    Assert.assertEquals(host.getAvailabilityZone(), "unknown");
    Assert.assertEquals(host.getShards().size(), 1);
    shard = findShard(host.getShards(), 2).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);

    // interest_pins
    SegmentBean interestPins = findSegment(bean.getSegments(), "interest_pins").get();
    Assert.assertEquals(interestPins.getNumShards(), 2);
    Assert.assertEquals(interestPins.getHosts().size(), 2);
    // 127.0.0.1:8090
    host = findHost(interestPins.getHosts(), "127.0.0.1", 8090).get();
    Assert.assertEquals(host.getAvailabilityZone(), "unknown");
    Assert.assertEquals(host.getShards().size(), 1);
    shard = findShard(host.getShards(), 0).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    //127.0.1:8091
    host = findHost(interestPins.getHosts(), "127.0.0.1", 8091).get();
    Assert.assertEquals(host.getAvailabilityZone(), "unknown");
    Assert.assertEquals(host.getShards().size(), 1);
    shard = findShard(host.getShards(), 1).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
  }

  @Test
  public void testParseClusterConfig2() {
    final String config =
        "{" +
        "  \"user_pins\": {" +
        "  \"num_shards\": 3," +
        "  \"127.0.0.1:8090:us-east-1a\": [\"00000:M\", \"00001:S\", \"00002:S\"]," +
        "  \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"]," +
        "  \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]" +
        "   }" +
        "}";

    ClusterBean cluster = ConfigParser.parseClusterConfig("test2", config.getBytes());
    Assert.assertNotNull(cluster);
    Assert.assertEquals(cluster.getName(), "test2");
    Assert.assertEquals(cluster.getSegments().size(), 1);

    SegmentBean userPins = findSegment(cluster.getSegments(), "user_pins").get();
    Assert.assertEquals(userPins.getNumShards(), 3);
    // 127.0.0.1:8090
    HostBean host = findHost(userPins.getHosts(), "127.0.0.1", 8090).get();
    Assert.assertEquals(host.getAvailabilityZone(), "us-east-1a");
    ShardBean shard = findShard(host.getShards(), 0).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    shard = findShard(host.getShards(), 1).get();
    Assert.assertEquals(shard.getRole(), Role.SLAVE);
    shard = findShard(host.getShards(), 2).get();
    Assert.assertEquals(shard.getRole(), Role.SLAVE);
    // 127.0.0.1:8090
    host = findHost(userPins.getHosts(), "127.0.0.1", 8091).get();
    Assert.assertEquals(host.getAvailabilityZone(), "us-east-1c");
    shard = findShard(host.getShards(), 0).get();
    Assert.assertEquals(shard.getRole(), Role.SLAVE);
    shard = findShard(host.getShards(), 1).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
    shard = findShard(host.getShards(), 2).get();
    Assert.assertEquals(shard.getRole(), Role.SLAVE);
    // 127.0.0.1:8090
    host = findHost(userPins.getHosts(), "127.0.0.1", 8092).get();
    Assert.assertEquals(host.getAvailabilityZone(), "us-east-1e");
    shard = findShard(host.getShards(), 0).get();
    Assert.assertEquals(shard.getRole(), Role.SLAVE);
    shard = findShard(host.getShards(), 1).get();
    Assert.assertEquals(shard.getRole(), Role.SLAVE);
    shard = findShard(host.getShards(), 2).get();
    Assert.assertEquals(shard.getRole(), Role.MASTER);
  }

  private Optional<SegmentBean> findSegment(List<SegmentBean> segments, String segmentName) {
    return segments.stream().filter(s -> s.getName().equals(segmentName)).findAny();
  }

  private Optional<HostBean> findHost(List<HostBean> hosts, String ip, int port) {
    return hosts.stream().filter(h -> h.getIp().equals(ip) && h.getPort() == port).findAny();
  }

  private Optional<ShardBean> findShard(List<ShardBean> shards, int id) {
    return shards.stream().filter(s -> s.getId() == id).findAny();
  }
}

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public final class ConfigParser {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigParser.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DELIMITER = ":";
  private static final String NUM_SHARDS = "num_shards";
  private static final String UNKNOWN_AZ = "unknown";

  private ConfigParser() {
  }

  /**
   * Convert cluster config data into a {@link ClusterBean}.
   *
   * @param clusterName name of the cluster
   * @param content binary config data
   * @return ClusterBean or null if parsing failed
   */
  @SuppressWarnings("unchecked")
  public static ClusterBean parseClusterConfig(String clusterName, byte[] content) {
    try {
      Map<String, Object> segmentMap =
          OBJECT_MAPPER.readValue(new String(content, UTF_8), HashMap.class);

      final List<SegmentBean> segments = new ArrayList<>();
      for (Map.Entry<String, Object> entry : segmentMap.entrySet()) {
        Map<String, Object> segmentInfo = (Map<String, Object>)entry.getValue();
        // num_leaf_segments must exist so that we can proceed
        if (!segmentInfo.containsKey(NUM_SHARDS)) { return null; }

        final SegmentBean segment = new SegmentBean()
            .setName(entry.getKey())
            .setNumShards((Integer)segmentInfo.get(NUM_SHARDS));

        final List<HostBean> hosts = new ArrayList<>();
        for (Map.Entry<String, Object> entry2 : segmentInfo.entrySet()) {
          // skip num_leaf_segments in shard map
          if (entry2.getKey().equals(NUM_SHARDS)) { continue; }

          HostBean host = parseHost(entry2.getKey());
          List<String> shardList = (List<String>)entry2.getValue();
          List<ShardBean> shards = shardList.stream().map(ConfigParser::parseShard).collect(Collectors.toList());
          host.setShards(shards);
          hosts.add(host);
        }
        segment.setHosts(hosts);
        segments.add(segment);
      }
      return new ClusterBean().setName(clusterName).setSegments(segments);
    } catch (IOException | IllegalArgumentException e) {
      LOG.error("Failed to parse cluster config.", e);
      return null;
    }
  }

  @VisibleForTesting
  static HostBean parseHost(String hostInfo) throws IllegalArgumentException {
    String[] split = hostInfo.split(DELIMITER);
    if (split.length < 2 || split.length > 3) {
      throw new IllegalArgumentException("Invalid hostInfo format: " + hostInfo);
    }
    return new HostBean()
        .setIp(split[0])
        .setPort(Integer.valueOf(split[1]))
        .setAvailabilityZone(split.length == 3 ? split[2] : UNKNOWN_AZ);
  }

  @VisibleForTesting
  static ShardBean parseShard(String shardInfo) throws IllegalArgumentException {
    String[] split = shardInfo.split(DELIMITER);
    if (split.length < 1 || split.length > 2) {
      throw new IllegalArgumentException("Invalid shardInfo format: " + shardInfo);
    }
    final Role role;
    if (split.length < 2 || split[1].equals("M")) {
      role = Role.MASTER;
    } else if (split[1].equals("S")) {
      role = Role.SLAVE;
    } else {
      throw new IllegalArgumentException("Unknown role in shardInfo: " + shardInfo);
    }
    return new ShardBean()
        .setId(Integer.valueOf(split[0]))
        .setRole(role);
  }
}

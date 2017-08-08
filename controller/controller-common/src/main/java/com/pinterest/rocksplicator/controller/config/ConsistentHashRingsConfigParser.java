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

import com.pinterest.rocksplicator.controller.bean.ConsistentHashRingBean;
import com.pinterest.rocksplicator.controller.bean.ConsistentHashRingsBean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * This is a parser used to handle consistent hash ring based configuration,
 * for example used by scorpion.
 *
 * Example Config:
 * {
 *   us-east-1a_0: {
 *    hosts: [
 *      127.0.0.1:8091,
 *      127.0.0.1:8092
 *    ],
 *    hash_salt: us-east-1a_0
 *  },
 *  us-east-1a_1: {
 *    hosts: [
 *      127.0.0.1:8093,
 *      127.0.0.1:8094,
 *      127.0.0.1:8095
 *     ],
 *     hash_salt: us-east-1a_1
 *  },
 *  us-east-1b_0: {
 *     hosts: [
 *       127.0.0.1:8096,
 *       127.0.0.1:8097,
 *       127.0.0.1:8098,
 *       127.0.0.1:8099
 *     ],
 *     hash_salt: us-east-1b_0
 *   }
 * };
 *
 * @author shu (shu@pinterest.com)
 *
 */
public class ConsistentHashRingsConfigParser {
  private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashRingsConfigParser.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String HOST_KEY = "hosts";
  private static final String HASH_SALT_KEY = "hash_salt";

  private ConsistentHashRingsConfigParser() {}

  @SuppressWarnings("unchecked")
  public static ConsistentHashRingsBean parseConsistentHashRingsConfig(
      String clusterName, byte[] content) {
    try {
      Map<String, Object> ringsMap = OBJECT_MAPPER.readValue(
          new String(content, UTF_8), HashMap.class);
      List<ConsistentHashRingBean> rings = new ArrayList<>();
      for (Map.Entry<String, Object> entry : ringsMap.entrySet()) {
        Map<String, Object> ringInfo = (Map<String, Object>)entry.getValue();
        String hashSalt = (String) ringInfo.getOrDefault(HASH_SALT_KEY, "");
        List<String> hostsString= (List<String>)
            ringInfo.getOrDefault(HOST_KEY, new ArrayList<String>());
        final List<HostBean> hosts = new ArrayList<>();
        for (String hostString : hostsString) {
          HostBean host = ConfigParser.parseHost(hostString);
          hosts.add(host);
        }
        final ConsistentHashRingBean consistentHashRingBean =
            new ConsistentHashRingBean()
                .setHashSalt(hashSalt)
                .setHosts(hosts)
                .setName(entry.getKey());
        rings.add(consistentHashRingBean);
      }
      return new ConsistentHashRingsBean().setConsistentHashRings(rings).setName(clusterName);
    } catch (IOException e) {
      LOG.error("Failed to parse consistent hash ring config.", e);
      return null;
    }
  }


}

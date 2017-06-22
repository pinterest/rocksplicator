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

package com.pinterest.rocksplicator.controller.util;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Parse the zookeeper host config file that consists of a list of ZK endpoints..
 * @author Shu Zhang (shu@pinterest.com)
 */
public class ZookeeperConfigParser {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConfigParser.class);

  // Parse endpoints separated by commas
  public static String parseEndpoints(String zkHostsFile, String zkCluster) {
    Yaml yaml = new Yaml();
    try {
      String cluster = "default";
      if (!Strings.isNullOrEmpty(zkCluster)) {
        cluster = zkCluster;
      }
      String content = Files.toString(new File(zkHostsFile), Charsets.US_ASCII);
      Map<String, Map<String, List<String>>> config = (Map) yaml.load(content);
      List<String> endpoints = config.get("clusters").get(cluster);
      return String.join(",", endpoints);
    } catch (Exception e) {
      LOG.error("Cannot parse Zookeeper endpoints!", e);
      return null;
    }
  }
}

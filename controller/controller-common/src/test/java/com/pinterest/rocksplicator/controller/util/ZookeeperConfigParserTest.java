/*
 * Copyright 2017 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.rocksplicator.controller.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ZookeeperConfigParserTest {

  @Test
  public void testParseZookeeperConfig() {
    String path = Thread.currentThread().getContextClassLoader().getResource("zookeeper_hosts.conf").getPath();
    String result = ZookeeperConfigParser.parseEndpoints(path, "default");
    Assert.assertEquals("observerzookeeper010:2181,observerzookeeper011:2181," +
        "observerzookeeper012:2181,observerzookeeper013:2181,observerzookeeper014:2181", result);
  }
}

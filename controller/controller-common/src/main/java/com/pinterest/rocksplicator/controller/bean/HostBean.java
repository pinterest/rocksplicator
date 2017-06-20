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

package com.pinterest.rocksplicator.controller.bean;

import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class HostBean {

  private static final Logger LOG = LoggerFactory.getLogger(HostBean.class);

  @NotEmpty
  /** host ip */
  private String ip;

  @Min(0)
  @Max(65535)
  /** host port */
  private int port;

  @NotEmpty
  /** availability zone */
  private String availabilityZone;

  /** shards managed by this host */
  private List<ShardBean> shards = Collections.emptyList();

  public String getIp() {
    return ip;
  }

  public HostBean setIp(String ip) {
    this.ip = ip;
    return this;
  }

  public int getPort() {
    return port;
  }

  public HostBean setPort(int port) {
    this.port = port;
    return this;
  }

  public String getAvailabilityZone() {
    return availabilityZone;
  }

  public HostBean setAvailabilityZone(String az) {
    this.availabilityZone = az;
    return this;
  }

  public List<ShardBean> getShards() {
    return new ArrayList<>(shards);
  }

  public HostBean setShards(List<ShardBean> shards) {
    this.shards = Collections.unmodifiableList(shards);
    return this;
  }

  @Override
  public String toString() {
    return ip + ":" + port + ":" + availabilityZone;
  }

  public static HostBean fromUrlParam(String queryParam) {
    // assuming the string is in the format like 127-0-0-1-9090-us-east-1a or
    // 127-0-0-1-9090
    try {
      String[] parts = queryParam.split("-", 6);
      if (parts.length < 5 || parts.length > 6) {
        return null;
      }
      String ip = String.join(".", Arrays.copyOfRange(parts, 0, 4));
      String zone = "";
      if (parts.length == 6) {
        zone = parts[5];
      }
      HostBean hostBean =
          new HostBean().setIp(ip).setPort(Integer.valueOf(parts[4])).setAvailabilityZone(zone);
      return hostBean;
    } catch (Exception e) {
      LOG.error("Invalid query param for creating host bean: " + queryParam, e);
      return null;
    }
  }
}

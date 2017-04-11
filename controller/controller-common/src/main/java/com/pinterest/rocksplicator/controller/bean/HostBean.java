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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class HostBean {

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
}

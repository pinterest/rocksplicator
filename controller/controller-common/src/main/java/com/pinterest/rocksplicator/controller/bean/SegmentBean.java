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
import javax.validation.constraints.Min;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class SegmentBean {

  @NotEmpty
  /** name of the segment */
  private String name;

  @Min(1)
  /** number of shards */
  private int numShards;

  /** list of hosts serve this segment */
  private List<HostBean> hosts = Collections.emptyList();

  public String getName() {
    return name;
  }

  public SegmentBean setName(String name) {
    this.name = name;
    return this;
  }

  public int getNumShards() {
    return numShards;
  }

  public SegmentBean setNumShards(int numShards) {
    this.numShards = numShards;
    return this;
  }

  public List<HostBean> getHosts() {
    return new ArrayList<>(hosts);
  }

  public SegmentBean setHosts(List<HostBean> hosts) {
    this.hosts = Collections.unmodifiableList(hosts);
    return this;
  }
}

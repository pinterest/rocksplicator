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

/**
 * Mapping to another type of config: a collection of consistent hash rings.
 *
 * @author shu (shu@pinterest.com)
 */
public class ConsistentHashRingsBean {

  @NotEmpty
  private String name;

  private List<ConsistentHashRingBean> consistentHashRings = Collections.emptyList();

  public String getName() {
    return name;
  }

  public ConsistentHashRingsBean setName(String name) {
    this.name = name;
    return this;
  }

  public List<ConsistentHashRingBean> getConsistentHashRings() {
    return new ArrayList<>(consistentHashRings);
  }

  public ConsistentHashRingsBean setConsistentHashRings(
      List<ConsistentHashRingBean> consistentHashRings) {
    this.consistentHashRings = consistentHashRings;
    return this;
  }

  @Override
  public String toString() {
    return name;
  }

}

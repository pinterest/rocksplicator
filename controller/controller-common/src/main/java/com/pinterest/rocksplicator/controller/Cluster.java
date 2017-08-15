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

package com.pinterest.rocksplicator.controller;

/**
 * Storing cluster metadata (not including detailed segment/shard info)
 *
 * @author shu (shu@pinterest.com)
 */
public class Cluster {
  private String namespace;
  private String name;

  public Cluster(final String namespace, final String name) {
    this.namespace = namespace;
    this.name = name;
  }

  public String getNamespace() {
    return namespace;
  }

  public Cluster setNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public String getName() {
    return name;
  }

  public Cluster setName(String name) {
    this.name = name;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Cluster cluster = (Cluster) o;

    if (namespace != null ? !namespace.equals(cluster.namespace) : cluster.namespace != null)
      return false;
    return name != null ? name.equals(cluster.name) : cluster.name == null;
  }

  @Override
  public int hashCode() {
    int result = namespace != null ? namespace.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return namespace + "/" + name;
  }
}

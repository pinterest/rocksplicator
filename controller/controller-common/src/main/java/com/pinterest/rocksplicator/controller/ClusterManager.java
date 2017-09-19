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

package com.pinterest.rocksplicator.controller;

import com.pinterest.rocksplicator.controller.bean.HostBean;

import java.util.Collections;
import java.util.Set;

/**
 *
 * This class handles hosts level management of clusters.
 *
 * @author shu (shu@pinterest.com)
 */
public interface ClusterManager {

  /**
   * Create the cluster in ClusterManager's world.
   * @param cluster
   * @return
   */
  default boolean createCluster(final Cluster cluster) {
    return true;
  }


  /**
   * Register a host to the cluster.
   * @param cluster
   * @return true if registration is successful.
   */
  default boolean registerToCluster(final Cluster cluster, final HostBean hostBean) {
    return true;
  }

  /**
   * Mark the cluster not belonging to the cluster any more.
   * @param cluster
   * @param hostBean
   * @return
   */
  default boolean unregisterFromCluster(final Cluster cluster, final HostBean hostBean) {
    return true;
  }

  /**
   * Sill keep the host in the cluster, but mark it as blacklisted.
   * @param cluster
   * @param hostBean
   * @return
   */
  default boolean blacklistHost(final Cluster cluster, final HostBean hostBean) {
    return true;
  }

  /**
   * Get all hosts in the cluster
   * @param cluster
   * @return
   */
  default Set<HostBean> getHosts(final Cluster cluster, boolean excludeBlacklisted) {
    return Collections.emptySet();
  }

  /**
   * Get all blacklisted hosts
   * @param cluster
   * @return
   */
  default Set<HostBean> getBlacklistedHosts(final Cluster cluster) {
    return Collections.emptySet();
  }

}

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

package com.pinterest.rocksplicator.controller.mysql;

import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.ClusterManager;
import com.pinterest.rocksplicator.controller.bean.HostBean;

import java.util.Collections;
import java.util.List;

/**
 * @author shu (shu@pinterest.com)
 */
public class MySQLClusterManager extends MySQLBase implements ClusterManager{

  public MySQLClusterManager(String jdbcUrl, String dbUser, String dbPassword) {
    super(jdbcUrl, dbUser, dbPassword);
  }

  @Override
  public boolean registerToCluster(final Cluster cluster, final HostBean hostBean) {
    return true;
  }

  @Override
  public boolean unregisterFromCluster(final Cluster cluster, final HostBean hostBean) {
    return true;
  }

  @Override
  public boolean blacklistHost(final Cluster cluster, final HostBean hostBean) {
    return true;
  }

  @Override
  public List<HostBean> getHosts(final Cluster cluster, boolean excludeBlacklisted) {
    return Collections.emptyList();
  }

  @Override
  public List<HostBean> getBlacklistedHosts(final Cluster cluster) {
    return Collections.emptyList();
  }

}

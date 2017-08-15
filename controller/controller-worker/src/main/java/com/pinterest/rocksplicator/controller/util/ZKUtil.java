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

import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.config.ConfigParser;
import com.pinterest.rocksplicator.controller.WorkerConfig;

import org.apache.curator.framework.CuratorFramework;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public final class ZKUtil {

  private ZKUtil() {
  }

  public static String getClusterConfigZKPath(Cluster cluster) {
    return WorkerConfig.getZKPath() + cluster.getNamespace() + "/" + cluster.getName();
  }

  /**
   * Get config for a given cluster from Zookeeper.
   *
   * @param zkClient zookeeper client to use
   * @param cluster the cluster
   * @return serialized cluster config, or null if there is an error
   */
  public static ClusterBean getClusterConfig(CuratorFramework zkClient,
                                             Cluster cluster) throws Exception {
    if (zkClient.checkExists().forPath(getClusterConfigZKPath(cluster)) == null) {
      return null;
    }

    byte[] data = zkClient.getData().forPath(ZKUtil.getClusterConfigZKPath(cluster));
    return ConfigParser.parseClusterConfig(cluster, data);
  }

  /**
   * Update config in zookeeper for a given cluster.
   *
   * @param zkClient zookeeper client to use
   * @param clusterBean config of the cluster
   */
  public static void updateClusterConfig(CuratorFramework zkClient, ClusterBean clusterBean)
      throws Exception {
    zkClient.setData().forPath(
        getClusterConfigZKPath(clusterBean.getCluster()),
        ConfigParser.serializeClusterConfig(clusterBean).getBytes()
    );
  }
}

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

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.bean.ShardBean;
import com.pinterest.rocksplicator.controller.config.ConfigParser;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;


/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class HealthCheckTask extends TaskBase<HealthCheckTask.Param> {

  public static final Logger LOG = LoggerFactory.getLogger(HealthCheckTask.class);

  @Inject
  public CuratorFramework zkClient;

  @Inject
  public AdminClientFactory clientFactory;

  public HealthCheckTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final String clusterName = ctx.getCluster();
    final String zkPath = getParameter().getZkPath();

    try {
      byte[] data = zkClient.getData().forPath(zkPath + clusterName);
      ClusterBean clusterBean = ConfigParser.parseClusterConfig(clusterName, data);

      Set<InetSocketAddress> hosts = new HashSet<>();
      for (SegmentBean segmentBean : clusterBean.getSegments()) {
        for (HostBean hostBean : segmentBean.getHosts()) {
          hosts.add(new InetSocketAddress(hostBean.getIp(), hostBean.getPort()));
        }
        // check segment replica
        checkSegment(segmentBean, getParameter().getNumReplicas());
      }

      // ping all hosts
      Set<String> badHosts = new HashSet<>();
      for (InetSocketAddress hostAddr : hosts) {
        try {
          clientFactory.getClient(hostAddr).ping();
        } catch (TException tex) {
          // record bad host
          badHosts.add(hostAddr.toString());
        }
      }

      if (!badHosts.isEmpty()) {
        ctx.getTaskQueue().failTask(ctx.getId(),
            String.format("Unable to ping hosts: %s", badHosts));
        return;
      }

      LOG.info("All hosts are good");
      ctx.getTaskQueue().finishTask(ctx.getId(),
          String.format("Cluster %s is healthy", clusterName));
    } catch (Exception ex) {
      ctx.getTaskQueue().failTask(ctx.getId(),
          String.format("Cluster %s is unhealthy, reason = %s", clusterName, ex.getMessage()));
    }
  }


  /**
   * Count the number of replicas for each shard within a segment.
   * Throws exception is the number doesn't match the expected one.
   */
  private void checkSegment(SegmentBean segment, int numReplicas) throws Exception {
    Map<Integer, Integer> shardCount = new HashMap<>();

    for (HostBean hostBean : segment.getHosts()) {
      for (ShardBean shardBean : hostBean.getShards()) {
        int cnt = shardCount.getOrDefault(shardBean.getId(), 0);
        shardCount.put(shardBean.getId(), cnt + 1);
      }
    }

    if (shardCount.size() != segment.getNumShards()) {
      throw new Exception(
          String.format("Incorrect number of shards. Expected %d but actually %d.",
              segment.getNumShards(), shardCount.size()));
    }

    Map<Integer, Integer> badShards = new HashMap<>();
    for (Map.Entry<Integer, Integer> entry : shardCount.entrySet()) {
      if (entry.getValue() != numReplicas) {
        badShards.put(entry.getKey(), entry.getValue());
      }
    }

    if (!badShards.isEmpty()) {
      throw new Exception("Incorrect number of replicas. Bad shards: " + badShards.toString());
    }
  }

  public static class Param extends Parameter {

    @JsonProperty
    private String zkPath;

    @JsonProperty
    private int numReplicas;


    public String getZkPath() {
      return zkPath;
    }

    public Param setZkPath(String zkPath) {
      this.zkPath = zkPath;
      return this;
    }

    public int getNumReplicas() {
      return numReplicas;
    }

    public Param setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }
  }
}

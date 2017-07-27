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

import com.pinterest.rocksplicator.controller.WorkerConfig;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.EmailSender;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;


/**
 * This task performs health-check for a given cluster. Things it checks on are:
 *
 * 1) whether all segments and shards have the expected number of replicas
 * 2) whether all hosts are up and running
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class HealthCheckTask extends AbstractTask<HealthCheckTask.Param> {

  public static final Logger LOG = LoggerFactory.getLogger(HealthCheckTask.class);

  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  @Inject
  private EmailSender emailSender;

  /**
   * Construct a new HealthCheckTask with number of replicas equals to 3
   */
  public HealthCheckTask() { this(new HealthCheckTask.Param());}

  public HealthCheckTask(HealthCheckTask.Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) {
    final String clusterName = ctx.getCluster();
    try {
      ClusterBean clusterBean = ZKUtil.getClusterConfig(
          zkClient, getParameter().getZkPrefix(), clusterName);
      if (clusterBean == null) {
        ctx.getTaskQueue().failTask(ctx.getId(), "Failed to read cluster config from zookeeper.");
        return;
      }

      Set<InetSocketAddress> hosts = new HashSet<>();
      for (SegmentBean segmentBean : clusterBean.getSegments()) {
        for (HostBean hostBean : segmentBean.getHosts()) {
          hosts.add(new InetSocketAddress(hostBean.getIp(), hostBean.getPort()));
        }
      }

      // ping all hosts
      Set<String> badHosts = new HashSet<>();
      for (InetSocketAddress hostAddr : hosts) {
        try {
          clientFactory.getClient(hostAddr).ping();
        } catch (TException | ExecutionException ex) {
          // record bad host
          badHosts.add(hostAddr.toString());
        }
      }

      if (!badHosts.isEmpty()) {
        String errorMessage = String.format("Unable to ping hosts: %s", badHosts);
        ctx.getTaskQueue().failTask(ctx.getId(),errorMessage);
        emailSender.sendEmail("Healthcheck Failed for " + clusterName, errorMessage);
        return;
      }

      LOG.info("All hosts are good");
      String output = String.format("Cluster %s is healthy", clusterName);
      ctx.getTaskQueue().finishTask(ctx.getId(), output);
    } catch (Exception ex) {
      String errorMessage =
          String.format("Cluster %s is unhealthy, reason = %s", clusterName, ex.getMessage());
      ctx.getTaskQueue().failTask(ctx.getId(), errorMessage);
      emailSender.sendEmail("Healthcheck Failed for " + clusterName, errorMessage);
    }
  }

  public static class Param extends Parameter {
    @JsonProperty
    private String zkPrefix = WorkerConfig.getZKPath();

    public String getZkPrefix() { return zkPrefix; }

    public Param setZkPrefix(String zkPrefix) {
      this.zkPrefix= zkPrefix;
      return this;
    }
  }
}

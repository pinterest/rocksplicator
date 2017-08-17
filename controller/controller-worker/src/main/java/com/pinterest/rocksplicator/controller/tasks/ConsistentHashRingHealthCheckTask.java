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

import com.pinterest.rocksplicator.controller.bean.ConsistentHashRingBean;
import com.pinterest.rocksplicator.controller.bean.ConsistentHashRingsBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.EmailSender;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * This task performs health-check for a given cluster using consistent hash ring config.
 * Things it checks on are:
 *
 * 1) whether all hosts are up and running
 *
 * @author shu (shu@pinterest.com)
 */
public class ConsistentHashRingHealthCheckTask extends AbstractTask<Parameter> {

  public static final Logger LOG = LoggerFactory.getLogger(ConsistentHashRingHealthCheckTask.class);

  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  @Inject
  private EmailSender emailSender;

  public ConsistentHashRingHealthCheckTask() { this(new Parameter()); }

  public ConsistentHashRingHealthCheckTask(Parameter parameter) {
    super(parameter);
  }

  @Override
  public void process(Context ctx) throws Exception {
    try {
      ConsistentHashRingsBean consistentHashRingsBean =
          ZKUtil.getConsistentHashRingsConfig(zkClient, ctx.getCluster());
      if (consistentHashRingsBean == null) {
        String errorMessage = String.format(
            "Failed to read cluster config from zookeeper: %s", ctx.getCluster());
        ctx.getTaskQueue().failTask(ctx.getId(), errorMessage);
        emailSender.sendEmail("Healthcheck Failed for " + ctx.getCluster(), errorMessage);
        return;
      }

      Set<InetAddress> hosts = new HashSet<>();
      for (ConsistentHashRingBean ring : consistentHashRingsBean.getConsistentHashRings()) {
        for (HostBean hostBean : ring.getHosts()) {
          hosts.add(InetAddress.getByName(hostBean.getIp()));
        }
      }

      Set<String> badHosts = new HashSet<>();
      for (InetAddress hostAddr : hosts) {
        if (!hostAddr.isReachable(5000)) {
          badHosts.add(hostAddr.toString());
        }
      }

      if (!badHosts.isEmpty()) {
        String errorMessage = String.format("Unable to ping hosts: %s", badHosts);
        ctx.getTaskQueue().failTask(ctx.getId(),errorMessage);
        emailSender.sendEmail("Healthcheck Failed for " + ctx.getCluster(), errorMessage);
        return;
      }

    } catch (Exception ex) {
      String errorMessage =
          String.format("Cluster %s is unhealthy, reason = %s", ctx.getCluster(), ex.getMessage());
      ctx.getTaskQueue().failTask(ctx.getId(), errorMessage);
      emailSender.sendEmail("Healthcheck Failed for " + ctx.getCluster(), errorMessage);
    }
  }
}

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

import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.config.ConfigParser;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * This task removes a given host from a cluster. It consists of the following steps:
 *
 * 1) Check if the host is still up and running. Fail if yes. (skipped if forceRemoval is true)
 * 2) Update cluster config by removing that host.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class RemoveHostTask extends AbstractTask<RemoveHostTask.Param> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveHostTask.class);

  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  public RemoveHostTask(HostBean hostToRemove) {
    this(hostToRemove, false);
  }

  public RemoveHostTask(HostBean hostToRemove, boolean forceRemoval) {
    this(
        new Param()
            .setHostToRemove(hostToRemove)
            .setForceRemoval(forceRemoval)
    );
  }

  public RemoveHostTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final HostBean toRemove = getParameter().getHostToRemove();
    final Admin.Client client = clientFactory.getClient(toRemove);

    InetSocketAddress hostAddr = new InetSocketAddress(toRemove.getIp(), toRemove.getPort());

    // 1) if it's not forceRemoval, ping the host to remove to make sure it's not running
    if (!getParameter().getForceRemoval()) {
      try {
        client.ping();
        LOG.error("Host {} is still alive!", hostAddr);
        // if we reach here, it means the process is still alive as #ping() succeeded.
        ctx.getTaskQueue().failTask(ctx.getId(), "Host is still alive!");
        return;
      } catch (TException tex) {
        // continue if #ping() failed.
      }
    }

    // 2) update cluster config to reflect the change
    ClusterBean clusterBean = ZKUtil.getClusterConfig(zkClient, ctx.getCluster());
    if (clusterBean == null) {
      LOG.error("Failed to get config for cluster {}.", ctx.getCluster());
      ctx.getTaskQueue().failTask(ctx.getId(), "Failed to read cluster config from zookeeper.");
      return;
    }
    for (SegmentBean segmentBean : clusterBean.getSegments()) {
      // filter out the host to remove
      List<HostBean> newHostList = segmentBean.getHosts().stream()
          .filter(hostBean ->
              !(hostBean.getIp().equals(toRemove.getIp()) &&
                    hostBean.getPort() == toRemove.getPort()))
          .collect(Collectors.toList());
      segmentBean.setHosts(newHostList);
    }
    ZKUtil.updateClusterConfig(zkClient, clusterBean);
    LOG.info("Updated config to {}", ConfigParser.serializeClusterConfig(clusterBean));
    ctx.getTaskQueue().finishTask(ctx.getId(),
        "Successfully removed host " + toRemove.getIp() + ":" + toRemove.getPort());
  }

  public static class Param extends Parameter {

    @JsonProperty
    private HostBean hostToRemove;

    @JsonProperty
    private boolean forceRemoval = false;

    public HostBean getHostToRemove() {
      return hostToRemove;
    }

    public Param setHostToRemove(HostBean hostToRemove) {
      this.hostToRemove = hostToRemove;
      return this;
    }

    public boolean getForceRemoval() {
      return forceRemoval;
    }

    public Param setForceRemoval(boolean forceRemoval) {
      this.forceRemoval = forceRemoval;
      return this;
    }
  }
}

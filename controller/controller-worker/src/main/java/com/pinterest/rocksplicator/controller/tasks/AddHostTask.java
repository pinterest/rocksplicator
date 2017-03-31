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
import com.pinterest.rocksdb_admin.thrift.BackupDBRequest;
import com.pinterest.rocksdb_admin.thrift.RestoreDBRequest;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.Role;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.bean.ShardBean;
import com.pinterest.rocksplicator.controller.config.ConfigParser;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.ShardUtil;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;

/**
 * This task adds a new host to the given cluster. It consists of the following steps:
 *
 * 1) Check if the host to add is up and running. Fail if not.
 * 2) Assign shard which doesn't have enough replicas to the new host
 * 3) Upload shard data to the new host
 * 4) Update cluster config in zookeeper to reflect the change
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class AddHostTask extends TaskBase<AddHostTask.Param> {
  private static final Logger LOG = LoggerFactory.getLogger(AddHostTask.class);
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyymmdd-hhmmss");


  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  public AddHostTask(String hostIp,
                     int hostPort,
                     String hostAZ,
                     String hdfsDir,
                     int rateLimitMbs) {
    this(new Param()
        .setHostToAdd(
            new HostBean()
                .setIp(hostIp)
                .setPort(hostPort)
                .setAvailabilityZone(hostAZ)
        )
        .setHdfsDir(hdfsDir)
        .setRateLimitMbs(rateLimitMbs)
    );
  }

  public AddHostTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final String clusterName = ctx.getCluster();
    final String hdfsDir = getParameter().getHdfsDir();
    final HostBean hostToAdd = getParameter().getHostToAdd();
    final int rateLimitMbs = getParameter().getRateLimitMbs();

    InetSocketAddress hostAddr = new InetSocketAddress(hostToAdd.getIp(), hostToAdd.getPort());
    Admin.Client client = clientFactory.getClient(hostAddr);

    // 1) ping the host to add to make sure it's up and running.
    try {
      client.ping();
      // continue if #ping() succeeds.
    } catch (TException tex) {
      ctx.getTaskQueue().failTask(ctx.getId(), "Host to add is not alive!");
      return;
    }

    ClusterBean clusterBean = ZKUtil.getClusterConfig(zkClient, clusterName);
    if (clusterBean == null) {
      ctx.getTaskQueue().failTask(ctx.getId(), "Failed to read cluster config from zookeeper.");
      return;
    }

    for (SegmentBean segment : clusterBean.getSegments()) {
      // 2) find shards to serve for new host
      Set<Integer> shardToServe =
          IntStream.range(0, segment.getNumShards())
                   .boxed()
                   .collect(Collectors.toSet());
      for (HostBean host : segment.getHosts()) {
        // ignore hosts in different AZ than the new host
        if (host.getAvailabilityZone().equals(hostToAdd.getAvailabilityZone())) {
          host.getShards().forEach(shard -> shardToServe.remove(shard.getId()));
        }
      }

      // 3) upload shard data to the new host
      for (int  shardId : shardToServe) {
        HostBean upstream = findMasterShard(shardId, segment.getHosts());
        if (upstream == null) {
          //TODO: should we fail the task in this case?
          LOG.error("Failed to find master shard for segment={}, shardId={}",
                    segment.getName(), shardId);
          continue;
        }
        Admin.Client upstreamClient = clientFactory.getClient(
            new InetSocketAddress(upstream.getIp(), upstream.getPort())
        );
        String dbName = ShardUtil.getDBNameFromSegmentAndShardId(segment.getName(), shardId);
        String hdfsPath = String.format("%s/%s/%s/%05d/%s/%s", hdfsDir, clusterName,
            segment.getName(), shardId, upstream.getIp(), getCurrentDateTime());

        upstreamClient.backupDB(new BackupDBRequest(dbName, hdfsPath).setLimit_mbs(rateLimitMbs));
        LOG.info("Backed up {} from {} to {}.", dbName, upstream.getIp(), hdfsPath);

        client.restoreDB(
            new RestoreDBRequest(dbName, hdfsPath, upstream.getIp(), (short)upstream.getPort())
                .setLimit_mbs(rateLimitMbs)
        );
        LOG.info("Restored {} from {} to {}.", dbName, hdfsPath, hostToAdd.getIp());
      }
      // add shard config to new host
      hostToAdd.setShards(
          shardToServe.stream()
                      .map(id -> new ShardBean().setId(id).setRole(Role.SLAVE))
                      .collect(Collectors.toList())
      );
      List<HostBean> newHostList = segment.getHosts();
      newHostList.add(hostToAdd);
      segment.setHosts(newHostList);
    }

    // 4) update cluster config in zookeeper
    ZKUtil.updateClusterConfig(zkClient, clusterBean);
    LOG.info("Updated config to {}", ConfigParser.serializeClusterConfig(clusterBean));
    ctx.getTaskQueue().finishTask(ctx.getId(),
        "Successfully added host " + hostToAdd.getIp() + ":" + hostToAdd.getPort());
  }

  /**
   * Find the host which serves master shard for a given shardId.
   */
  private HostBean findMasterShard(int shardId, List<HostBean> hosts) {
    for (HostBean host : hosts) {
      for (ShardBean shard : host.getShards()) {
        if (shard.getId() == shardId && shard.getRole() == Role.MASTER) {
          return host;
        }
      }
    }
    return null;
  }

  /**
   * Get current date and time.
   */
  private String getCurrentDateTime() {
    return DATE_FORMAT.format(new Date());
  }

  public static class Param extends Parameter {

    @JsonProperty
    private HostBean hostToAdd;

    @JsonProperty
    private String hdfsDir;

    @JsonProperty
    private int rateLimitMbs;

    public HostBean getHostToAdd() {
      return hostToAdd;
    }

    public Param setHostToAdd(HostBean hostToAdd) {
      this.hostToAdd = hostToAdd;
      return this;
    }

    public  String getHdfsDir() {
      return hdfsDir;
    }

    public Param setHdfsDir(String hdfsDir) {
      this.hdfsDir = hdfsDir;
      return this;
    }

    public int getRateLimitMbs() {
      return rateLimitMbs;
    }

    public Param setRateLimitMbs(int rateLimitMbs) {
      this.rateLimitMbs = rateLimitMbs;
      return this;
    }
  }
}

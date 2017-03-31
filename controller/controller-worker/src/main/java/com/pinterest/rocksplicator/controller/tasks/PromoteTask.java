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
import com.pinterest.rocksdb_admin.thrift.ChangeDBRoleAndUpstreamRequest;
import com.pinterest.rocksdb_admin.thrift.GetSequenceNumberRequest;
import com.pinterest.rocksdb_admin.thrift.GetSequenceNumberResponse;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.Role;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.bean.ShardBean;
import com.pinterest.rocksplicator.controller.config.ConfigParser;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.ShardUtil;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;

/**
 * This task picks masters for shards without a master and then promotes them.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class PromoteTask extends TaskBase<PromoteTask.Param> {
  private static final Logger LOG = LoggerFactory.getLogger(PromoteTask.class);

  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  public PromoteTask() {
    this(new Param());
  }

  public PromoteTask(Param parameter) {
    super(parameter);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final String clusterName = ctx.getCluster();
    final ClusterBean clusterBean = ZKUtil.getClusterConfig(zkClient, clusterName);

    boolean updated = false;
    for (SegmentBean segment : clusterBean.getSegments()) {
      // map from shardId which doesn't have a master to a list of hosts serving slaves.
      Map<Integer, List<HostBean>> shardWithoutMaster =
          IntStream.range(0, segment.getNumShards())
                   .boxed()
                   .collect(Collectors.toMap(k -> k, v -> new ArrayList<HostBean>()));

      // find shards without master
      for (HostBean host : segment.getHosts()) {
        for (ShardBean shard : host.getShards()) {
          if (shard.getRole() == Role.MASTER) {
            // remove from the map if it's a master shard
            shardWithoutMaster.remove(shard.getId());
          } else if (shardWithoutMaster.containsKey(shard.getId())){
            // this shardId doesn't have a master
            shardWithoutMaster.get(shard.getId()).add(host);
          }
        }
      }

      // pick and promote new master
      for (Map.Entry<Integer, List<HostBean>> entry : shardWithoutMaster.entrySet()) {
        String dbName = ShardUtil.getDBNameFromSegmentAndShardId(segment.getName(), entry.getKey());
        // pick new master
        HostBean master = pickNewMaster(entry.getValue(), dbName);
        // promote new mater
        promoteMaster(master, dbName);
        // flip slave to master in config
        List<ShardBean> newShardList = master.getShards().stream()
            .map(shard -> {
              if (shard.getId() == entry.getKey()) {
                return new ShardBean().setId(entry.getKey()).setRole(Role.MASTER);
              } else {
                return shard;
              }
            })
            .collect(Collectors.toList());
        // assign new shard list
        master.setShards(newShardList);
        updated = true;
      }
    }

    if (!updated) {
      ctx.getTaskQueue().finishTask(ctx.getId(), "Nothing needs to promote");
    } else {
      ZKUtil.updateClusterConfig(zkClient, clusterBean);
      LOG.info("Updated config to {}", ConfigParser.serializeClusterConfig(clusterBean));
      ctx.getTaskQueue().finishTask(ctx.getId(), "Successfully promoted new masters");
    }
  }

  /**
   * Pick new master shard out of a list of slaves. The slave with highest
   * sequence number will be picked.
   *
   * @param hosts list of candidate hosts
   * @param dbName name of the DB to serve new master shard
   * @return host being picked as new master
   */
  private HostBean pickNewMaster(List<HostBean> hosts, String dbName) throws TException{
    HostBean newMaster = null;
    long maxSeqNum = Long.MIN_VALUE;
    for (HostBean host : hosts) {
      Admin.Client client = clientFactory.getClient(
          new InetSocketAddress(host.getIp(), host.getPort())
      );

      long seqNum = client.getSequenceNumber(new GetSequenceNumberRequest(dbName)).getSeq_num();
      if (seqNum > maxSeqNum) {
        maxSeqNum = seqNum;
        newMaster = host;
      }
    }
    return newMaster;
  }

  private void promoteMaster(HostBean master, String dbName) throws TException {
    Admin.Client client = clientFactory.getClient(
        new InetSocketAddress(master.getIp(), master.getPort())
    );
    client.changeDBRoleAndUpStream(new ChangeDBRoleAndUpstreamRequest(dbName, "MASTER"));
  }

  // empty parameter
  public static class Param extends Parameter {
  }
}

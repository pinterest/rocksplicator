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
import com.pinterest.rocksdb_admin.thrift.GetSequenceNumberRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

/**
 * This task tries to rebalance the given cluster as even as possible by re-assigning
 * master shards from overloaded hosts to underloaded hosts. A host is considered
 * overloaded if it has more master shards than
 * {@code Math.ceil(num_shards / num_hosts)},
 * and is considered underloaded if it has less master shards than
 * {@code Math.floor(num_shards / num_hosts)}.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class RebalanceTask extends AbstractTask<RebalanceTask.Param> {
  private static final Logger LOG = LoggerFactory.getLogger(PromoteTask.class);

  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  public RebalanceTask() {
    this(new Param());
  }

  public RebalanceTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final String clusterName = ctx.getCluster();
    final ClusterBean clusterBean = ZKUtil.getClusterConfig(zkClient, clusterName);

    for (SegmentBean segmentBean : clusterBean.getSegments()) {
      int numHosts = segmentBean.getHosts().size();
      long lowerBound = segmentBean.getNumShards() / numHosts;
      long upperBound = Math.round(Math.ceil(segmentBean.getNumShards() / (double)numHosts));

      for (int i = 0; i < segmentBean.getNumShards(); ++i) {
        HostBean master = findMaster(segmentBean, i);
        List<HostBean> slaves = findSlaves(segmentBean, i);

        if (slaves.size() == 0 || (master != null && countMasterShards(master) <= upperBound)) {
          // no slaves for this shard or master is not overloaded.
          continue;
        }

        // find the slave with least load.
        long leastNumMasters = Long.MAX_VALUE;
        HostBean leastLoadedSlave = null;
        for (HostBean slave : slaves) {
          long cnt = countMasterShards(slave);
          if (cnt < leastNumMasters) {
            leastNumMasters = cnt;
            leastLoadedSlave = slave;
          }
        }

        if (leastNumMasters >= lowerBound) {
          // no slaves are underloaded.
          continue;
        }

        final String dbName = ShardUtil.getDBNameFromSegmentAndShardId(segmentBean.getName(), i);
        final Admin.Client oldMasterClient = clientFactory.getClient(master);
        final Admin.Client newMasterClient = clientFactory.getClient(leastLoadedSlave);
        // demote the current master
        ShardUtil.demote(oldMasterClient, master, dbName);
        // update shard config for current master
        for (ShardBean shard : master.getShards()) {
          if (shard.getId() == i) {
            shard.setRole(Role.SLAVE);
          }
        }
        ZKUtil.updateClusterConfig(zkClient, clusterBean);
        LOG.info("Updated cluster config to {} after demoting the old master {}.",
            ConfigParser.serializeClusterConfig(clusterBean), master.getIp());

        // wait for the new master to catch up
        long oldMasterSeqNum =
            oldMasterClient.getSequenceNumber(new GetSequenceNumberRequest(dbName)).getSeq_num();
        while (true) {
          long newMasterSeqNum =
              newMasterClient.getSequenceNumber(new GetSequenceNumberRequest(dbName)).getSeq_num();
          if (newMasterSeqNum == oldMasterSeqNum) {
            break;
          } else {
            Thread.sleep(100);
          }
        }

        // promote the new master
        ShardUtil.promoteNewMaster(newMasterClient, dbName);
        // update shard config for the new master
        for (ShardBean shard : leastLoadedSlave.getShards()) {
          if (shard.getId() == i) {
            shard.setRole(Role.MASTER);
          }
        }
        ZKUtil.updateClusterConfig(zkClient, clusterBean);
        LOG.info("Updated cluster config to {} after promoting the new master {}.",
            ConfigParser.serializeClusterConfig(clusterBean), leastLoadedSlave.getIp());

        // change upstream to the new master
        ShardUtil.changeUpstream(oldMasterClient, leastLoadedSlave, dbName);
        for (HostBean slave : slaves) {
          if (slave != leastLoadedSlave) {
            ShardUtil.changeUpstream(clientFactory.getClient(slave), leastLoadedSlave, dbName);
          }
        }
      }
    }
    ctx.getTaskQueue().finishTask(ctx.getId(), "Successfully rebalanced cluster " + clusterName);
  }

  /**
   * Find host which is serving the master shard for the given shardId
   * from segment config.
   *
   * @param segment config of the segment
   * @param shardId id of the master shard
   * @return the host serving master shard, or null if no such host
   */
  private HostBean findMaster(SegmentBean segment, int shardId) {
    for (HostBean host : segment.getHosts()) {
      for (ShardBean shard : host.getShards()) {
        if (shard.getId() == shardId && shard.getRole() == Role.MASTER) {
          return host;
        }
      }
    }
    return null;
  }

  /**
   * Find a list of hosts which are serving slave shard for the given shardId
   * from segment config.
   *
   * @param segment config of the segment
   * @param shardId id of the slave shard
   * @return a list of hosts serving slave shard
   */
  private List<HostBean> findSlaves(SegmentBean segment, int shardId) {
    List<HostBean> slaves = new ArrayList<>();
    for (HostBean host : segment.getHosts()) {
      for (ShardBean shard : host.getShards()) {
        if (shard.getId() == shardId && shard.getRole() == Role.SLAVE) {
          slaves.add(host);
          break;
        }
      }
    }
    return slaves;
  }

  /**
   * Count the number of master shards served by a particular host.
   *
   * @param host host to count master shards
   * @return number of master shards
   */
  private long countMasterShards(HostBean host) {
    return host.getShards().stream().filter(shard -> shard.getRole() == Role.MASTER).count();
  }

  // empty parameter
  public static class Param extends Parameter {
  }
}

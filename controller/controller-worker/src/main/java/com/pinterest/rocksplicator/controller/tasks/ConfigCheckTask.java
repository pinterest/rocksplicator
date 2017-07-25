package com.pinterest.rocksplicator.controller.tasks;


import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.Role;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.bean.ShardBean;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.EmailSender;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The task checks the cluster configuration for the following invariance:
 * 1. Every shard has numReplicas replica across zones.
 * 2. There are 1 master and numReplicas - 1 slaves for all shards.
 */
public class ConfigCheckTask extends AbstractTask<ConfigCheckTask.Param> {

  public static final Logger LOG = LoggerFactory.getLogger(ConfigCheckTask.class);

  @Inject
  private CuratorFramework zkClient;

  @Inject
  private AdminClientFactory clientFactory;

  @Inject
  private EmailSender emailSender;

  public ConfigCheckTask(int numReplicas) {
    this(new Param().setNumReplicas(numReplicas));
  }

  public ConfigCheckTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    final String clusterName = ctx.getCluster();
    ClusterBean clusterBean = ZKUtil.getClusterConfig(zkClient, clusterName);
    if (clusterBean == null) {
      ctx.getTaskQueue().failTask(ctx.getId(), "Failed to read cluster config from zookeeper.");
      return;
    }
    try {
      for (SegmentBean segmentBean : clusterBean.getSegments()) {
        checkSegment(segmentBean, getParameter().getNumReplicas());
      }
      ctx.getTaskQueue().finishTask(ctx.getId(), "Cluster " + clusterName + " has good config");
    } catch (Exception e) {
      String errorMessage =
          String.format("Cluster %s doesn't have good shard distribution, reason = %s,",
              clusterName, e.getMessage());
      ctx.getTaskQueue().failTask(ctx.getId(), errorMessage);
      emailSender.sendEmail("Config Check failed for " + clusterName, errorMessage);
    }
  }

  /**
   * Count the number of replicas for each shard within a segment.
   * Throws exception if the number doesn't match the expected one.
   */
  private void checkSegment(SegmentBean segment, int numReplicas) throws Exception {
    Map<Integer, Integer> shardCount = new HashMap<>();
    Map<Integer, Boolean> seenMaster = new HashMap<>();

    for (HostBean hostBean : segment.getHosts()) {
      for (ShardBean shardBean : hostBean.getShards()) {
        int cnt = shardCount.getOrDefault(shardBean.getId(), 0);
        shardCount.put(shardBean.getId(), cnt + 1);
        if (shardBean.getRole() == Role.MASTER) {
          seenMaster.put(shardBean.getId(), true);
        } else {
          seenMaster.putIfAbsent(shardBean.getId(), false);
        }
      }
    }

    if (shardCount.size() != segment.getNumShards()) {
      throw new Exception(
          String.format("Incorrect number of shards. Expected %d but actually %d.",
              segment.getNumShards(), shardCount.size()));
    }

    Map<String, Integer> badShards = new HashMap<>();
    shardCount.forEach((shardId, replicas) -> {
      if (replicas != numReplicas) {
        badShards.put(segment.getName() + shardId, replicas);
      }
    });

    if (!badShards.isEmpty()) {
      throw new Exception("Incorrect number of replicas. Bad shards: " + badShards.toString());
    }

    Set<String> missingMasters = new HashSet<>();
    seenMaster.forEach((shardId, seen) -> {
      if (!seen) {
        missingMasters.add(segment.getName() + shardId);
      }
    });

    if (!missingMasters.isEmpty()) {
      throw new Exception("Missing masters for some shards: " + missingMasters.toString());
    }
  }

  public static class Param extends Parameter {
    @JsonProperty
    private int numReplicas;

    public int getNumReplicas() { return numReplicas; }

    public Param setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
      return this;
    }
  }
}

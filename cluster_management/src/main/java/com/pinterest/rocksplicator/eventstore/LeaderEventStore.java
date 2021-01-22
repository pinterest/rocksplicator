package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class LeaderEventStore {
  private CuratorFramework zkClient;
  private String clusterName;
  private String resourceName;
  private String partitionName;
  private InterProcessMutex partitionMutex;
  private Codec<LeaderEventsHistory, byte[]> leaderEventsHistoryCodec;


  public LeaderEventStore(String zkConnectString, String clusterName, String resourceName, String partitionName) {
    this.zkClient = CuratorFrameworkFactory.newClient(Preconditions.checkNotNull(zkConnectString),
        new ExponentialBackoffRetry(1000, 3));

    this.clusterName = Preconditions.checkNotNull(clusterName);
    this.resourceName = Preconditions.checkNotNull(resourceName);
    this.partitionName = Preconditions.checkNotNull(partitionName);
    this.leaderEventsHistoryCodec = new WrappedDataThriftCodec(
        LeaderEventsHistory.class, SerializationProtocol.BINARY, CompressionAlgorithm.GZIP);
    this.zkClient.start();
    this.partitionMutex = new InterProcessMutex(
        this.zkClient, getPartitionLockPath(clusterName, resourceName, partitionName));
  }

  public void append(LeaderEvent leaderEvent) {
    Preconditions.checkArgument(leaderEvent.isSetEvent_type());
    Preconditions.checkArgument(leaderEvent.isSetEvent_timestamp_ms());
    Preconditions.checkArgument(leaderEvent.isSetOriginating_node());

    try {
      try (Locker locker = new Locker(partitionMutex)) {
        String eventHistoryPath = getLeaderEventHistoryPath(clusterName, resourceName, partitionName);
        zkClient.sync().forPath(getLeaderEventHistoryPath(clusterName, resourceName, partitionName));
        byte[] data = zkClient.getData().forPath(eventHistoryPath);
        LeaderEventsHistory history = leaderEventsHistoryCodec.decode(data);
        history.addToEvents(leaderEvent);
        zkClient.setData().forPath(eventHistoryPath, leaderEventsHistoryCodec.encode(history));
      }
    } catch (Exception e) {
      // Eat away the exception....
    }
  }

  private byte[] createIfDoesntExist(String path) {
    try {
      zkClient.checkExists().creatingParentsIfNeeded().forPath(path);
      zkClient.create().orSetData().creatingParentsIfNeeded().forPath(path, leaderEventsHistoryCodec.encode(new LeaderEventsHistory()));
    } catch (Exception e) {
    }
  }

  private byte[] getData(String path) {
    try {
      return zkClient.getData().forPath(path);
    } catch (Exception e) {
      return null;
    }
  }

  private static String getPartitionLockPath(String cluster, String resourceName, String partitionName) {
      return "/rocksplicator/eventhistory/" + cluster + "/" + resourceName + "/" + partitionName + "/lock";
    }

  private static String getLeaderEventHistoryPath(String cluster, String resourceName, String partitionName) {
    return "/rocksplicator/eventhistory/" + cluster + "/" + resourceName + "/" + partitionName + "/eventhistory";
  }
}


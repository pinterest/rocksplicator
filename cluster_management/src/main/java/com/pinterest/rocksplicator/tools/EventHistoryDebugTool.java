package com.pinterest.rocksplicator.tools;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
import com.pinterest.rocksplicator.eventstore.LeaderEventTypes;
import com.pinterest.rocksplicator.eventstore.ZkMergeableEventStore;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class EventHistoryDebugTool {

  private static final String zkSvr = "zkSvr";
  private static final String cluster = "cluster";
  private static final String resource = "resource";
  private static final String min_partition = "min_partition";
  private static final String num_partitions = "num_partitions";
  private static final String max_events = "max_events";

  private static Options constructCommandLineOptions() {
    Option zkServerOption =
        OptionBuilder.withLongOpt(zkSvr).withDescription("Provide zookeeper addresses").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddresses(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option resourceOption =
        OptionBuilder.withLongOpt(resource).withDescription("Provide resource name").create();
    resourceOption.setArgs(1);
    resourceOption.setRequired(true);
    resourceOption.setArgName("Resource name (Required)");

    Option minPartitionOption =
        OptionBuilder.withLongOpt(min_partition).withDescription("starting partition to display")
            .create();
    minPartitionOption.setArgs(1);
    minPartitionOption.setRequired(false);
    minPartitionOption.setArgName("min partition number (Optional, default to 0)");

    Option numPartitionsOption =
        OptionBuilder.withLongOpt(num_partitions).withDescription("number of partitions to display")
            .create();
    numPartitionsOption.setArgs(1);
    numPartitionsOption.setRequired(false);
    numPartitionsOption.setArgName("num of partitions to display (Optional, default to 1)");

    Option maxEventsOption =
        OptionBuilder.withLongOpt(max_events)
            .withDescription("maximum number of events to display per partition").create();
    maxEventsOption.setArgs(1);
    maxEventsOption.setRequired(false);
    maxEventsOption
        .setArgName("max number of events to display per partition (Optional, default to 25)");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(resourceOption)
        .addOption(minPartitionOption)
        .addOption(numPartitionsOption)
        .addOption(maxEventsOption);
    return options;
  }

  private static CommandLine processCommandLineArgs(String[] cliArgs) throws ParseException {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    return cliParser.parse(cliOptions, cliArgs);
  }

  public static void main(String[] args) {
    CommandLine cmd = null;
    try {
      cmd = processCommandLineArgs(args);
    } catch (ParseException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final String zkConnectString = cmd.getOptionValue(zkSvr);
    final String clusterName = cmd.getOptionValue(cluster);
    final String resourceName = cmd.getOptionValue(resource);
    final int minPartitionId = Integer.parseInt(cmd.getOptionValue(min_partition, "0"));
    final int numPartitions = Integer.parseInt(cmd.getOptionValue(num_partitions, "1"));
    final int maxEvents = Integer.parseInt(cmd.getOptionValue(max_events, "25"));

    System.out.println(String.format("zkSvr: %s", zkConnectString));
    System.out.println(String.format("cluster: %s", clusterName));
    System.out.println(String.format("resource: %s", resourceName));
    System.out.println(String.format("partitionId (start): %s", minPartitionId));
    System.out.println(String.format("numPartitions: %s", numPartitions));
    System.out.println(String.format("maxEvents: %s", maxEvents));

    System.out.println(String.format("Connecting to zk: %s", zkConnectString));

    final CuratorFramework
        zkClient =
        CuratorFrameworkFactory.newClient(Preconditions.checkNotNull(zkConnectString),
            new ExponentialBackoffRetry(1000, 3));

    zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
          case CONNECTED:
            System.out.println(String.format("Connected to zk: %s", zkConnectString));
        }
      }
    });
    zkClient.start();
    try {
      zkClient.blockUntilConnected(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.out.println("Cannot connect to zk in 10 seconds");
      throw new RuntimeException(e);
    }

    final Codec<LeaderEventsHistory, byte[]> leaderEventsHistoryCodec = new WrappedDataThriftCodec(
        LeaderEventsHistory.class, SerializationProtocol.COMPACT, CompressionAlgorithm.GZIP);

    for (int partitionId = minPartitionId; partitionId < minPartitionId + numPartitions;
         ++partitionId) {
      final String partitionName = String.format("%s_%d", resourceName, partitionId);
      final String partitionPath =
          ZkMergeableEventStore.getLeaderEventHistoryPath(clusterName, resourceName, partitionName);
      try {
        System.err.println(String.format("Retrieving %s", partitionPath));
        zkClient.sync().forPath(partitionPath);
        byte[] data = zkClient.getData().forPath(partitionPath);
        LeaderEventsHistory history = leaderEventsHistoryCodec.decode(data);
        System.out.println(String
            .format("cluster: %s, resource: %s, partition: %s, history_size:%s", clusterName,
                resourceName, partitionName, history.getEventsSize()));

        int numEvents = Math.min(history.getEventsSize(), maxEvents);

        long lastEventTimeMillis = (history.getEventsSize() > 0)? history.getEvents().get(0).getEvent_timestamp_ms() : -1;
        long currentTimeMillis = System.currentTimeMillis();
        // Print them in reverse order, with latest first.
        for (int eventId = 0; eventId < Math.min(history.getEventsSize(), maxEvents); ++eventId) {
          LeaderEvent leaderEvent = history.getEvents().get(numEvents - eventId - 1);
          processLeaderEvent(leaderEvent);
          System.out.println(String.format(
              "ts: ts:%d, old: %8d s, delay: %8d ms, origin: %-18s, leader: %-18s, event: %s",
              leaderEvent.getEvent_timestamp_ms(),
              (currentTimeMillis -leaderEvent.getEvent_timestamp_ms())/1000,
              lastEventTimeMillis - leaderEvent.getEvent_timestamp_ms(),
              leaderEvent.getOriginating_node(),
              (leaderEvent.isSetObserved_leader_node())? leaderEvent.getObserved_leader_node():"not_known",
              leaderEvent.getEvent_type()));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void processLeaderEvent(LeaderEvent leaderEvent) {
    if (LeaderEventTypes.participantEventTypes.contains(leaderEvent.getEvent_type())) {
      if (!leaderEvent.isSetObserved_leader_node()) {
        leaderEvent.setObserved_leader_node(leaderEvent.getOriginating_node());
      }
    }
  }
}

package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.ThriftStringEncoder;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
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
        OptionBuilder.withLongOpt(min_partition).withDescription("Provide cluster name").create();
    minPartitionOption.setArgs(1);
    minPartitionOption.setRequired(false);
    minPartitionOption.setArgName("min partition number (Optional, default to 0)");

    Option numPartitionsOption =
        OptionBuilder.withLongOpt(num_partitions).withDescription("Provide cluster name").create();
    numPartitionsOption.setArgs(1);
    numPartitionsOption.setRequired(false);
    numPartitionsOption.setArgName("max partition number (Optional, default to 0)");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(resourceOption)
        .addOption(minPartitionOption)
        .addOption(numPartitionsOption);
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

    System.out.println(String.format("zkSvr: %s", zkConnectString));
    System.out.println(String.format("cluster: %s", clusterName));
    System.out.println(String.format("resource: %s", resourceName));
    System.out.println(String.format("partitionId (start): %s", minPartitionId));
    System.out.println(String.format("numPartitions: %s", numPartitions));

    System.out.println(String.format("Connecting to zk: %s", zkConnectString));

    final CuratorFramework
        zkClient =
        CuratorFrameworkFactory.newClient(Preconditions.checkNotNull(zkConnectString),
            new ExponentialBackoffRetry(1000, 3));
    zkClient.start();

    zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
          case CONNECTED:
            System.out.println(String.format("Connected to zk: %s", zkConnectString));
        }
      }
    });
    try {
      zkClient.blockUntilConnected(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.out.println("Cannot connect to zk in 10 seconds");
      throw new RuntimeException(e);
    }

    final Codec<LeaderEventsHistory, byte[]> leaderEventsHistoryCodec = new WrappedDataThriftCodec(
        LeaderEventsHistory.class, SerializationProtocol.COMPACT, CompressionAlgorithm.GZIP);

    final ThriftStringEncoder<LeaderEvent>
        jsonEncoder =
        ThriftStringEncoder.createToStringEncoder(LeaderEvent.class);

    for (int partitionId = minPartitionId; partitionId < numPartitions; ++partitionId) {
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
        for (int eventId = 0; eventId < history.getEventsSize(); ++eventId) {
          System.out.println(
              String.format("\t event: %s", jsonEncoder.encode(history.getEvents().get(eventId))));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

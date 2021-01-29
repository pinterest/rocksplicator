package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.ThriftJSONEncoder;
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
import org.apache.curator.retry.ExponentialBackoffRetry;

public class EventHistoryDebugTool {

  private static final String zkSvr = "zkSvr";
  private static final String cluster = "cluster";
  private static final String resource = "resource";
  private static final String min_partition = "min_partition";
  private static final String max_partition = "max_partition";

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
        OptionBuilder.withLongOpt(resource).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Resource name (Required)");

    Option minPartitionOption =
        OptionBuilder.withLongOpt(min_partition).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(false);
    clusterOption.setArgName("min partition number (Optional, default to 0)");

    Option maxPartitionOption =
        OptionBuilder.withLongOpt(max_partition).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(false);
    clusterOption.setArgName("max partition number (Optional, default to 0)");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(resourceOption)
        .addOption(minPartitionOption)
        .addOption(maxPartitionOption);
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
    final int maxPartitionId = Integer.parseInt(cmd.getOptionValue(max_partition, "0"));

    final CuratorFramework
        zkClient =
        CuratorFrameworkFactory.newClient(Preconditions.checkNotNull(zkConnectString),
            new ExponentialBackoffRetry(1000, 3));
    zkClient.start();

    final Codec<LeaderEventsHistory, byte[]> leaderEventsHistoryCodec = new WrappedDataThriftCodec(
        LeaderEventsHistory.class, SerializationProtocol.COMPACT, CompressionAlgorithm.GZIP);

    final ThriftJSONEncoder<LeaderEvent>
        jsonEncoder =
        ThriftJSONEncoder.createJSONEncoder(LeaderEvent.class);

    for (int partitionId = minPartitionId; partitionId <= maxPartitionId; ++partitionId) {
      String partitionName = String.format("%s_%d", resourceName, partitionId);
      String
          partitionPath =
          ZkMergeableEventStore.getLeaderEventHistoryPath(clusterName, resourceName, partitionName);
      try {
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

package com.pinterest.rocksplicator.tools;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
import com.pinterest.rocksplicator.eventstore.LeaderEventTypes;
import com.pinterest.rocksplicator.eventstore.ZkMergeableEventStore;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
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

import java.io.Console;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class EventHistoryAnalysisTool {

  private static final String zkSvr = "zkSvr";
  private static final String cluster = "cluster";
  private static final String resource = "resource";
  private static final String min_partition = "min_partition";
  private static final String num_partitions = "num_partitions";
  private static final String num_seconds_ago = "num_seconds_ago";

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

    Option numSecondsAgoOption =
        OptionBuilder.withLongOpt(num_seconds_ago)
            .withDescription("maximum number of events to display per partition").create();
    numSecondsAgoOption.setArgs(1);
    numSecondsAgoOption.setRequired(false);
    numSecondsAgoOption
        .setArgName("max number of events to display per partition (Optional, default to 25)");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(resourceOption)
        .addOption(minPartitionOption)
        .addOption(numPartitionsOption)
        .addOption(numSecondsAgoOption);
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
    final long numSecondsAgo = Long.parseLong(cmd.getOptionValue(num_seconds_ago, "3600"));

    System.out.println(String.format("zkSvr: %s", zkConnectString));
    System.out.println(String.format("cluster: %s", clusterName));
    System.out.println(String.format("resource: %s", resourceName));
    System.out.println(String.format("partitionId (start): %s", minPartitionId));
    System.out.println(String.format("numPartitions: %s", numPartitions));
    System.out.println(String.format("numSecondsAgo: %s", numSecondsAgo));

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

    long currentTimeMillis = System.currentTimeMillis();

    long maxE2EDelay = -1;
    List<LeaderEvent> maxE2EDelayEvents = null;

    long[] e2eLatencies = new long[numPartitions];
    long[] controllerDelays = new long[numPartitions];
    long[] generateDelays = new long[numPartitions];
    long[] postDelays = new long[numPartitions];

    boolean skipRest = false;

    final Codec<LeaderEventsHistory, byte[]> leaderEventsHistoryCodec = new WrappedDataThriftCodec(
        LeaderEventsHistory.class, SerializationProtocol.COMPACT, CompressionAlgorithm.GZIP);

    for (int partitionId = minPartitionId; partitionId < minPartitionId + numPartitions;
         ++partitionId) {
      final String partitionName = String.format("%s_%d", resourceName, partitionId);
      final String partitionPath =
          ZkMergeableEventStore.getLeaderEventHistoryPath(clusterName, resourceName, partitionName);
      try {
        zkClient.sync().forPath(partitionPath);
        byte[] data = zkClient.getData().forPath(partitionPath);
        LeaderEventsHistory history = leaderEventsHistoryCodec.decode(data);
        long
            lastEventTimeMillis =
            (history.getEventsSize() > 0) ? history.getEvents().get(0).getEvent_timestamp_ms()
                                          : System.currentTimeMillis();
        // First filter out all events before the numSecondsAgo filter. This applies based on the
        // last event.
        List<LeaderEvent> filteredEvents = history.getEvents()
            .stream()
            .filter(e -> (lastEventTimeMillis - e.getEvent_timestamp_ms()) < numSecondsAgo * 1000)
            .map(EventHistoryAnalysisTool::processLeaderEvent)
            .collect(Collectors.toList());

        Delays delay = processE2ELatencyMillis(filteredEvents);
        long e2eLatency = delay.getE2eLatency();
        long externalViewDelay = delay.getExternalViewDelay();
        long generateDelay = delay.getShardGenDelay();
        long postDelay = delay.getClientAvailableDelay();

        e2eLatencies[partitionId - minPartitionId] = e2eLatency;
        controllerDelays[partitionId - minPartitionId] = externalViewDelay;
        generateDelays[partitionId - minPartitionId] = generateDelay;
        postDelays[partitionId - minPartitionId] = postDelay;

        if (maxE2EDelay < e2eLatency) {
          maxE2EDelayEvents = filteredEvents;
          maxE2EDelay = e2eLatency;
        }

        if (e2eLatency < 1) {
          if (!skipRest) {
            printLeaderEvents(filteredEvents, currentTimeMillis);
            skipRest = confirm();
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Spit out every 5%
    Arrays.sort(controllerDelays);
    Arrays.sort(generateDelays);
    Arrays.sort(postDelays);
    Arrays.sort(e2eLatencies);
    for (int i = 0; i < 20; ++i) {
      int index = (e2eLatencies.length * (i+1) ) / 20 - 1;
      System.out.println(
          String.format("%2d percentile (ms): %6d %6d %8d %8d",
              ((i+1)*5),
              controllerDelays[index],
              generateDelays[index],
              postDelays[index],
              e2eLatencies[index]));
    }

    // Print out all leaderEvents for max latency events.
    printLeaderEvents(maxE2EDelayEvents, currentTimeMillis);
  }

  private static boolean confirm() {
    while (true) {
      System.out.print("Do you want to skip the subsequent leader events? y/n: ");
      try {
        int readByte = System.in.read();
        if (readByte < 0) {
          return false;
        } else if (readByte == 'y' || readByte == 'Y') {
          return true;
        } else if (readByte == 'n' || readByte == 'N') {
          return false;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static void printLeaderEvents(List<LeaderEvent> leaderEvents, long currentTimeMillis) {
    // Print out all leaderEvents for max latency events.
    for (int eventId = 0; eventId < leaderEvents.size(); ++eventId) {
      LeaderEvent leaderEvent = leaderEvents.get(leaderEvents.size() - eventId - 1);
      System.out.println(String.format(
          "ts:%d msec, age: %8d sec, relative_age: %8d ms, from: %18s, leader: %18s, type: %s",
          leaderEvent.getEvent_timestamp_ms(),
          (currentTimeMillis - leaderEvent.getEvent_timestamp_ms()) / 1000,
          leaderEvents.get(0).getEvent_timestamp_ms() - leaderEvent.getEvent_timestamp_ms(),
          leaderEvent.getOriginating_node(),
          (leaderEvent.isSetObserved_leader_node()) ? leaderEvent.getObserved_leader_node()
                                                    : "not_known",
          leaderEvent.getEvent_type()));
    }
  }



  private static class Delays {
    private long e2eLatency = -1;
    private long externalViewDelay = -1;
    private long shardGenDelay = -1;
    private long clientAvailableDelay = -1;


    public long getE2eLatency() {
      return e2eLatency;
    }

    public long getExternalViewDelay() {
      return externalViewDelay;
    }

    public long getShardGenDelay() {
      return shardGenDelay;
    }

    public long getClientAvailableDelay() {
      return clientAvailableDelay;
    }
  }


  private static Delays processE2ELatencyMillis(
      final List<LeaderEvent> leaderEvents) {
    // First find the last event where a leader is up.
    int eventId = 0;

    Delays delays = new Delays();

    int participantUpIndex = -1;
    LeaderEvent participantUp = null;

    /**
     * Find the latest participant leader that came up and was successful.
     */
    for (eventId = 0; eventId < leaderEvents.size(); ++eventId) {
      LeaderEvent leaderEvent = leaderEvents.get(eventId);
      LeaderEventType eventType = leaderEvent.getEvent_type();
      if (eventType == LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS) {
        participantUpIndex = eventId;
        participantUp = leaderEvent;
        break;
      }
    }

    if (participantUpIndex < 0) {
      return delays;
    }

    int shardObservedUpIndex = -1;
    LeaderEvent shardObservedUp = null;
    /**
     * Next go back towards latest events, and find out the first spectator that observed
     * the same participant as leader up.
     */
    for (eventId = participantUpIndex - 1; eventId >= 0; --eventId) {
      LeaderEvent leaderEvent = leaderEvents.get(eventId);
      LeaderEventType eventType = leaderEvent.getEvent_type();
      if (eventType != LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP) {
        continue;
      }
      if (leaderEvent.getObserved_leader_node() == null || !leaderEvent.getObserved_leader_node()
          .equals(participantUp.getObserved_leader_node())) {
        continue;
      }
      shardObservedUpIndex = eventId;
      shardObservedUp = leaderEvent;
      break;
    }
    if (shardObservedUpIndex < 0) {
      return delays;
    }

    int shardPostUpIndex = -1;
    LeaderEvent shardPostUp = null;

    for (eventId = shardObservedUpIndex - 1; eventId >= 0; --eventId) {
      LeaderEvent leaderEvent = leaderEvents.get(eventId);
      LeaderEventType eventType = leaderEvent.getEvent_type();
      if (eventType != LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP) {
        continue;
      }
      if (leaderEvent.getObserved_leader_node() == null || !leaderEvent.getObserved_leader_node()
          .equals(participantUp.getObserved_leader_node())) {
        continue;
      }
      shardPostUpIndex = eventId;
      shardPostUp = leaderEvent;
      break;
    }
    if (shardPostUpIndex < 0) {
      return delays;
    }

    int clientUpIndex = -1;
    LeaderEvent clientUp = null;

    // Now find the first time the client reports
    for (eventId = shardPostUpIndex - 1; eventId >= 0; --eventId) {
      LeaderEvent leaderEvent = leaderEvents.get(eventId);
      LeaderEventType eventType = leaderEvent.getEvent_type();
      if (eventType != LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP) {
        continue;
      }
      if (leaderEvent.getObserved_leader_node() == null || !leaderEvent.getObserved_leader_node()
          .equals(participantUp.getObserved_leader_node())) {
        continue;
      }
      clientUpIndex = eventId;
      clientUp = leaderEvent;
      break;
    }
    if (clientUpIndex < 0) {
      return delays;
    }

    delays.e2eLatency = clientUp.getEvent_timestamp_ms() - participantUp.getEvent_timestamp_ms();
    delays.externalViewDelay = shardObservedUp.getEvent_timestamp_ms() - participantUp.getEvent_timestamp_ms();
    delays.shardGenDelay = shardPostUp.getEvent_timestamp_ms() - shardObservedUp.getEvent_timestamp_ms();
    delays.clientAvailableDelay = clientUp.getEvent_timestamp_ms() - shardPostUp.getEvent_timestamp_ms();

    return delays;
  }


  private static LeaderEvent processLeaderEvent(LeaderEvent leaderEvent) {
    if (LeaderEventTypes.participantEventTypes.contains(leaderEvent.getEvent_type())) {
      if (!leaderEvent.isSetObserved_leader_node()) {
        leaderEvent.setObserved_leader_node(leaderEvent.getOriginating_node());
      }
    }
    return leaderEvent;
  }
}


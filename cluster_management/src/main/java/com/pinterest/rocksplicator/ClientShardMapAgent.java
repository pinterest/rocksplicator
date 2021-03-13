/// Copyright 2021 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.config.ConfigCodecEnum;
import com.pinterest.rocksplicator.config.ConfigCodecs;
import com.pinterest.rocksplicator.config.ConfigStore;
import com.pinterest.rocksplicator.shardmapagent.ClusterShardMapAgentManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A Java agent deployed as side-car along with the clients or application that needs routing
 * data downloaded from the zk, as published by the zk based publisher.
 *
 * It can watch multiple cluster's shard_map data. The clusters can be either provided as a static
 * comman separated list or as JSON_ARRAY formatted file. If the names of clusters to fetch
 * shard_map data for is provided through file, the agent will watch for any changes in file
 * and corresponding to that it will either starting watching any new clusters added or stop
 * watching any clusters that are removed from the file.
 *
 * This can also be used as a tool to manually download a cluster's latest shard_map and dump it
 * to the specified directory. by running command as
 *
 * java -cp cluster_management/target/cluster_management-0.0.1-SNAPSHOT-jar-with-dependencies.jar \\
 *    com.pinterest.rocksplicator.ShardMapAgent \\
 *    --shardMapZkSvr zookeeper-server:2181 \\
 *    --clusters=rocksplicator-cluster-name \\
 *    --shardMapDownloadDir=directory_where_shard_maps_are_downloaded
 */
public class ClientShardMapAgent {

  private static final Logger LOG = LoggerFactory.getLogger(ClientShardMapAgent.class);

  private static final String shardMapZkSvrArg = "shardMapZkSvr";
  private static final String clustersArg = "clusters";
  private static final String clustersFileArg = "clustersFile";
  private static final String shardMapDownloadDirArg = "shardMapDownloadDir";
  private static final String disableSharingZkClientArg = "disableSharingZkClient";

  private static Options constructCommandLineOptions() {
    Option shardMapZkSvrOption =
        OptionBuilder.withLongOpt(shardMapZkSvrArg)
            .withDescription("Provide zk server connect string hosting the shard_maps [Required]")
            .create();
    shardMapZkSvrOption.setArgs(1);
    shardMapZkSvrOption.setRequired(true);
    shardMapZkSvrOption.setArgName(shardMapZkSvrArg);

    Option clustersOption = OptionBuilder
        .withLongOpt(clustersArg)
        .withDescription("Provide comma separated clusters to download shard_map for [Optional]")
        .create();
    clustersOption.setArgs(1);
    clustersOption.setRequired(false);
    clustersOption.setArgName(clustersArg);

    Option clustersFileOption = OptionBuilder
        .withLongOpt(clustersFileArg)
        .withDescription("Provide file path containing clusters to download shard_maps [Optional,"
            + " at least one of clusters or clustersFile argument must be provided ]").create();
    clustersFileOption.setArgs(1);
    clustersFileOption.setRequired(false);
    clustersFileOption.setArgName(clustersFileArg);

    Option shardMapDownloadDirOption = OptionBuilder
        .withLongOpt(shardMapDownloadDirArg)
        .withDescription("Provide directory to download shardMap for each cluster").create();
    shardMapDownloadDirOption.setArgs(1);
    shardMapDownloadDirOption.setRequired(true);
    shardMapDownloadDirOption.setArgName(shardMapDownloadDirArg);

    Option disableSharingZkClientOption = OptionBuilder
        .withLongOpt(disableSharingZkClientArg)
        .withDescription("Explicitly disable sharing zk connection for watching multiple clusters")
        .create();
    disableSharingZkClientOption.setArgs(0);
    disableSharingZkClientOption.setRequired(false);
    disableSharingZkClientOption.setArgName(disableSharingZkClientArg);

    Options options = new Options();
    options.addOption(shardMapZkSvrOption)
        .addOption(clustersOption)
        .addOption(clustersFileOption)
        .addOption(shardMapDownloadDirOption)
        .addOption(disableSharingZkClientOption);

    return options;
  }

  private static CommandLine processCommandLineArgs(String[] cliArgs) throws ParseException {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    return cliParser.parse(cliOptions, cliArgs);
  }

  public static void main(String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    BasicConfigurator.configure(new ConsoleAppender(
        new PatternLayout("%d{HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n")
    ));
    CommandLine cmd = processCommandLineArgs(args);

    final String zkConnectString = cmd.getOptionValue(shardMapZkSvrArg);
    final String shardMapDownloadDir = cmd.getOptionValue(shardMapDownloadDirArg);
    final String csClusters = cmd.getOptionValue(clustersArg, "");
    final String clustersFile = cmd.getOptionValue(clustersFileArg, "");
    final boolean disableSharingZkClient = cmd.hasOption(disableSharingZkClientArg);

    Preconditions.checkArgument(!(csClusters.isEmpty() && clustersFile.isEmpty()));

    Supplier<Set<String>> clustersSupplier = null;
    if (!csClusters.isEmpty()) {
      final Set<String> clusters = new HashSet<>();
      String[] clustersArray = csClusters.split(",");
      Preconditions.checkNotNull(clustersArray);
      Preconditions.checkArgument(clustersArray.length > 0);
      for (String cluster : clustersArray) {
        Preconditions.checkNotNull(cluster);
        Preconditions.checkArgument(!cluster.isEmpty());
        clusters.add(cluster);
      }
      Preconditions.checkArgument(!clusters.isEmpty());
      final ImmutableSet<String> immutableClusters = ImmutableSet.copyOf(clusters);
      clustersSupplier = new Supplier<Set<String>>() {
        @Override
        public Set<String> get() {
          return immutableClusters;
        }
      };
    } else {
      final ConfigStore<Set<String>> configStore = new ConfigStore<Set<String>>(
          ConfigCodecs.getDecoder(
              ConfigCodecEnum.JSON_ARRAY), clustersFile);
      clustersSupplier = new Supplier<Set<String>>() {
        @Override
        public Set<String> get() {
          return configStore.get();
        }
      };
    }

    final AtomicReference<CuratorFramework> zkShardMapClientRef = new AtomicReference<>(null);
    if (!disableSharingZkClient) {
      CuratorFramework zkShardMapClient = CuratorFrameworkFactory
          .newClient(zkConnectString,
              new BoundedExponentialBackoffRetry(
                  250, 10000, 60));

      zkShardMapClient.start();
      try {
        zkShardMapClient.blockUntilConnected(120, TimeUnit.SECONDS);
        zkShardMapClientRef.set(zkShardMapClient);
      } catch (InterruptedException e) {
        zkShardMapClient.close();
        throw new RuntimeException();
      }
    }

    ClusterShardMapAgentManager handler =
        new ClusterShardMapAgentManager(zkConnectString, zkShardMapClientRef.get(),
            shardMapDownloadDir, clustersSupplier);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          handler.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        if (zkShardMapClientRef.get() != null) {
          zkShardMapClientRef.get().close();
        }
      }
    });

    LOG.error("ShardMapAgent running");
    Thread.currentThread().join();
  }
}

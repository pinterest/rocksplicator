/// Copyright 2017 Pinterest Inc.
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
// @author Terry Shen (fshen@pinterest.com)
//

package com.pinterest.rocksplicator;

import static org.apache.curator.framework.state.ConnectionState.CONNECTED;

import com.pinterest.rocksplicator.eventstore.ClientShardMapLeaderEventLoggerDriver;
import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisherBuilder;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.HelixManagerShutdownHook;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Spectator {
  private static final Logger LOG = LoggerFactory.getLogger(Spectator.class);
  private static final String zkServer = "zkSvr";
  private static final String cluster = "cluster";
  private static final String hostAddress = "host";
  private static final String hostPort = "port";
  private static final String configPostUrl = "configPostUrl";
  private static final String shardMapZkSvrArg = "shardMapZkSvr";

  private static final String handoffEventHistoryzkSvr = "handoffEventHistoryzkSvr";
  private static final String handoffEventHistoryConfigPath = "handoffEventHistoryConfigPath";
  private static final String handoffEventHistoryConfigType = "handoffEventHistoryConfigType";
  private static final String  handoffClientEventHistoryJsonShardMapPath = "handoffClientEventHistoryJsonShardMapPath";

  private static LeaderEventsLogger staticClientLeaderEventsLogger = null;
  private static ClientShardMapLeaderEventLoggerDriver
      staticClientShardMapLeaderEventLoggerDriver = null;


  private final HelixManager helixManager;
  private final RocksplicatorMonitor monitor;
  private LeaderEventsLogger spectatorLeaderEventsLogger;

  private ConfigGenerator configGenerator = null;

  private static Options constructCommandLineOptions() {
    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServer).withDescription("Provide zookeeper addresses").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddresses(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option hostOption =
        OptionBuilder.withLongOpt(hostAddress).withDescription("Provide host name").create();
    hostOption.setArgs(1);
    hostOption.setRequired(true);
    hostOption.setArgName("Host name (Required)");

    Option portOption =
        OptionBuilder.withLongOpt(hostPort).withDescription("Provide host port").create();
    portOption.setArgs(1);
    portOption.setRequired(true);
    portOption.setArgName("Host port (Required)");

    // we keep this optional, so that we can test without posting to any url
    Option configPostUrlOption =
        OptionBuilder.withLongOpt(configPostUrl).withDescription("URL to post config (Optional)").create();
    configPostUrlOption.setArgs(1);
    configPostUrlOption.setRequired(false);
    configPostUrlOption.setArgName("URL to post config (Optional)");

    Option shardMapZkSvrOption =
        OptionBuilder.withLongOpt(shardMapZkSvrArg).withDescription("Zk Server to post shard_map").create();
    shardMapZkSvrOption.setArgs(1);
    shardMapZkSvrOption.setRequired(false);
    shardMapZkSvrOption.setArgName(shardMapZkSvrArg);

    Option handoffEventHistoryzkSvrOption =
        OptionBuilder.withLongOpt(handoffEventHistoryzkSvr)
            .withDescription(
                "zk Connect String to enable state transitions event history logging").create();
    handoffEventHistoryzkSvrOption.setArgs(1);
    handoffEventHistoryzkSvrOption.setRequired(false);
    handoffEventHistoryzkSvrOption.setArgName(
        "Zk connect string for logging state transitions for leader handoff profiling (Optional)");

    Option  handoffEventHistoryConfigPathOption =
        OptionBuilder.withLongOpt(handoffEventHistoryConfigPath)
            .withDescription(
                "local disk path to config containing resources to enable for handoff events history tracking")
            .create();
    handoffEventHistoryConfigPathOption.setArgs(1);
    handoffEventHistoryConfigPathOption.setRequired(false);
    handoffEventHistoryConfigPathOption.setArgName(
        "config path containing resources to enabled for handoff events history (Optional)");

    Option  handoffEventHistoryConfigTypeOption =
        OptionBuilder.withLongOpt(handoffEventHistoryConfigType)
            .withDescription(
                "format of config for resources to track handoff events [LINE_TERMINATED / JSON_ARRAY]")
            .create();
    handoffEventHistoryConfigTypeOption.setArgs(1);
    handoffEventHistoryConfigTypeOption.setRequired(false);
    handoffEventHistoryConfigTypeOption.setArgName(
        "format of config for resources to track handoff events [LINE_TERMINATED / JSON_ARRAY] (Optional)");

    Option  handoffClientEventHistoryJsonShardMapPathOption =
        OptionBuilder.withLongOpt(handoffClientEventHistoryJsonShardMapPath)
            .withDescription(
                "path to shard_map in json format, generated by Spectator")
            .create();
    handoffClientEventHistoryJsonShardMapPathOption.setArgs(1);
    handoffClientEventHistoryJsonShardMapPathOption.setRequired(false);
    handoffClientEventHistoryJsonShardMapPathOption.setArgName(
        "path to shard_map in json format, generated by Spectator");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(configPostUrlOption)
        .addOption(shardMapZkSvrOption)
        .addOption(handoffEventHistoryzkSvrOption)
        .addOption(handoffEventHistoryConfigPathOption)
        .addOption(handoffEventHistoryConfigTypeOption)
        .addOption(handoffClientEventHistoryJsonShardMapPathOption);
    return options;
  }

  private static CommandLine processCommandLineArgs(String[] cliArgs) throws ParseException {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    return cliParser.parse(cliOptions, cliArgs);
  }

  /**
   * Start a Helix spectator.
   * @param args command line parameters
   */
  public static void main(String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.WARN);
    BasicConfigurator.configure(new ConsoleAppender(
        new PatternLayout("%d{HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n")
    ));
    CommandLine cmd = processCommandLineArgs(args);
    final String zkConnectString = cmd.getOptionValue(zkServer);
    final String clusterName = cmd.getOptionValue(cluster);
    final String host = cmd.getOptionValue(hostAddress);
    final String port = cmd.getOptionValue(hostPort);
    final String postUrl = cmd.getOptionValue(configPostUrl, "");
    final String shardMapZkSvr = cmd.getOptionValue(shardMapZkSvrArg, "");
    final String instanceName = host + "_" + port;

    final String zkEventHistoryStr = cmd.getOptionValue(handoffEventHistoryzkSvr, "");
    final String resourceConfigPath = cmd.getOptionValue(handoffEventHistoryConfigPath, "");
    final String resourceConfigType = cmd.getOptionValue(handoffEventHistoryConfigType, "");
    final String shardMapPath = cmd.getOptionValue(handoffClientEventHistoryJsonShardMapPath, "");

    LeaderEventsLogger spectatorLeaderEventsLogger =
        new LeaderEventsLoggerImpl(instanceName,
            zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType,
            Optional.of(128));

    if ((shardMapPath != null || !shardMapPath.isEmpty())) {
      staticClientLeaderEventsLogger = new LeaderEventsLoggerImpl(instanceName,
          zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType, Optional.empty());

      staticClientShardMapLeaderEventLoggerDriver = new ClientShardMapLeaderEventLoggerDriver(
          clusterName, shardMapPath, staticClientLeaderEventsLogger, zkEventHistoryStr);
    }

    LOG.error("Starting spectator with ZK:" + zkConnectString);
    final Spectator spectator = new Spectator(zkConnectString, clusterName, instanceName,
        spectatorLeaderEventsLogger);

    CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkConnectString, new ExponentialBackoffRetry(1000, 3));

    zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
      // If the connection state changes to LOST/SUSPENDED, then it means it
      // already lost the lock/would release the lock and the current leader
      // role is not legal any more. Here we simply terminate it and rely
      // on restarting to re-acquire the inter-process lock.
        if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
          LOG.error("ZK lock is lost and the process needs to be terminated");
          // Stop all the listeners before exiting
          try {
            spectator.stopListeners();
          } catch (Exception e) {
            LOG.error("Failed to stop the listeners", e);
          }
          System.exit(0);
        } else if (newState == CONNECTED) {
          LOG.error("Connected to zk: " + zkConnectString);
        }
      }
    });
    zkClient.start();

    try {
      zkClient.blockUntilConnected(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.out.println("Cannot connect to zk in 60 seconds");
      throw new RuntimeException(e);
    }

    InterProcessMutex mutex = new InterProcessMutex(zkClient, getClusterLockPath(clusterName));
    LOG.error("Trying to obtain lock");

    try (Locker locker = new Locker(mutex)) {
      LOG.error("Obtained lock");
      spectator.startListener(postUrl, shardMapZkSvr);
      Thread.currentThread().join();
    } catch (RuntimeException e) {
      LOG.error("RuntimeException thrown by cluster " + clusterName, e);
    } catch (Exception e) {
      LOG.error("Failed to release the mutex for cluster " + clusterName, e);
    }
    LOG.error("Returning from main");
  }

  public Spectator(String zkConnectString, String clusterName, String instanceName,
                   LeaderEventsLogger spectatorLeaderEventsLogger) throws Exception {
    helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectString);

    monitor = new RocksplicatorMonitor(clusterName, instanceName);
    this.spectatorLeaderEventsLogger = spectatorLeaderEventsLogger;
    helixManager.connect();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          stopListeners();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  private void startListener(String postUrl, String shardMapZkSvr) throws Exception {
    if (this.configGenerator == null) {
      ShardMapPublisherBuilder publisherBuilder = ShardMapPublisherBuilder
          .create(helixManager.getClusterName()).withLocalDump();
      if (postUrl != null && !postUrl.isEmpty()) {
        publisherBuilder = publisherBuilder.withPostUrl(postUrl);
      }
      if (shardMapZkSvr != null && !shardMapZkSvr.isEmpty()) {
        publisherBuilder = publisherBuilder.withZkShardMap(shardMapZkSvr);
      }

      this.configGenerator = new ConfigGenerator(
          helixManager.getClusterName(),
          helixManager,
          publisherBuilder.build(),
          monitor, new ExternalViewLeaderEventsLoggerImpl(spectatorLeaderEventsLogger));

      /**
       * Add to the helixManager, message handlers.
       */
      helixManager.addExternalViewChangeListener(configGenerator);
      helixManager.addConfigChangeListener(configGenerator);
    }
  }

  private void stopListeners() throws Exception {
    if (helixManager != null) {
      Thread thread = new HelixManagerShutdownHook(helixManager);
      thread.start();
      thread.join();
    }

    if (configGenerator != null) {
      try {
        LOG.error("Stopping ConfigGenerator");
        configGenerator.close();
      } catch (IOException ioe) {
        LOG.error("Exception while closing ConfigGenertor", ioe);
      } finally {
      configGenerator = null;
      }
    }

    if (spectatorLeaderEventsLogger != null) {
      try {
        LOG.error("Stopping Spectator LeaderEventsLogger");
        spectatorLeaderEventsLogger.close();
      } catch (IOException ioe) {
        LOG.error("Exception while closing Spectator's LeaderEventsLogger", ioe);
      }
      spectatorLeaderEventsLogger = null;
    }

    if (staticClientShardMapLeaderEventLoggerDriver != null) {
      try {
        LOG.error("Stopping ClientShardMap LeaderEventLogger Driver");
        staticClientShardMapLeaderEventLoggerDriver.close();
      } catch (IOException ioe) {
        LOG.error("Exception while closing Spectator's ClientShardMapLeaderEventLoggerDriver", ioe);
      }
      staticClientShardMapLeaderEventLoggerDriver = null;
    }

    if (staticClientLeaderEventsLogger != null) {
      try {
        LOG.error("Stopping client LeaderEventsLogger");
        staticClientLeaderEventsLogger.close();
      } catch (IOException ioe) {
        LOG.error("Exception while closing Spectator's Client LeaderEventLogger", ioe);
      }
      staticClientLeaderEventsLogger = null;
    }
  }

  private static String getClusterLockPath(String cluster) {
    return "/rocksplicator/" + cluster + "/spectator/lock";
  }
}

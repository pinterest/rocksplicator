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

import com.pinterest.rocksplicator.spectator.ConfigGeneratorClusterSpectatorFactory;
import com.pinterest.rocksplicator.spectator.DistClusterSpectatorStateModelFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.HelixManagerShutdownHook;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * start spectator cluster manager controller
 * spectator cluster manager controller runs in distributed mode:
 * distributed mode: in this mode each spectator first joins as controller and participant into
 *   a special spectator CONTROLLER_CLUSTER. Leader election happens in this special
 *   cluster. The one that becomes the leader controls all spectators (including itself
 *   to become leaders of other clusters.
 *
 * The participant clusters are added as a resource into the special spectator controller cluster.
 * The leader of the spectator controller cluster assign each resource (aka the helix participant
 * clusters to be managed) one leader and multiple standbys. The leader instance of this special
 * cluster performs the task of spectator for the participant cluster.
 */

public class DistributedSpectatorMain {

  private static final Logger logger = LoggerFactory.getLogger(DistributedSpectatorMain.class);

  private static final String zkSvr = "zkSvr";
  private static final String cluster = "cluster";
  private static final String help = "help";
  private static final String host = "host";
  private static final String port = "port";
  private static final String shardMapPostUriPattern = "shardMapPostUriPattern";
  private static final String shardMapZkSvrArg = "shardMapZkSvr";
  private static final String shardMapDownloadDirArg = "shardMapDownloadDir";
  private static final String enableCurrentStatesRouterArgs = "enableCurrentStatesRouter";

  // hack: OptionalBuilder is not thread safe
  @SuppressWarnings("static-access")
  synchronized private static Options constructCommandLineOptions() {
    Option
        helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

    Option
        zkServerOption =
        OptionBuilder.withLongOpt(zkSvr).withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option
        clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide spectator cluster name")
            .create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("spectator cluster name (Required)");

    Option
        hostOption =
        OptionBuilder.withLongOpt(host).withDescription("Provide cluster host").create();
    hostOption.setArgs(1);
    hostOption.setRequired(true);
    hostOption.setArgName("cluster spectator host (Required)");

    Option
        portOption =
        OptionBuilder.withLongOpt(port).withDescription("Provide cluster port").create();
    portOption.setArgs(1);
    portOption.setRequired(true);
    portOption.setArgName("cluster spectator port (Required)");

    Option shardMapPostUriPatternOption =
        OptionBuilder.withLongOpt(shardMapPostUriPattern)
            .withDescription("Provide uri pattern with template for clusterName,"
                + " e.g..  http://config.company.com?configDomain=rocksplicator&"
                + "configKey=[PARTICIPANT_CLUSTER]&overwrite=true."
                + " Spectator leader will automaticallt replace [PARTICIPANT_CLUSTER] "
                + "part with the participant cluster for which the config"
                + "is being generated").create();
    portOption.setArgs(1);
    portOption.setRequired(false);
    portOption.setArgName("uri pattern to post json shard_map [Optional]");

    Option shardMapZkSvrOption =
        OptionBuilder.withLongOpt(shardMapZkSvrArg).withDescription("Zk Server to post shard_map")
            .create();
    shardMapZkSvrOption.setArgs(1);
    shardMapZkSvrOption.setRequired(false);
    shardMapZkSvrOption.setArgName(shardMapZkSvrArg);

    Option shardMapDownloadDirOption = OptionBuilder
        .withLongOpt(shardMapDownloadDirArg)
        .withDescription("Provide directory to download shardMap for each cluster [Optional]"
            + " If this option is provided, also must provide shardMapZkSvr option to download"
            + " cluster sghard_map data from").create();
    shardMapDownloadDirOption.setArgs(1);
    shardMapDownloadDirOption.setRequired(false);
    shardMapDownloadDirOption.setArgName(shardMapDownloadDirArg);

    Option enableCurrentStatesRouterOption =
        OptionBuilder.withLongOpt(enableCurrentStatesRouterArgs)
            .withDescription("Enable to use currentStates based RoutingTable for ConfigGenerator")
            .create();
    enableCurrentStatesRouterOption.setArgs(0);
    enableCurrentStatesRouterOption.setRequired(false);
    enableCurrentStatesRouterOption.setArgName(enableCurrentStatesRouterArgs);

    Options options = new Options();
    options.addOption(helpOption)
        .addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(shardMapPostUriPatternOption)
        .addOption(shardMapZkSvrOption)
        .addOption(shardMapDownloadDirOption)
        .addOption(enableCurrentStatesRouterOption);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter
        .printHelp("java " + DistributedSpectatorMain.class
                .getName(),
            cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();

    try {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      logger.error("fail to parse command-line options. cliArgs: " + Arrays.toString(cliArgs), pe);
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static HelixManager startHelixController(
      final String zkConnectString,
      final String clusterName,
      final String controllerName,
      final String shardMapPostUriPattern,
      final String shardMapZkSvr,
      final String shardMapDownloadDir,
      final boolean enableCurrentStatesRouter) {
    HelixManager manager = null;
    try {
      manager = HelixManagerFactory.getZKHelixManager(
          clusterName,
          controllerName,
          InstanceType.CONTROLLER_PARTICIPANT,
          zkConnectString);

      DistClusterSpectatorStateModelFactory stateModelFactory =
          new DistClusterSpectatorStateModelFactory(zkConnectString,
              new ConfigGeneratorClusterSpectatorFactory(
                  shardMapPostUriPattern,
                  shardMapZkSvr,
                  shardMapDownloadDir,
                  enableCurrentStatesRouter));

      StateMachineEngine stateMach = manager.getStateMachineEngine();
      stateMach.registerStateModelFactory("LeaderStandby", stateModelFactory);
      manager.connect();
    } catch (Exception e) {
      logger.error("Exception while starting controller", e);
    }
    return manager;
  }

  public static void main(String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    BasicConfigurator.configure(new ConsoleAppender(
        new PatternLayout("%d{HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n")
    ));

    CommandLine cmd = processCommandLineArgs(args);
    final String zkConnectString = cmd.getOptionValue(zkSvr);
    final String clusterName = cmd.getOptionValue(cluster);
    final String hostName = cmd.getOptionValue(host);
    final Integer portInt = Integer.parseInt(cmd.getOptionValue(port));
    final String configPostUriPattern = cmd.getOptionValue(shardMapPostUriPattern, "");
    final String shardMapZkSvr = cmd.getOptionValue(shardMapZkSvrArg, "");
    final String shardMapDownloadDir = cmd.getOptionValue(shardMapDownloadDirArg, "");
    final boolean enableCurrentStatesRouter = cmd.hasOption(enableCurrentStatesRouterArgs);

    String instanceId = String.format("%s_%d", hostName, portInt);

    logger.info("Cluster manager started, zkServer: " + zkConnectString + ", clusterName:"
        + clusterName + ", spectatorControllerName:" + instanceId + ", mode:" + "DISTRIBUTED");

    HelixManager manager =
        startHelixController(
            zkConnectString,
            clusterName,
            instanceId,
            configPostUriPattern,
            shardMapZkSvr,
            shardMapDownloadDir,
            enableCurrentStatesRouter);

    Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(manager));

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      logger.info("spectatorController:" + instanceId + ", " + Thread.currentThread().getName()
          + " interrupted");
    } finally {
      manager.disconnect();
    }
  }
}


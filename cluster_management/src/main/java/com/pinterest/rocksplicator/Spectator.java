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

import java.util.HashMap;
import java.util.Map;
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
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.HelixManagerShutdownHook;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
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

  private HelixManager helixManager;

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

    Option configPostUrlOption =
        OptionBuilder.withLongOpt(configPostUrl).withDescription("URL to post config").create();
    configPostUrlOption.setArgs(1);
    configPostUrlOption.setRequired(true);
    configPostUrlOption.setArgName("URL to post config (Required)");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(configPostUrlOption);
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
    final String postUrl = cmd.getOptionValue(configPostUrl);
    final String instanceName = host + "_" + port;

    LOG.error("Starting spectator with ZK:" + zkConnectString);
    Spectator spectator= new Spectator(zkConnectString, clusterName, instanceName);

    CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkConnectString, new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
    InterProcessMutex mutex = new InterProcessMutex(zkClient, getClusterLockPath(clusterName));
    try (Locker locker = new Locker(mutex)) {
      spectator.startListener(postUrl);
      Thread.currentThread().join();
    } catch (RuntimeException e) {
      LOG.error("RuntimeException thrown by cluster " + clusterName, e);
    } catch (Exception e) {
      LOG.error("Failed to release the mutex for cluster " + clusterName, e);
    }
  }

  public Spectator(String zkConnectString, String clusterName, String instanceName) throws Exception {
    helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectString);
    helixManager.connect();
    Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(helixManager));
  }

  private void startListener(String postUrl) throws Exception {
    ConfigGenerator configGenerator = new ConfigGenerator(helixManager.getClusterName(), helixManager, postUrl);
    helixManager.addExternalViewChangeListener(configGenerator);
    helixManager.addConfigChangeListener(configGenerator);
  }

  private static String getClusterLockPath(String cluster) {
    return "/rocksplicator/" + cluster + "/spectator/lock";
  }
}

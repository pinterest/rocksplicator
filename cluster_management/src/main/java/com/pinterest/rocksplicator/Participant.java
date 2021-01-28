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
// @author bol (bol@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.task.BackupTaskFactory;
import com.pinterest.rocksplicator.task.DedupTaskFactory;
import com.pinterest.rocksplicator.task.RestoreTaskFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.HelixManagerShutdownHook;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.HelixCustomCodeRunner;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Participant {

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);
  private static final String zkServer = "zkSvr";
  private static final String cluster = "cluster";
  private static final String domain = "domain";
  private static final String hostAddress = "host";
  private static final String hostPort = "port";
  private static final String stateModel = "stateModelType";
  private static final String configPostUrl = "configPostUrl";
  private static final String s3Bucket = "s3Bucket";
  private static final String disableSpectator = "disableSpectator";
  private static final String handoffEventHistoryzkSvr = "handoffEventHistoryzkSvr";
  private static final String handoffEventHistoryConfigPath = "handoffEventHistoryConfigPath";
  private static final String handoffEventHistoryConfigType = "handoffEventHistoryConfigType";

  private static HelixManager helixManager = null;
  private static LeaderEventsLogger leaderEventsLogger = null;
  private StateModelFactory<StateModel> stateModelFactory;
  private final RocksplicatorMonitor monitor;

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

    Option domainOption =
        OptionBuilder.withLongOpt(domain).withDescription("Provide instance domain").create();
    domainOption.setArgs(1);
    domainOption.setRequired(true);
    domainOption.setArgName("Instance domain (Required)");

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

    Option stateModelOption =
        OptionBuilder.withLongOpt(stateModel).withDescription("StateModel Type").create();
    stateModelOption.setArgs(1);
    stateModelOption.setRequired(true);
    stateModelOption.setArgName("StateModel Type (Required)");

    Option configPostUrlOption =
        OptionBuilder.withLongOpt(configPostUrl).withDescription("URL to post config").create();
    configPostUrlOption.setArgs(1);
    configPostUrlOption.setRequired(true);
    configPostUrlOption.setArgName("URL to post config (Required)");

    Option s3BucketOption =
        OptionBuilder.withLongOpt(s3Bucket).withDescription("S3 Bucket").create();
    s3BucketOption.setArgs(1);
    s3BucketOption.setRequired(false);
    s3BucketOption.setArgName("S3 Bucket (Optional)");

    Option disableSpectatorOption =
        OptionBuilder.withLongOpt(disableSpectator).withDescription("Disable Spectator").create();
    disableSpectatorOption.setArgs(0);
    disableSpectatorOption.setRequired(false);
    disableSpectatorOption.setArgName("Disable Spectator (Optional)");

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

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(domainOption)
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(stateModelOption)
        .addOption(configPostUrlOption)
        .addOption(s3BucketOption)
        .addOption(disableSpectatorOption)
        .addOption(handoffEventHistoryzkSvrOption)
        .addOption(handoffEventHistoryConfigPathOption)
        .addOption(handoffEventHistoryConfigTypeOption);
    return options;
  }

  private static CommandLine processCommandLineArgs(String[] cliArgs) throws ParseException {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    return cliParser.parse(cliOptions, cliArgs);
  }

  /**
   * Start a Helix participant.
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
    final String domainName = cmd.getOptionValue(domain);
    final String host = cmd.getOptionValue(hostAddress);
    final String port = cmd.getOptionValue(hostPort);
    final String stateModelType = cmd.getOptionValue(stateModel);
    final String postUrl = cmd.getOptionValue(configPostUrl);
    final String instanceName = host + "_" + port;
    String s3BucketName = "";
    final boolean useS3Backup = cmd.hasOption(s3Bucket);
    if (useS3Backup) {
      s3BucketName = cmd.getOptionValue(s3Bucket);
    }
    final boolean runSpectator = !cmd.hasOption(disableSpectator);

    final String zkEventHistoryStr = cmd.getOptionValue(handoffEventHistoryzkSvr, "");
    final String resourceConfigPath = cmd.getOptionValue(handoffEventHistoryConfigPath, "");
    final String resourceConfigType = cmd.getOptionValue(handoffEventHistoryConfigType, "");

    /**
     * Note the last parameter is empty, since we don't dictate maxEventsToKeep from participants.
     */
    leaderEventsLogger =
        new LeaderEventsLoggerImpl(instanceName,
            zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType, Optional.empty());

    LOG.error("Starting participant with ZK:" + zkConnectString);
    Participant participant = new Participant(zkConnectString, clusterName, instanceName,
        stateModelType, Integer.parseInt(port), postUrl, useS3Backup, s3BucketName, runSpectator, leaderEventsLogger);

    HelixAdmin helixAdmin = new ZKHelixAdmin(zkConnectString);
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
            .forCluster(clusterName).forParticipant(instanceName)
            .build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("DOMAIN", domainName + ",instance=" + instanceName);
    helixAdmin.setConfig(scope, properties);

    LOG.error("Participant running");
    Thread.currentThread().join();
  }

  /**
   * This method is called from CPP code, when the service is about to do down.
   * This will cause all states to transition to offline eventually.
   */
  public static void shutDownParticipant() throws Exception {
    LOG.error("Disconnect the helixManager");
    if (helixManager != null) {
      helixManager.disconnect();
      helixManager = null;
    }
    if (leaderEventsLogger != null) {
      leaderEventsLogger.close();
      leaderEventsLogger = null;
    }
  }

  private Participant(String zkConnectString, String clusterName, String instanceName,
                      String stateModelType, int port, String postUrl, boolean useS3Backup,
                      String s3BucketName, boolean runSpectator, LeaderEventsLogger leaderEventsLogger)
      throws Exception {
    helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, zkConnectString);

    monitor = new RocksplicatorMonitor(clusterName, instanceName);

    Map<String, TaskFactory> taskFactoryRegistry = new HashMap<>();

    if (stateModelType.equals("OnlineOffline")) {
      stateModelFactory = new OnlineOfflineStateModelFactory(port, zkConnectString, clusterName);
    } else if (stateModelType.equals("Cache")) {
      stateModelType = "OnlineOffline";
      stateModelFactory = new CacheStateModelFactory();
    } else if (stateModelType.equals("MasterSlave")) {
      stateModelFactory = new MasterSlaveStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName, leaderEventsLogger);
    } else if (stateModelType.equals("LeaderFollower")) {
      stateModelFactory = new LeaderFollowerStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName, leaderEventsLogger);
    } else if (stateModelType.equals("Bootstrap")) {
      stateModelFactory = new BootstrapStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName);
    } else if (stateModelType.equals("MasterSlave;Task")) {
      stateModelType = "MasterSlave";
      stateModelFactory = new MasterSlaveStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName, leaderEventsLogger);

      taskFactoryRegistry
          .put("Backup", new BackupTaskFactory(clusterName, port, useS3Backup, s3BucketName));
      taskFactoryRegistry
          .put("Restore", new RestoreTaskFactory(clusterName, port, useS3Backup, s3BucketName));
    } else if (stateModelType.equals("LeaderFollower;Task")) {
      stateModelType = "LeaderFollower";
      stateModelFactory = new LeaderFollowerStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName, leaderEventsLogger);

      taskFactoryRegistry
          .put("Backup", new BackupTaskFactory(clusterName, port, useS3Backup, s3BucketName));
      taskFactoryRegistry
          .put("Restore", new RestoreTaskFactory(clusterName, port, useS3Backup, s3BucketName));
    } else if (stateModelType.equals("Task")) {
      taskFactoryRegistry
          .put("Dedup", new DedupTaskFactory(clusterName, port, useS3Backup, s3BucketName));
    } else {
      LOG.error("Unknown state model: " + stateModelType);
    }

    StateMachineEngine stateMach = helixManager.getStateMachineEngine();

    if (stateModelFactory != null) {
      // for Task only, stateModelFactory declared without instantiation, thus, null
      stateMach.registerStateModelFactory(stateModelType, stateModelFactory);
      LOG.error(
          String.format("%s has registered to state model: %s", clusterName, stateModelType));
    }
    if (!taskFactoryRegistry.isEmpty()) {
      TaskStateModelFactory helixTaskFactory =
          new TaskStateModelFactory(helixManager, taskFactoryRegistry);

      stateMach.registerStateModelFactory("Task", helixTaskFactory);

      LOG.error(clusterName + " has registered to Tasks: " + taskFactoryRegistry.keySet());
    }

    helixManager.connect();
    helixManager.getMessagingService().registerMessageHandlerFactory(
        Message.MessageType.STATE_TRANSITION.name(), stateMach);

    Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(helixManager));

    if (runSpectator) {
      // Add callback to create rocksplicator shard config
      HelixCustomCodeRunner codeRunner = new HelixCustomCodeRunner(helixManager, zkConnectString)
          .invoke(new ConfigGenerator(clusterName, helixManager, postUrl, monitor))
          .on(HelixConstants.ChangeType.EXTERNAL_VIEW, HelixConstants.ChangeType.CONFIG)
          .usingLeaderStandbyModel("ConfigWatcher" + clusterName);

      codeRunner.start();
    }
  }
}

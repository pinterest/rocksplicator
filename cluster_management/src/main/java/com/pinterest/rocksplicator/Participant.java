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

import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.task.BackupTaskFactory;
import com.pinterest.rocksplicator.task.DedupTaskFactory;
import com.pinterest.rocksplicator.task.RestoreTaskFactory;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


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
  private static LeaderEventsLogger staticParticipantLeaderEventsLogger = null;
  private static LeaderEventsLogger staticSpectatorLeaderEventsLogger = null;
  private static StateModelFactory<StateModel> stateModelFactory;
  private static String staticClusterName = null;
  private static String staticInstanceId = null;
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
     * Note that the last parameter is empty, since we don't dictate
     * maxEventsToKeep from participants.
     */
    staticParticipantLeaderEventsLogger =
        new LeaderEventsLoggerImpl(instanceName,
            zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType, Optional.empty());

    if (runSpectator) {
      staticSpectatorLeaderEventsLogger =
          new LeaderEventsLoggerImpl(instanceName,
              zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType,
              Optional.of(128));
    }

    staticClusterName = clusterName;
    staticInstanceId = instanceName;

    LOG.error("Starting participant with ZK:" + zkConnectString);
    Participant participant = new Participant(zkConnectString, clusterName, instanceName, domainName,
        stateModelType, Integer.parseInt(port), postUrl, useS3Backup, s3BucketName, runSpectator);

    LOG.error("Participant running");
    Thread.currentThread().join();
  }

  private Participant(String zkConnectString, String clusterName, String instanceName, String domainName,
                      String stateModelType, int port, String postUrl, boolean useS3Backup,
                      String s3BucketName, boolean runSpectator)
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
          port, zkConnectString, clusterName, useS3Backup, s3BucketName,
          staticParticipantLeaderEventsLogger);
    } else if (stateModelType.equals("LeaderFollower")) {
      stateModelFactory = new LeaderFollowerStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName,
          staticParticipantLeaderEventsLogger);
    } else if (stateModelType.equals("Bootstrap")) {
      stateModelFactory = new BootstrapStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName);
    } else if (stateModelType.equals("MasterSlave;Task")) {
      stateModelType = "MasterSlave";
      stateModelFactory = new MasterSlaveStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName,
          staticParticipantLeaderEventsLogger);

      taskFactoryRegistry
          .put("Backup", new BackupTaskFactory(clusterName, port, useS3Backup, s3BucketName));
      taskFactoryRegistry
          .put("Restore", new RestoreTaskFactory(clusterName, port, useS3Backup, s3BucketName));
    } else if (stateModelType.equals("LeaderFollower;Task")) {
      stateModelType = "LeaderFollower";
      stateModelFactory = new LeaderFollowerStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName,
          staticParticipantLeaderEventsLogger);

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

    helixManager.connect();

    registerInstanceDomain(helixManager.getClusterManagmentTool(), clusterName, instanceName, domainName);

    enableInstance(helixManager.getClusterManagmentTool(), clusterName, instanceName);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (helixManager != null && helixManager.isConnected()) {
          disableInstance(helixManager.getClusterManagmentTool(), clusterName, instanceName);
          new HelixManagerShutdownHook(helixManager).run();
        }
      }
    });

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

    helixManager.getMessagingService().registerMessageHandlerFactory(
        Message.MessageType.STATE_TRANSITION.name(), stateMach);

    if (runSpectator) {
      // Add callback to create rocksplicator shard config
      ConfigGenerator configGenerator = new ConfigGenerator(clusterName, helixManager, postUrl, monitor,
          new ExternalViewLeaderEventsLoggerImpl(staticSpectatorLeaderEventsLogger));

      HelixCustomCodeRunner codeRunner = new HelixCustomCodeRunner(helixManager, zkConnectString)
          .invoke(configGenerator)
          .on(HelixConstants.ChangeType.EXTERNAL_VIEW, HelixConstants.ChangeType.CONFIG)
          .usingLeaderStandbyModel("ConfigWatcher" + clusterName);

      codeRunner.start();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOG.error("Stopping HelixCustomCodeRunner running spectator config generator");
          codeRunner.stop();
          try {
            LOG.error("Stopping ConfigGenerator");
            configGenerator.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
          try {
            LOG.error("Stopping Spectator LeaderEventsLogger");
            staticSpectatorLeaderEventsLogger.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  /**
   * This method is called from CPP code, when the service is about to do down.
   * This will cause all states to transition to offline eventually.
   */
  public static void shutDownParticipant() throws Exception {

    /**
     * TODO: grajpurohit: Improve the logic to cleanly shutdown.
     *
     * This is probably not the right way. When a participant is going down,
     * we need a chance to cleanly offline the currently top-state and second
     * top-state resources (e.g.. all partitions state should be brought to offline
     * state) before going down. That will be a graceful shutdown. However, when
     * helixManager disconnects, it releases all message handlers / listeners there after
     * there is no way for resources to be brought down through state transition.
     *
     * Ideal way will be whereby
     *
     * 1. We setup instanceTag "disabled" so that spectator has a first chance to be notified that
     * this participant is going down without bringing down the hosted partition replicas.
     *
     * 2. Wait to ensure that the instanceTag has been added to the participant instance.
     *
     * 3. We should then disable this instance (currently this is the only way we figured out to
     * force controller to issue state transitions to existing current states of all partitions
     * hosted by this participant) to move to OFFLINE state.
     *
     * 4. Wait until all partition replicas hosted by this participant to have moved to OFFLINE state.
     *
     * 5. Now we are free to disconnect to HelixManager, so that all listeners and message callback
     * handlers are properly un-registered.
     */
    if (helixManager != null && helixManager.isConnected()) {
      disableInstance(helixManager.getClusterManagmentTool(), staticClusterName, staticInstanceId);
    }

    if (helixManager != null) {
      new HelixManagerShutdownHook(helixManager).run();
      helixManager = null;
    }
    if (staticParticipantLeaderEventsLogger != null) {
      LOG.error("Stopping Participant LeaderEventsLogger");
      staticParticipantLeaderEventsLogger.close();
      staticParticipantLeaderEventsLogger = null;
    }
  }

  private static void registerInstanceDomain(
      final HelixAdmin helixAdmin,
      final String clusterName,
      final String instanceName,
      final String domainName) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
            .forCluster(clusterName).forParticipant(instanceName)
            .build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("DOMAIN", domainName + ",instance=" + instanceName);
    helixAdmin.setConfig(scope, properties);
  }

  private static void disableInstance(
      final HelixAdmin helixAdmin,
      final String clusterName,
      final String instanceId) {
    retryWithBoundedExponentialBackOff(
        new ExceptionalAction() {
          @Override
          public void apply() throws Exception {
            LOG.error("Disabling the instance to force offlining existing resources");
            helixAdmin.enableInstance(clusterName, instanceId, false);
          }
        },
        new ExceptionalCondition() {
          @Override
          public boolean apply() throws Exception {
            // When the instance is no longer enabled
            return ! helixAdmin.getInstanceConfig(clusterName, instanceId).getInstanceEnabled();
          }
        }, 10);

    retryWithBoundedExponentialBackOff(
        new ExceptionalAction() {
          @Override
          public void apply() throws Exception {
            LOG.error(
                "Adding \"disabled\" instance tag, to force spectator to remove this instance from "
                    + "shard_map ");
            helixAdmin.addInstanceTag(clusterName, instanceId, "disabled");
          }
        },
        new ExceptionalCondition() {
          @Override
          public boolean apply() throws Exception {
            // When the instance config tag contains the "disabled" tag.
            return helixAdmin.getInstanceConfig(clusterName, instanceId).containsTag("disabled");
          }
        }, 10);

    /**
     * Here we need a mechanism to wait for all instances to go offline. However currently that logic
     * is complicated with available java api. Hence we simply wait for 5 seconds, in the hope
     * that the resources will be offlined in 5 seconds.
     */
    try {
      Thread.currentThread().sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void enableInstance(
      final HelixAdmin helixAdmin,
      final String clusterName,
      final String instanceId) {

    retryWithBoundedExponentialBackOff(
        new ExceptionalAction() {
          @Override
          public void apply() throws Exception {
            LOG.error(
                "Removing any previously added \"disabled\" instance tag,"
                    + " to force spectator to consider this instance in shard_map "
                    + "potentially even befoer any partitions have been active ");
            helixAdmin.removeInstanceTag(clusterName, instanceId, "disabled");
          }
        },
        new ExceptionalCondition() {
          @Override
          public boolean apply() throws Exception {
            // When the instance config tag contains the "disabled" tag.
            return ! helixAdmin.getInstanceConfig(clusterName, instanceId).containsTag("disabled");
          }
        }, 10);

    retryWithBoundedExponentialBackOff(
        new ExceptionalAction() {
          @Override
          public void apply() throws Exception {
            LOG.error("Enabling the instance to prepare for state transitions");
            helixAdmin.enableInstance(clusterName, instanceId, true);
          }
        },
        new ExceptionalCondition() {
          @Override
          public boolean apply() throws Exception {
            // When the instance is  enabled
            return helixAdmin.getInstanceConfig(clusterName, instanceId).getInstanceEnabled();
          }
        }, 10);
  }

  private static void retryWithBoundedExponentialBackOff(
      ExceptionalAction action, ExceptionalCondition condition, int maxRetries) {
    long minSleep = 200;
    double factor = 1.5;
    long maxSleep = 10000;

    long attemptNum = 0;
    Throwable lastThrowable = null;

    long currentWaitTimeMillis = minSleep;
      do {
        if (attemptNum > maxRetries) {
          throw new RuntimeException(lastThrowable);
        }

        try {
          action.apply();
        } catch (Exception e) {
          lastThrowable = e;
          e.printStackTrace();
        }

        try {
          Thread.sleep(currentWaitTimeMillis);
        } catch (InterruptedException ie) {
          ie.printStackTrace();
        }

        try {
          if (condition.apply()) {
            break;
          }
        } catch (Exception e) {
          lastThrowable = e;
          e.printStackTrace();
        }

        // Update the wait time
        currentWaitTimeMillis = Math.min(maxSleep, (long) (currentWaitTimeMillis * factor));
        ++attemptNum;
      } while (true);
    }

  private interface ExceptionalAction {

    void apply() throws Exception;
  }

  private interface ExceptionalCondition {

    boolean apply() throws Exception;
  }
}

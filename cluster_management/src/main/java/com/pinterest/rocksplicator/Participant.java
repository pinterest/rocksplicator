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

import com.pinterest.rocksplicator.eventstore.ClientShardMapLeaderEventLoggerDriver;
import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisherBuilder;
import com.pinterest.rocksplicator.task.BackupTaskFactory;
import com.pinterest.rocksplicator.task.DedupTaskFactory;
import com.pinterest.rocksplicator.task.IngestTaskFactory;
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
  private static final String shardMapZkSvrArg = "shardMapZkSvr";

  private static final String s3Bucket = "s3Bucket";
  private static final String disableSpectator = "disableSpectator";
  private static final String handoffEventHistoryzkSvr = "handoffEventHistoryzkSvr";
  private static final String handoffEventHistoryConfigPath = "handoffEventHistoryConfigPath";
  private static final String handoffEventHistoryConfigType = "handoffEventHistoryConfigType";
  private static final String
      handoffClientEventHistoryJsonShardMapPath =
      "handoffClientEventHistoryJsonShardMapPath";

  private static HelixManager helixManager = null;
  private static LeaderEventsLogger staticParticipantLeaderEventsLogger = null;
  private static LeaderEventsLogger staticSpectatorLeaderEventsLogger = null;
  private static LeaderEventsLogger staticClientLeaderEventsLogger = null;
  private static ClientShardMapLeaderEventLoggerDriver
      staticClientShardMapLeaderEventLoggerDriver =
      null;
  private static StateModelFactory<StateModel> stateModelFactory;
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
    hostOption.setArgName("Host ip (Required)");

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
        OptionBuilder.withLongOpt(configPostUrl).withDescription("URL to post config (Optional)").create();
    configPostUrlOption.setArgs(1);
    configPostUrlOption.setRequired(false);
    configPostUrlOption.setArgName("URL to post config (Optional)");

    Option shardMapZkSvrOption =
        OptionBuilder.withLongOpt(shardMapZkSvrArg).withDescription("Zk Server to post shard_map").create();
    shardMapZkSvrOption.setArgs(1);
    shardMapZkSvrOption.setRequired(false);
    shardMapZkSvrOption.setArgName(shardMapZkSvrArg);

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

    Option handoffEventHistoryConfigPathOption =
        OptionBuilder.withLongOpt(handoffEventHistoryConfigPath)
            .withDescription(
                "local disk path to config containing resources to enable for handoff events "
                    + "history tracking")
            .create();
    handoffEventHistoryConfigPathOption.setArgs(1);
    handoffEventHistoryConfigPathOption.setRequired(false);
    handoffEventHistoryConfigPathOption.setArgName(
        "config path containing resources to enabled for handoff events history (Optional)");

    Option handoffEventHistoryConfigTypeOption =
        OptionBuilder.withLongOpt(handoffEventHistoryConfigType)
            .withDescription(
                "format of config for resources to track handoff events [LINE_TERMINATED / "
                    + "JSON_ARRAY]")
            .create();
    handoffEventHistoryConfigTypeOption.setArgs(1);
    handoffEventHistoryConfigTypeOption.setRequired(false);
    handoffEventHistoryConfigTypeOption.setArgName(
        "format of config for resources to track handoff events [LINE_TERMINATED / JSON_ARRAY] "
            + "(Optional)");

    Option handoffClientEventHistoryJsonShardMapPathOption =
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
        .addOption(domainOption)
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(stateModelOption)
        .addOption(configPostUrlOption)
        .addOption(shardMapZkSvrOption)
        .addOption(s3BucketOption)
        .addOption(disableSpectatorOption)
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
   * Start a Helix participant.
   * @param args command line parameters
   */
  public static void main(String[] args) throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.WARN);
    BasicConfigurator.configure(new ConsoleAppender(
        new PatternLayout("%d{dd MMM yyyy HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n")
    ));
    CommandLine cmd = processCommandLineArgs(args);
    final String zkConnectString = cmd.getOptionValue(zkServer);
    final String clusterName = cmd.getOptionValue(cluster);
    final String domainName = cmd.getOptionValue(domain);
    final String host = cmd.getOptionValue(hostAddress);
    final String port = cmd.getOptionValue(hostPort);
    final String stateModelType = cmd.getOptionValue(stateModel);
    final String postUrl = cmd.getOptionValue(configPostUrl, "");
    final String shardMapZkSvr = cmd.getOptionValue(shardMapZkSvrArg, "");
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
    final String shardMapPath = cmd.getOptionValue(handoffClientEventHistoryJsonShardMapPath, "");

    /**
     * Note that the last parameter is empty, since we don't dictate
     * maxEventsToKeep from participants.
     */
    staticParticipantLeaderEventsLogger = new LeaderEventsLoggerImpl(instanceName,
        zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType, Optional.empty());

    if (runSpectator) {
      staticSpectatorLeaderEventsLogger = new LeaderEventsLoggerImpl(instanceName,
          zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType, Optional.of(128));

      if (shardMapPath != null && !shardMapPath.isEmpty()) {
        staticClientLeaderEventsLogger = new LeaderEventsLoggerImpl(instanceName,
            zkEventHistoryStr, clusterName, resourceConfigPath, resourceConfigType,
            Optional.empty());

        staticClientShardMapLeaderEventLoggerDriver = new ClientShardMapLeaderEventLoggerDriver(
            clusterName, shardMapPath, staticClientLeaderEventsLogger, zkEventHistoryStr);
      }
    }

    LOG.error("Starting participant with ZK:" + zkConnectString);
    Participant participant = new Participant(zkConnectString, clusterName, instanceName,
        stateModelType, Integer.parseInt(port), postUrl, shardMapZkSvr, useS3Backup, s3BucketName, runSpectator);

    /** TODO:grajpurohit
     * This should probably be done right after connecting to the zk with HelixManager.
     * Ideal way would be to first be able to register a participant and then register the
     * stateModelFactory with the stateMachine of the helixManager. There is a race
     * condition here, since we connect to helixManager right after registering the
     * stateModelFactory. In this case the state transitions can start, even before
     * we had a chance to setup the participant's domain information, which is critical
     * to helix when assigning partitions for resources which needs domain-aware assignment.
     */
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
     * 4. Wait until all partition replicas hosted by this participant to have moved to OFFLINE
     * state.
     *
     * 5. Now we are free to disconnect to HelixManager, so that all listeners and message callback
     * handlers are properly un-registered.
     */
    if (helixManager != null) {
      helixManager.disconnect();
      helixManager = null;
    }
    if (staticParticipantLeaderEventsLogger != null) {
      LOG.error("Stopping Participant LeaderEventsLogger");
      staticParticipantLeaderEventsLogger.close();
      staticParticipantLeaderEventsLogger = null;
    }
  }

  private Participant(String zkConnectString, String clusterName, String instanceName,
                      String stateModelType, int port, String postUrl, String shardMapZkSvr,
                      boolean useS3Backup, String s3BucketName, boolean runSpectator)
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
      taskFactoryRegistry.put(
          "Ingest", new IngestTaskFactory(clusterName, instanceName.split("_")[0], port, s3Bucket));
    } else if (stateModelType.equals("LeaderFollower;Task")) {
      stateModelType = "LeaderFollower";
      stateModelFactory = new LeaderFollowerStateModelFactory(instanceName.split("_")[0],
          port, zkConnectString, clusterName, useS3Backup, s3BucketName,
          staticParticipantLeaderEventsLogger);

      taskFactoryRegistry
          .put("Backup", new BackupTaskFactory(clusterName, port, useS3Backup, s3BucketName));
      taskFactoryRegistry
          .put("Restore", new RestoreTaskFactory(clusterName, port, useS3Backup, s3BucketName));
      taskFactoryRegistry.put(
          "Ingest", new IngestTaskFactory(clusterName, instanceName.split("_")[0], port, s3Bucket));
    } else if (stateModelType.equals("Task")) {
      taskFactoryRegistry
          .put("Dedup", new DedupTaskFactory(clusterName, port, useS3Backup, s3BucketName));
    } else if (stateModelType.equals("CdcLeaderStandby")) {
      stateModelFactory = new CdcLeaderStandbyStateModelFactory(
          zkConnectString, port);
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

    /** TODO: grajpurohit
     * As soon as the helixManager is connected, we should setup the participant's DOMAIN
     * information here, and not outside this constructor.
     *
     * Next we should first give Spectator a chance to remove this participant from
     * disabled instances so that next state transitions will quickly be picked up and will
     * start receiving the traffic (if it applies)
     *
     * In order to do as above, we should first remove the "disabled" instanceTag from this
     * instance, if it exists and ensure that the "disabled" tag has been removed.
     *
     * Next we should make sure that instance is enabled (e.g. if it was previously disabled)
     * Until the instance is enabled, we should wait in loop and throw exception if we can't
     * enable this instance. This will trigger all state transitions for the partition replicas
     * with their states that would otherwise be assigned to this Participant.
     *
     * We do not need to wait here until all offline partitions of this participant moves to next
     * state. However for the purpose of being sure that the participant is correctly registered,
     * we should at least wait until at least one of the OFFLINE partitions moves to next up-ward
     * state (SLAVE, MASTER, FOLLOWER, LEADER, ONLINE...).
     */

    /**
     * This is again, as explained above, not the clean / gracious way to shutdown as explained
     * above. We need to ensure a proper procedure to shutdown the participant is followed, and
     * graciously handles shutdown. Simply disconnecting from helixManager is not graceful.
     */
    Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(helixManager));

    if (runSpectator) {
      ShardMapPublisherBuilder publisherBuilder = ShardMapPublisherBuilder
          .create(clusterName).withLocalDump();
      if (postUrl != null && !postUrl.isEmpty()) {
        publisherBuilder = publisherBuilder.withPostUrl(postUrl);
      }
      if (shardMapZkSvr != null && !shardMapZkSvr.isEmpty()) {
        publisherBuilder = publisherBuilder.withZkShardMap(shardMapZkSvr);
      }

      // Add callback to create rocksplicator shard config
      ConfigGenerator configGenerator = new ConfigGenerator(
          clusterName,
          helixManager,
          publisherBuilder.build(),
          monitor,
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
          } catch (Throwable throwable) {
            throwable.printStackTrace();
          }
          if (staticSpectatorLeaderEventsLogger != null) {
            try {
              LOG.error("Stopping Spectator LeaderEventsLogger");
              staticSpectatorLeaderEventsLogger.close();
            } catch (Throwable throwable) {
              throwable.printStackTrace();
            } finally {
              staticSpectatorLeaderEventsLogger = null;
            }
          }
          if (staticClientShardMapLeaderEventLoggerDriver != null) {
            try {
              LOG.error("Stopping Spectator Client ShardMapLeaderEventLoggerDriver");
              staticClientShardMapLeaderEventLoggerDriver.close();
            } catch (Throwable throwable) {
              throwable.printStackTrace();
            } finally {
              staticClientShardMapLeaderEventLoggerDriver = null;
            }
          }
          if (staticClientLeaderEventsLogger != null) {
            try {
              LOG.error("Stopping Spectator Client LeaderEventsLogger");
              staticClientLeaderEventsLogger.close();
            } catch (Throwable throwable) {
              throwable.printStackTrace();
              staticClientLeaderEventsLogger = null;
            }
          }
        }
      });
    }
  }
}

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

import java.util.HashMap;
import java.util.Map;
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
import org.apache.log4j.BasicConfigurator;
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

  private HelixManager helixManager;
  private StateModelFactory<StateModel> stateModelFactory;

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

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(domainOption)
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(stateModelOption)
        .addOption(configPostUrlOption);
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
    BasicConfigurator.configure();
    CommandLine cmd = processCommandLineArgs(args);
    final String zkConnectString = cmd.getOptionValue(zkServer);
    final String clusterName = cmd.getOptionValue(cluster);
    final String domainName = cmd.getOptionValue(domain);
    final String host = cmd.getOptionValue(hostAddress);
    final String port = cmd.getOptionValue(hostPort);
    final String stateModelType = cmd.getOptionValue(stateModel);
    final String postUrl = cmd.getOptionValue(configPostUrl);
    final String instanceName = host + "_" + port;

    LOG.info("Starting participant with ZK:" + zkConnectString);
    Participant participant = new Participant(zkConnectString, clusterName, instanceName,
        stateModelType, Integer.parseInt(port), postUrl);

    HelixAdmin helixAdmin = new ZKHelixAdmin(zkConnectString);
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
            .forCluster(clusterName).forParticipant(instanceName)
            .build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("DOMAIN", domainName + ",instance=" + instanceName);
    helixAdmin.setConfig(scope, properties);

    Thread.currentThread().join();
  }

  public Participant(String zkConnectString, String clusterName, String instanceName,
                     String stateModelType, int port, String postUrl) throws Exception {
    helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
        InstanceType.PARTICIPANT, zkConnectString);
    stateModelFactory = new OnlineOfflineStateModelFactory(port, zkConnectString, clusterName);
    StateMachineEngine stateMach = helixManager.getStateMachineEngine();
    stateMach.registerStateModelFactory(stateModelType, stateModelFactory);
    helixManager.connect();
    helixManager.getMessagingService().registerMessageHandlerFactory(
        Message.MessageType.STATE_TRANSITION.name(), stateMach);
    Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(helixManager));

    // Add callback to create rocksplicator shard config
    HelixCustomCodeRunner codeRunner = new HelixCustomCodeRunner(helixManager, zkConnectString)
        .invoke(new OnlineOfflineConfigGenerator(clusterName, helixManager, postUrl))
        .on(HelixConstants.ChangeType.EXTERNAL_VIEW)
        .usingLeaderStandbyModel("ConfigWatcher" + clusterName);

    codeRunner.start();
  }
}
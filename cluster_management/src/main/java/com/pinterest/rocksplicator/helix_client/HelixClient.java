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
// @author rajathprasad (rajathprasad@pinterest.com)
//

package com.pinterest.rocksplicator.helix_client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class HelixClient {

  private static final Logger LOG = LoggerFactory.getLogger(HelixClient.class);
  private static final String zkServer = "zkSvr";
  private static final String cluster = "cluster";
  private static final String resource = "resource";
  private static final String partition = "partition";

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

    Option resourceOption =
        OptionBuilder.withLongOpt(resource).withDescription("Provide resource name").create();
    resourceOption.setArgs(1);
    resourceOption.setRequired(true);
    resourceOption.setArgName("Resource name (Required)");

    Option partitionOption =
        OptionBuilder.withLongOpt(partition).withDescription("Provide partition name").create();
    partitionOption.setArgs(1);
    partitionOption.setRequired(true);
    partitionOption.setArgName("Partition name (Required)");

    Options options = new Options();
    options.addOption(zkServerOption)
        .addOption(clusterOption)
        .addOption(resourceOption)
        .addOption(partitionOption);
    return options;
  }

  private static CommandLine processCommandLineArgs(String[] cliArgs) throws ParseException {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    return cliParser.parse(cliOptions, cliArgs);
  }

  /**
   * Given the names of a cluster, resource and partition, this function returns the ID of the
   * instance which hosts the leader replica of the partition. Returns an empty string in case
   * an error or if there is no leader. In our case helix instace ID is the IP of the instance
   * and the port in which the service is running.
   */
  public static String getleaderInstanceId(String zkConnectString,
                                           String clusterName,
                                           String resourceName,
                                           String partitionName) {
    HelixAdmin helixAdmin = null;
    try {
      LOG.error("Starting helix with ZK:" + zkConnectString);
      helixAdmin = new ZKHelixAdmin(zkConnectString);
      String leaderInstanceId = "";
      if (helixAdmin == null) {
        LOG.error("Could not get helix admin for: " + zkConnectString);
        return leaderInstanceId;
      }
      ExternalView view = helixAdmin.getResourceExternalView(clusterName, resourceName);
      if (view == null) {
        LOG.error("Could not get external view for: " + resourceName);
        return leaderInstanceId;
      }

      Map<String, String> stateMap = view.getStateMap(partitionName);

      if (stateMap == null) {
        LOG.error("Could not get statemap for: " + resourceName);
        return leaderInstanceId;
      }

      LOG.error("State map :" + stateMap);

      if (!stateMap.containsValue("MASTER") && !stateMap.containsValue("LEADER")) {
        LOG.error("No leader found");
        return leaderInstanceId;
      }

      for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
        String role = instanceNameAndRole.getValue();
        if (role.equals("MASTER") || role.equals("LEADER")) {
          leaderInstanceId = instanceNameAndRole.getKey();
          LOG.error("Found leader: " + leaderInstanceId);
          break;
        }
      }

      LOG.error("Done finding leader: " + leaderInstanceId);
      return leaderInstanceId;
    } catch (Exception e) {
      LOG.error("Caught exception while trying to get current leader: %s", e);
      return "";
    } finally {
      if (helixAdmin != null) {
        helixAdmin.close();
      }
    }
  }

  /**
   * Access a Helix client.
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
    final String resourceName = cmd.getOptionValue(resource);
    final String partitionName = cmd.getOptionValue(partition);

    // For now, we need only one function to get the current leader.
    getleaderInstanceId(zkConnectString, clusterName, resourceName, partitionName);
  }

}

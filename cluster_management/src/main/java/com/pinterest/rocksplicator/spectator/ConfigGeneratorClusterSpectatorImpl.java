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

package com.pinterest.rocksplicator.spectator;

import com.pinterest.rocksplicator.ConfigGenerator;
import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventLogger;
import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLoggerImpl;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;

import java.util.Optional;

public class ConfigGeneratorClusterSpectatorImpl implements ClusterSpectator {

  private String zkConnectString;
  private String clusterName;
  private String instanceName;
  private String configPostUri;

  private HelixManager helixManager;
  private ConfigGenerator configGenerator;

  private Optional<String> zkLeaderHandOffConnectString;
  private Optional<String> resourceHandOffEnabledPath;
  private Optional<String> resourceConfigType;
  private Optional<String> shardMapPath;

  private LeaderEventsLogger leaderEventsLoger;
  private ExternalViewLeaderEventLogger externalViewLeaderEventLogger;

  public ConfigGeneratorClusterSpectatorImpl(
      final String zkConnectString,
      final String clusterName,
      final String instanceName,
      final String configPostUri,
      final Optional<String> zkLeaderHandOff,
      final Optional<String> handOffResourcedPath,
      final Optional<String> shardMapPath) {
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this.configPostUri = configPostUri;
  }

  /**
   * May be called multiple times. Multiple sequential calls should be idemPotent.
   */
  @Override
  public synchronized void prepare() {
    if (this.helixManager == null || !this.helixManager.isConnected()) {
      this.helixManager = HelixManagerFactory.getZKHelixManager(
          clusterName,
          instanceName,
          InstanceType.SPECTATOR,
          zkConnectString);
    }
  }

  /**
   * May be called multiple times. Multiple sequential calls should be idemPotent.
   */
  @Override
  public synchronized void start() throws Exception {
    // Start the spectator functionality
    prepare();
    this.helixManager.connect();

    /**
     * TODO: grajpurohit
     * Construct the ConfigGenerator instance here, and any of other
     * dependencies should be generated here.
     *
     * ConfigGenerator should also start listening to the ExternalView
     * notifications and publishing ShardMap config here.
     */
    LeaderEventsLogger leaderEventsLogger = new LeaderEventsLoggerImpl(
        instanceName,
        zkLeaderHandOffConnectString.get(),
        clusterName,
        resourceHandOffEnabledPath.get(),
        resourceConfigType.get(),
        Optional.of(128));

    ExternalViewLeaderEventLogger externalViewLeaderEventLogger = new ExternalViewLeaderEventsLoggerImpl(leaderEventsLogger);

    RocksplicatorMonitor rocksplicatorMonitor = new RocksplicatorMonitor(clusterName, instanceName);
    ConfigGenerator configGenerator = new ConfigGenerator(clusterName, this.helixManager, configPostUri, rocksplicatorMonitor, externalViewLeaderEventLogger);

    this.helixManager.addExternalViewChangeListener(configGenerator);
    this.helixManager.addConfigChangeListener(configGenerator);


    /**
     * Make client to monitor the shard_map configuration.
     */
  }

  /**
   * May be called multiple times. Multiple sequential calls should be idemPotent.
   */
  @Override
  public synchronized void stop() {
    // stop being the spectator anymore

    /**
     * Stop generating config shards here. Once we are done.
     * Perform all the cleanup work in release method.
     */
    release();
  }

  /**
   * May be called multiple times. Multiple sequential calls should be idemPotent.
   */
  @Override
  public synchronized void release() {
    if (this.helixManager != null && helixManager.isConnected()) {
      this.helixManager.disconnect();
      this.helixManager = null;
    }
  }
}

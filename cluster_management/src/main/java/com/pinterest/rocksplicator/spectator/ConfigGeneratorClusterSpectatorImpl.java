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
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisher;
import com.pinterest.rocksplicator.publisher.ShardMapPublisherBuilder;
import com.pinterest.rocksplicator.shardmapagent.ClusterShardMapAgent;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of Spectator running ConfigGenerator.
 *
 * In the current version, it doesn't support LeaderEvents handoff reporting.
 */
public class ConfigGeneratorClusterSpectatorImpl implements ClusterSpectator {

  private final Logger LOGGER;
  private final String zkHelixConnectString;
  private final String clusterName;
  private final String instanceName;
  private final String configPostUri;
  private final String shardMapZkSvr;
  private final String shardMapDownloadDir;

  /**
   * Live State.
   */
  private HelixManager helixManager = null;
  private ShardMapPublisher<JSONObject> shardMapPublisher = null;
  private ConfigGenerator configGenerator = null;
  private RocksplicatorMonitor monitor = null;
  private ClusterShardMapAgent clusterShardMapAgent = null;

  public ConfigGeneratorClusterSpectatorImpl(
      final String zkHelixConnectString,
      final String clusterName,
      final String instanceName,
      final String configPostUri,
      final String zkShardMapConnectString,
      final String shardMapDownloadDir) {
    this.zkHelixConnectString = zkHelixConnectString;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this.configPostUri = configPostUri;
    this.shardMapZkSvr = zkShardMapConnectString;
    this.shardMapDownloadDir = shardMapDownloadDir;
    this.LOGGER = LoggerFactory.getLogger(String.format("ConfigGenerator-%s", clusterName));
  }

  /**
   * May be called multiple times. Multiple sequential calls should be idemPotent.
   */
  @Override
  public synchronized void prepare() {
    if (this.helixManager == null) {
      LOGGER.info(String.format("Initializing Spectator HelixManager for zkSvr: %s cluster=%s",
          zkHelixConnectString, clusterName));

      this.helixManager = HelixManagerFactory.getZKHelixManager(
          clusterName,
          instanceName,
          InstanceType.SPECTATOR,
          zkHelixConnectString);
    }

    if (shardMapPublisher == null) {
      ShardMapPublisherBuilder publisherBuilder
          = ShardMapPublisherBuilder.create(this.clusterName).withLocalDump();

      /**
       * Enable publishing entire shardMap to given http post uri.
       */
      if (configPostUri != null &&
          !configPostUri.isEmpty() &&
          (configPostUri.startsWith("http://") || configPostUri.startsWith("https://"))) {
        LOGGER.info(String.format("cluster=%s : Enabled posting to uri: %s", configPostUri));
        publisherBuilder.withPostUrl(configPostUri);
      }

      /**
       * Enable publishing per resource shardMap to zk.
       */
      if (shardMapZkSvr != null && !shardMapZkSvr.isEmpty()) {
        LOGGER.info(
            String.format("cluster=%s : Enabled posting to zkSvr: %s", clusterName, shardMapZkSvr));
        publisherBuilder = publisherBuilder.withZkShardMap(shardMapZkSvr);
      }
      this.shardMapPublisher = publisherBuilder.build();
    }

    if (monitor == null) {
      this.monitor = new RocksplicatorMonitor(this.clusterName, this.instanceName);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          ConfigGeneratorClusterSpectatorImpl.this.stop();
          ConfigGeneratorClusterSpectatorImpl.this.release();
        } catch (Throwable throwable) {
          LOGGER.error(String.format("Error in shutdownHook, cluster=%s", clusterName), throwable);
        }
      }
    });
  }

  /**
   * May be called multiple times. Multiple sequential calls should be idemPotent.
   */
  @Override
  public synchronized void start() throws Exception {
    // Start the spectator functionality
    prepare();

    LOGGER.info(String.format("Connecting HelixManager to zkSvr: %s for cluster=%s",
        zkHelixConnectString, clusterName));
    this.helixManager.connect();

    /**
     * TODO: grajpurohit
     * Construct the ConfigGenerator instance here, and any of other
     * dependencies should be generated here.
     *
     * ConfigGenerator should also start listening to the ExternalView
     * notifications and publishing ShardMap config here.
     */
    this.configGenerator = new ConfigGenerator(
        this.clusterName,
        this.helixManager,
        this.shardMapPublisher,
        this.monitor, null);

    LOGGER.info(String.format("Starging HelixManager Notification to ConfigGenerator"
        + " to zkSvr: %s for cluster=%s", zkHelixConnectString, clusterName));
    this.helixManager.addExternalViewChangeListener(configGenerator);
    this.helixManager.addConfigChangeListener(configGenerator);

    /**
     * If the zkShardMapServer is given and the download directory is given,
     * start with downloading initial shard_map for this cluster.
     */
    if (this.clusterShardMapAgent == null) {
      if (shardMapZkSvr != null && !shardMapZkSvr.isEmpty()) {
        if (shardMapDownloadDir != null && !shardMapDownloadDir.isEmpty()) {
          /**
           * There is no need to share the zk client, as number of spectator instances is expected
           * to be really small and total number of zk clients created will be linear to number of
           * participant clusters being watched in total by all spectator instances...
           */
          this.clusterShardMapAgent =
              new ClusterShardMapAgent(shardMapZkSvr, null, clusterName, shardMapDownloadDir);
          this.clusterShardMapAgent.startNotification();
          LOGGER.info(
              String.format("Successfully started ShardMapAgent for cluster=%s", clusterName));
        }
      }
    }
    LOGGER.info(String.format("Started Spectator ConfigGenerator for cluster: %s", clusterName));
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
    // No more notifications to config generator
    if (this.helixManager != null) {
      if (helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
      this.helixManager = null;
    }

    /**
     * Close down any internally running threads.
     */
    if (this.configGenerator != null) {
      try {
        this.configGenerator.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      this.configGenerator = null;
    }

    /**
     * Shutdown the shardMapPublisher
     */
    if (shardMapPublisher != null) {
      try {
        this.shardMapPublisher.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      this.shardMapPublisher = null;
    }

    if (monitor != null) {
      this.monitor.close();
      this.monitor = null;
    }

    if (this.clusterShardMapAgent != null) {
      try {
        this.clusterShardMapAgent.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      this.clusterShardMapAgent = null;
    }
  }
}

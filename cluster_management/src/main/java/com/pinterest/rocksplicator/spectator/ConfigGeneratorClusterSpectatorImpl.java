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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;

public class ConfigGeneratorClusterSpectatorImpl implements ClusterSpectator {

  private HelixManager helixManager;
  private String zkConnectString;
  private String clusterName;
  private String instanceName;
  private String configPostUri;

  public ConfigGeneratorClusterSpectatorImpl(
      String zkConnectString,
      String clusterName,
      String instanceName,
      String configPostUri) {
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
     *
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

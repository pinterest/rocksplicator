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

public class ConfigGeneratorClusterSpectatorFactory implements ClusterSpectatorFactory {

  private final String configPostUriPattern;
  private final String zkShardMapConnectString;
  private final String shardMapDownloadDir;
  private final boolean enableCurrentStatesRouter;

  public ConfigGeneratorClusterSpectatorFactory(
      final String configPostUriPattern,
      final String zkShardMapConnectString,
      final String shardMapDownloadDir,
      final boolean enableCurrentStatesRouter) {
    this.configPostUriPattern = configPostUriPattern;
    this.zkShardMapConnectString = zkShardMapConnectString;
    this.shardMapDownloadDir = shardMapDownloadDir;
    this.enableCurrentStatesRouter = enableCurrentStatesRouter;
  }

  private String getConfigPostUri(String participantClusterName) {
    if (configPostUriPattern == null || configPostUriPattern.isEmpty()) {
      return null;
    } else {
      return configPostUriPattern.replace("[PARTICIPANT_CLUSTER]", participantClusterName);
    }
  }

  @Override
  public ClusterSpectator createClusterSpectator(
      String zkHelixConnectString,
      String participantClusterName,
      String myInstanceName) {
    return new ConfigGeneratorClusterSpectatorImpl(
        zkHelixConnectString,
        participantClusterName,
        myInstanceName,
        getConfigPostUri(participantClusterName),
        this.zkShardMapConnectString,
        this.shardMapDownloadDir,
        this.enableCurrentStatesRouter);
  }
}

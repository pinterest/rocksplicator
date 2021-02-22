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

  private final String uriPattern;

  public ConfigGeneratorClusterSpectatorFactory(String uriPattern) {
    this.uriPattern = uriPattern;
  }

  private String getConfigPostUri(String participantClusterName) {
    return uriPattern.replace("[PARTICIPANT_CLUSTER]", participantClusterName);
  }

  @Override
  public ClusterSpectator createClusterSpectator(
      String zkServerConnectString,
      String participantClusterName,
      String myInstanceName) {
    return new ConfigGeneratorClusterSpectatorImpl(
        zkServerConnectString,
        participantClusterName,
        myInstanceName,
        getConfigPostUri(participantClusterName));
  }
}

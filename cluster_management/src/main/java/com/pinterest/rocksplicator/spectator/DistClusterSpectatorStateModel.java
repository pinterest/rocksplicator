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

import com.google.common.base.Preconditions;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.AbstractHelixLeaderStandbyStateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {
    "LEADER", "STANDBY"
})
public class DistClusterSpectatorStateModel extends AbstractHelixLeaderStandbyStateModel {

  private static Logger logger = LoggerFactory.getLogger(DistClusterSpectatorStateModel.class);
  private final ClusterSpectatorFactory clusterSpectatorFactory;
  private final String resourceName;
  private ClusterSpectator clusterSpectator = null;
  private boolean isStarted = false;

  public DistClusterSpectatorStateModel(
      final String resourceName,
      final String zkConnectString,
      final ClusterSpectatorFactory clusterSpectatorFactory) {
    super(zkConnectString);
    this.resourceName = Preconditions.checkNotNull(resourceName);
    this.clusterSpectatorFactory = Preconditions.checkNotNull(clusterSpectatorFactory);
  }

  @Override
  public synchronized void onBecomeStandbyFromOffline(Message message,
                                                      NotificationContext context) {
    logStateTransition("OFFLINE", "STANDBY", message.getPartitionName(), message.getTgtName());

    String clusterName = message.getPartitionName();
    String spectatorInstanceName = message.getTgtName();

    Preconditions.checkArgument(resourceName.equals(clusterName));

    if (clusterSpectator == null) {
      this.clusterSpectator = this.clusterSpectatorFactory.createClusterSpectator(
          this._zkAddr, clusterName, spectatorInstanceName);
      this.clusterSpectator.prepare();
    }
  }

  @Override
  public synchronized void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String clusterName = message.getPartitionName();
    String spectatorInstanceName = message.getTgtName();

    logger.info(spectatorInstanceName + " becoming leader from standby for " + clusterName);

    Preconditions.checkArgument(resourceName.equals(clusterName));

    Preconditions.checkNotNull(clusterSpectator);

    if (!isStarted) {
      clusterSpectator.start();
      isStarted = true;
    }

    logStateTransition("STANDBY", "LEADER", clusterName, spectatorInstanceName);
  }

  @Override
  public synchronized void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    String clusterName = message.getPartitionName();
    String spectatorInstanceName = message.getTgtName();

    if (clusterSpectator != null) {
      if (isStarted) {
        this.clusterSpectator.stop();
        isStarted = false;
      }
    }

    logger.info(spectatorInstanceName + " becoming prepare from leader for " + clusterName);
    logStateTransition("LEADER", "STANDBY", clusterName, spectatorInstanceName);
  }

  @Override
  public synchronized void onBecomeOfflineFromStandby(Message message,
                                                      NotificationContext context) {
    String clusterName = message.getPartitionName();
    String spectatorInstanceName = message.getTgtName();

    if (clusterSpectator != null) {
      this.clusterSpectator.release();
      this.clusterSpectator = null;
    }

    logStateTransition("STANDBY", "OFFLINE", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    String clusterName = message.getPartitionName();
    String spectatorInstanceName = message.getTgtName();

    if (clusterSpectator != null) {
      this.clusterSpectator.stop();
      this.clusterSpectator.release();
      this.clusterSpectator = null;
    }

    logStateTransition("OFFLINE", "DROPPED", message == null ? "" : message.getPartitionName(),
        message == null ? "" : message.getTgtName());
  }

  @Override
  public String getStateModeInstanceDescription(String partitionName, String instanceName) {
    return String.format("Spectator for cluster %s on instance %s", partitionName, instanceName);
  }

  @Override
  public void reset() {
  }
}

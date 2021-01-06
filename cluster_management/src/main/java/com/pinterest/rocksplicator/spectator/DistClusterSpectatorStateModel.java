package com.pinterest.rocksplicator.spectator;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
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
  private HelixManager _spectatorManager = null;
  private SpectatorLeadershipCallback _spectatorCallbk = null;

  public DistClusterSpectatorStateModel(
      SpectatorLeadershipCallback callback,
      String zkAddr) {
    super(zkAddr);
    _spectatorCallbk = callback;
  }

  @Override
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    logStateTransition("OFFLINE", "STANDBY", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String clusterName = message.getPartitionName();
    String spectatorInstanceName = message.getTgtName();

    logger.info(spectatorInstanceName + " becoming leader from standby for " + clusterName);

    if (_spectatorManager == null) {
      _spectatorManager =
          HelixManagerFactory.getZKHelixManager(clusterName, spectatorInstanceName,
              InstanceType.SPECTATOR, _zkAddr);
      _spectatorManager.connect();
      if (_spectatorCallbk != null) {
        _spectatorCallbk.onAcquire(_spectatorManager);
      }
      logStateTransition("STANDBY", "LEADER", clusterName, spectatorInstanceName);
    } else {
      logger.error("spectator already exists:" + _spectatorManager.getInstanceName() + " for "
          + clusterName);
    }

  }

  @Override
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    String clusterName = message.getPartitionName();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming standby from leader for " + clusterName);

    if (_spectatorManager != null) {
      if (_spectatorCallbk != null) {
        _spectatorCallbk.onRelease(_spectatorManager);
      }
      reset();
      logStateTransition("LEADER", "STANDBY", clusterName, controllerName);
    } else {
      logger.error("No spectator exists for " + clusterName);
    }
  }

  @Override
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    logStateTransition("STANDBY", "OFFLINE", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    reset();
    logStateTransition("OFFLINE", "DROPPED", message == null ? "" : message.getPartitionName(),
        message == null ? "" : message.getTgtName());
  }

  @Override
  public String getStateModeInstanceDescription(String partitionName, String instanceName) {
    return String.format("Spectator for cluster %s on instance %s", partitionName, instanceName);
  }

  @Override
  public void reset() {
    if (_spectatorManager != null) {
      _spectatorManager.disconnect();
      _spectatorManager = null;
    }

  }
}

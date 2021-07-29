package com.pinterest.rocksplicator;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.AbstractHelixLeaderStandbyStateModel;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;


public class CdcLeaderStandbyStateModelFactory extends StateModelFactory<StateModel> {
  public CdcLeaderStandbyStateModelFactory() {

  }

  public static class CdcLeaderStandbyStateModel extends AbstractHelixLeaderStandbyStateModel  {
    private final String resourceName;
    private final int adminPort;

    @StateModelInfo(initialState = "OFFLINE", states = {
        "LEADER", "STANDBY"
    })
    public CdcLeaderStandbyStateModel(String resourceName, final String zkConnectString, String partitionName) {
      super(zkConnectString);
      super.setPartitionName(partitionName);
      this.resourceName = resourceName;
    }

    @Override
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
      Utils.checkStateTransitions("STANDBY", "LEADER", message, resourceName, partitionName);
      Utils.logTransitionMessage(message)
      // Add observer
      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
      Utils.checkStateTransitions("LEADER", "STANDBY", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);
        String dbName = Utils.getDbName(this.partitionName);
      // Remove observer
      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "STANDBY", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
      Utils.checkStateTransitions("STANDBY", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "DROPPED", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    public void onBecomeOfflineFromDropped(Message message, NotificationContext context) {
  	  super.onBecomeOfflineFromDropped(message, context);
      Utils.checkStateTransitions("DROPPED", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }
  
    @Override
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
  	  super.onBecomeOfflineFromError(message, context);
      Utils.checkStateTransitions("ERROR", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    public void reset() {
    }

    }
}

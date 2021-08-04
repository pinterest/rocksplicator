package com.pinterest.rocksplicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.AbstractHelixLeaderStandbyStateModel;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;


/*
 * FROM PARENT CLASS:
 * Generic leader-standby state model impl for helix services. It requires implementing
 * service-specific o->s, s->l, l->s, s->o, and reset methods, and provides
 * default impl for the reset
 *
 * @StateModelInfo(initialState = "OFFLINE", states = {
 *     "LEADER", "STANDBY"
 * })

 * The LeaderStandby state machine has 5 possible states. There are 8 possible state transitions
 * that
 * we need to handle.
 *                         1           3             5
 *                LEADER  <-   STANDBY  <-   OFFLINE  ->  DROPPED
 *                        ->           ->        ^   <-  ^
 *                         2           4        7 \  6  / 8
 *                                                 ERROR
 *
 * 1) Standby to Leader
 *    TODO(indy): Add observer
 *
 * 2) Leader to Standby
 *    TODO(indy): Remove observer
 *
 * 3) Offline to Standby
 *
 * 4) Standby to Offline
 *
 * 5) Offline to Dropped
 *
 * 6) Dropped to Offline
 *
 * 7) Error to Offline
 *
 * 8) Error to Dropped
 */
public class CdcLeaderStandbyStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOG = LoggerFactory.getLogger(CdcLeaderStandbyStateModelFactory.class);
  final String zkConnectString;

  public CdcLeaderStandbyStateModelFactory(final String zkConnectString) {
    this.zkConnectString = zkConnectString;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.error("Create a new state for " + partitionName);
    return new CdcLeaderStandbyStateModel(
        resourceName,
        zkConnectString,
        partitionName);
  }


  public static class CdcLeaderStandbyStateModel extends AbstractHelixLeaderStandbyStateModel  {
    private final String resourceName;
    private final String partitionName;

    @StateModelInfo(initialState = "OFFLINE", states = {
        "LEADER", "STANDBY"
    })
    public CdcLeaderStandbyStateModel(String resourceName, final String zkConnectString, String partitionName) {
      super(zkConnectString);
      this.partitionName = partitionName;
      this.resourceName = resourceName;
    }

    @Override
    /**
     * 1) Standby to Leader
     */
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
      Utils.checkStateTransitions("STANDBY", "LEADER", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);
      // Add observer
      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    /**
     * 2) Leader to Standby
     */
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
      Utils.checkStateTransitions("LEADER", "STANDBY", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);
        String dbName = Utils.getDbName(this.partitionName);
      // Remove observer
      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    /**
     * 3) Offline to Standby
     */
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "STANDBY", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    /**
     * 4) Standby to Offline
     */
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
      Utils.checkStateTransitions("STANDBY", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    /**
     * 5) Offline to Dropped
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "DROPPED", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    /**
     * 6) Dropped to Offline
     */
    public void onBecomeOfflineFromDropped(Message message, NotificationContext context) {
  	  super.onBecomeOfflineFromDropped(message, context);
      Utils.checkStateTransitions("DROPPED", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }
  
    @Override
    /**
     * 7) Error to Offline
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
  	  super.onBecomeOfflineFromError(message, context);
      Utils.checkStateTransitions("ERROR", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }

    @Override
    /**
     * 8) Error to Dropped
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) throws Exception {
  	  super.onBecomeDroppedFromError(message, context);
      Utils.checkStateTransitions("ERROR", "DROPPED", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.logTransitionCompletionMessage(message);
    }


    @Override
    public void reset() {
    }

    }
}

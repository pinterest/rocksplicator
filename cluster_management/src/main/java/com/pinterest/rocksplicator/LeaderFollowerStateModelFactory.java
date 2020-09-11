package com.pinterest.rocksplicator;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderFollowerStateModelFactory extends StateModelFactory<StateModel> {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderFollowerStateModelFactory.class);
  private final ReplicatedStateModelFactory delegateFactory;

  public LeaderFollowerStateModelFactory(String host,
                                         int adminPort,
                                         String zkConnectString,
                                         String cluster,
                                         boolean useS3Backup,
                                         String s3Bucket) {
    this.delegateFactory =
        new ReplicatedStateModelFactory(LOG, host, adminPort, zkConnectString, cluster, useS3Backup,
            s3Bucket, "LEADER", "FOLLOWER");
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new LeaderFollowerStateModel((ReplicatedStateModelFactory.ReplicatedStateModel)
        delegateFactory.createNewStateModel(LeaderFollowerStateModel.LOG, resourceName, partitionName));
  }

  public static class LeaderFollowerStateModel extends StateModel {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderFollowerStateModel.class);

    private final ReplicatedStateModelFactory.ReplicatedStateModel delegateModel;

    /**
     * State model that handles the state machine of a single replica
     */
    public LeaderFollowerStateModel(
        ReplicatedStateModelFactory.ReplicatedStateModel delegateModel) {
      this.delegateModel = delegateModel;
    }

    /**
     * 1) Follower to Leader
     */
    public void onBecomeLeaderFromFollower(Message message, NotificationContext context) {
      delegateModel.onBecomeMasterFromSlave(message, context);
    }

    /**
     * 2) Leader to Follower
     */
    public void onBecomeFollowerFromLeader(Message message, NotificationContext context) {
      delegateModel.onBecomeSlaveFromMaster(message, context);
    }

    /**
     * 3) Offline to Follower
     */
    public void onBecomeFollowerFromOffline(Message message, NotificationContext context) {
      delegateModel.onBecomeSlaveFromOffline(message, context);
    }

    /**
     * 4) Follower to Offline
     */
    public void onBecomeOfflineFromFollower(Message message, NotificationContext context) {
      delegateModel.onBecomeOfflineFromSlave(message, context);
    }

    /**
     * 5) Offline to Dropped
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      delegateModel.onBecomeDroppedFromOffline(message, context);
    }

    /**
     * 6) Error to Offline
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      delegateModel.onBecomeOfflineFromError(message, context);
    }

    /**
     * 7) Error to Dropped
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      onBecomeDroppedFromOffline(message, context);
    }
  }
}

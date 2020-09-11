package com.pinterest.rocksplicator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.helix.HelixAdmin;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The MasterSlave state machine has 5 possible states. There are 7 possible state transitions that
 * we need to handle.
 *                         1           3             5
 *                MASTER  <-   SLAVE  <-   OFFLINE  ->  DROPPED
 *                        ->          ->         ^     ^
 *                         2          4         6 \   / 7
 *                                                ERROR
 *
 * In every state transition handler function, we need to first acquire a global lock for the shard.
 * This guarantees 1) there is no concurrent transitions happening; 2) a partition state observed
 * inside a transition handler is either the actual current state of the partition or the actual
 * current state is one step away in the state machine graph from what's observed.
 *
 *
 * 1) Slave to Master
 *    a) sanity check that there is no Master existing in the cluster
 *    b) make sure the local replica has the highest seq # among all existing Slaves
 *    c) changeDBRoleAndUpStream(me, "Master")
 *    d) changeDBRoleAndUpStream(all_other_slaves_or_offlines, "Slave", "my_ip_port")
 *
 * 2) Master to Slave
 *    a) changeDBRoleAndUpStream(me, "Slave", "127.0.0.1:9090")
 *
 * 3) Offline to Slave
 *    a) addDB("127.0.0.1:9090")
 *    b) Check if the local replica needs to be rebuilt, i.e., there exist some other replicas in
 *       the cluster and latest data in local WAL is too old and way behind other replicas
 *    c) if yes, backupDB(// prefer Master), and then restoreDB()
 *    d) changeDBRoleAndUpStream(me, "Slave", "Master_ip_port") if Master exists
 *
 * 4) Slave to Offline
 *    a) changeDBRoleAndUpStream(all_other_slaves_or_offlines, "Slave", "live_master_or_slave")
 *    a) closeDB()
 *
 * 5) Offline to Dropped
 *    a) clearDB()
 *
 * 6) Error to Offline
 *    a) closeDB()
 *
 * 7) Error to Dropped
 *    a) clearDB()
 */
public class MasterSlaveStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOG = LoggerFactory.getLogger(MasterSlaveStateModelFactory.class);
  private final ReplicatedStateModelFactory delegateFactory;

  public MasterSlaveStateModelFactory(
      String host,
      int adminPort,
      String zkConnectString,
      String cluster,
      boolean useS3Backup,
      String s3Bucket) {
    this.delegateFactory = new ReplicatedStateModelFactory(
        LOG,
        host, adminPort, zkConnectString, cluster, useS3Backup, s3Bucket,
        "MASTER", "SLAVE");
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new MasterSlaveStateModel((ReplicatedStateModelFactory.ReplicatedStateModel)
        delegateFactory.createNewStateModel(MasterSlaveStateModel.LOG, resourceName, partitionName));
  }

  public static class MasterSlaveStateModel extends StateModel {
    private static final Logger LOG = LoggerFactory.getLogger(MasterSlaveStateModel.class);

    private final ReplicatedStateModelFactory.ReplicatedStateModel delegateModel;

    /**
     * State model that handles the state machine of a single replica
     */
    public MasterSlaveStateModel(ReplicatedStateModelFactory.ReplicatedStateModel delegateModel) {
      this.delegateModel = delegateModel;
    }

    /**
     * 1) Slave to Master
     */
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      delegateModel.onBecomeMasterFromSlave(message, context);
    }

    /**
     * 2) Master to Slave
     */
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      delegateModel.onBecomeSlaveFromMaster(message, context);
    }

    /**
     * 3) Offline to Slave
     */
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      delegateModel.onBecomeSlaveFromOffline(message, context);
    }

    /**
     * 4) Slave to Offline
     */
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
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

    private static String getLockPath(String cluster, String resourceName, String partitionName) {
      return "/rocksplicator/" + cluster + "/" + resourceName + "/" + partitionName + "/lock";
    }
  }
}

/// Copyright 2017 Pinterest Inc.
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
// @author bol (bol@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksdb_admin.thrift.CheckDBResponse;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.curator.retry.ExponentialBackoffRetry;
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

  private final String host;
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;

  public MasterSlaveStateModelFactory(
      String host, int adminPort, String zkConnectString, String cluster) {
    this.host = host;
    this.adminPort = adminPort;
    this.cluster = cluster;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.error("Create a new state for " + partitionName);
    return new MasterSlaveStateModel(
        resourceName, partitionName, host, adminPort, cluster, zkClient);
  }


  public static class MasterSlaveStateModel extends StateModel {
    private static final Logger LOG = LoggerFactory.getLogger(MasterSlaveStateModel.class);

    private final String resourceName;
    private final String partitionName;
    private final String host;
    private final int adminPort;
    private final String cluster;
    private CuratorFramework zkClient;
    private InterProcessMutex partitionMutex;


    /**
     * State model that handles the state machine of a single replica
     */
    public  MasterSlaveStateModel(String resourceName, String partitionName, String host,
                                  int adminPort, String cluster, CuratorFramework zkClient) {
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.host = host;
      this.adminPort = adminPort;
      this.cluster = cluster;
      this.zkClient = zkClient;
      this.partitionMutex = new InterProcessMutex(zkClient,
          getLockPath(cluster, resourceName, partitionName));
    }

    /**
     * 1) Slave to Master
     */
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        HelixAdmin admin = context.getManager().getClusterManagmentTool();
        Map<String, String> stateMap = null;

        // sanity check no existing Master for up to 59 seconds
        for (int i = 0; i < 60; ++i) {
          ExternalView view = admin.getResourceExternalView(cluster, resourceName);
          stateMap = view.getStateMap(partitionName);

          if (!stateMap.containsValue("MASTER")) {
            break;
          }

          if (i == 59) {
            throw new RuntimeException("Existing Master detected!");
          }

          TimeUnit.SECONDS.sleep(1);
          LOG.error("Slept for " + String.valueOf(i + 1) + " seconds for 0 Master");
        }

        final String dbName = Utils.getDbName(partitionName);
        // make sure local replica has the highest sequence number
        long localSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
        String hostWithHighestSeq = null;
        long highestSeq = localSeq;
        for (String instanceName : stateMap.keySet()) {
          String hostName = instanceName.split("_")[0];
          int port = Integer.parseInt(instanceName.split("_")[1]);
          if (this.host.equals(hostName)) {
            // myself
            continue;
          }

          long seq = Utils.getLatestSequenceNumber(dbName, hostName, port);
          if (seq == -1) {
            LOG.error("Failed to get latest sequence number from " + hostName + " for " + dbName);
            continue;
          }
          if (highestSeq < seq) {
            highestSeq = seq;
            hostWithHighestSeq = hostName;
          }
        }

        if (hostWithHighestSeq != null) {
          LOG.error("Found another host " + hostWithHighestSeq + " with higher seq num: " +
              String.valueOf(highestSeq) + " for " + dbName);
          Utils.changeDBRoleAndUpStream(
              "localhost", adminPort, dbName, "SLAVE", hostWithHighestSeq, adminPort);

          // wait for up to 10 mins
          for (int i = 0; i < 600; ++i) {
            TimeUnit.SECONDS.sleep(1);
            long newLocalSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            LOG.error("Replicated [" + String.valueOf(localSeq) + ", " + String.valueOf(newLocalSeq) +
              ") from " + hostWithHighestSeq + " for " + dbName);
            localSeq = newLocalSeq;
            if (highestSeq <= localSeq) {
              LOG.error(dbName + " catched up!");
              break;
            }
            LOG.error("Slept for " + String.valueOf(i + 1) + " seconds for replicating " + dbName);
          }

          if (highestSeq > localSeq) {
            LOG.error("Couldn't catch up after 10 mins for " + dbName);
            throw new RuntimeException("Couldn't catch up after 10 mins");
          }
        }

        // changeDBRoleAndUpStream(me, "Master")
        Utils.changeDBRoleAndUpStream("localhost", adminPort, dbName, "MASTER",
        "", adminPort);

        // changeDBRoleAndUpStream(all_other_slaves_or_offlines, "Slave", "my_ip_port")
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String hostName = instanceNameAndRole.getKey().split("_")[0];
          int port = Integer.parseInt(instanceNameAndRole.getKey().split("_")[1]);
          if (this.host.equals(hostName)) {
            // myself
            continue;
          }

          try {
            // setup upstream for Slaves and Offlines with best-efforts
            if (instanceNameAndRole.getValue().equalsIgnoreCase("SLAVE") ||
                instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
              Utils.changeDBRoleAndUpStream(
                  hostName, port, dbName, "SLAVE", this.host, adminPort);
            }
          } catch (RuntimeException e) {
            LOG.error("Failed to set upstream for " + dbName + " on " + hostName + e.toString());
          }
        }
      } catch (RuntimeException e) {
        LOG.error(e.toString());
        throw e;
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
    }

    /**
     * 2) Master to Slave
     */
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.changeDBRoleAndUpStream("localhost", adminPort, Utils.getDbName(partitionName),
            "SLAVE", "127.0.0.1", adminPort);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
    }

    /**
     * 3) Offline to Slave
     */
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);
      String dbName = Utils.getDbName(partitionName);
      String snapshotHost = null;
      int snapshotPort = 0;
      int checkTimes = 0;
      while (true) {
        ++checkTimes;
        LOG.error("Finding upstream for " + dbName + " (" + String.valueOf(checkTimes) + ")");
        try (Locker locker = new Locker(partitionMutex)) {
          // open the DB if it's currently not opened yet
          Utils.addDB(dbName, adminPort);

          HelixAdmin admin = context.getManager().getClusterManagmentTool();
          ExternalView view = admin.getResourceExternalView(cluster, resourceName);
          Map<String, String> stateMap = view.getStateMap(partitionName);

          // find live replicas
          Map<String, String> liveHostAndRole = new HashMap<>();
          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (!role.equalsIgnoreCase("MASTER") &&
                !role.equalsIgnoreCase("SLAVE") &&
                !role.equalsIgnoreCase("OFFLINE")) {
              continue;
            }

            String hostPort = instanceNameAndRole.getKey();
            String host = hostPort.split("_")[0];
            int port = Integer.parseInt(hostPort.split("_")[1]);

            if (this.host.equals(host)) {
              // myself
              continue;
            }

            if (Utils.getLatestSequenceNumber(dbName, host, port) != -1) {
              liveHostAndRole.put(hostPort, role);
            }
          }

          // Find upstream, prefer Master
          String upstream = null;
          for (Map.Entry<String, String> instanceNameAndRole : liveHostAndRole.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (role.equalsIgnoreCase("MASTER")) {
              upstream = instanceNameAndRole.getKey();
              break;
            } else {
              upstream = instanceNameAndRole.getKey();
            }
          }
          String upstreamHost = (upstream == null ? "127.0.0.1" : upstream.split("_")[0]);
          snapshotHost = upstreamHost;
          int upstreamPort =
              (upstream == null ? adminPort : Integer.parseInt(upstream.split("_")[1]));
          snapshotPort = upstreamPort;

          // check if the local replica needs rebuild
          CheckDBResponse localStatus = Utils.checkLocalDB(dbName, adminPort);

          boolean needRebuild = true;
          if (liveHostAndRole.isEmpty()) {
            LOG.error("No other live replicas, skip rebuild " + dbName);
            needRebuild = false;
          } else if (System.currentTimeMillis() <
              localStatus.last_update_timestamp_ms + localStatus.wal_ttl_seconds * 1000) {
            LOG.error("Replication lag is within the range, skip rebuild " + dbName);
            LOG.error("Last update timestamp in ms: " + String
                .valueOf(localStatus.last_update_timestamp_ms));
            needRebuild = false;
          } else if (localStatus.seq_num ==
              Utils.getLatestSequenceNumber(dbName, upstreamHost, upstreamPort)) {
            // this could happen if no update to the db for a long time
            LOG.error("Upstream seq # is identical to local seq #, skip rebuild " + dbName);
            needRebuild = false;
          }

          // if rebuild is not needed, setup upstream and return
          if (!needRebuild) {
            Utils.changeDBRoleAndUpStream("localhost", adminPort, dbName, "SLAVE",
                upstreamHost, upstreamPort);

            return;
          }
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          LOG.error(
              "Failed to release the mutex for partition " + resourceName + "/" + partitionName,
              e);
          // we may or may not have finished the transition successfully when we get to here.
          // still want to mark the partition as ERROR for safety
          throw new RuntimeException(e);
        }

        // rebuild is needed
        // We don't want to hold the lock while backup and restore the DB as it can a while to
        // finish.
        // There is a small chance that snapshotHost removes the db after we release the lock and
        // before we call backupDB() below. In that case, we will throw and let Helix mark the
        // local partition as error.
        String hdfsPath = "/rocksplicator/" + cluster + "/" + dbName + "/" + snapshotHost + "_"
            + String.valueOf(snapshotPort) + "/" + String.valueOf(System.currentTimeMillis());

        // backup a snapshot from the upstream host, and restore it locally
        LOG.error("Backup " + dbName + " from " + snapshotHost);
        Utils.backupDB(snapshotHost, snapshotPort, dbName, hdfsPath);
        LOG.error("Restore " + dbName + " from " + hdfsPath);
        Utils.closeDB(dbName, adminPort);
        Utils.restoreLocalDB(adminPort, dbName, hdfsPath, snapshotHost, snapshotPort);
      }
    }

    /**
     * 4) Slave to Offline
     */
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        // changeDBRoleAndUpStream(all_other_slaves_or_offlines, "Slave", "live_master_or_slave")
        HelixAdmin admin = context.getManager().getClusterManagmentTool();
        ExternalView view = admin.getResourceExternalView(cluster, resourceName);
        Map<String, String> stateMap = view.getStateMap(partitionName);

        // find upstream which is not me, and prefer master
        String upstream = null;
        String dbName = Utils.getDbName(partitionName);
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String hostPort = instanceNameAndRole.getKey();
          String hostName = hostPort.split("_")[0];
          int port = Integer.parseInt(hostPort.split("_")[1]);
          if (this.host.equals(hostName) ||
              Utils.getLatestSequenceNumber(dbName, hostName, port) == -1) {
            // myself or dead
            continue;
          }

          String role = instanceNameAndRole.getValue();
          if (role.equalsIgnoreCase("MASTER")) {
            upstream = hostPort;
            break;
          }

          if (role.equalsIgnoreCase("SLAVE")) {
            upstream = hostPort;
            if (Utils.isMasterReplica(hostName, port, dbName)) {
              break;
            }
          }
        }

        if (upstream != null) {
          // setup upstream for all other slaves and offlines
          String upstreamName = upstream.split("_")[0];
          int upstreamPort = Integer.parseInt(upstream.split("_")[1]);

          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String hostName = instanceNameAndRole.getKey().split("_")[0];
            int port = Integer.parseInt(instanceNameAndRole.getKey().split("_")[1]);
            if (this.host.equals(hostName) || upstreamName.equals(hostName)) {
              //  mysel or upstream
              continue;
            }

            try {
              // setup upstream for Slaves and Offlines with best-efforts
              if (instanceNameAndRole.getValue().equalsIgnoreCase("SLAVE") ||
                  instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
                Utils.changeDBRoleAndUpStream(
                    hostName, port, dbName, "SLAVE", upstreamName, upstreamPort);
              }
            } catch (RuntimeException e) {
              LOG.error("Failed to set upstream for " + dbName + " on " + hostName + e.toString());
            }
          }
        }

        // close DB
        Utils.closeDB(dbName, adminPort);
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
    }

    /**
     * 5) Offline to Dropped
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.clearDB(Utils.getDbName(partitionName), adminPort);
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
    }

    /**
     * 6) Error to Offline
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.closeDB(Utils.getDbName(partitionName), adminPort);
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
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

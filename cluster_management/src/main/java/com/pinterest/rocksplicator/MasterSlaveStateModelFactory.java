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
 * This is to ensure that there is no concurrent state transitions for the same shard. Helix has no
 * partition level throttling support yet.
 *
 * 1) Slave to Master
 *    a) sanity check that there is no Master existing in the cluster
 *    b) make sure the local replica has the highest seq # among all existing Slaves
 *    c) changeDBRoleAndUpStream(me, "Master")
 *    d) changeDBRoleAndUpStream(all_other_slaves, "Slave", "my_ip_port")
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
 *    a) closeDB()
 *
 * 5) Offline to Dropped
 *    a) clearDB()
 *
 * 6) Error to Offline
 *    a) no-op
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
    LOG.info("Create a new state for " + partitionName);
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
        ExternalView view = admin.getResourceExternalView(cluster, resourceName);
        Map<String, String> stateMap = view.getStateMap(partitionName);

        // sanity check
        if (stateMap.containsValue("MASTER")) {
          throw new RuntimeException("Existing Master detected!");
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
          if (Long.compareUnsigned(highestSeq, seq) < 0) {
            highestSeq = seq;
            hostWithHighestSeq = hostName;
          }
        }

        if (hostWithHighestSeq != null) {
          LOG.info("Found another host with higher sequence number: " + hostWithHighestSeq);
          Utils.changeDBRoleAndUpStream(
              "localhost", adminPort, dbName, "SLAVE", hostWithHighestSeq, adminPort);

          // wait for 10 mins
          for (int i = 0; i < 600; ++i) {
            TimeUnit.SECONDS.sleep(1);
            localSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            if (Long.compareUnsigned(highestSeq, localSeq) <= 0) {
              LOG.info("Catched up!");
              break;
            }
          }

          if (Long.compareUnsigned(highestSeq, localSeq) > 0) {
            LOG.error("Couldn't catch up after 10 mins");
            throw new RuntimeException("Couldn't catch up after 10 mins");
          }
        }

        // changeDBRoleAndUpStream(me, "Master")
        Utils.changeDBRoleAndUpStream("localhost", adminPort, dbName, "MASTER",
        "", adminPort);

        // changeDBRoleAndUpStream(all_other_slaves, "Slave", "my_ip_port")
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String hostName = instanceNameAndRole.getKey().split("_")[0];
          int port = Integer.parseInt(instanceNameAndRole.getKey().split("_")[1]);
          if (this.host.equals(hostName) ||
              !instanceNameAndRole.getValue().equalsIgnoreCase("SLAVE")) {
            // myself or not slave
            continue;
          }

          try {
            // setup upstream for Slaves is best-effort
            Utils.changeDBRoleAndUpStream(
                hostName, port, dbName, "SLAVE", this.host, adminPort);
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

      try (Locker locker = new Locker(partitionMutex)) {
        String dbName = Utils.getDbName(partitionName);

        // open the DB if it's currently not opened yet
        Utils.addDB(dbName, adminPort);

        // Find upstream, prefer Master
        HelixAdmin admin = context.getManager().getClusterManagmentTool();
        ExternalView view = admin.getResourceExternalView(cluster, resourceName);
        Map<String, String> stateMap = view.getStateMap(partitionName);

        String upstream = null;
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String role = instanceNameAndRole.getValue();
          if (role.equalsIgnoreCase("MASTER")) {
            upstream = instanceNameAndRole.getKey();
            break;
          } else if (role.equalsIgnoreCase("SLAVE")) {
            upstream = instanceNameAndRole.getKey();
          }
        }

        if (upstream == null) {
          LOG.error("Couldn't find an upstream");
          throw new RuntimeException("Couldn't find an upstream");
        }

        String upstreamHost = upstream.split("_")[0];
        int upstreamPort = Integer.parseInt(upstream.split("_")[1]);

        // check if the local replica needs rebuild
        CheckDBResponse localStatus = Utils.checkLocalDB(dbName, adminPort);
        boolean needRebuild = true;
        if (!stateMap.containsValue("MASTER") && !stateMap.containsValue("SLAVE")) {
          LOG.info("No other replicas, skip rebuild");
          needRebuild = false;
        } else if (System.currentTimeMillis() <
            localStatus.last_update_timestamp_ms + localStatus.wal_ttl_seconds * 1000) {
          LOG.info("Replication lag is within the range, skip rebuild");
          needRebuild = false;
        }

        if (needRebuild) {
          // backup a snapshot from the upstream host, and restore it locally
          String hdfsPath = "/rocksplicator/" + cluster + "/" + dbName + "/" + upstream + "/"
              + String.valueOf(System.currentTimeMillis());

          LOG.info("Backup " + dbName + " from " + upstreamHost);
          Utils.backupDB(upstreamHost, upstreamPort, dbName, hdfsPath);
          LOG.info("Restore " + dbName + " from " + hdfsPath);
          Utils.restoreLocalDB(adminPort, dbName, hdfsPath, upstreamHost, upstreamPort);
        }

        // setup upstream
        Utils.changeDBRoleAndUpStream("localhost", adminPort, dbName, "SLAVE",
            upstreamHost, upstreamPort);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
    }

    /**
     * 4) Slave to Offline
     */
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.closeDB(Utils.getDbName(partitionName), adminPort);
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
      // do nothing
    }

    /**
     * 7) Error to Dropped
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.clearDB(Utils.getDbName(partitionName), adminPort);
      } catch (Exception e) {
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
    }

    private static String getLockPath(String cluster, String resourceName, String partitionName) {
      return "/rocksplicator/" + cluster + "/" + resourceName + "/" + partitionName + "/lock";
    }
  }
}

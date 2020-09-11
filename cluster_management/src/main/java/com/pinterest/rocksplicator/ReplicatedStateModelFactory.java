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
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ReplicatedStateModelFactory {

  private final Logger LOGGER;
  private final String host;
  private final int adminPort;
  private final String cluster;
  private final boolean useS3Backup;
  private final String s3Bucket;
  private final String sourceRoleName;
  private final String sinkRoleName;
  private CuratorFramework zkClient;

  public ReplicatedStateModelFactory(
      Logger logger,
      String host,
      int adminPort,
      String zkConnectString,
      String cluster,
      boolean useS3Backup,
      String s3Bucket,
      String sourceRoleName,
      String sinkRoleName) {
    this.LOGGER = logger;
    this.host = host;
    this.adminPort = adminPort;
    this.cluster = cluster;
    this.useS3Backup = useS3Backup;
    this.s3Bucket = s3Bucket;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new ExponentialBackoffRetry(1000, 3));
    this.sourceRoleName = sourceRoleName;
    this.sinkRoleName = sinkRoleName;
    zkClient.start();
  }

  public StateModel createNewStateModel(Logger logger, String resourceName, String partitionName) {
    LOGGER.error("Create a new state for " + partitionName);
    return new ReplicatedStateModel(
        logger,
        resourceName, partitionName, host, adminPort, cluster, zkClient, useS3Backup, s3Bucket,
        sourceRoleName, sinkRoleName);
  }


  public static class ReplicatedStateModel extends StateModel {

    private final Logger LOGGER;
    private final String resourceName;
    private final String partitionName;
    private final String host;
    private final int adminPort;
    private final String cluster;
    private final boolean useS3Backup;
    private final String s3Bucket;
    private final String sourceRoleName;
    private final String sinkRoleName;
    private CuratorFramework zkClient;
    private InterProcessMutex partitionMutex;


    /**
     * State model that handles the state machine of a single replica
     */
    public ReplicatedStateModel(Logger logger,
                                String resourceName, String partitionName, String host,
                                int adminPort, String cluster, CuratorFramework zkClient,
                                boolean useS3Backup, String s3Bucket,
                                String sourceRoleName,
                                String sinkRoleName) {
      this.LOGGER = logger;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.host = host;
      this.adminPort = adminPort;
      this.cluster = cluster;
      this.zkClient = zkClient;
      this.useS3Backup = useS3Backup;
      this.s3Bucket = s3Bucket;
      this.sourceRoleName = sourceRoleName;
      this.sinkRoleName = sinkRoleName;
      this.partitionMutex = new InterProcessMutex(zkClient,
          getLockPath(cluster, resourceName, partitionName));
    }

    private static String getLockPath(String cluster, String resourceName, String partitionName) {
      return "/rocksplicator/" + cluster + "/" + resourceName + "/" + partitionName + "/lock";
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

          if (!stateMap.containsValue(sourceRoleName)) {
            break;
          }

          if (i == 59) {
            throw new RuntimeException("Existing Master detected!");
          }

          TimeUnit.SECONDS.sleep(1);
          LOGGER.error("Slept for " + String.valueOf(i + 1) + " seconds for 0 Master");
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
            LOGGER
                .error("Failed to get latest sequence number from " + hostName + " for " + dbName);
            continue;
          }
          if (highestSeq < seq) {
            highestSeq = seq;
            hostWithHighestSeq = hostName;
          }
        }

        if (hostWithHighestSeq != null) {
          LOGGER.error("Found another host " + hostWithHighestSeq + " with higher seq num: " +
              String.valueOf(highestSeq) + " for " + dbName);
          Utils.changeDBRoleAndUpStream(
              "localhost", adminPort, dbName, sinkRoleName, hostWithHighestSeq, adminPort);

          // wait for up to 10 mins
          for (int i = 0; i < 600; ++i) {
            TimeUnit.SECONDS.sleep(1);
            long newLocalSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            LOGGER.error(
                "Replicated [" + String.valueOf(localSeq) + ", " + String.valueOf(newLocalSeq) +
                    ") from " + hostWithHighestSeq + " for " + dbName);
            localSeq = newLocalSeq;
            if (highestSeq <= localSeq) {
              LOGGER.error(dbName + " catched up!");
              break;
            }
            LOGGER
                .error("Slept for " + String.valueOf(i + 1) + " seconds for replicating " + dbName);
          }

          if (highestSeq > localSeq) {
            LOGGER.error("Couldn't catch up after 10 mins for " + dbName);
            throw new RuntimeException("Couldn't catch up after 10 mins");
          }
        }

        // changeDBRoleAndUpStream(me, "Leader")
        Utils.changeDBRoleAndUpStream("localhost", adminPort, dbName, sourceRoleName,
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
            if (instanceNameAndRole.getValue().equalsIgnoreCase(sinkRoleName) ||
                instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
              Utils.changeDBRoleAndUpStream(
                  hostName, port, dbName, sinkRoleName, this.host, adminPort);
            }
          } catch (RuntimeException e) {
            LOGGER.error("Failed to set upstream for " + dbName + " on " + hostName + e.toString());
          }
        }
      } catch (RuntimeException e) {
        LOGGER.error(e.toString());
        throw e;
      } catch (Exception e) {
        LOGGER.error(
            "Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * 2) Master to Slave
     */
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.changeDBRoleAndUpStream("localhost", adminPort, Utils.getDbName(partitionName),
            sinkRoleName, "127.0.0.1", adminPort);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        LOGGER.error(
            "Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
      Utils.logTransitionCompletionMessage(message);
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
        LOGGER.error("Finding upstream for " + dbName + " (" + String.valueOf(checkTimes) + ")");
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
            if (!role.equalsIgnoreCase(sourceRoleName) &&
                !role.equalsIgnoreCase(sinkRoleName) &&
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
            if (role.equalsIgnoreCase(sourceRoleName)) {
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
            LOGGER.error("No other live replicas, skip rebuild " + dbName);
            needRebuild = false;
          } else if (System.currentTimeMillis() <
              localStatus.last_update_timestamp_ms + localStatus.wal_ttl_seconds * 1000) {
            LOGGER.error("Replication lag is within the range, skip rebuild " + dbName);
            LOGGER.error("Last update timestamp in ms: " + String
                .valueOf(localStatus.last_update_timestamp_ms));
            needRebuild = false;
          } else if (localStatus.seq_num ==
              Utils.getLatestSequenceNumber(dbName, upstreamHost, upstreamPort)) {
            // this could happen if no update to the db for a long time
            LOGGER.error("Upstream seq # is identical to local seq #, skip rebuild " + dbName);
            needRebuild = false;
          }

          // if rebuild is not needed, setup upstream and return
          if (!needRebuild) {
            Utils.changeDBRoleAndUpStream("localhost", adminPort, dbName, sinkRoleName,
                upstreamHost, upstreamPort);
            Utils.logTransitionCompletionMessage(message);
            return;
          }
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          LOGGER.error(
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
        if (!useS3Backup) {
          String hdfsPath = "/rocksplicator/" + cluster + "/" + dbName + "/" + snapshotHost + "_"
              + String.valueOf(snapshotPort) + "/" + String.valueOf(System.currentTimeMillis());

          // backup a snapshot from the upstream host, and restore it locally
          LOGGER.error("Backup " + dbName + " from " + snapshotHost);
          Utils.backupDB(snapshotHost, snapshotPort, dbName, hdfsPath);
          LOGGER.error("Restore " + dbName + " from " + hdfsPath);
          Utils.closeDB(dbName, adminPort);
          Utils.restoreLocalDB(adminPort, dbName, hdfsPath, snapshotHost, snapshotPort);
        } else {
          String s3Path = "backup/" + cluster + "/" + dbName + "/" + snapshotHost + "_"
              + String.valueOf(snapshotPort) + "/" + String.valueOf(System.currentTimeMillis());

          // backup a snapshot from the upstream host, and restore it locally
          LOGGER.error("S3 Backup " + dbName + " from " + snapshotHost);
          Utils.backupDBToS3(snapshotHost, snapshotPort, dbName, s3Bucket, s3Path);
          LOGGER.error("S3 Restore " + dbName + " from " + s3Path);
          Utils.closeDB(dbName, adminPort);
          Utils.restoreLocalDBFromS3(adminPort, dbName, s3Bucket, s3Path, snapshotHost,
              snapshotPort);
        }
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
          if (role.equalsIgnoreCase(sourceRoleName)) {
            upstream = hostPort;
            break;
          }

          if (role.equalsIgnoreCase(sinkRoleName)) {
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
              if (instanceNameAndRole.getValue().equalsIgnoreCase(sinkRoleName) ||
                  instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
                Utils.changeDBRoleAndUpStream(
                    hostName, port, dbName, sinkRoleName, upstreamName, upstreamPort);
              }
            } catch (RuntimeException e) {
              LOGGER
                  .error("Failed to set upstream for " + dbName + " on " + hostName + e.toString());
            }
          }
        }

        // close DB
        Utils.closeDB(dbName, adminPort);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * 5) Offline to Dropped
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.clearDB(Utils.getDbName(partitionName), adminPort);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * 6) Error to Offline
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.closeDB(Utils.getDbName(partitionName), adminPort);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * 7) Error to Dropped
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      onBecomeDroppedFromOffline(message, context);
    }
  }
}

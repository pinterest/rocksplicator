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
// @author rajathprasad (rajathprasad@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksdb_admin.thrift.CheckDBResponse;
import com.pinterest.rocksplicator.eventstore.LeaderEventsCollector;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;

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
 * The LeaderFollower state machine has 5 possible states. There are 7 possible state transitions
 * that
 * we need to handle.
 *                         1           3             5
 *                LEADER  <-   FOLLOWER  <-   OFFLINE  ->  DROPPED
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
 * 1) Follower to Leader
 *    a) sanity check that there is no Leader existing in the cluster
 *    b) make sure the local replica has the highest seq # among all existing Followers
 *    c) changeDBRoleAndUpStream(me, "Leader")
 *    d) changeDBRoleAndUpStream(all_other_followers_or_offlines, "Follower", "my_ip_port")
 *
 * 2) Leader to Follower
 *    a) changeDBRoleAndUpStream(me, "Follower", "127.0.0.1:9090")
 *
 * 3) Offline to Follower
 *    a) addDB("127.0.0.1:9090")
 *    b) Check if the local replica needs to be rebuilt, i.e., there exist some other replicas in
 *       the cluster and latest data in local WAL is too old and way behind other replicas
 *    c) if yes, backupDB(// prefer Leader), and then restoreDB()
 *    d) changeDBRoleAndUpStream(me, "Follower", "Leader_ip_port") if Leader exists
 *
 * 4) Follower to Offline
 *    a) changeDBRoleAndUpStream(all_other_followers_or_offlines, "Follower",
 *    "live_leader_or_follower")
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
public class LeaderFollowerStateModelFactory extends StateModelFactory<StateModel> {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderFollowerStateModelFactory.class);
  private static final String LOCAL_HOST_IP = "127.0.0.1";

  private final String host;
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;
  private final boolean useS3Backup;
  private final String s3Bucket;
  private final LeaderEventsLogger leaderEventsLogger;

  public LeaderFollowerStateModelFactory(
      final String host,
      final int adminPort,
      final String zkConnectString,
      final String cluster,
      final boolean useS3Backup,
      final String s3Bucket,
      final LeaderEventsLogger leaderEventsLogger) {
    this.host = host;
    this.adminPort = adminPort;
    this.cluster = cluster;
    this.useS3Backup = useS3Backup;
    this.s3Bucket = s3Bucket;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
    this.leaderEventsLogger = leaderEventsLogger;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.error("Create a new state for " + partitionName);
    return new LeaderFollowerStateModel(
        resourceName,
        partitionName,
        host,
        adminPort,
        cluster,
        zkClient,
        useS3Backup,
        s3Bucket,
        leaderEventsLogger);
  }


  public static class LeaderFollowerStateModel extends StateModel {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderFollowerStateModel.class);

    private final String resourceName;
    private final String partitionName;
    private final String host;
    private final int adminPort;
    private final String cluster;
    private CuratorFramework zkClient;
    private final boolean useS3Backup;
    private final String s3Bucket;
    private InterProcessMutex partitionMutex;
    private final LeaderEventsLogger leaderEventsLogger;


    /**
     * State model that handles the state machine of a single replica
     */
    public LeaderFollowerStateModel(
        final String resourceName,
        final String partitionName,
        final String host,
        final int adminPort,
        final String cluster,
        final CuratorFramework zkClient,
        final boolean useS3Backup,
        final String s3Bucket,
        final LeaderEventsLogger leaderEventsLogger) {
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.host = host;
      this.adminPort = adminPort;
      this.cluster = cluster;
      this.zkClient = zkClient;
      this.useS3Backup = useS3Backup;
      this.s3Bucket = s3Bucket;
      this.partitionMutex = new InterProcessMutex(zkClient,
          getLockPath(cluster, resourceName, partitionName));
      this.leaderEventsLogger = leaderEventsLogger;
    }

    /**
     * 1) Follower to Leader
     */
    public void onBecomeLeaderFromFollower(Message message, NotificationContext context) {
      LeaderEventsCollector leaderEventsCollector
          = leaderEventsLogger.newEventsCollector(resourceName, partitionName);

      leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_INIT, null);

      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        HelixAdmin admin = context.getManager().getClusterManagmentTool();
        Map<String, String> stateMap = null;
        ExternalView view = null;

        // sanity check no existing Leader for up to 59 seconds
        for (int i = 0; i < 60; ++i) {
          view = admin.getResourceExternalView(cluster, resourceName);
          stateMap = view.getStateMap(partitionName);

          if (!stateMap.containsValue("LEADER")) {
            break;
          }

          if (i == 59) {
            throw new RuntimeException("Existing Leader detected!");
          }

          TimeUnit.SECONDS.sleep(1);
          LOG.error("Slept for " + String.valueOf(i + 1) + " seconds for 0 Leader");
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
              LOCAL_HOST_IP, adminPort, dbName, "FOLLOWER", hostWithHighestSeq, adminPort);

          // wait for up to 10 mins
          for (int i = 0; i < 600; ++i) {
            TimeUnit.SECONDS.sleep(1);
            long newLocalSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            LOG.error(
                "Replicated [" + String.valueOf(localSeq) + ", " + String.valueOf(newLocalSeq) +
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

        // changeDBRoleAndUpStream(me, "Leader")
        Utils.changeDBRoleAndUpStream(LOCAL_HOST_IP, adminPort, dbName, "LEADER",
            "", adminPort);

        // Get the latest external view and state map
        view = admin.getResourceExternalView(cluster, resourceName);
        stateMap = view.getStateMap(partitionName);
        // changeDBRoleAndUpStream(all_other_followers_or_offlines, "Follower", "my_ip_port")
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String hostName = instanceNameAndRole.getKey().split("_")[0];
          int port = Integer.parseInt(instanceNameAndRole.getKey().split("_")[1]);
          if (this.host.equals(hostName)) {
            // myself
            continue;
          }

          try {
            // setup upstream for Followers and Offlines with best-efforts
            if (instanceNameAndRole.getValue().equalsIgnoreCase("FOLLOWER") ||
                instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
              Utils.changeDBRoleAndUpStream(
                  hostName, port, dbName, "FOLLOWER", this.host, adminPort);
            }
          } catch (RuntimeException e) {
            LOG.error("Failed to set upstream for " + dbName + " on " + hostName + e.toString());
          }
        }
        leaderEventsCollector
            .addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS, null);
      } catch (RuntimeException e) {
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_FAILURE, null);
        LOG.error(e.toString());
        throw e;
      } catch (Exception e) {
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_FAILURE, null);
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      } finally {
        leaderEventsCollector.commit();
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * 2) Leader to Follower
     */
    public void onBecomeFollowerFromLeader(Message message, NotificationContext context) {
      LeaderEventsCollector leaderEventsCollector
          = leaderEventsLogger.newEventsCollector(resourceName, partitionName);

      leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_DOWN_INIT, null);
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        Utils.changeDBRoleAndUpStream(LOCAL_HOST_IP, adminPort, Utils.getDbName(partitionName),
            "FOLLOWER", LOCAL_HOST_IP, adminPort);
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_DOWN_SUCCESS, null);
      } catch (RuntimeException e) {
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_DOWN_FAILURE, null);
        throw e;
      } catch (Exception e) {
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_DOWN_FAILURE, null);
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      } finally {
        leaderEventsCollector.commit();
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * 3) Offline to Follower
     */
    public void onBecomeFollowerFromOffline(Message message, NotificationContext context) {
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
          Utils.addDB(dbName, adminPort, "FOLLOWER");

          HelixAdmin admin = context.getManager().getClusterManagmentTool();
          ExternalView view = admin.getResourceExternalView(cluster, resourceName);
          Map<String, String> stateMap = view.getStateMap(partitionName);

          // find live replicas
          Map<String, String> liveHostAndRole = new HashMap<>();
          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (!role.equalsIgnoreCase("LEADER") &&
                !role.equalsIgnoreCase("FOLLOWER") &&
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

          // Find upstream, prefer Leader
          String upstream = null;
          for (Map.Entry<String, String> instanceNameAndRole : liveHostAndRole.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (role.equalsIgnoreCase("LEADER")) {
              upstream = instanceNameAndRole.getKey();
              break;
            } else {
              upstream = instanceNameAndRole.getKey();
            }
          }
          String upstreamHost = (upstream == null ? LOCAL_HOST_IP : upstream.split("_")[0]);
          snapshotHost = upstreamHost;
          int upstreamPort =
              (upstream == null ? adminPort : Integer.parseInt(upstream.split("_")[1]));
          snapshotPort = upstreamPort;

          // check if the local replica needs rebuild
          CheckDBResponse
              localStatus =
              Utils.checkRemoteOrLocalDB(LOCAL_HOST_IP, adminPort, dbName, true, null);
          CheckDBResponse upstreamStatus = null;
          if (!upstreamHost.equals(LOCAL_HOST_IP) && !upstreamHost.equals(this.host)) {
            upstreamStatus =
                Utils.checkRemoteOrLocalDB(upstreamHost, upstreamPort, dbName, true, null);
          }

          boolean needRebuild = true;
          if (upstreamStatus != null && !upstreamStatus.db_metas.equals(localStatus.db_metas)) {
            LOG.error("upstreamStatus exist and differ from localStatus, rebuild.");
          } else if (liveHostAndRole.isEmpty()) {
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
            Utils.changeDBRoleAndUpStream(LOCAL_HOST_IP, adminPort, dbName, "FOLLOWER",
                upstreamHost, upstreamPort);
            Utils.logTransitionCompletionMessage(message);
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
        if (!useS3Backup) {
          String hdfsPath = "/rocksplicator/" + cluster + "/" + dbName + "/" + snapshotHost + "_"
              + String.valueOf(snapshotPort) + "/" + String.valueOf(System.currentTimeMillis());

          // backup a snapshot from the upstream host, and restore it locally
          LOG.error("Backup " + dbName + " from " + snapshotHost);
          Utils.backupDB(snapshotHost, snapshotPort, dbName, hdfsPath);
          LOG.error("Restore " + dbName + " from " + hdfsPath);
          Utils.closeDB(dbName, adminPort);
          Utils.restoreLocalDB(adminPort, dbName, hdfsPath, snapshotHost, snapshotPort);
        } else {
          String s3Path = "backup/" + cluster + "/" + dbName + "/" + snapshotHost + "_"
              + String.valueOf(snapshotPort) + "/" + String.valueOf(System.currentTimeMillis());

          // backup a snapshot from the upstream host, and restore it locally
          LOG.error("S3 Backup " + dbName + " from " + snapshotHost);
          Utils.backupDBToS3(snapshotHost, snapshotPort, dbName, s3Bucket, s3Path);
          LOG.error("S3 Restore " + dbName + " from " + s3Path);
          Utils.closeDB(dbName, adminPort);
          Utils.restoreLocalDBFromS3(adminPort, dbName, s3Bucket, s3Path, snapshotHost,
              snapshotPort);
        }
      }
    }

    /**
     * 4) Follower to Offline
     */
    public void onBecomeOfflineFromFollower(Message message, NotificationContext context) {
      Utils.logTransitionMessage(message);

      try (Locker locker = new Locker(partitionMutex)) {
        // changeDBRoleAndUpStream(all_other_followers_or_offlines, "Follower",
        // "live_leader_or_follower")
        HelixAdmin admin = context.getManager().getClusterManagmentTool();
        ExternalView view = admin.getResourceExternalView(cluster, resourceName);
        Map<String, String> stateMap = view.getStateMap(partitionName);

        // find upstream which is not me, and prefer leader
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
          if (role.equalsIgnoreCase("LEADER")) {
            upstream = hostPort;
            break;
          }

          if (role.equalsIgnoreCase("FOLLOWER")) {
            upstream = hostPort;
            if (Utils.isLeaderReplica(hostName, port, dbName)) {
              break;
            }
          }
        }

        if (upstream != null) {
          // setup upstream for all other followers and offlines
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
              // setup upstream for Followers and Offlines with best-efforts
              if (instanceNameAndRole.getValue().equalsIgnoreCase("FOLLOWER") ||
                  instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
                Utils.changeDBRoleAndUpStream(
                    hostName, port, dbName, "FOLLOWER", upstreamName, upstreamPort);
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
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
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
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
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

    private static String getLockPath(String cluster, String resourceName, String partitionName) {
      return "/rocksplicator/" + cluster + "/" + resourceName + "/" + partitionName + "/lock";
    }
  }
}

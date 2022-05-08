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
import com.pinterest.rocksplicator.utils.PartitionState;
import com.pinterest.rocksplicator.utils.PartitionStateManager;
import com.pinterest.rocksplicator.utils.PartitionStateUpdater;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.lang.Math;

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
  private static final int LEADER_CATCH_UP_THRESHOLD = 100;
  private static final int MAX_ZK_RETRIES = 3;

  private final String host;
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;
  private final boolean useS3Backup;
  private final String s3Bucket;
  private final LeaderEventsLogger leaderEventsLogger;
  public final PartitionStateUpdater partitionStateUpdater;

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
    this.partitionStateUpdater = new PartitionStateUpdater(this.zkClient, this.cluster, this.adminPort);
    this.partitionStateUpdater.start();
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
        leaderEventsLogger,
        partitionStateUpdater);
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
    public final PartitionStateUpdater partitionStateUpdater;

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
        final LeaderEventsLogger leaderEventsLogger,
        PartitionStateUpdater partitionStateUpdater) {
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
      this.partitionStateUpdater = partitionStateUpdater;
      LOG.error("LeaderFollowerStateModel has been instantiated [" + partitionName + "]");
    }

    /**
     * 1) Follower to Leader
     */
    public void onBecomeLeaderFromFollower(Message message, NotificationContext context) {
      LeaderEventsCollector leaderEventsCollector
          = leaderEventsLogger.newEventsCollector(resourceName, partitionName);

      leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_INIT, null);

      Utils.logTransitionMessage(message);
      final String dbName = Utils.getDbName(partitionName);
      PartitionStateManager stateManager = new PartitionStateManager(cluster, resourceName, partitionName, zkClient);

      try (Locker locker = new Locker(partitionMutex)) {
        HelixAdmin admin = context.getManager().getClusterManagmentTool();
        Map<String, String> stateMap = null;
        ExternalView view = null;

        // sanity check no existing Leader for up to 59 seconds
        for (int i = 0; i < 60; ++i) {
          view = admin.getResourceExternalView(cluster, resourceName);
          stateMap = Utils.getStateMap(view, partitionName);

          if (!stateMap.containsValue("LEADER")) {
            break;
          }

          if (i == 59) {
            throw new RuntimeException("Existing Leader detected!");
          }

          TimeUnit.SECONDS.sleep(1);
          LOG.error("Slept for " + String.valueOf(i + 1) + " seconds for 0 Leader");
        }

        // make sure local replica has the highest sequence number
        long localSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
        String hostWithHighestSeq = null;
        long highestSeq = localSeq;
        long numActiveReplicas = 1;
        long totalReplicas = stateMap.keySet().size();
        for (String instanceName : stateMap.keySet()) {
          String[] hostPort = instanceName.split("_");
          String hostName = hostPort[0];
          int port = Integer.parseInt(hostPort[1]);
          if (this.host.equals(hostName)) {
            // myself
            continue;
          }

          long seq = Utils.getLatestSequenceNumber(dbName, hostName, port);
          if (seq < 0) {
            LOG.error("Failed to get latest sequence number from " + hostName + " for " + dbName);
            continue;
          }

          numActiveReplicas++;

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

          // wait for up to 10 mins to catch up to at least highestSeq
          for (int i = 0; i < 600; ++i) {
            TimeUnit.SECONDS.sleep(1);
            long newLocalSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            LOG.error(
                "Replicated [" + String.valueOf(localSeq) + ", " + String.valueOf(newLocalSeq) +
                    ") from " + hostWithHighestSeq + " for " + dbName);
            localSeq = newLocalSeq;
            if (highestSeq <= localSeq) {
              LOG.error(dbName + " caught up!");
              break;
            }
            LOG.error("Slept for " + String.valueOf(i + 1) + " seconds for replicating " + dbName);
          }

          if (highestSeq > localSeq) {
            LOG.error("Couldn't catch up after 10 mins for " + dbName);
            throw new RuntimeException("Couldn't catch up after 10 mins");
          }
        }

        // At this point, this replica has caught up to have the highest sequence number, except the replicas
        // it failed to contact with.
        LOG.error("local.seq:" + localSeq + " replica:[" + numActiveReplicas + "/"+ totalReplicas + "]");

        // 3-Node failure case
        if (localSeq <= 0) {
          PartitionState lastState = stateManager.getState();
          if (!lastState.isValid()) {
            LOG.error("Invalid last state [part: " + partitionName + "] State:" + lastState.toString());
          } else if (lastState.seqNum > 0) {
            // At this point, the leader replica doesn't have any updates, but the zk state shows the shard has historical updates;
            // To avoid electing a new leader with no data when the old 3 nodes of the shard are currently down, 
            // it's safer to error out, in case any of the old 3 nodes come back up.
            String errorString = String.format("Cannot Transition [last.seq:%d curr.seq:%d]", lastState.seqNum, localSeq);
            LOG.error(errorString);
            throw new RuntimeException(errorString);
          }
        }

        // changeDBRoleAndUpStream(me, "Leader")
        Utils.changeDBRoleAndUpStream(LOCAL_HOST_IP, adminPort, dbName, "LEADER",
            "", adminPort);

        partitionStateUpdater.addLeader(resourceName, partitionName);
        // store the latest seqNum as last state
        stateManager.saveSeqNum(localSeq);

        // Get the latest external view and state map
        LOG.error("[" + dbName + "] Getting external view");
        view = Utils.getHelixExternalViewWithFixedRetry(admin, cluster, resourceName);
        LOG.error("[" + dbName + "] Got external view");
        stateMap = Utils.getStateMap(view, partitionName);
        // changeDBRoleAndUpStream(all_other_followers_or_offlines, "Follower", "my_ip_port")
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String[] hostPort = instanceNameAndRole.getKey().split("_");
          String hostName = hostPort[0];
          int port = Integer.parseInt(hostPort[1]);
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
              LOG.error("[" + dbName + "] Done calling changeDBRoleAndUpStream");
            }
          } catch (RuntimeException e) {
            LOG.error("Failed to changeDBRoleAndUpStream for " + dbName + " on " + hostName + e.toString());
          }
        }
        leaderEventsCollector
            .addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS, null);
        LOG.error("[" + dbName + "] Done calling leaderEventsCollector.addEvent");
      } catch (RuntimeException e) {
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_FAILURE, null);
        LOG.error(e.toString());
        throw e;
      } catch (Exception e) {
        leaderEventsCollector.addEvent(LeaderEventType.PARTICIPANT_LEADER_UP_FAILURE, null);
        LOG.error("Failed to release the mutex for partition " + resourceName + "/" + partitionName,
            e);
      } finally {
        LOG.error("[" + dbName + "] Calling leaderEventsCollector.commit");
        leaderEventsCollector.commit();
        LOG.error("[" + dbName + "] Done calling leaderEventsCollector.commit");
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
      partitionStateUpdater.removeLeader(resourceName, partitionName);
    }

    public String getLeaderInstance(NotificationContext context) {
      HelixAdmin admin = context.getManager().getClusterManagmentTool();
      ExternalView view = Utils.getHelixExternalViewWithFixedRetry(admin, cluster, resourceName);
      Map<String, String> stateMap = Utils.getStateMap(view, partitionName);
      for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
        if (instanceNameAndRole.getValue().equalsIgnoreCase("LEADER")) {
          return instanceNameAndRole.getKey();
        }
      }
      return null;
    }

    public Map<String, String> getLiveHostAndRole(NotificationContext context, String dbName) {
      HelixAdmin admin = context.getManager().getClusterManagmentTool();
      ExternalView view = Utils.getHelixExternalViewWithFixedRetry(admin, cluster, resourceName);
      Map<String, String> stateMap = Utils.getStateMap(view, partitionName);

      // find live replicas
      Map<String, String> liveHostAndRole = new HashMap<>();
      for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
        String role = instanceNameAndRole.getValue();
        if (!role.equalsIgnoreCase("LEADER") &&
            !role.equalsIgnoreCase("FOLLOWER") &&
            !role.equalsIgnoreCase("OFFLINE")) {
          continue;
        }

        String instanceName = instanceNameAndRole.getKey();
        String[] hostPort = instanceName.split("_");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        if (this.host.equals(host)) {
          // myself
          continue;
        }

        if (Utils.getLatestSequenceNumber(dbName, host, port) != -1) {
          liveHostAndRole.put(instanceName, role);
        }
      }
      return liveHostAndRole;
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

          // find live replicas
          Map<String, String> liveHostAndRole = getLiveHostAndRole(context, dbName);

          // Find upstream, prefer Leader
          String upstream = null;
          for (Map.Entry<String, String> instanceNameAndRole : liveHostAndRole.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (role.equalsIgnoreCase("LEADER")) {
              upstream = instanceNameAndRole.getKey();
              LOG.error("Found leader address for " + dbName + ": " + upstream);
              break;
            } else {
              upstream = instanceNameAndRole.getKey();
            }
          }
          String upstreamHost = (upstream == null ? LOCAL_HOST_IP : upstream.split("_")[0]);
          LOG.error("Use " + upstreamHost + " as the upstreamHost for " + dbName);
          snapshotHost = upstreamHost;
          int upstreamPort =
              (upstream == null ? adminPort : Integer.parseInt(upstream.split("_")[1]));
          snapshotPort = upstreamPort;

          // check if the local replica needs rebuild
          CheckDBResponse
              localStatus =
              Utils.checkRemoteOrLocalDB(LOCAL_HOST_IP, adminPort, dbName, true, null, null);
          CheckDBResponse upstreamStatus = null;
          if (!upstreamHost.equals(LOCAL_HOST_IP) && !upstreamHost.equals(this.host)) {
            upstreamStatus =
                Utils.checkRemoteOrLocalDB(upstreamHost, upstreamPort, dbName, true, null, null);
          }

          boolean needRebuild = needRebuildDB(dbName, upstreamStatus, localStatus, liveHostAndRole, upstreamHost, upstreamPort);

          // if rebuild is not needed, setup upstream and return
          if (!needRebuild) {
            // It's possible we may have used a follower for catching up updates (when leader is unhealthy),
            // now it's time to use the true leader as the upstream for subsequent replication requests.
            // Otherwise it leads to various failures/degradations:
            // - follower not receiving any updates (when upstream is itself, or followers using each other as upstream)
            // - follower getting updates from another follower, causing replication tag and read consistency issue
            // - leader not receiving ACK for this replica (2-ack mode at risk)
            String leaderInstance = getLeaderInstance(context);
            if (leaderInstance != null && leaderInstance != upstream) {
              String[] leaderHostPort = leaderInstance.split("_");
              upstreamHost = leaderHostPort[0];
              upstreamPort = Integer.parseInt(leaderHostPort[1]);
              LOG.error("Leader address differs from current upstream. Using leader " + upstreamHost + " as the upstream for " + dbName);
            }

            Utils.changeDBRoleAndUpStream(LOCAL_HOST_IP, adminPort, dbName, "FOLLOWER",
                upstreamHost, upstreamPort);

            // Reset RocksDB options based on resource_configs stored on zk.
            // At service restart, db options are created fresh from code&GFlags and overwrite
            // the existing db OPTIONS at disk, which will erase any options changes made by
            // AdminHandler's setDBOptions API. Thus, to inherit those options changes, user also
            // need to store those options-changes at zk path as json string:
            // metadata/<cluster>/<seg>/resource_configs. Upon service reload DBs at restart, the
            // DB will always goes through Offline->follower, then following logic will set
            // options changes based on resource_configs from zk.
            String metaResourceCfg =
                Utils.getMetaResourceConfigs(zkClient, cluster, resourceName, MAX_ZK_RETRIES);
            if (metaResourceCfg.isEmpty()) {
              LOG.error("resource_configs from metadata is not set. Skip setting and return OK");
            } else {
              JsonObject resCfg = new JsonParser().parse(metaResourceCfg).getAsJsonObject();
              Map<String, String> dbOptions = new HashMap<>();

              String disableAutoCompactionsName =
                  Utils.ResourceConfigProperty.DISABLE_AUTO_COMPACTIONS.name().toLowerCase();
              JsonElement disableAutoCompactionsEl = resCfg.get(disableAutoCompactionsName);
              if (disableAutoCompactionsEl != null) {
                String dbOptionsDisableAutoCompactions = disableAutoCompactionsEl.getAsString();
                dbOptions.put(disableAutoCompactionsName, dbOptionsDisableAutoCompactions);
              }

              Utils.setDBOptions(LOCAL_HOST_IP, adminPort, dbName, dbOptions);
            }

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

        try {
          LOG.error(dbName + " Catching up live updates to leader");
          Map<String, String> liveHostAndRole = getLiveHostAndRole(context, dbName);
          // Find upstream, prefer Leader
          String upstream = null;
          for (Map.Entry<String, String> instanceNameAndRole : liveHostAndRole.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (role.equalsIgnoreCase("OFFLINE")) {
              continue;
            }
            if (role.equalsIgnoreCase("LEADER")) {
              upstream = instanceNameAndRole.getKey();
              break;
            } else {
              upstream = instanceNameAndRole.getKey();
            }
          }

          String upstreamHost = (upstream == null ? LOCAL_HOST_IP : upstream.split("_")[0]);
          LOG.error(dbName + " got leader " + upstreamHost);
          int upstreamPort =
              (upstream == null ? adminPort : Integer.parseInt(upstream.split("_")[1]));

          long local_seq_num;
          long leader_seq_num;
          // wait for up to 10 mins
          for (int i = 0; i < 600; ++i) {
            local_seq_num = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            leader_seq_num = Utils.getLatestSequenceNumber(dbName, upstreamHost, upstreamPort);

            // If leader sequence number is within threshold, consider caught up.
            if (leader_seq_num < local_seq_num + LEADER_CATCH_UP_THRESHOLD) {
              LOG.error(dbName + " caught up!");
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          }
        } catch (Exception e) {
          // We've already backed up leader data and restored, ignore exception while
          // waiting to catch up to leader
          // TODO (rajathprasad): Add a stat for monitoring.
          LOG.error("Failed to catch up to leader after backup " + dbName, e);
        }
      }
    }

    // rebuild the replica if there is at least one live replica to rebuild from AND it meets either of TWO conditions:
    // Case 1. DB local meta is different from upstream
    // Case 2. DB local meta is the same as upstream, but latest local data update is too old (WAL TTL expired).
    //         However, skip rebuild if sequence number is the same as upstream (nothing to rebuild from).
    private boolean needRebuildDB(String dbName, CheckDBResponse upstreamStatus, CheckDBResponse localStatus, Map<String, String> liveHostAndRole, String upstreamHost, int upstreamPort) {
      if (liveHostAndRole.isEmpty()) {
        LOG.error("No other live replicas, skip rebuild " + dbName);
        return false;
      } 

      // check DB meta first before checking replication lag & sequence number, since it's possible
      // an upstream replica has ingested new data offline without updating its sequence number.
      if (upstreamStatus != null && upstreamStatus.isSetDb_metas() && !upstreamStatus.db_metas
          .equals(localStatus.db_metas)) {
        LOG.error(String.format(
            "upstreamStatus exist and differ from localStatus for %s, rebuild. "
                + "upstreamStatus: %s, localStatus: %s", dbName, upstreamStatus.toString(),
            localStatus.toString()));
        return true;
      } 

      // do not trigger rebuild if the latest db update is within min(wal_ttl_seconds, 1h).
      // the 1h upper bound is to ensure we are not too lagging behind when catching up from upstream via replication,
      // since wal_ttl_seconds can be large.
      long maxLogCatchupTimeSec = Math.min(localStatus.wal_ttl_seconds, 60*60 /* 1h */);
      if (System.currentTimeMillis() <
          localStatus.last_update_timestamp_ms + maxLogCatchupTimeSec * 1000) {
        LOG.error("Replication lag is within the range, skip rebuild " + dbName);
        LOG.error("Last update timestamp in ms: " + String
            .valueOf(localStatus.last_update_timestamp_ms));
        return false;
      } 
      
      if (localStatus.seq_num ==
          Utils.getLatestSequenceNumber(dbName, upstreamHost, upstreamPort)) {
        // this could happen if no update to the db for a long time
        LOG.error("Upstream seq # is identical to local seq #, skip rebuild " + dbName);
        return false;
      }

      return true;
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
        ExternalView view = Utils.getHelixExternalViewWithFixedRetry(admin, cluster, resourceName);
        Map<String, String> stateMap = Utils.getStateMap(view, partitionName);

        // find upstream which is not me, and prefer leader
        String upstream = null;
        String dbName = Utils.getDbName(partitionName);
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String instanceName = instanceNameAndRole.getKey();
          String[] hostPort = instanceName.split("_");
          String hostName = hostPort[0];
          int port = Integer.parseInt(hostPort[1]);
          if (this.host.equals(hostName) ||
              Utils.getLatestSequenceNumber(dbName, hostName, port) == -1) {
            // myself or dead
            continue;
          }

          String role = instanceNameAndRole.getValue();
          if (role.equalsIgnoreCase("LEADER")) {
            upstream = instanceName;
            break;
          }

          if (role.equalsIgnoreCase("FOLLOWER")) {
            upstream = instanceName;
            if (Utils.isLeaderReplica(hostName, port, dbName)) {
              break;
            }
          }
        }

        if (upstream != null) {
          // setup upstream for all other followers and offlines
          String[] upstreamHostPort = upstream.split("_");
          String upstreamHost = upstreamHostPort[0];
          int upstreamPort = Integer.parseInt(upstreamHostPort[1]);

          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String[] hostPort = instanceNameAndRole.getKey().split("_");
            String hostName = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);
            if (this.host.equals(hostName) || upstreamHost.equals(hostName)) {
              //  mysel or upstream
              continue;
            }

            try {
              // setup upstream for Followers and Offlines with best-efforts
              if (instanceNameAndRole.getValue().equalsIgnoreCase("FOLLOWER") ||
                  instanceNameAndRole.getValue().equalsIgnoreCase("OFFLINE")) {
                Utils.changeDBRoleAndUpStream(
                    hostName, port, dbName, "FOLLOWER", upstreamHost, upstreamPort);
              }
            } catch (RuntimeException e) {
              LOG.error("Failed to set upstream for " + dbName + " on " + hostName + e.toString());
            }
          }
        }

        partitionStateUpdater.removeLeader(resourceName, partitionName);
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

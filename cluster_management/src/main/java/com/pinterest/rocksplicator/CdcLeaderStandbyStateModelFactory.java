package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.helix_client.HelixClient;
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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
 * 1) Standby to Leader: Add observer
 *
 * 2) Leader to Standby: Remove observer
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
  final int adminPort;

  public CdcLeaderStandbyStateModelFactory(final String zkConnectString, final int adminPort) {
    this.adminPort = adminPort;
    this.zkConnectString = zkConnectString;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.error("Create a new state for " + partitionName);
    return new CdcLeaderStandbyStateModel(
        resourceName, adminPort,
        zkConnectString,
        partitionName);
  }


  public static class CdcLeaderStandbyStateModel extends AbstractHelixLeaderStandbyStateModel  {
    private final String resourceName;
    private final int adminPort;
    private final String partitionName;
    private final String upstreamClusterConnectString;
    private final String upstreamClusterName;

    @StateModelInfo(initialState = "OFFLINE", states = {
        "LEADER", "STANDBY"
    })
    public CdcLeaderStandbyStateModel(final String resourceName, final int adminPort, final String zkConnectString, final String partitionName) {
      super(zkConnectString);
      this.partitionName = partitionName;
      this.adminPort = adminPort;
      this.resourceName = resourceName;
      String rocksObserverMetadataPath = ZkPathUtils.getRocksObserverMetadataPath(resourceName);
      CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
          new ExponentialBackoffRetry(1000, 3));
      zkClient.start();
      try {
        zkClient.sync().forPath(rocksObserverMetadataPath);
        String resourceMetadata = new String(zkClient.getData().forPath(rocksObserverMetadataPath));
        JSONParser parser = new JSONParser();
        try {
          JSONObject obj = (JSONObject) parser.parse(resourceMetadata);
          if (!obj.containsKey("resource_zk") || !obj.containsKey("resource_cluster")) {
            throw new RuntimeException("Missing resource_zk or resource_cluster key in " + rocksObserverMetadataPath);
          }
          this.upstreamClusterConnectString = (String)obj.get("resource_zk");
          this.upstreamClusterName = (String)obj.get("resource_cluster");
        } catch (ParseException e) {
          LOG.error("json parse ex for rocksobserver metadata " + e );
          throw new RuntimeException(e);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    /**
     * 1) Standby to Leader
     */
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
      Utils.checkStateTransitions("STANDBY", "LEADER", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);
      // DB name = partition name padded with 0s
      String dbName = Utils.getDbName(this.partitionName);
      String hostPort = HelixClient.getleaderInstanceId(upstreamClusterConnectString, upstreamClusterName, this.resourceName, this.partitionName);
      String ip = hostPort.split("_")[0];
      CdcUtils.addObserver(dbName, ip, this.adminPort);
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
      CdcUtils.removeObserver(dbName, adminPort);
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

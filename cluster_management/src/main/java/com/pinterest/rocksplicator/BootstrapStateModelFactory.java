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
// @author cdonaghy (cdonaghy@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksdb_admin.thrift.AddS3SstFilesToDBRequest;
import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksdb_admin.thrift.StartMessageIngestionRequest;
import com.pinterest.rocksdb_admin.thrift.StopMessageIngestionRequest;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Bootstrap state machine has 5 possible states. There are 7 possible state transitions that
 * we need to handle.
 *                         1              3             5
 *                ONLINE  <-   BOOTSTRAP  <-   OFFLINE  ->  DROPPED
 *                      \                 ->     ^      ^
 *                       \                4     / \   / 7
 *                        ---------------------   ERROR
 *                                   2
 *
 *
 * 1) Bootstrap to Online
 *    a) Take the zk metadata and send a StartMessageIngestionRequest, will return when we have hit kafka EOF
 *
 * 2) Online to Offline
 *    a) send a StopMessageIngestionRequest for the given partition
 *
 * 3) Offline to Bootstrap
 *    a) Download the sst files specified from the zk metadata
 *
 * 4) Bootstrap to Offline
 *    a) send a StopMessageIngestionRequest for the given partition
 *
 * 5) Offline to Dropped
 *    a) clear the DB
 *
 * 6) Error to Offline
 *    a) do nothing
 *
 * 7) Error to Dropped
 *    a) clear the DB
 */

public class BootstrapStateModelFactory extends StateModelFactory<StateModel> {
  private static final int MAX_ZK_RETRIES = 7;
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;

  private static final Logger LOG = LoggerFactory.getLogger(BootstrapStateModelFactory.class);

  BootstrapStateModelFactory(String host, int adminPort, String zkConnectString, String cluster) {
    this.adminPort = adminPort;
    this.cluster = cluster;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new BoundedExponentialBackoffRetry(1000,  10000, MAX_ZK_RETRIES));
    zkClient.start();
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.error("Create a new state for " + partitionName);
    return new BootstrapStateModel(resourceName, partitionName, adminPort, cluster, zkClient);
  }

  public static class BootstrapStateModel extends StateModel {
    private static final Logger LOG = LoggerFactory.getLogger(BootstrapStateModel.class);

    private final String resourceName;
    private final String partitionName;
    private final int adminPort;
    private final String cluster;
    private CuratorFramework zkClient;

    /**
     * State model that handles the state machine of a single partition.
     */
    public BootstrapStateModel(String resourceName, String partitionName, int adminPort,
                                   String cluster, CuratorFramework zkClient) {
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.adminPort = adminPort;
      this.cluster = cluster;
      this.zkClient = zkClient;
    }

    /**
     * Callback for OFFLINE to BOOTSTRAP transition.
     * This can happen when 1) a new partition is first launched on a host; or 2) an existing
     * partition is reopened when restart/zk temporarily expires.
     * We first make sure the DB is open and then load SST files into the DB.
     */
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "BOOTSTRAP", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      try{
        Utils.addDB(Utils.getDbName(partitionName), adminPort, "NOOP");
      } catch (Exception e) {
        LOG.error("addDB with dbRole: NOOP failed with exception during offline->bootstrap");
      }

      try {
        String meta = Utils.getMetaData(zkClient, cluster, resourceName, MAX_ZK_RETRIES);
        JsonObject jsonObject = new JsonParser().parse(meta).getAsJsonObject();

        String s3Path = jsonObject.get("s3_path").getAsString();
        String s3Bucket = jsonObject.get("s3_bucket").getAsString();
        int s3_download_limit_mb = 64;
        try {
          JsonElement je = jsonObject.get("s3_download_limit_mb");
          if (je != null) {
            s3_download_limit_mb = je.getAsInt();
          }
        } catch (Exception e) {
          LOG.error("Failed to parse s3_download_limit_mb", e);
        }

        AddS3SstFilesToDBRequest req = new AddS3SstFilesToDBRequest(Utils.getDbName(partitionName),
                s3Bucket, s3Path + Utils.getS3PartPrefix(partitionName));
        req.setS3_download_limit_mb(s3_download_limit_mb);
        Admin.Client client = Utils.getLocalAdminClient(adminPort);
        client.addS3SstFilesToDB(req);
      } catch (Exception e) {
        LOG.error("Failed to add S3 files for " + partitionName, e);
        throw new RuntimeException(e);
      }

      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * Callback for ONLINE to OFFLINE transition.
     * The callback will stop the kafka ingestion then close the db
     */
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("ONLINE", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      try {
        StopMessageIngestionRequest req = new StopMessageIngestionRequest(Utils.getDbName(partitionName));
        Admin.Client client = Utils.getLocalAdminClient(adminPort);
        client.stopMessageIngestion(req);
      } catch (Exception e) {
        LOG.error("Failed to Offline " + partitionName, e);
        throw new RuntimeException(e);
      }

      Utils.closeDB(Utils.getDbName(partitionName), adminPort);
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * Callback for BOOTSTRAP to OFFLINE transition.
     * The callback will stop the kafka ingestion then close the db
     * This might be called if there is an error from BOOTSTRAP->ONLINE
     * or if we reset the resource while it is in BOOTSTRAP->ONLINE
     */
    public void onBecomeOfflineFromBootstrap(Message message, NotificationContext context) {
      Utils.checkStateTransitions("BOOTSTRAP", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      try {
        StopMessageIngestionRequest req = new StopMessageIngestionRequest(Utils.getDbName(partitionName));
        Admin.Client client = Utils.getLocalAdminClient(adminPort);
        client.stopMessageIngestion(req);
      } catch (Exception e) {
        LOG.error("Failed to Offline " + partitionName, e);
        throw new RuntimeException(e);
      }

      Utils.closeDB(Utils.getDbName(partitionName), adminPort);
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * Callback for BOOTSTRAP to ONLINE transition
     * We will tail kafka until we hit EOF and then return
     */
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
      Utils.checkStateTransitions("BOOTSTRAP", "ONLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      try {
        String meta = Utils.getMetaData(zkClient, cluster, resourceName, MAX_ZK_RETRIES);
        JsonObject jsonObject = new JsonParser().parse(meta).getAsJsonObject();
        long replay_timestamp_ms = jsonObject.get("replay_timestamp_ms").getAsLong();
        String kafkaBrokerServersetPath = jsonObject.get("kafka_broker_serverset_path").getAsString();
        String topicName = jsonObject.get("topic_name").getAsString();
        boolean isKafkaPayloadSerialized = jsonObject.get("is_kafka_payload_serialized").getAsBoolean();

        StartMessageIngestionRequest req =
                new StartMessageIngestionRequest(Utils.getDbName(partitionName), topicName,
                        kafkaBrokerServersetPath, replay_timestamp_ms, isKafkaPayloadSerialized);
        Admin.Client client = Utils.getLocalAdminClient(adminPort);
        client.startMessageIngestion(req);
      } catch (Exception e) {
        LOG.error("Failed to replay kafka for : " + partitionName, e);
        throw new RuntimeException(e);
      }
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * Callback for OFFLINE to DROPPED transition.
     * The callback simply clear the DB.
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "DROPPED", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.clearDB(Utils.getDbName(partitionName), adminPort);
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * Callback for ERROR to DROPPED transition.
     * The callback simply clear the DB.
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      Utils.checkStateTransitions("ERROR", "DROPPED", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.clearDB(Utils.getDbName(partitionName), adminPort);
      Utils.logTransitionCompletionMessage(message);
    }

    /**
     * Callback for ERROR to OFFLINE transition.
     * The callback does nothing
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      Utils.checkStateTransitions("ERROR", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);
      Utils.logTransitionCompletionMessage(message);
    }
  }
}

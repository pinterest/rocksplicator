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

import com.pinterest.rocksdb_admin.thrift.AddS3SstFilesToDBRequest;
import com.pinterest.rocksdb_admin.thrift.Admin;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;

  private static final Logger LOG = LoggerFactory.getLogger(OnlineOfflineStateModelFactory.class);

  OnlineOfflineStateModelFactory(int adminPort, String zkConnectString, String cluster) {
    this.adminPort = adminPort;
    this.cluster = cluster;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LOG.error("Create a new state for " + partitionName);
    return new OnlineOfflineStateModel(resourceName, partitionName, adminPort, cluster, zkClient);
  }

  public static class OnlineOfflineStateModel extends StateModel {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineOfflineStateModel.class);

    private final String resourceName;
    private final String partitionName;
    private final int adminPort;
    private final String cluster;
    private CuratorFramework zkClient;

    /**
     * State model that handles the state machine of a single partition.
     */
    public OnlineOfflineStateModel(String resourceName, String partitionName, int adminPort,
                                   String cluster, CuratorFramework zkClient) {
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.adminPort = adminPort;
      this.cluster = cluster;
      this.zkClient = zkClient;
    }

    /**
     * Callback for OFFLINE to ONLINE transition.
     * This can happen when 1) a new partition is first launched on a host; or 2) an existing
     * partition is reopened when restart/zk temporarily expires.
     * We first make sure the DB is open and then load SST files into the DB.
     */
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("OFFLINE", "ONLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.addDB(Utils.getDbName(partitionName), adminPort);

      try {
        zkClient.sync().forPath(Utils.getMetaLocation(cluster, resourceName));
        String meta = new String(zkClient.getData().forPath(Utils.getMetaLocation(cluster, resourceName)));
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
     * The callback simply close the DB.
     */
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      Utils.checkStateTransitions("ONLINE", "OFFLINE", message, resourceName, partitionName);
      Utils.logTransitionMessage(message);

      Utils.closeDB(Utils.getDbName(partitionName), adminPort);
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

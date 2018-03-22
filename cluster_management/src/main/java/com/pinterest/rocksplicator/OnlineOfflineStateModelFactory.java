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
    LOG.info("Create a new state for " + partitionName);
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
      checkSanity("OFFLINE", "ONLINE", message);
      Utils.logTransitionMessage(message);

      Utils.addDB(Utils.getDbName(partitionName), adminPort);

      try {
        zkClient.sync().forPath(getMetaLocation());
        String meta = new String(zkClient.getData().forPath(getMetaLocation()));
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
            s3Bucket, s3Path + getS3PartPrefix());
        req.setS3_download_limit_mb(s3_download_limit_mb);
        Admin.Client client = Utils.getLocalAdminClient(adminPort);
        client.addS3SstFilesToDB(req);
      } catch (Exception e) {
        LOG.error("Failed to add S3 files for " + partitionName, e);
        throw new RuntimeException(e);
      }
    }

    /**
     * Callback for ONLINE to OFFLINE transition.
     * The callback simply close the DB.
     */
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      checkSanity("ONLINE", "OFFLINE", message);
      Utils.logTransitionMessage(message);

      Utils.closeDB(Utils.getDbName(partitionName), adminPort);
    }

    /**
     * Callback for OFFLINE to DROPPED transition.
     * The callback simply clear the DB.
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      checkSanity("OFFLINE", "DROPPED", message);
      Utils.logTransitionMessage(message);

      Utils.clearDB(Utils.getDbName(partitionName), adminPort);
    }

    /**
     * Callback for ERROR to DROPPED transition.
     * The callback simply clear the DB.
     */
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      checkSanity("ERROR", "DROPPED", message);
      Utils.logTransitionMessage(message);

      Utils.clearDB(Utils.getDbName(partitionName), adminPort);
    }

    /**
     * Callback for ERROR to OFFLINE transition.
     * The callback does nothing
     */
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      checkSanity("ERROR", "OFFLINE", message);
      Utils.logTransitionMessage(message);
    }

    private Admin.Client getLocalAdminClient() throws TTransportException {
      TSocket sock = new TSocket("localhost", adminPort);
      sock.open();
      return new Admin.Client(new TBinaryProtocol(sock));
    }

    private void checkSanity(String fromState, String toState, Message message) {
      if (fromState.equalsIgnoreCase(message.getFromState())
          && toState.equalsIgnoreCase(message.getToState())
          && resourceName.equalsIgnoreCase(message.getResourceName())
          && partitionName.equalsIgnoreCase(message.getPartitionName())) {
        return;
      }

      LOG.error("Invalid meesage: " + message.toString());
      LOG.error("From " + fromState + " to " + toState + " for " + partitionName);
    }

    private String getMetaLocation() {
      return "/metadata/" + this.cluster + "/" + this.resourceName + "/resource_meta";
    }

    // partition name is in format: test_0
    // S3 part prefix is in format: part-00000-
    private String getS3PartPrefix() {
      String[] parts = partitionName.split("_");
      return String.format("part-%05d-", Integer.parseInt(parts[parts.length - 1]));
    }
  }
}

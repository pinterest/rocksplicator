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

import com.pinterest.rocksdb_admin.thrift.AddDBRequest;
import com.pinterest.rocksdb_admin.thrift.AddS3SstFilesToDBRequest;
import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksdb_admin.thrift.AdminException;
import com.pinterest.rocksdb_admin.thrift.ClearDBRequest;
import com.pinterest.rocksdb_admin.thrift.CloseDBRequest;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.thrift.TException;
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
      LOG.info("Switching from " + message.getFromState() + " to " + message.getToState()
          + " for " + message.getPartitionName());

      Admin.Client client;
      try {
        client = getLocalAdminClient();
      } catch (TTransportException e) {
        LOG.error("Failed to connect to local Admin port", e);
        throw new RuntimeException(e);
      }

      try {
        AddDBRequest req = new AddDBRequest(getDbName(), "127.0.0.1");
        client.addDB(req);
      } catch (Exception e) {
        // adding db is best-effort
        LOG.info("Failed to add " + partitionName, e);
      }

      try {
        zkClient.sync().forPath(getMetaLocation());
        String meta = new String(zkClient.getData().forPath(getMetaLocation()));
        JsonObject jsonObject = new JsonParser().parse(meta).getAsJsonObject();

        String s3Path = jsonObject.get("s3_path").getAsString();
        String s3Bucket = jsonObject.get("s3_bucket").getAsString();

        AddS3SstFilesToDBRequest req = new AddS3SstFilesToDBRequest(getDbName(),
            s3Bucket, s3Path + getS3PartPrefix());
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
      LOG.info("Switching from " + message.getFromState() + " to " + message.getToState()
          + " for " + message.getPartitionName());

      try {
        Admin.Client client = getLocalAdminClient();
        CloseDBRequest req = new CloseDBRequest(getDbName());
        client.closeDB(req);
      } catch (AdminException e) {
        LOG.info(partitionName + " doesn't exist", e);
      } catch (TTransportException e) {
        LOG.error("Failed to connect to local Admin port", e);
      } catch (TException e) {
        LOG.error("CloseDB() request failed", e);
      }
    }

    /**
     * Callback for OFFLINE to DROPPED transition.
     * The callback simply clear the DB.
     */
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      checkSanity("OFFLINE", "DROPPED", message);
      LOG.info("Switching from " + message.getFromState() + " to " + message.getToState()
          + " for " + message.getPartitionName());

      try {
        Admin.Client client = getLocalAdminClient();
        ClearDBRequest req = new ClearDBRequest(getDbName());
        req.setReopen_db(false);
        client.clearDB(req);
      } catch (AdminException e) {
        LOG.error("Failed to destroy DB", e);
      } catch (TTransportException e) {
        LOG.error("Failed to connect to local Admin port", e);
      } catch (TException e) {
        LOG.error("ClearDB() request failed", e);
      }
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
    // DB name is in format: test00000
    private String getDbName() {
      String[] parts = partitionName.split("_");
      return String.format("%s%05d", parts[0], Integer.parseInt(parts[1]));
    }

    // partition name is in format: test_0
    // S3 part prefix is in format: part-00000-
    private String getS3PartPrefix() {
      String[] parts = partitionName.split("_");
      return String.format("part-%05d-", Integer.parseInt(parts[1]));
    }
  }
}

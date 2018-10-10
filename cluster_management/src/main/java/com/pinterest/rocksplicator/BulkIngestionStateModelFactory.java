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

public class BulkIngestionStateModelFactory extends StateModelFactory<StateModel> {
  private final String host;
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;

  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestionStateModelFactory.class);

  BulkIngestionStateModelFactory(String host, int adminPort, String zkConnectString, String cluster) {
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
    return new BulkIngestionStateModel(resourceName, partitionName, adminPort, cluster, zkClient, host);
  }

  public static class BulkIngestionStateModel extends OnlineOfflineStateModelFactory.OnlineOfflineStateModel {
    private static final Logger LOG = LoggerFactory.getLogger(BulkIngestionStateModel.class);

    private final String resourceName;
    private final String partitionName;
    private final int adminPort;
    private final String cluster;
    private final String host;

    /**
     * State model that handles the state machine of a single partition.
     */
    public BulkIngestionStateModel(String resourceName, String partitionName, int adminPort,
                                   String cluster, CuratorFramework zkClient, String host) {
      super(resourceName, partitionName, adminPort, cluster, zkClient);
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.host = host;
      this.adminPort = adminPort;
      this.cluster = cluster;
    }

    /**
     * Callback for OFFLINE to ONLINE transition.
     * This can happen when 1) a new partition is first launched on a host; or 2) an existing
     * partition is reopened when restart/zk temporarily expires.
     * We first make sure the DB is open and then load SST files into the DB.
     */
    @Override
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      super.checkSanity("OFFLINE", "ONLINE", message);
      Utils.logTransitionMessage(message);
      Utils.addDB(Utils.getDbName(partitionName), adminPort);

      try {
        String dbName = Utils.getDbName(partitionName);

        // open the DB if it's currently not opened yet
        Utils.addDB(dbName, adminPort);

        // see if the DB needs to be rebuilt. If there is anything in the meta,
        // assume that we have a complete local replica. We can ensure this
        // by locking the helix shard config during data uploads
        CheckDBResponse localStatus = Utils.checkLocalDB(dbName, adminPort);

        // check that the metadata is set and well formed, otherwise we
        // build a replica from another host serving.
        boolean needRebuild = !localStatus.isSetDb_meta_data();
        needRebuild = needRebuild && !localStatus.getDb_meta_data().isSetS3_bucket();
        needRebuild = needRebuild && !localStatus.getDb_meta_data().isSetS3_path();
        if (needRebuild) {
          HelixAdmin admin = context.getManager().getClusterManagmentTool();
          ExternalView view = admin.getResourceExternalView(cluster, resourceName);
          Map<String, String> stateMap = view.getStateMap(partitionName);
          // find live replicas

          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String role = instanceNameAndRole.getValue();
            if (!role.equalsIgnoreCase("ONLINE")) {
              continue;
            }

            String hostPort = instanceNameAndRole.getKey();
            String host = hostPort.split("_")[0];
            int port = Integer.parseInt(hostPort.split("_")[1]);

            if (this.host.equals(host)) {
              // myself
              continue;
            }

            // We've got an online host that has the partition. Copy the data
            String hdfsPath = "/rocksplicator/" + cluster + "/" + dbName + "/" + hostPort + "/"
                + String.valueOf(System.currentTimeMillis());

            // backup a snapshot from the upstream host, and restore it locally
            LOG.error("Backup " + dbName + " from " + hostPort);
            Utils.backupDB(host, port, dbName, hdfsPath);
            LOG.error("Restore " + dbName + " from " + hdfsPath);
            Utils.closeDB(dbName, adminPort);
            Utils.restoreLocalDB(adminPort, dbName, hdfsPath, host, port);
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to onlined resource " + partitionName, e);
        throw new RuntimeException(e);
      }
    }

  }
}

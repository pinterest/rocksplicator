/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksdb_admin.thrift.BackupDBRequest;
import com.pinterest.rocksdb_admin.thrift.BackupDBResponse;
import com.pinterest.rocksdb_admin.thrift.RestoreDBRequest;
import com.pinterest.rocksdb_admin.thrift.RestoreDBResponse;
import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import org.apache.thrift.TException;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

import java.util.List;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class AddHostTaskTest extends TaskBaseTest {

  private static final String INCOMPLETE_CONFIG =
      "{" +
          "  \"user_pins\": {" +
          "  \"num_shards\": 3," +
          "  \"127.0.0.1:8090:us-east-1a\": [\"00000:M\"]," +
          "  \"127.0.0.1:8091:us-east-1c\": [\"00000:M\", \"00001:M\", \"00002:S\"]," +
          "  \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]" +
          "   }" +
          "}";

  @BeforeMethod
  public void setup() throws Exception {
    super.setup();

    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER),
        INCOMPLETE_CONFIG.getBytes());
  }

  @Test
  public void testSuccessful() throws Exception {
    ArgumentCaptor<BackupDBRequest> backupDBCaptor = ArgumentCaptor.forClass(BackupDBRequest.class);
    when(client.backupDB(backupDBCaptor.capture())).thenReturn(new BackupDBResponse());
    ArgumentCaptor<RestoreDBRequest> restoreDBCaptor = ArgumentCaptor.forClass(RestoreDBRequest.class);
    when(client.restoreDB(restoreDBCaptor.capture())).thenReturn(new RestoreDBResponse());

    AddHostTask task = new AddHostTask("127.0.0.1", 8093, "us-east-1a", "/hdfs", 100);
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Successfully added host 127.0.0.1:8093");

    ClusterBean newConfig = ZKUtil.getClusterConfig(zkClient, CLUSTER);
    Assert.assertEquals(newConfig.getSegments().size(), 1);
    SegmentBean segment = newConfig.getSegments().get(0);
    Assert.assertEquals(segment.getHosts().size(), 4);
    HostBean newHost = segment.getHosts().stream().filter(host -> host.getPort() == 8093).findAny().get();
    Assert.assertEquals(newHost.getShards().size(), 2);
    Assert.assertEquals(newHost.getShards().stream().filter(shard -> shard.getId() == 1).count(), 1);
    Assert.assertEquals(newHost.getShards().stream().filter(shard -> shard.getId() == 2).count(), 1);

    List<BackupDBRequest> backupDBRequests = backupDBCaptor.getAllValues();
    Assert.assertEquals(backupDBRequests.size(), 2);
    Assert.assertEquals(backupDBRequests.get(0).getDb_name(), "user_pins00001");
    Assert.assertEquals(backupDBRequests.get(0).getLimit_mbs(), 100);
    Assert.assertTrue(backupDBRequests.get(0).getHdfs_backup_dir().startsWith(
        "/hdfs/devtest/user_pins/00001/127.0.0.1/"));

    List<RestoreDBRequest> restoreDBRequests = restoreDBCaptor.getAllValues();
    Assert.assertEquals(restoreDBRequests.size(), 2);
    Assert.assertEquals(restoreDBRequests.get(0).getDb_name(), "user_pins00001");
    Assert.assertEquals(restoreDBRequests.get(0).getHdfs_backup_dir(),
                        backupDBRequests.get(0).getHdfs_backup_dir());
    Assert.assertEquals(restoreDBRequests.get(0).getUpstream_ip(), "127.0.0.1");
    Assert.assertEquals(restoreDBRequests.get(0).getUpstream_port(), 8091);
    Assert.assertEquals(restoreDBRequests.get(0).getLimit_mbs(), 100);
  }

  @Test
  public void testFail() throws Exception {
    doThrow(new TException()).when(client).ping();

    AddHostTask task = new AddHostTask("127.0.0.1", 8093, "us-east-1a", "/hdfs", 100);
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Host to add is not alive!");
  }
}

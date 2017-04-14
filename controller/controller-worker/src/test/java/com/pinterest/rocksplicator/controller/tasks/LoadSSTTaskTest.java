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

import com.pinterest.rocksdb_admin.thrift.AddS3SstFilesToDBRequest;
import com.pinterest.rocksdb_admin.thrift.AddS3SstFilesToDBResponse;
import com.pinterest.rocksdb_admin.thrift.ClearDBRequest;
import com.pinterest.rocksdb_admin.thrift.ClearDBResponse;
import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.util.ShardUtil;

import org.apache.thrift.TException;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class LoadSSTTaskTest extends TaskBaseTest {

  @Test
  public void testSuccessful() throws Exception {
    ArgumentCaptor<ClearDBRequest> clearDBCaptor = ArgumentCaptor.forClass(ClearDBRequest.class);
    when(client.clearDB(clearDBCaptor.capture())).thenReturn(new ClearDBResponse());
    ArgumentCaptor<AddS3SstFilesToDBRequest> addS3SstCaptor =
        ArgumentCaptor.forClass(AddS3SstFilesToDBRequest.class);
    when(client.addS3SstFilesToDB(addS3SstCaptor.capture()))
        .thenReturn(new AddS3SstFilesToDBResponse());


    LoadSSTTask task = new LoadSSTTask("user_pins", "s3", "prefix", 2, 100);
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Finished loading sst to devtest");
    Assert.assertEquals(clearDBCaptor.getAllValues().size(), 9);
    Assert.assertEquals(addS3SstCaptor.getAllValues().size(), 9);
    for (AddS3SstFilesToDBRequest request : addS3SstCaptor.getAllValues()) {
      String dbName = request.getDb_name();
      Assert.assertTrue(dbName.startsWith("user_pins"));
      Assert.assertEquals(request.getS3_bucket(), "s3");
      int shardId = Integer.valueOf(dbName.substring("user_pins".length()));
      Assert.assertEquals(request.getS3_path(), ShardUtil.getS3Path("prefix", shardId));
      Assert.assertEquals(request.getS3_download_limit_mb(), 100);
    }
  }

  @Test
  public void testBadSegment() throws Exception {
    LoadSSTTask task = new LoadSSTTask("unknown", "s3", "prefix", 2, 100);
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Segment unknown not in cluster devtest.");
  }

  @Test
  public void testLoadSSTFailure() throws Exception {
    doThrow(new TException("Boom!")).when(client).addS3SstFilesToDB(any());

    LoadSSTTask task = new LoadSSTTask("user_pins", "s3", "prefix", 2, 100);
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123),
        "Failed to load sst, error=org.apache.thrift.TException: Boom!");
  }
}

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

import com.pinterest.rocksdb_admin.thrift.ChangeDBRoleAndUpstreamRequest;
import com.pinterest.rocksdb_admin.thrift.ChangeDBRoleAndUpstreamResponse;
import com.pinterest.rocksdb_admin.thrift.GetSequenceNumberResponse;
import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.config.ConfigParserTest;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class PromoteTaskTest extends TaskBaseTest {

  private static final String INCOMPLETE_CONFIG =
      "{" +
      "  \"user_pins\": {" +
      "    \"num_shards\": 3," +
      "    \"127.0.0.1:8090:us-east-1a\": [\"00000:S\", \"00001:S\", \"00002:S\"]," +
      "    \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"]," +
      "    \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]" +
      "   }" +
      "}";

  @Test
  public void testCompleteCluster() throws Exception {
    PromoteTask task = new PromoteTask();
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Nothing needs to promote");
    Assert.assertEquals(zkClient.getData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER)),
                        CONFIG.getBytes());
  }

  @Test
  public void testIncompleteCluster() throws Exception {
    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER),
        INCOMPLETE_CONFIG.getBytes());

    ArgumentCaptor<ChangeDBRoleAndUpstreamRequest> changeDBCaptor =
        ArgumentCaptor.forClass(ChangeDBRoleAndUpstreamRequest.class);
    when(client.changeDBRoleAndUpStream(changeDBCaptor.capture()))
        .thenReturn(new ChangeDBRoleAndUpstreamResponse());
    when(client.getSequenceNumber(any()))
        .thenReturn(new GetSequenceNumberResponse(3))
        .thenReturn(new GetSequenceNumberResponse(2))
        .thenReturn(new GetSequenceNumberResponse(1));

    PromoteTask task = new PromoteTask();
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Successfully promoted new masters");

    ChangeDBRoleAndUpstreamRequest request = changeDBCaptor.getValue();
    Assert.assertEquals(request.getDb_name(), "user_pins00000");
    Assert.assertEquals(request.getNew_role(), "MASTER");

    byte[] newConfigBytes = zkClient.getData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER));
    ConfigParserTest.assertConfigEquals(new String(newConfigBytes), CONFIG);

  }
}

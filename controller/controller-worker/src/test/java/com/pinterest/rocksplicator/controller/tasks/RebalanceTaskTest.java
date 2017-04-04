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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.pinterest.rocksdb_admin.thrift.ChangeDBRoleAndUpstreamRequest;
import com.pinterest.rocksdb_admin.thrift.ChangeDBRoleAndUpstreamResponse;
import com.pinterest.rocksdb_admin.thrift.GetSequenceNumberResponse;
import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.config.ConfigParserTest;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class RebalanceTaskTest extends TaskBaseTest {

  @DataProvider(name = "imbalanced_cluster")
  public Object[][] createClusterConfig() {
    return new Object[][] {
        {
            "{" +
            "  \"user_pins\": {" +
            "    \"num_shards\": 3," +
            "    \"127.0.0.1:8090:us-east-1a\": [\"00000:S\", \"00001:S\", \"00002:S\"]," +
            "    \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:S\", \"00002:S\"]," +
            "    \"127.0.0.1:8092:us-east-1e\": [\"00000:M\", \"00001:M\", \"00002:M\"]" +
            "   }" +
            "}"
        },
        {
            "{" +
            "  \"user_pins\": {" +
            "    \"num_shards\": 3," +
            "    \"127.0.0.1:8090:us-east-1a\": [\"00000:M\", \"00001:S\", \"00002:S\"]," +
            "    \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:S\", \"00002:S\"]," +
            "    \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:M\", \"00002:M\"]" +
            "   }" +
            "}"
        }
    };
  }

  @Test(dataProvider = "imbalanced_cluster")
  public void testImbalancedCluster(String clusterConfig) throws Exception {
    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER),
        clusterConfig.getBytes());

    ArgumentCaptor<ChangeDBRoleAndUpstreamRequest> changeDBCaptor =
        ArgumentCaptor.forClass(ChangeDBRoleAndUpstreamRequest.class);
    when(client.changeDBRoleAndUpStream(changeDBCaptor.capture()))
        .thenReturn(new ChangeDBRoleAndUpstreamResponse());
    when(client.getSequenceNumber(any()))
        .thenReturn(new GetSequenceNumberResponse(1024));

    RebalanceTask task = new RebalanceTask();
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Successfully rebalanced cluster devtest");

    byte[] newConfigBytes = zkClient.getData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER));
    ConfigParserTest.assertConfigEquals(new String(newConfigBytes), CONFIG);

  }
}

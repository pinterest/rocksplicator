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

import static org.mockito.Mockito.doThrow;

import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.bean.ClusterBean;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.bean.SegmentBean;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class RemoveHostTaskTest extends TaskBaseTest {

  @Test
  public void testSuccessful() throws Exception {
    doThrow(new TException()).when(client).ping();

    RemoveHostTask task = new RemoveHostTask(
        new HostBean().setIp("127.0.0.1").setPort(8090)
    );
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Successfully removed host 127.0.0.1:8090");

    ClusterBean newConfig = ZKUtil.getClusterConfig(zkClient, CLUSTER);
    Assert.assertEquals(newConfig.getSegments().size(), 1);
    SegmentBean segment = newConfig.getSegments().get(0);
    Assert.assertEquals(segment.getHosts().size(), 2);
    Assert.assertEquals(segment.getHosts().stream().filter(host -> host.getPort() == 8090).count(), 0);
  }

  @Test
  public void testHostStillAlive() throws Exception {
    RemoveHostTask task = new RemoveHostTask(
        new HostBean().setIp("127.0.0.1").setPort(8090)
    );
    injector.injectMembers(task);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    task.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Host is still alive!");
    Assert.assertEquals(zkClient.getData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER)),
        CONFIG.getBytes());
  }


}

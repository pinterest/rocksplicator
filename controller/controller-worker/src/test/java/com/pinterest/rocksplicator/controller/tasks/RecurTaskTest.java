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

import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Shu Zhang
 */

class DummyTask extends AbstractTask<Parameter> {

  public DummyTask() {
    this(new Parameter());
  }

  public DummyTask(Parameter param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    ctx.getTaskQueue().finishTask(ctx.getId(), "Successful");
  }

}

public class RecurTaskTest extends TaskBaseTest {

  @Test
  public void testOneOffSuccess() throws Exception {
    DummyTask successfulTask = new DummyTask();
    RecurTask recurTask = new RecurTask(new RecurTask.Param().setTask(
        successfulTask.getEntity()).setIntervalSeconds(0));
    injector.injectMembers(recurTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    recurTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123), "Successful");
    Assert.assertEquals(taskQueue.getBlockingQueue().size(), 0);
  }

  @Test
  public void testRecurSuccess() throws Exception {
    DummyTask successfulTask = new DummyTask();
    RecurTask recurTask = new RecurTask(new RecurTask.Param().setTask(
        successfulTask.getEntity()).setIntervalSeconds(10));
    injector.injectMembers(recurTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    recurTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123), "Successful");
    Assert.assertEquals(taskQueue.getBlockingQueue().size(), 1);
  }

  @Test
  public void testOneOffFailure() throws Exception {
    ThrowingTask throwingTask = new ThrowingTask("Failed");
    RecurTask recurTask = new RecurTask(new RecurTask.Param().setTask(
        throwingTask.getEntity()).setIntervalSeconds(0));
    injector.injectMembers(recurTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    recurTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123), "Failed");
    Assert.assertEquals(taskQueue.getBlockingQueue().size(), 0);
  }

  @Test
  public void testRecurFailure() throws Exception {
    ThrowingTask throwingTask = new ThrowingTask("Failed");
    RecurTask recurTask = new RecurTask(new RecurTask.Param().setTask(
        throwingTask.getEntity()).setIntervalSeconds(10));
    injector.injectMembers(recurTask);
    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    recurTask.process(ctx);
    Assert.assertEquals(taskQueue.getResult(123), "Failed");
    Assert.assertEquals(taskQueue.getBlockingQueue().size(), 1);
  }
}

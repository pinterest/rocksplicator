/*
 * Copyright 2017 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.rocksplicator.controller;

import com.pinterest.rocksplicator.controller.tasks.SleepIncrementTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskQueue.class)
public class DispatcherTest {

  private static Integer nameCounter = 0;
  private TaskQueue taskQueue;

  private Task getSleepIncrementTaskFromQueue() {
    Task task = new Task();
    task.name = "SleepIncrementTask";
    task.body = "{}";
    task.clusterName = nameCounter.toString();
    nameCounter += 1;
    return task;
  }

  @Before
  public void setup() {
    SleepIncrementTask.executionCounter.set(0);
    SleepIncrementTask.sleepTimeMillis = 1000;
    taskQueue = PowerMockito.mock(TaskQueue.class);
    // We will need to dequeue 5 times at most
    PowerMockito.when(taskQueue.dequeueTask(anyString()))
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(getSleepIncrementTaskFromQueue());
    PowerMockito.when(taskQueue.failTask(anyLong())).thenReturn(true);
  }

  @Test
  public void testSingleTaskLifeCycle() throws Exception {
    Semaphore idleWorkersSemaphore = new Semaphore(1);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore);
    TaskDispatcher dispatcher = new TaskDispatcher(2, idleWorkersSemaphore, workerPool, taskQueue);
    dispatcher.start();
    // Give it enough time for the initialization
    Thread.sleep(1500);
    verify(taskQueue, atLeastOnce()).dequeueTask(anyString());
    Assert.assertEquals(0, idleWorkersSemaphore.availablePermits());
    Thread.sleep(800);
    Assert.assertEquals(1, SleepIncrementTask.executionCounter.get());
    Assert.assertEquals(1, idleWorkersSemaphore.availablePermits());
    dispatcher.stop();
  }

  @Test
  public void testingMultiTasks() throws Exception {
    SleepIncrementTask.sleepTimeMillis = 3000;
    Semaphore idleWorkersSemaphore = new Semaphore(2);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(2, 2, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore);
    TaskDispatcher dispatcher = new TaskDispatcher(1, idleWorkersSemaphore, workerPool, taskQueue);
    dispatcher.start();
    // Give it enough time for the initialization;
    Thread.sleep(4900);
    // The test is sometimes flaky, 2 and 3 are both possible
    Assert.assertTrue(SleepIncrementTask.executionCounter.get() >= 2 &&
            SleepIncrementTask.executionCounter.get() <= 3);
    Assert.assertEquals(0, idleWorkersSemaphore.availablePermits());
  }

}

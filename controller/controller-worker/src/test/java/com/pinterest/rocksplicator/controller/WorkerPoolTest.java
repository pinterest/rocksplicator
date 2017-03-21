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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerPoolTest {
  static Integer nameCounter = 0;

  private TaskInternal getSleepIncrementTask() throws JsonProcessingException {
    TaskInternal task = new TaskInternal(
        new SleepIncrementTask(1000)
            .getBean()
    );
    task.clusterName = nameCounter.toString();
    nameCounter += 1;
    return task;
  }

  @Before
  public void setup() {
    SleepIncrementTask.executionCounter = 0;
  }

  @Test
  public void testAssignSingleTask() throws Exception {
    Semaphore idleWorkersSemaphore = new Semaphore(0);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, new TaskQueue());
    workerPool.assignTask(getSleepIncrementTask());
    Thread.sleep(2000);
    Assert.assertEquals(1, SleepIncrementTask.executionCounter.intValue());
    workerPool.assignTask(getSleepIncrementTask());
    Thread.sleep(2000);
    Assert.assertEquals(2, SleepIncrementTask.executionCounter.intValue());
    Assert.assertEquals(2, idleWorkersSemaphore.availablePermits());
  }

  @Test
  public void testAssignSameCluster() throws Exception {
    Semaphore idleWorkersSemaphore = new Semaphore(0);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, new TaskQueue());
    TaskInternal task = getSleepIncrementTask();
    workerPool.assignTask(task);
    Thread.sleep(2000);
    Assert.assertEquals(1, SleepIncrementTask.executionCounter.intValue());
    workerPool.assignTask(task);
    Thread.sleep(2000);
    Assert.assertEquals(2, (int)SleepIncrementTask.executionCounter);
    Assert.assertEquals(2, idleWorkersSemaphore.availablePermits());
  }

  @Test
  public void testAssignSameClusterConflict() throws Exception {
    Semaphore idleWorkersSemaphore = new Semaphore(0);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, new TaskQueue());
    TaskInternal task = getSleepIncrementTask();
    workerPool.assignTask(task);
    Thread.sleep(2000);
    Assert.assertEquals(1, SleepIncrementTask.executionCounter.intValue());
    workerPool.assignTask(task);
    Assert.assertEquals(1, idleWorkersSemaphore.availablePermits());
    Thread.sleep(100);
    Assert.assertEquals(1, SleepIncrementTask.executionCounter.intValue());
    Thread.sleep(1000);
  }

  @Test
  public void testCancelSingleTask() throws Exception {
    Semaphore idleWorkersSemaphore = new Semaphore(0);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, new TaskQueue());
    TaskInternal task = getSleepIncrementTask();
    workerPool.assignTask(task);
    Thread.sleep(10);
    workerPool.abortTask(task.clusterName);
    Thread.sleep(100);
    Assert.assertEquals(1, idleWorkersSemaphore.availablePermits());
  }

  @Test
  public void testAssignMultiTask() throws Exception {
    Semaphore idleWorkersSemaphore = new Semaphore(0);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, new TaskQueue());
    workerPool.assignTask(getSleepIncrementTask());
    workerPool.assignTask(getSleepIncrementTask());
    workerPool.assignTask(getSleepIncrementTask());
    Thread.sleep(1500);
    // Only expect 2 to finish because the pool size is 2
    Assert.assertEquals(2, SleepIncrementTask.executionCounter.intValue());
    Assert.assertEquals(2, idleWorkersSemaphore.availablePermits());
    Thread.sleep(1000);
  }

}

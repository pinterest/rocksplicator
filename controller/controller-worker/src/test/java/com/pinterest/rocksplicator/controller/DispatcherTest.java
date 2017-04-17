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
import com.pinterest.rocksplicator.controller.tasks.ThrowingTask;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@PrepareForTest(TaskQueue.class)
public class DispatcherTest {

  private static Integer nameCounter = 0;
  private int sleepTimeMillis;
  private TaskQueue taskQueue;

  private Task getSleepIncrementTaskFromQueue() throws JsonProcessingException {
    Task task = new Task(
        new SleepIncrementTask(sleepTimeMillis)
            .getEntity()
    );
    task.clusterName = nameCounter.toString();
    nameCounter += 1;
    return task;
  }


  @BeforeMethod
  public void setup() {
    SleepIncrementTask.executionCounter = 0;
    sleepTimeMillis = 1000;
    taskQueue = PowerMockito.mock(TaskQueue.class);
    PowerMockito.when(taskQueue.failTask(anyLong(), anyString())).thenReturn(true);
  }

  @Test
  public void testNoPendingTask() throws Exception {
    // Assuming there is no task in the queue in the test.
    PowerMockito.when(taskQueue.dequeueTask(anyString())).thenReturn(null);
    Semaphore idleWorkersSemaphore = new Semaphore(1);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, taskQueue);
    TaskDispatcher dispatcher = new TaskDispatcher(1, idleWorkersSemaphore, workerPool, taskQueue);
    dispatcher.start();
    Thread.sleep(1000);
    Assert.assertEquals(1, idleWorkersSemaphore.availablePermits());
    Thread.sleep(1000);
    Assert.assertEquals(1, idleWorkersSemaphore.availablePermits());
    dispatcher.stop();
  }

  @Test
  public void testSingleTaskLifeCycle() throws Exception {
    // Assuming there is only one task in the queue
    PowerMockito.when(taskQueue.dequeueTask(anyString()))
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(null);
    Semaphore idleWorkersSemaphore = new Semaphore(1);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, taskQueue);
    TaskDispatcher dispatcher = new TaskDispatcher(2, idleWorkersSemaphore, workerPool, taskQueue);
    dispatcher.start();
    // Wait for first task to be done
    synchronized (SleepIncrementTask.notifyObject) {
      SleepIncrementTask.notifyObject.wait();
    }
    verify(taskQueue, atLeastOnce()).dequeueTask(anyString());
    Assert.assertEquals(1, SleepIncrementTask.executionCounter.intValue());
    Assert.assertEquals(1, idleWorkersSemaphore.availablePermits());
    dispatcher.stop();
  }

  @Test
  public void testingMultiTasks() throws Exception {
    sleepTimeMillis = 3000;
    PowerMockito.when(taskQueue.dequeueTask(anyString()))
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(getSleepIncrementTaskFromQueue())
        .thenReturn(null);
    sleepTimeMillis = 3000;
    Semaphore idleWorkersSemaphore = new Semaphore(2);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(2, 2, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, taskQueue);
    TaskDispatcher dispatcher = new TaskDispatcher(2, idleWorkersSemaphore, workerPool, taskQueue);
    dispatcher.start();
    synchronized (SleepIncrementTask.notifyObject) {
      SleepIncrementTask.notifyObject.wait();
      SleepIncrementTask.notifyObject.wait();
    }
    Assert.assertTrue(SleepIncrementTask.executionCounter.intValue() <= 3);
    Assert.assertTrue(SleepIncrementTask.executionCounter.intValue() >= 2);
    dispatcher.stop();
  }

  @Test
  public void testChainedTask() throws Exception {
    TaskBase task = new SleepIncrementTask(100)
        .andThen(new SleepIncrementTask(150))
        .andThen(new SleepIncrementTask(200))
        .getEntity();

    final CountDownLatch latch = new CountDownLatch(3);
    FIFOTaskQueue tq = new FIFOTaskQueue(10) {
      @Override
      public boolean finishTask(final long id, final String output) {
        latch.countDown();
        return super.finishTask(id, output);
      }

      @Override
      public long finishTaskAndEnqueueRunningTask(final long id,
                                                  final String output,
                                                  final TaskBase newTask,
                                                  final String worker) {
        latch.countDown();
        return super.finishTaskAndEnqueueRunningTask(id, output, newTask, worker);
      }
    };
    tq.enqueueTask(task, Integer.toString(++nameCounter), 0);

    Semaphore idleWorkersSemaphore = new Semaphore(2);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(2, 2, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, tq);
    TaskDispatcher dispatcher = new TaskDispatcher(2, idleWorkersSemaphore, workerPool, tq);
    dispatcher.start();

    Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(SleepIncrementTask.executionCounter.intValue(), 3);

    Assert.assertEquals(tq.getResult(0), "0");
    Assert.assertEquals(tq.getResult(1), "1");
    Assert.assertEquals(tq.getResult(2), "2");
    dispatcher.stop();
  }

  @Test
  public void testChainedTaskWithError() throws Exception {
    TaskBase task = new SleepIncrementTask(100)
        .andThen(new ThrowingTask("Oops..."))
        .andThen(new SleepIncrementTask(150))
        .getEntity();

    final CountDownLatch latch = new CountDownLatch(2);
    FIFOTaskQueue tq = new FIFOTaskQueue(10) {
      @Override
      public boolean finishTask(final long id, final String output) {
        latch.countDown();
        return super.finishTask(id, output);
      }

      @Override
      public boolean failTask(final long id, final String reason) {
        latch.countDown();
        return super.failTask(id, reason);
      }

      @Override
      public long finishTaskAndEnqueueRunningTask(final long id,
                                                  final String output,
                                                  final TaskBase newTask,
                                                  final String worker) {
        latch.countDown();
        return super.finishTaskAndEnqueueRunningTask(id, output, newTask, worker);
      }
    };
    tq.enqueueTask(task, Integer.toString(++nameCounter), 0);

    Semaphore idleWorkersSemaphore = new Semaphore(2);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(2, 2, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, tq);
    TaskDispatcher dispatcher = new TaskDispatcher(2, idleWorkersSemaphore, workerPool, tq);
    dispatcher.start();

    Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(SleepIncrementTask.executionCounter.intValue(), 1);

    Assert.assertEquals(tq.getResult(0), "0");
    Assert.assertEquals(tq.getResult(1), "Oops...");
    dispatcher.stop();
  }



  @Test
  public void testRetryTask() throws Exception {
    final String errorMsg = "Boom!!!";
    TaskBase task = new ThrowingTask(errorMsg).retry(3).getEntity();
    final CountDownLatch latch = new CountDownLatch(3);
    FIFOTaskQueue tq = new FIFOTaskQueue(10) {
      @Override
      public boolean finishTask(final long id, final String output) {
        latch.countDown();
        return super.finishTask(id, output);
      }

      @Override
      public long finishTaskAndEnqueueRunningTask(final long id,
                                                  final String output,
                                                  final TaskBase newTask,
                                                  final String worker) {
        latch.countDown();
        return super.finishTaskAndEnqueueRunningTask(id, output, newTask, worker);
      }
    };
    tq.enqueueTask(task, Integer.toString(++nameCounter), 0);

    Semaphore idleWorkersSemaphore = new Semaphore(2);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(2, 2, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2));
    WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore, tq);
    TaskDispatcher dispatcher = new TaskDispatcher(2, idleWorkersSemaphore, workerPool, tq);
    dispatcher.start();

    Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(tq.getResult(0), errorMsg);
    Assert.assertEquals(tq.getResult(1), errorMsg);
    Assert.assertEquals(tq.getResult(2), errorMsg);
    dispatcher.stop();
  }
}

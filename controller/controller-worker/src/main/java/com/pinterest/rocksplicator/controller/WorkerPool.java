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

import com.pinterest.rocksplicator.controller.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * Thread pool executor service for executing Rocksplicator tasks.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public class WorkerPool extends ThreadPoolExecutor {

  // TODO: graceful shutdown.
  private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);
  private volatile Semaphore idleWorkersSemaphore;
  private volatile ConcurrentHashMap<String, FutureTask> runningTasks;

  public WorkerPool(int workerPoolSize, Semaphore idleWorkersSemaphore) {
    super(workerPoolSize, workerPoolSize, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
    this.idleWorkersSemaphore = idleWorkersSemaphore;
  }

  private class TaskWrapper extends FutureTask {
    private Task task;

    public TaskWrapper(Task task) {
      super(task);
      this.task = task;
    }

    public Task getTask() {
      return task;
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    TaskWrapper taskWrapper = (TaskWrapper) r;
    runningTasks.put(taskWrapper.getTask().getCluster(), taskWrapper);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    TaskWrapper taskWrapper = (TaskWrapper) r;
    runningTasks.remove(taskWrapper.getTask().getCluster());
    idleWorkersSemaphore.release();
    // TODO: Write the TaskExecutionResponse back to MySQL
  }

  /**
   * Assign a task to a worker thread.
   * @param task the task to execute
   * @throws Exception if there is a running task for the cluster.
   */
  public void assignTask(Task task) throws Exception {
    if (runningTasks.containsKey(task.getCluster())) {
      throw new Exception("Cannot execute more than 1 task for a cluster");
    }
    TaskWrapper taskWrapper = new TaskWrapper(task);
    submit(taskWrapper);
  }

  /**
   * Abort the running task on a cluster.
   * @param cluster the name of the cluster
   * @return
   */
  public boolean abortTask(String cluster) throws Exception {
    FutureTask runningTask = runningTasks.get(cluster);
    if (runningTask.isCancelled()) {
      throw new Exception("Task is already cancelled");
    }
    // An Interrupted exception will be thrown to Task, and onFailure() will be triggered.
    return runningTask.cancel(true);
  }

  /**
   * Check if a task is running for a cluster
   * @param cluster
   * @return
   */
  public boolean hasRunningTask(String cluster) {
    return runningTasks.containsKey(cluster);
  }


}

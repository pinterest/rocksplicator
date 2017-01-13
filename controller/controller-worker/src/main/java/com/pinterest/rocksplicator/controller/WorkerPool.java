/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.pinterest.rocksplicator.controller.tasks.TaskExecutionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
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
  private static volatile WorkerPool instance = null;
  private volatile ConcurrentHashMap<String, FutureTask<TaskExecutionResponse>> runningTasks;

  private WorkerPool(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                     BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  private class TaskWrapper extends FutureTask<TaskExecutionResponse> {
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
    // TODO: Write the TaskExecutionResponse back to MySQL
  }

  public static WorkerPool getInstance() {
    if (instance == null) {
      synchronized (TaskDispatcher.class) {
        if (instance == null) {
          int workerPoolSize = WorkerConfig.getWorkerPoolSize();
          instance = new WorkerPool(workerPoolSize, workerPoolSize,
              0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(workerPoolSize));
          return instance;
        }
      }
    }
    return instance;
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
    FutureTask<TaskExecutionResponse> runningTask = runningTasks.get(cluster);
    if (runningTask.isCancelled()) {
      throw new Exception("Task is already cancelled");
    }
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

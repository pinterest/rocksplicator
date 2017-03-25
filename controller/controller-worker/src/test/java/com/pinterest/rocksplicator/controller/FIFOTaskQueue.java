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

package com.pinterest.rocksplicator.controller;

import java.sql.Timestamp;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A FIFO task queue for testing purpose
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class FIFOTaskQueue implements TaskQueue {

  private final AtomicLong currentId = new AtomicLong(0);
  private final BlockingQueue<Task> taskQueue;
  private final ConcurrentMap<Long, String> result = new ConcurrentHashMap<>();

  public FIFOTaskQueue(int capacity) {
    this.taskQueue = new ArrayBlockingQueue<>(capacity);
  }

  public String getResult(long id) {
    return result.get(id);
  }

  @Override
  public boolean enqueueTask(final TaskEntity task,
                      final String clusterName,
                      final int runDelaySeconds) {

    Task taskInternal = new Task(task);
    taskInternal.id = currentId.getAndIncrement();
    taskInternal.clusterName = clusterName;
    taskInternal.runAfter = new Timestamp(System.currentTimeMillis() + runDelaySeconds * 1000);
    taskQueue.offer(taskInternal);
    return true;
  }

  @Override
  public Task dequeueTask(final String worker) {
    if (taskQueue.peek() == null) {
      return null;
    }

    Task taskInternal = taskQueue.poll();
    if (taskInternal != null) {
      taskInternal.claimedWorker = worker;
    }
    return taskInternal;
  }

  @Override
  public boolean finishTask(final long id, final String output) {
    return result.putIfAbsent(id, output) == null;
  }

  @Override
  public boolean failTask(final long id, final String reason) {
    return result.putIfAbsent(id, reason) == null;
  }

  @Override
  public long finishTaskAndEnqueueRunningTask(final long id,
                                              final String output,
                                              final TaskEntity newTask,
                                              final String worker) {
    result.putIfAbsent(id, output);
    return currentId.getAndIncrement();
  }

}

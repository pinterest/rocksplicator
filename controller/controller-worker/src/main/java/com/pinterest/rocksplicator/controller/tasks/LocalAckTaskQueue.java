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

import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskBase;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.util.Result;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TaskQueue} wrapper which saves the task execution result locally instead of in mysql
 * backend. It enables task-chaining by allowing the next task in the chain to get the result of
 * previous task and do ack-and-enqueue operation atomically.
 */
class LocalAckTaskQueue implements TaskQueue {

  /** the actual TaskQueue being wrapped */
  private final TaskQueue taskQueue;
  /** an atomic reference to store the task execution result locally */
  private final AtomicReference<State> stateRef;

  public static class State {

    public enum StateName {
      UNFINISHED,
      DONE,
      FAILED
    }

    public final StateName state;
    public final String output;

    State(StateName state, String output) {
      this.state = state;
      this.output = output;
    }
  }

  LocalAckTaskQueue(TaskQueue taskQueue) {
    this.taskQueue = taskQueue;
    this.stateRef = new AtomicReference<>(new State(State.StateName.UNFINISHED, ""));
  }

  public State getState() {
    return stateRef.get();
  }

  @Override
  public boolean finishTask(final long id, final String output) {
    State s;
    do {
      s = stateRef.get();
      if (s.state != State.StateName.UNFINISHED) {
        return false;
      }
    } while (!stateRef.compareAndSet(s, new State(State.StateName.DONE, output)));

    return true;
  }

  @Override
  public boolean failTask(final long id, final String reason) {
    State s;
    do {
      s = stateRef.get();
      if (s.state != State.StateName.UNFINISHED) {
        return false;
      }
    } while (!stateRef.compareAndSet(s, new State(State.StateName.FAILED, reason)));

    return true;
  }

  /** methods below simply delegates calls to {@code taskQueue} **/
  @Override
  public Result createCluster(final String clusterName) {
    return taskQueue.createCluster(clusterName);
  }

  @Override
  public boolean enqueueTask(final TaskBase task,
                             final String clusterName,
                             final int runDelaySeconds) {
    return taskQueue.enqueueTask(task, clusterName, runDelaySeconds);
  }

  @Override
  public Task dequeueTask(final String worker) {
    return taskQueue.dequeueTask(worker);
  }

  @Override
  public long finishTaskAndEnqueueRunningTask(final long id,
                                              final String output,
                                              final TaskBase newTask,
                                              final String worker) {
    return taskQueue.finishTaskAndEnqueueRunningTask(id, output, newTask, worker);
  }

  @Override
  public boolean finishTaskAndEnqueuePendingTask(final long id,
                                                 final String output,
                                                 final TaskBase newTask,
                                                 final int runDelaySeconds) {
    return taskQueue.finishTaskAndEnqueuePendingTask(id, output, newTask, runDelaySeconds);
  }

  @Override
  public Result<Boolean> lockCluster(final String cluster) {
    return taskQueue.lockCluster(cluster);
  }

  @Override
  public Result<Boolean> unlockCluster(final String cluster) {
    return taskQueue.unlockCluster(cluster);
  }

  @Override
  public boolean removeCluster(final String cluster) {
    return taskQueue.removeCluster(cluster);
  }

  @Override
  public int removeFinishedTasks(final int secondsAgo) {
    return taskQueue.removeFinishedTasks(secondsAgo);
  }

  @Override
  public int resetZombieTasks(final int zombieThresholdSeconds) {
    return taskQueue.resetZombieTasks(zombieThresholdSeconds);
  }

  @Override
  public boolean keepTaskAlive(final long id) {
    return taskQueue.keepTaskAlive(id);
  }

  @Override
  public Result<List<Task>> peekTasks(final String clusterName,
                                      final Integer state) {
    return taskQueue.peekTasks(clusterName, state);
  }

  @Override
  public Result<Task> findTask(long id) {
    return taskQueue.findTask(id);
  }

  @Override
  public Result<Set<String>> getAllClusters() {
    return taskQueue.getAllClusters();
  }
}

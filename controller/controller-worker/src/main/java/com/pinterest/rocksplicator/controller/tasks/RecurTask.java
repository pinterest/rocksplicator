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

import com.pinterest.rocksplicator.controller.TaskBase;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.util.EmailSender;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * The task is used for tasks that happens every period of time.
 * It also cleans up the finished recurring tasks order than 1 hour.
 * (TODO: make the rolling window configurable)
 *
 * @author Shu Zhang (shu@pinterest.com)
 */
public class RecurTask extends AbstractTask<RecurTask.Param>{

  private static final Logger LOG = LoggerFactory.getLogger(RecurTask.class);
  private static final int RECUR_TASKS_STORAGE_ROLLING_WINDOW = 3600;

  @Inject
  private EmailSender emailSender;

  public RecurTask(Param param) { super(param); }

  private abstract class TaskAcker {

    TaskQueue taskQueue;
    TaskAcker(TaskQueue taskQueue) { this.taskQueue = taskQueue; }

    abstract boolean ackTaskAndEnqueuePendingTask(final long id, final String output,
                                                  final TaskBase taskBase,
                                                  final int runDelaySeconds);
    abstract boolean ackTask(final long id, final String output);
  }

  private class SuccessAcker extends TaskAcker {

    SuccessAcker(TaskQueue taskQueue) {
      super(taskQueue);
    }

    @Override
    public boolean ackTaskAndEnqueuePendingTask(
        long id, String output, TaskBase taskBase, int runDelaySeconds) {
      return taskQueue.finishTaskAndEnqueuePendingTask(id, output, taskBase, runDelaySeconds);
    }

    @Override
    public boolean ackTask(long id, String output) {
      return taskQueue.finishTask(id, output);
    }
  }

  private class FailureAcker extends TaskAcker {

    FailureAcker(TaskQueue taskQueue) {
      super(taskQueue);
    }

    @Override
    public boolean ackTaskAndEnqueuePendingTask(
        long id, String output, TaskBase taskBase, int runDelaySeconds) {
      return taskQueue.failTaskAndEnqueuePendingTask(id, output, taskBase, runDelaySeconds);
    }

    @Override
    public boolean ackTask(long id, String output) {
      return taskQueue.failTask(id, output);
    }
  }

  private void ackTask(TaskAcker taskAcker, Context ctx, String output) throws Exception {
    String message;
    if (getParameter().getIntervalSeconds() > 0) {
      if (!taskAcker.ackTaskAndEnqueuePendingTask(
          ctx.getId(), output, this.getEntity(), getParameter().getIntervalSeconds())) {
        message = "Cannot finish and enqueue pending task for " +
            ctx.getCluster() + ": " + this.getEntity().toString();
        LOG.error(message);
        emailSender.sendEmail("Task re-enqueue failure for " + ctx.getCluster(), message);
      }
    } else {
      if (!taskAcker.ackTask(ctx.getId(), output)) {
        message = "Cannot finish task for " + ctx.getCluster() +
            " : " + this.getEntity().toString();
        LOG.error(message);
        emailSender.sendEmail("Task ack failure for " + ctx.getCluster(), message);
      }
    }
  }

  @Override
  public void process(Context ctx) throws Exception {
    long id = ctx.getId();
    final TaskQueue remoteTaskQueue = ctx.getTaskQueue();

    AbstractTask task = TaskFactory.getWorkerTask(getParameter().getTask());
    Throwable t = null;
    LocalAckTaskQueue localAckTaskQueue = new LocalAckTaskQueue(remoteTaskQueue);
    try {
      ctx = new Context(id, ctx.getCluster(), localAckTaskQueue, ctx.getWorker());
      task.process(ctx);
      // in case the current body is changed, and we need to carry the state
      // to next enqueued job
      getParameter().getTask().setBody(task.getParameter().serialize());
    } catch (Throwable th) {
      t = th;
    }

    LocalAckTaskQueue.State state = localAckTaskQueue.getState();
    if (t != null || state.state == LocalAckTaskQueue.State.StateName.FAILED) {
      ackTask(new FailureAcker(remoteTaskQueue), ctx, t == null ? state.output : t.getMessage());
    } else if (state.state == LocalAckTaskQueue.State.StateName.DONE) {
      ackTask(new SuccessAcker(remoteTaskQueue), ctx, state.output);
    }
    // delete old tasks
    remoteTaskQueue.removeFinishedTasks(RECUR_TASKS_STORAGE_ROLLING_WINDOW,
        ctx.getCluster().getNamespace(), ctx.getCluster().getName(), RecurTask.class.getName());
  }

  @Override
  public AbstractTask retry(int maxRetry) throws Exception {
    throw new UnsupportedOperationException("We can't retry a recurring task");
  }

  public static class Param extends Parameter {
    @JsonProperty
    private TaskBase task;

    @JsonProperty
    private int intervalSeconds = 0; // If set < 1, the task will only be executed once.

    public TaskBase getTask() {
      return task;
    }

    public RecurTask.Param setTask(TaskBase task) {
      this.task = task;
      return this;
    }

    public RecurTask.Param setIntervalSeconds(int intervalSeconds) {
      this.intervalSeconds = intervalSeconds;
      return this;
    }

    public int getIntervalSeconds() {
      return intervalSeconds;
    }
  }
}
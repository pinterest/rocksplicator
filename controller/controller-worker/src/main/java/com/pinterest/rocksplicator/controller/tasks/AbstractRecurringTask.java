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

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksplicator.controller.TaskBase;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.util.EmailSender;
import org.slf4j.Logger;

import javax.inject.Inject;

/**
 * Tasks which are optional to recurring or one-off can inherit this class.
 * The subclasses should throw exception directly instead of fail the task themselves and
 * let AbstractRecurringTask to take over the task acknowledgement.
 *
 * It's user's option to use this class or not. If user wants to handle recurring logic themselves,
 * he can directly inherit from AbstractTask too.
 */
public abstract class AbstractRecurringTask<PARAM extends Parameter> extends AbstractTask<PARAM> {

  public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(AbstractRecurringTask.class);

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

  /**
   * If not given or smaller than 1, the task is one-off.
   */
  private int intervalSeconds;

  @Inject
  private EmailSender emailSender;

  public AbstractRecurringTask(PARAM parameter) {
    this(parameter, 0);
  }

  public AbstractRecurringTask(PARAM parameter, int intervalSeconds) {
    super(parameter);
    this.intervalSeconds = intervalSeconds;
  }

  private void ackTask(TaskAcker taskAcker, Context ctx, String output) throws Exception {
    String message;
    if (intervalSeconds > 0) {
      if (!taskAcker.ackTaskAndEnqueuePendingTask(
          ctx.getId(), output, this.getEntity(), intervalSeconds)) {
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
  public final void process(Context ctx) throws Exception {
    try {
      innerProcess(ctx);
      SuccessAcker successAcker = new SuccessAcker(ctx.getTaskQueue());
      ackTask(successAcker, ctx, ctx.getOutput());
    } catch (Exception e) {
      FailureAcker failureAcker = new FailureAcker(ctx.getTaskQueue());
      ackTask(failureAcker, ctx, e.getMessage());
    }
  }

  /**
   * Subclasses should always implement this method, and handle over task acks to process()
   * framework.
   * @param ctx
   * @throws Exception
   */
  public abstract void innerProcess(Context ctx) throws Exception;
}

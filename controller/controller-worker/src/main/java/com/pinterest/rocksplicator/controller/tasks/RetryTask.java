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
import com.pinterest.rocksplicator.controller.TaskQueue;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
final class RetryTask extends TaskBase<RetryTask.Param> {
  private static final Logger LOG = LoggerFactory.getLogger(RetryTask.class);

  public RetryTask(Param param) {
    super(param);
  }

  @Override
  public int getPriority() {
    return RESERVED_PRIORITY;
  }

  @Override
  public void process(Context ctx) throws Exception {
    long id = ctx.getId();
    final TaskQueue taskQueue = ctx.getTaskQueue();

    TaskBase task = TaskFactory.getWorkerTask(getParameter().getTask());
    int retry = 0;
    while (retry < getParameter().getMaxRetry()) {
      Throwable t = null;
      DelayAckTaskQueue dq = new DelayAckTaskQueue(taskQueue);
      try {
        ctx = new Context(id, ctx.getCluster(), dq, ctx.getWorker());
        task.process(ctx);
      } catch (Throwable th) {
        t = th;
      }

      DelayAckTaskQueue.State state = dq.getState();
      if (t != null || state.state == DelayAckTaskQueue.State.FAILED) {
        String output = (t == null ? state.output : t.getMessage());
        long nextId = taskQueue.finishTaskAndEnqueueRunningTask(id, output, getParameter().getTask(), ctx.getWorker());
        if (nextId < 0) {
          LOG.error("Failed to ack task {} and enqueue retry task.", id);
          return;
        }
        id = nextId;
        retry ++;
      } else if (state.state == DelayAckTaskQueue.State.DONE) {
        if (!taskQueue.finishTask(id, state.output)) {
          LOG.error("Failed to finish task {} with result {}", id, state.output);
        }
        return;
      } else {
        LOG.error("Task {} finished processing without acking result", id);
        return;
      }
    }
    LOG.error("Task {} reached maximum retry {}", getParameter().getTask(), getParameter().getMaxRetry());
  }

  public static class Param extends Parameter {
    @JsonProperty
    private Task task;

    @JsonProperty
    private int maxRetry;

    public Task getTask() {
      return task;
    }

    public RetryTask.Param setTask(Task task) {
      this.task = task;
      return this;
    }

    public int getMaxRetry() {
      return maxRetry;
    }

    public RetryTask.Param setMaxRetry(int maxRetry) {
      this.maxRetry = maxRetry;
      return this;
    }
  }
}

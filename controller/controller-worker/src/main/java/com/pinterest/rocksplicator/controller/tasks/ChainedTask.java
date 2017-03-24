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

import java.util.Stack;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
final class ChainedTask extends TaskBase<ChainedTask.Param> {

  private static final Logger LOG = LoggerFactory.getLogger(ChainedTask.class);

  public ChainedTask(Param param) {
    super(param);
  }

  @Override
  public int getPriority() {
    return RESERVED_PRIORITY;
  }

  @Override
  public void process(Context ctx) throws Exception {
    long id = ctx.getId();
    final String cluster = ctx.getCluster();
    final String worker = ctx.getWorker();
    final TaskQueue taskQueue = ctx.getTaskQueue();

    Stack<Task> tasks = new Stack<>();
    tasks.push(getParameter().getT2());
    tasks.push(getParameter().getT1());

    while (!tasks.isEmpty()) {
      TaskBase task = TaskFactory.getWorkerTask(tasks.pop());
      if (task instanceof ChainedTask) {
        ChainedTask chainedTask = (ChainedTask)task;
        tasks.push(chainedTask.getParameter().getT2());
        tasks.push(chainedTask.getParameter().getT1());
      } else {
        DelayAckTaskQueue dq = new DelayAckTaskQueue(taskQueue);
        ctx = new Context(id, cluster, dq, worker);
        task.process(ctx);

        DelayAckTaskQueue.State state = dq.getState();
        if (state.state == DelayAckTaskQueue.State.UNFINISHED) {
          LOG.error("Task {} finished processing without ack", id);
          return;
        } else if (state.state == DelayAckTaskQueue.State.FAILED) {
          LOG.error("Task {} failed with reason: {}. Abort the task chain.", id, state.output);
          return;
        } else if (tasks.isEmpty()) {
          LOG.info("Finished processing chained task");
          taskQueue.finishTask(id, state.output);
          return;
        }

        long nextId = taskQueue.finishTaskAndEnqueueRunningTask(id, state.output, tasks.peek(), worker);
        if (nextId < 0) {
          LOG.error("Failed to finish task {} and enqueue new task {}", id, tasks.peek());
          return;
        } else {
          id = nextId;
        }
      }
    }

  }

  public static class Param extends Parameter {
    @JsonProperty
    private Task t1; // first task to execute

    @JsonProperty
    private Task t2; // second task to execute

    public Task getT1() {
      return t1;
    }

    public Param setT1(Task t1) {
      this.t1 = t1;
      return this;
    }

    public Task getT2() {
      return t2;
    }

    public Param setT2(Task t2) {
      this.t2 = t2;
      return this;
    }
  }
}

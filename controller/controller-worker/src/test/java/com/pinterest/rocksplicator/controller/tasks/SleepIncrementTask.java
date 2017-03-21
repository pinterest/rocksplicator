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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The task sleeps X seconds and increment an Atomic Integer.
 * Its purpose is for integration tests.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public final class SleepIncrementTask extends TaskBase<SleepIncrementTask.Param> {

  public static Integer executionCounter = 0;
  public static Object notifyObject = new Object();

  public SleepIncrementTask(int sleepTimeMillis) {
    this(new Param().setSleepTimeMillis(sleepTimeMillis));
  }

  public SleepIncrementTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    Thread.sleep(getParameter().getSleepTimeMillis());
    int result;
    synchronized (notifyObject) {
      SleepIncrementTask.notifyObject.notify();
      result = executionCounter++;
    }
    ctx.getTaskQueue().finishTask(ctx.getId(), Integer.toString(result));
  }

  public static class Param extends Parameter {

    @JsonProperty
    private int sleepTimeMillis;

    public int getSleepTimeMillis() {
      return sleepTimeMillis;
    }

    public Param setSleepTimeMillis(int sleepTimeMillis) {
      this.sleepTimeMillis = sleepTimeMillis;
      return this;
    }
  }

}

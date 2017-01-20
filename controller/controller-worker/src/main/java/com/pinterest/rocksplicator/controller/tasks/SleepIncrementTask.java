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

import org.codehaus.jackson.JsonNode;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The task sleeps X seconds and increment an Atomic Integer.
 * Its purpose is for integration tests.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public final class SleepIncrementTask extends TaskBase<Integer> {

  public static Integer executionCounter = 0;
  public static Object notifyObject = new Object();
  // Can be adjusted in unit tests.
  public static int sleepTimeMillis = 1000;

  public SleepIncrementTask(long id, String cluster, JsonNode taskBody) {
    super(id, cluster, taskBody);
  }

  @Override
  public void preProcess() {}

  @Override
  public void postProcess(Integer response) {}

  @Override
  public Integer process() throws Exception{
    Thread.sleep(sleepTimeMillis);
    synchronized (notifyObject) {
      SleepIncrementTask.notifyObject.notify();
      return executionCounter ++;
    }
  }

  @Override
  public Integer onFailure() {
    return null;
  }
}

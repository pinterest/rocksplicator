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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * Task interface.
 *
 * @author Shu Zhang (shu@pinterest.com)
 */
public abstract class TaskBase<T> implements Callable<T> {

  public enum State {
    RUNNING,
    DONE,
    FAILED
  }

  private static final Logger LOG = LoggerFactory.getLogger(TaskBase.class);
  protected final JsonNode taskBody;
  protected final String cluster;
  protected final long id;
  protected State state;

  public TaskBase(long id, String cluster, JsonNode taskBody) {
    this.id = id;
    this.taskBody = taskBody;
    this.cluster = cluster;
    this.state = State.RUNNING;
  }

  public String getCluster() {
    return this.cluster;
  }

  public State getState() {
    return this.state;
  }

  public long getId() { return this.id; }

  /**
   * A hook method for doing works before process() is called.
   */
  public abstract void preProcess();

  /**
   * A hook method for doing works after process() is called.
   */
  public abstract void postProcess(T response);

  /**
   * Subclasses implement this method for task logic.
   * @return task response
   */
  public abstract T process() throws Exception;

  /**
   * Subclasses implement this method for task failure handling.
   * @return task response when it is failed
   */
  public abstract T onFailure();


  @Override
  public T call() {
    Date date = new Date();
    long start = date.getTime();
    T response = null;
    preProcess();
    try {
      response = process();
      this.state = State.DONE;
      return response;
    } catch (Exception e) {
      LOG.warn("Task " + this.getClass().getName() + " Failed!", e);
      response = onFailure();
      this.state = State.FAILED;
      return response;
    } finally {
      Date endDate = new Date();
      long duration = endDate.getTime() - start;
      postProcess(response);
    }
  }
}

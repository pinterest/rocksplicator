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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * Periodically querying MySQL for claiming tasks, checking abort signals, etc.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public class TaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);
  private static volatile TaskDispatcher instance = null;
  private boolean isRunning = false;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private TaskDispatcher() {}

  public static TaskDispatcher getInstance() {
    if (instance == null) {
      synchronized (TaskDispatcher.class) {
        if (instance == null) {
          instance = new TaskDispatcher();
          return instance;
        }
      }
    }
    return instance;
  }

  /**
   * Start to dispatch tasks.
   */
  public boolean start(long dispatcherPollInterval) {
    synchronized (TaskDispatcher.class) {
      if (isRunning) {
        LOG.error("Dispatcher is already running, cannot start.");
        return false;
      }
      final Runnable dispatcher = new Runnable() {
        public void run() {
          // TODO: Add task dispatch logic to assign tasks to worker pool
        }
      };
      scheduler.scheduleAtFixedRate(dispatcher, 0, dispatcherPollInterval, TimeUnit.SECONDS);
      isRunning = true;
      return true;
    }
  }

  /**
   * Stop to dispatch tasks.
   */
  public synchronized boolean stop() {
    synchronized (TaskDispatcher.class) {
      if (!isRunning) {
        LOG.error("Dispatcher is not running, cannot stop.");
        return false;
      }
      scheduler.shutdown();
      isRunning = false;
      return true;
    }
  }
}

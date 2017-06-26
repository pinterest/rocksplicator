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

package com.pinterest.rocksplicator.controller.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class ShutdownHook implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ShutdownHook.class);

  private final Deque<Runnable> shutdownables = new ArrayDeque<>();

  /**
   * Register a new runnable to the ShutdownHook. The runnables will
   * be executed in the reverse order as they were registered.
   * @param r
   */
  public synchronized void register(Runnable r) {
    shutdownables.offerLast(r);
  }

  @Override
  public synchronized void run() {
    while (!shutdownables.isEmpty()) {
      Runnable r = shutdownables.pollLast();
      try {
        LOG.info("Running shutdown hook: ", r.toString());
        r.run();
      } catch (RuntimeException e) {
        LOG.error("Failed to execute shutdown hook " + r.toString(), e);
        // continue
      }
    }
  }
}

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


package com.pinterest.rocksplicator.controller;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerConfig {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);
  private static final String WORKER_POOL_SIZE_KEY = "worker_pool_size";
  private static final String DISPATCHER_POLL_INTERVAL_KEY = "dispatcher_poll_interval";
  private static PropertiesConfiguration configuration;

  public static void initialize() throws ConfigurationException {
    String workerConfig = System.getProperty("worker_config");
    configuration = new PropertiesConfiguration();
    configuration.load(ClassLoader.getSystemResourceAsStream(workerConfig));
    LOG.info("Worker config loaded: " + workerConfig);
  }

  public static long getDispatcherPollInterval() {
    return configuration == null? 60 : configuration.getLong(DISPATCHER_POLL_INTERVAL_KEY);
  }

  public static int getWorkerPoolSize() {
    return configuration == null? 10 : configuration.getInt(WORKER_POOL_SIZE_KEY);
  }

}

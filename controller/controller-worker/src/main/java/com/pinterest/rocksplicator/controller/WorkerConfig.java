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

import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Contains all configuration about the worker.
 *
 * @author Shu Zhang (shu@pinterest.com)
 */
public final class WorkerConfig {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);
  private static final String WORKER_POOL_SIZE_KEY = "worker_pool_size";
  private static final String DISPATCHER_POLL_INTERVAL_KEY = "dispatcher_poll_interval";
  private static final String ZK_PATH_KEY = "zk_path";
  private static String HOST_NAME;
  private static PropertiesConfiguration configuration;

  private static final String DEFAULT_ZK_PATH = "/config/services/rocksdb/";

  static {
    String workerConfig = System.getProperty("worker_config", "controller.worker.properties");
    configuration = new PropertiesConfiguration();
    try {
      configuration.load(ClassLoader.getSystemResourceAsStream(workerConfig));
    } catch (Exception e) {
      LOG.error("Cannot load worker configuration", e);
      configuration = null;
    }
    LOG.info("Worker config loaded: " + workerConfig);
    try {
      HOST_NAME = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      HOST_NAME = "UnKnown";
    }
  }

  public static long getDispatcherPollIntervalSec() {
    return configuration == null ? 60 : configuration.getLong(DISPATCHER_POLL_INTERVAL_KEY);
  }

  public static int getWorkerPoolSize() {
    return configuration == null ? 10 : configuration.getInt(WORKER_POOL_SIZE_KEY);
  }

  public static String getHostName() {
    return HOST_NAME;
  }

  public static String getZKPath() {
    return configuration == null ? DEFAULT_ZK_PATH :
                                   configuration.getString(ZK_PATH_KEY, DEFAULT_ZK_PATH);
  }

}

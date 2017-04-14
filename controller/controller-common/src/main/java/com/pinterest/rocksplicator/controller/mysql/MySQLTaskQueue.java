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

package com.pinterest.rocksplicator.controller.mysql;

import com.pinterest.rocksplicator.controller.TaskQueue;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TaskQueue} which talks to MySQL directly on all kinds of queue operations.
 *
 * MySQL task queue table schema:
 *  id BIGINT AUTO_INCREMENT,
 *  name VARCHAR(128),
 *  priority TINYINT UNSIGNED NOT NULL, # 0 is the highest priority
 *  state  TINYINT UNSIGNED NOT NULL, # 0: Pending, 1: Running, 2: Done, 3: FAILED
 *  tag_name VARCHAR(128) NOT NULL,
 *  body TEXT NOT NULL,
 *  created_at DATETIME NOT NULL,
 *  run_after DATETIME NOT NULL,
 *  claimed_worker VARCHAR(128),
 *  last_alive_at DATETIME,
 *  output TEXT,
 *  PRIMARY KEY (id),
 *  FOREIGN KEY (tag_name) REFERENCES tag(name) ON UPDATE RESTRICT ON DELETE CASCADE
 *
 * MySQL tag table schema:
 *  name VARCHAR(128) NOT NULL,
 *  locks TINYINT UNSIGNED NOT NULL,
 *  created_at DATETIME NOT NULL,
 *  owner VARCHAR(256),
 *  PRIMARY KEY (name)
 *
 */
public class MySQLTaskQueue implements TaskQueue {

  class MySQLTaskQueueException extends Exception {
    public MySQLTaskQueueException(String message) {
      super(message);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskQueue.class);
  private static final String TAG_TABLE_NAME = "tag";
  private static final String TASK_TABLE_NAME = "task";
  private static final String SELECT_ALL_TEMPLATE =
      "SELECT * FROM %s";
  private static final String INSERT_TAG_QUERY_TEMPLATE =
      "INSERT IGNORE INTO " + TAG_TABLE_NAME + " (name,locks,created_at) VALUES ('%s', %d, NOW())";
  private static final String LOCK_CLUSTER_QUERY_TEMPLATE =
      "UPDATE " + TAG_TABLE_NAME + " SET locks=1 WHERE name='%s' AND locks=0";
  private static final String UNLOCK_CLUSTER_QUERY_TEMPLATE =
      "UPDATE " + TAG_TABLE_NAME + " SET locks=0 WHERE name='%s' AND locks=1";
  private static final String PEEK_CLUSTER_LOCK_QUERY_TEMPLATE =
      "SELECT locks FROM " + TAG_TABLE_NAME + " WHERE name='%s'";
  private static final String REMOVE_CLUSTER_QUERY_TEMPLATE =
      "DELETE FROM " + TAG_TABLE_NAME + " WHERE name='%s'";

  private final Connection connection;

  MySQLTaskQueue(Connection connection) {
    this.connection = connection;
  }

  public boolean isClusterLocked(final String clusterName) throws MySQLTaskQueueException {
    String peekLockQuery = String.format(PEEK_CLUSTER_LOCK_QUERY_TEMPLATE, clusterName);
    List<HashMap<String, Object>> result = JdbcUtils.executeQuery(connection, peekLockQuery);
    if (result.isEmpty()) {
      throw new MySQLTaskQueueException("Cluster doens't exist");
    }
    Integer locks = (Integer) result.get(0).get("locks");
    return locks == 1;
  }

  @Override
  public boolean createCluster(final String clusterName) {
    String insertQuery = String.format(INSERT_TAG_QUERY_TEMPLATE, clusterName, 0);
    return JdbcUtils.executeUpdateQuery(connection, insertQuery);
  }

  @Override
  public boolean lockCluster(final String clusterName) {
    boolean locked = true;
    try {
      locked = isClusterLocked(clusterName);
    } catch (MySQLTaskQueueException e) {
      LOG.error("Cluster hasn't been created");
    } finally {
      if (locked) {
        return false;
      }
    }
    String lockQuery = String.format(LOCK_CLUSTER_QUERY_TEMPLATE, clusterName);
    return JdbcUtils.executeUpdateQuery(connection, lockQuery);
  }

  @Override
  public boolean unlockCluster(final String clusterName) {
    String unlockQuery = String.format(UNLOCK_CLUSTER_QUERY_TEMPLATE, clusterName);
    return JdbcUtils.executeUpdateQuery(connection, unlockQuery);
  }

  @Override
  public boolean removeCluster(final String clusterName) {
    boolean locked = true;
    try {
      locked = isClusterLocked(clusterName);
    } catch (MySQLTaskQueueException e) {
      LOG.error("Cluster hasn't been created");
    } finally {
      if (locked) {
        return false;
      }
    }
    String removeQuery = String.format(REMOVE_CLUSTER_QUERY_TEMPLATE, clusterName);
    return JdbcUtils.executeUpdateQuery(connection, removeQuery);
  }

  @Override
  public Set<String> getAllClusters() {
    String globalQuery = String.format(SELECT_ALL_TEMPLATE, TAG_TABLE_NAME);
    List<HashMap<String, Object>> resultSet = JdbcUtils.executeQuery(connection, globalQuery);
    Set<String> clusters = new HashSet<>();
    for (HashMap<String, Object> result : resultSet) {
      clusters.add((String) result.get("name"));
    }
    return clusters;
  }

}

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
public class MySQLTaskQueue implements TaskQueue{

  private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskQueue.class);
  private static final String TAG_TABLE_NAME = "tag";
  private static final String TASK_TABLE_NAME = "task";
  private static final String INSERT_TAG_QUERY_TEMPLATE =
      "INSERT IGNORE INTO %s (name, locks, created_at) VALUES ('%s', %d, NOW())";
  private static final String LOCK_CLUSTER_QUERY_TEMPLATE =
      "UPDATE TAG SET locks=1 WHERE name='%s' AND locks=0";
  private static final String UNLOCK_CLUSTER_QUERY_TEMPLATE =
      "UPDATE TAG SET locks=0 WHERE name='%s' AND locks=1";
  private final Connection connection;

  MySQLTaskQueue(Connection connection) {
    this.connection = connection;
  }

  @Override
  public boolean createCluster(final String clusterName) {
    String insertQuery = String.format(
        INSERT_TAG_QUERY_TEMPLATE, TAG_TABLE_NAME, clusterName, 0);
    return JdbcUtils.executeUpdateQuery(connection, insertQuery);
  }

  @Override
  public boolean lockCluster(final String clusterName) {
    String lockQuery = String.format(LOCK_CLUSTER_QUERY_TEMPLATE, clusterName);
    return JdbcUtils.executeUpdateQuery(connection, lockQuery);
  }

  @Override
  public boolean unlockCluster(final String clusterName) {
    String unlockQuery = String.format(UNLOCK_CLUSTER_QUERY_TEMPLATE, clusterName);
    return JdbcUtils.executeUpdateQuery(connection, unlockQuery);
  }

}

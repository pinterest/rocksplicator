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

package com.pinterest.rocksplicator.controller.mysql.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * MySQL task table schema:
 * id BIGINT AUTO_INCREMENT,
 * name VARCHAR(128),
 * priority TINYINT UNSIGNED NOT NULL, # 0 is the highest priority
 * state  TINYINT UNSIGNED NOT NULL, # 0: Pending, 1: Running, 2: Done, 3: FAILED
 * tag_name VARCHAR(128) NOT NULL,
 * body TEXT NOT NULL,
 * created_at DATETIME NOT NULL,
 * run_after DATETIME NOT NULL,
 * claimed_worker VARCHAR(128),
 * last_alive_at DATETIME,
 * output TEXT,
 * PRIMARY KEY (id),
 * FOREIGN KEY (tag_name) REFERENCES tag(name) ON UPDATE RESTRICT ON DELETE CASCADE
 */
@Entity (name = "task")
@Table (name = "task")
@NamedQueries({
    @NamedQuery(name = "task.peekDequeue",
                query = "SELECT t FROM task t INNER JOIN t.cluster c WHERE t.state = 0 " +
                    "AND t.runAfter < CURRENT_TIMESTAMP AND c.locks = 0 ORDER BY t.priority ASC"),
    @NamedQuery(name = "task.findRunning",
                query = "SELECT t FROM task t WHERE t.id = :id AND t.state = 1"),
    @NamedQuery(name = "task.peekAllTasks",
                query = "SELECT t FROM task t INNER JOIN t.cluster c"),
    @NamedQuery(name = "task.peekTasksFromCluster",
                query = "SELECT t FROM task t INNER JOIN t.cluster c WHERE c.name = :name"),
    @NamedQuery(name = "task.peekTasksWithState",
                query = "SELECT t FROM task t WHERE t.state = :state"),
    @NamedQuery(name = "task.peekTasksFromClusterWithState",
                query = "SELECT t FROM task t INNER JOIN t.cluster c WHERE t.state = :state AND " +
                    "c.name = :name"),
})
public class TaskEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private long id;

  @Column(name = "name")
  private String name;

  @Column(name = "priority")
  @NotNull
  private int priority;

  @Column(name = "state")
  @NotNull
  private int state;

  @JoinColumn(name="tag_name", referencedColumnName="name")
  @ManyToOne
  @NotNull
  private TagEntity cluster;

  @Column(name="body")
  @NotNull
  private String body;

  @Column(name = "created_at")
  @Temporal(TemporalType.TIMESTAMP)
  @NotNull
  private Date createdAt;

  @Column(name = "run_after")
  @Temporal(TemporalType.TIMESTAMP)
  @NotNull
  private Date runAfter;

  @Column(name = "last_alive_at")
  @Temporal(TemporalType.TIMESTAMP)
  private Date lastAliveAt;

  @Column(name = "output")
  private String output;

  @Column(name = "claimed_worker")
  private String claimedWorker;

  public TaskEntity() {
    this.createdAt = new Date();
  }

  public TaskEntity setName(String name) {
    this.name = name;
    return this;
  }

  public TaskEntity setPriority(int priority) {
    this.priority = priority;
    return this;
  }

  public TaskEntity setState(int state) {
    this.state = state;
    return this;
  }

  public TaskEntity setBody(String body) {
    this.body = body;
    return this;
  }

  public TaskEntity setRunAfter(Date runAfter) {
    this.runAfter = runAfter;
    return this;
  }

  public TaskEntity setLastAliveAt(Date lastAliveAt) {
    this.lastAliveAt = lastAliveAt;
    return this;
  }

  public TaskEntity setOutput(String output) {
    this.output = output;
    return this;
  }

  public TaskEntity setCluster(TagEntity cluster) {
    this.cluster = cluster;
    return this;
  }

  public TaskEntity setClaimedWorker(String claimedWorker) {
    this.claimedWorker = claimedWorker;
    return this;
  }

  public String getName() {
    return name;
  }

  public int getPriority() {
    return priority;
  }

  public int getState() {
    return state;
  }

  public String getBody() {
    return body;
  }

  public Date getRunAfter() {
    return runAfter;
  }

  public Date getLastAliveAt() {
    return lastAliveAt;
  }

  public String getOutput() {
    return output;
  }

  public Date getCreatedAt() { return createdAt; }

  public TagEntity getCluster() {
    return cluster;
  }

  public long getId() { return id; }

  public String getClaimedWorker() { return claimedWorker; }
}

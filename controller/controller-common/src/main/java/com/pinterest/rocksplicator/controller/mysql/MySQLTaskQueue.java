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

import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskBase;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.mysql.entity.TagEntity;
import com.pinterest.rocksplicator.controller.mysql.entity.TaskEntity;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link TaskQueue} which talks to MySQL directly on all kinds of queue operations through JPA.
 */
public class MySQLTaskQueue implements TaskQueue {

  class MySQLTaskQueueException extends Exception {
    public MySQLTaskQueueException() { super(); }
  }

  private EntityManager entityManager;
  private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskQueue.class);

  public MySQLTaskQueue(EntityManager entityManager) {
    this.entityManager = entityManager;
  }

  @Override
  public boolean createCluster(final String clusterName) {
    TagEntity cluster = entityManager.find(TagEntity.class, clusterName);
    if (cluster != null) {
      LOG.error("Cluster {} is already existed", clusterName);
      return false;
    }
    TagEntity newCluster = new TagEntity().setName(clusterName);
    entityManager.getTransaction().begin();
    entityManager.persist(newCluster);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public boolean lockCluster(final String clusterName) {
    entityManager.getTransaction().begin();
    TagEntity cluster = entityManager.find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    try {
      if (cluster == null) {
        LOG.error("Cluster {} hasn't been created", clusterName);
        throw new MySQLTaskQueueException();
      }
      if (cluster.getLocks() == 1) {
        LOG.error("Cluster {} is already locked, cannot double lock", clusterName);
        throw new MySQLTaskQueueException();
      }
    } catch (MySQLTaskQueueException e) {
      entityManager.getTransaction().rollback();
      return false;
    }
    cluster.setLocks(1);
    entityManager.persist(cluster);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public boolean unlockCluster(final String clusterName) {
    entityManager.getTransaction().begin();
    TagEntity cluster = entityManager.find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    if (cluster == null) {
      LOG.error("Cluster {} hasn't been created", clusterName);
      entityManager.getTransaction().rollback();
      return false;
    }
    cluster.setLocks(0);
    entityManager.persist(cluster);
    entityManager.getTransaction().commit();
    return true;
  }


  @Override
  public boolean removeCluster(final String clusterName) {
    entityManager.getTransaction().begin();
    TagEntity cluster = entityManager.find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    try {
      if (cluster == null) {
        LOG.error("Cluster {} hasn't been created", clusterName);
        throw new MySQLTaskQueueException();
      }
      if (cluster.getLocks() == 1) {
        LOG.error("Cluster {} is already locked, cannot remove.", clusterName);
        throw new MySQLTaskQueueException();
      }
    } catch (MySQLTaskQueueException e) {
      entityManager.getTransaction().rollback();
      return false;
    }
    entityManager.remove(cluster);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public Set<String> getAllClusters() {
    Query query = entityManager.createNamedQuery("tag.findAll");
    List<String> result = query.getResultList();
    Set<String> clusterNames = new HashSet<>();
    result.stream().forEach(name -> {
      clusterNames.add(name);
    });
    return clusterNames;
  }

  @Override
  public boolean enqueueTask(final TaskBase taskBase,
                             final String clusterName,
                             final int runDelaySeconds) {
    entityManager.getTransaction().begin();
    TagEntity cluster = entityManager.find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    if (cluster == null) {
      LOG.error("Cluster {} is not created", clusterName);
      entityManager.getTransaction().rollback();
      return false;
    }
    TaskEntity entity = new TaskEntity()
        .setName(taskBase.name)
        .setPriority(taskBase.priority)
        .setBody(taskBase.body)
        .setCluster(cluster)
        .setState(Task.PENDING);
    entity.setRunAfter(DateUtils.addSeconds(entity.getCreatedAt(), runDelaySeconds));
    entityManager.persist(entity);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public Task dequeueTask(final String worker) {
    entityManager.getTransaction().begin();
    Query query = entityManager.createNamedQuery("task.peekDequeue").setMaxResults(1);
    query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
    List<TaskEntity> resultList = query.getResultList();
    if (resultList.isEmpty()) {
      LOG.info("No pending task to be dequeud");
      entityManager.getTransaction().rollback();
      return null;
    }
    TaskEntity claimedTask = resultList.get(0);
    claimedTask.setState(Task.RUNNING);

    Task task = new Task()
        .setId(claimedTask.getId())
        .setState(claimedTask.getState())
        .setClusterName(claimedTask.getCluster().getName())
        .setName(claimedTask.getName())
        .setCreatedAt(claimedTask.getCreatedAt())
        .setRunAfter(claimedTask.getRunAfter())
        .setLastAliveAt(claimedTask.getLastAliveAt())
        .setOutput(claimedTask.getOutput())
        .setPriority(claimedTask.getPriority())
        .setBody(claimedTask.getBody())
        .setClaimedWorker(worker);

    claimedTask.setLastAliveAt(new Date());
    entityManager.persist(claimedTask);
    claimedTask.getCluster().setLocks(1);
    entityManager.persist(claimedTask.getCluster());
    entityManager.getTransaction().commit();
    return task;
  }

  private boolean actTask(final long id, final String output, int ackState) {
    entityManager.getTransaction().begin();
    Query query = entityManager.createNamedQuery("task.findRunning").setParameter("id", id);
    query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
    List<TaskEntity> resultList = query.getResultList();
    if (resultList.isEmpty()) {
      LOG.info("No matching task to ack: {}", id);
      entityManager.getTransaction().rollback();
      return false;
    }
    TaskEntity taskEntity = resultList.get(0);
    entityManager.lock(taskEntity, LockModeType.PESSIMISTIC_WRITE);
    TagEntity cluster = taskEntity.getCluster();
    entityManager.lock(cluster, LockModeType.PESSIMISTIC_WRITE);
    taskEntity.setState(ackState);
    taskEntity.setOutput(output);
    cluster.setLocks(0);
    entityManager.persist(taskEntity);
    entityManager.persist(cluster);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public boolean finishTask(final long id, final String output) {
    return actTask(id, output, Task.DONE);
  }

  @Override
  public boolean failTask(final long id, final String reason) {
    return actTask(id, reason, Task.FAILED);
  }
}

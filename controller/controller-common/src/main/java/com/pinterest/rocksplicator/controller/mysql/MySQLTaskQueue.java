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

import com.google.common.collect.ImmutableMap;
import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskBase;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.bean.TaskState;
import com.pinterest.rocksplicator.controller.mysql.entity.TagEntity;
import com.pinterest.rocksplicator.controller.mysql.entity.TaskEntity;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link TaskQueue} which talks to MySQL directly on all kinds of queue operations through JPA.
 */
public class MySQLTaskQueue implements TaskQueue {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskQueue.class);

  class MySQLTaskQueueException extends Exception {
    public MySQLTaskQueueException() { super(); }
  }

  private ThreadLocal<EntityManager> entityManager;
  private EntityManagerFactory entityManagerFactory;

  private class JDBC_CONFIGS {
    static final String PERSISTENCE_UNIT_NAME = "controller";
    static final String DRIVER_PROPERTY = "javax.persistence.jdbc.driver";
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String URL_PROPERTY = "javax.persistence.jdbc.url";
    static final String USER_PROPERTY = "javax.persistence.jdbc.user";
    static final String PASSWORD_PROPERTY = "javax.persistence.jdbc.password";
  }

  EntityManager getEntityManager() {
    if (entityManager.get() == null) {
      entityManager.set(entityManagerFactory.createEntityManager());
    }
    return entityManager.get();
  }

  void beginTransaction() {
    if (!getEntityManager().getTransaction().isActive()) {
      getEntityManager().getTransaction().begin();
    }
  }


  public MySQLTaskQueue(String jdbcUrl, String dbUser, String dbPassword) {
    this.entityManagerFactory = Persistence.createEntityManagerFactory(
        JDBC_CONFIGS.PERSISTENCE_UNIT_NAME, new ImmutableMap.Builder<String, String>()
            .put(JDBC_CONFIGS.DRIVER_PROPERTY, JDBC_CONFIGS.JDBC_DRIVER)
            .put(JDBC_CONFIGS.URL_PROPERTY, jdbcUrl)
            .put(JDBC_CONFIGS.USER_PROPERTY, dbUser)
            .put(JDBC_CONFIGS.PASSWORD_PROPERTY, dbPassword)
            .build()
    );
    entityManager = new ThreadLocal<EntityManager>();
  }

  static Task convertTaskEntityToTask(TaskEntity taskEntity) {
    return new Task()
        .setId(taskEntity.getId())
        .setState(taskEntity.getState())
        .setClusterName(taskEntity.getCluster().getName())
        .setName(taskEntity.getName())
        .setCreatedAt(taskEntity.getCreatedAt())
        .setRunAfter(taskEntity.getRunAfter())
        .setLastAliveAt(taskEntity.getLastAliveAt())
        .setOutput(taskEntity.getOutput())
        .setPriority(taskEntity.getPriority())
        .setBody(taskEntity.getBody())
        .setClaimedWorker(taskEntity.getClaimedWorker());
  }


  @Override
  public boolean createCluster(final String clusterName) {
    TagEntity cluster = getEntityManager().find(TagEntity.class, clusterName);
    if (cluster != null) {
      LOG.error("Cluster {} is already existed", clusterName);
      return false;
    }
    TagEntity newCluster = new TagEntity().setName(clusterName);
    beginTransaction();
    getEntityManager().persist(newCluster);
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public boolean lockCluster(final String clusterName) {
    beginTransaction();
    TagEntity cluster = getEntityManager().find(
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
      getEntityManager().getTransaction().rollback();
      return false;
    }
    cluster.setLocks(1);
    getEntityManager().persist(cluster);
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public boolean unlockCluster(final String clusterName) {
    beginTransaction();
    TagEntity cluster = getEntityManager().find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    if (cluster == null) {
      LOG.error("Cluster {} hasn't been created", clusterName);
      getEntityManager().getTransaction().rollback();
      return false;
    }
    cluster.setLocks(0);
    getEntityManager().persist(cluster);
    getEntityManager().getTransaction().commit();
    return true;
  }


  @Override
  public boolean removeCluster(final String clusterName) {
    beginTransaction();
    TagEntity cluster = getEntityManager().find(
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
      getEntityManager().getTransaction().rollback();
      return false;
    }
    getEntityManager().remove(cluster);
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public Set<String> getAllClusters() {
    Query query = getEntityManager().createNamedQuery("tag.findAll");
    List<String> result = query.getResultList();
    Set<String> clusterNames = new HashSet<>();
    result.stream().forEach(name -> {
      clusterNames.add(name);
    });
    return clusterNames;
  }

  private TaskEntity enqueueTaskImpl(final TaskBase taskBase,
                                     final String clusterName,
                                     final int runDelaySeconds,
                                     final TaskState state,
                                     final String claimedWorker) {
    TagEntity cluster = getEntityManager().find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    if (cluster == null) {
      LOG.error("Cluster {} is not created", clusterName);
      getEntityManager().getTransaction().rollback();
      return null;
    }
    TaskEntity entity = new TaskEntity()
        .setName(taskBase.name)
        .setPriority(taskBase.priority)
        .setBody(taskBase.body)
        .setCluster(cluster)
        .setLastAliveAt(new Date())
        .setClaimedWorker(claimedWorker)
        .setState(state.intValue())
        .setRunAfter(DateUtils.addSeconds(new Date(), runDelaySeconds));
    getEntityManager().persist(entity);
    getEntityManager().flush();
    return entity;
  }

  @Override
  public boolean enqueueTask(final TaskBase taskBase,
                             final String clusterName,
                             final int runDelaySeconds) {
    beginTransaction();
    TaskEntity taskEntity = enqueueTaskImpl(taskBase, clusterName, runDelaySeconds, TaskState
        .PENDING, null);
    if (taskEntity == null) {
      return false;
    }
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public Task dequeueTask(final String worker) {
    beginTransaction();
    Query query = getEntityManager().createNamedQuery("task.peekDequeue").setMaxResults(1);
    query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
    List<TaskEntity> resultList = query.getResultList();
    if (resultList.isEmpty()) {
      LOG.info("No pending task to be dequeud");
      getEntityManager().getTransaction().rollback();
      return null;
    }
    TaskEntity claimedTask = resultList.get(0);
    claimedTask.setState(TaskState.RUNNING.intValue());
    claimedTask.setLastAliveAt(new Date());
    claimedTask.setClaimedWorker(worker);

    getEntityManager().persist(claimedTask);
    claimedTask.getCluster().setLocks(1);
    getEntityManager().persist(claimedTask.getCluster());
    getEntityManager().getTransaction().commit();
    return convertTaskEntityToTask(claimedTask);
  }

  private String ackTask(final long id,
                         final String output,
                         TaskState ackState,
                         boolean unlockCluster) {
    Query query = getEntityManager().createNamedQuery("task.findRunning").setParameter("id", id);
    query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
    List<TaskEntity> resultList = query.getResultList();
    if (resultList.isEmpty()) {
      LOG.info("No matching task to ack: {}", id);
      getEntityManager().getTransaction().rollback();
      return null;
    }
    TaskEntity taskEntity = resultList.get(0);

    taskEntity.setState(ackState.intValue());
    taskEntity.setOutput(output);
    getEntityManager().persist(taskEntity);
    TagEntity cluster = taskEntity.getCluster();
    if (unlockCluster) {
      getEntityManager().lock(cluster, LockModeType.PESSIMISTIC_WRITE);
      cluster.setLocks(0);
      getEntityManager().persist(cluster);
    }
    return cluster.getName();
  }

  @Override
  public boolean finishTask(final long id, final String output) {
    beginTransaction();
    String clusterName = ackTask(id, output, TaskState.DONE, true);
    getEntityManager().getTransaction().commit();
    return clusterName != null;
  }

  @Override
  public boolean failTask(final long id, final String reason) {
    beginTransaction();
    String clusterName = ackTask(id, reason, TaskState.FAILED, true);
    getEntityManager().getTransaction().commit();
    return clusterName != null;
  }

  @Override
  public int resetZombieTasks(final int zombieThresholdSeconds) {
    beginTransaction();
    List<TaskEntity> runningTasks = getEntityManager()
        .createNamedQuery("task.findAllRunning")
        .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
    List<TaskEntity> zombieTasks = runningTasks.stream()
        .filter(t -> t.getState() == TaskState.RUNNING.intValue() &&
            DateUtils.addSeconds(t.getLastAliveAt(), zombieThresholdSeconds)
        .before(new Date())).collect(Collectors.toList());
    for (TaskEntity zombieTask : zombieTasks) {
      getEntityManager().lock(zombieTask.getCluster(), LockModeType.PESSIMISTIC_WRITE);
      zombieTask.setState(TaskState.PENDING.intValue());
      zombieTask.getCluster().setLocks(0);
      getEntityManager().persist(zombieTask);
      getEntityManager().persist(zombieTask.getCluster());
    }
    getEntityManager().getTransaction().commit();
    return zombieTasks.size();
  }

  @Override
  public boolean keepTaskAlive(final long id) {
    beginTransaction();
    TaskEntity taskEntity = getEntityManager().find(TaskEntity.class, id);
    if (taskEntity == null) {
      LOG.error("Cannot find task {}.", id);
      getEntityManager().getTransaction().rollback();
      return false;
    }
    taskEntity.setLastAliveAt(new Date());
    getEntityManager().persist(taskEntity);
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public long finishTaskAndEnqueueRunningTask(final long id,
                                              final String output,
                                              final TaskBase newTaskBase,
                                              final String worker) {
    beginTransaction();
    String clusterName = ackTask(id, output, TaskState.DONE, false);
    if (clusterName == null) {
      return -1;
    }
    TaskEntity entity = enqueueTaskImpl(newTaskBase, clusterName, 0, TaskState.RUNNING, worker);
    getEntityManager().getTransaction().commit();
    return entity.getId();
  }

  @Override
  public boolean finishTaskAndEnqueuePendingTask(final long id,
                                                 final String output,
                                                 final TaskBase newTaskBase,
                                                 final int runDelaySeconds) {
    beginTransaction();
    String clusterName = ackTask(id, output, TaskState.DONE, true);
    if (clusterName == null) {
      return false;
    }
    TaskEntity entity = enqueueTaskImpl(newTaskBase, clusterName, runDelaySeconds,
        TaskState.PENDING, null);
    if (entity == null) {
      return false;
    }
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public List<Task> peekTasks(final String clusterName, final Integer state) {
    Query query = getEntityManager()
        .createNamedQuery("task.peekTasks")
        .setParameter("state", state).setParameter("name", clusterName);
    List<TaskEntity> result = query.getResultList();
    return result.stream().map(
        MySQLTaskQueue::convertTaskEntityToTask).collect(Collectors.toList());
  }

  @Override
  public Task findTask(long id) {
    TaskEntity taskEntity = getEntityManager().find(TaskEntity.class, id);
    if (taskEntity == null) {
      LOG.error("Cannot find task {}.", id);
      return null;
    }
    return convertTaskEntityToTask(taskEntity);
  }

  @Override
  public int removeFinishedTasks(final int secondsAgo) {
    beginTransaction();
    List<TaskEntity> finishedTasks = getEntityManager()
        .createNamedQuery("task.findFinished")
        .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
    List<TaskEntity> removingTasks = finishedTasks.stream().filter(t -> DateUtils.addSeconds(t
        .getCreatedAt(), secondsAgo).before(new Date())).collect(Collectors.toList());
    removingTasks.stream().forEach(t -> getEntityManager().remove(t));
    getEntityManager().getTransaction().commit();
    return removingTasks.size();
  }
}

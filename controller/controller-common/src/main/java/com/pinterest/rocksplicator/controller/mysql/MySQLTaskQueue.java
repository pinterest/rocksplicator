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
import java.util.stream.Collectors;

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

  private long enqueueTaskImpl(final TaskBase taskBase,
                               final String clusterName,
                               final int runDelaySeconds,
                               final Task.TaskState state,
                               final String claimedWorker,
                               boolean inTransaction) {
    if (!inTransaction)
      entityManager.getTransaction().begin();
    TagEntity cluster = entityManager.find(
        TagEntity.class, clusterName, LockModeType.PESSIMISTIC_WRITE);
    if (cluster == null) {
      LOG.error("Cluster {} is not created", clusterName);
      entityManager.getTransaction().rollback();
      return -1;
    }
    TaskEntity entity = new TaskEntity()
        .setName(taskBase.name)
        .setPriority(taskBase.priority)
        .setBody(taskBase.body)
        .setCluster(cluster)
        .setLastAliveAt(new Date())
        .setClaimedWorker(claimedWorker)
        .setState(state.ordinal())
        .setRunAfter(DateUtils.addSeconds(new Date(), runDelaySeconds));
    entityManager.persist(entity);
    entityManager.flush();
    if (!inTransaction)
      entityManager.getTransaction().commit();
    return entity.getId();
  }

  @Override
  public boolean enqueueTask(final TaskBase taskBase,
                             final String clusterName,
                             final int runDelaySeconds) {
    return enqueueTaskImpl(
        taskBase, clusterName, runDelaySeconds, Task.TaskState.PENDING, null, false) != -1;
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
    claimedTask.setState(Task.TaskState.RUNNING.ordinal());
    claimedTask.setLastAliveAt(new Date());
    claimedTask.setClaimedWorker(worker);

    entityManager.persist(claimedTask);
    claimedTask.getCluster().setLocks(1);
    entityManager.persist(claimedTask.getCluster());
    entityManager.getTransaction().commit();
    return convertTaskEntityToTask(claimedTask);
  }

  private String ackTask(final long id,
                         final String output,
                         Task.TaskState ackState,
                         boolean unlockCluster,
                         boolean inTransaction) {
    if (!inTransaction)
      entityManager.getTransaction().begin();
    Query query = entityManager.createNamedQuery("task.findRunning").setParameter("id", id);
    query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
    List<TaskEntity> resultList = query.getResultList();
    if (resultList.isEmpty()) {
      LOG.info("No matching task to ack: {}", id);
      entityManager.getTransaction().rollback();
      return null;
    }
    TaskEntity taskEntity = resultList.get(0);

    taskEntity.setState(ackState.ordinal());
    taskEntity.setOutput(output);
    entityManager.persist(taskEntity);

    TagEntity cluster = taskEntity.getCluster();
    if (unlockCluster) {
      entityManager.lock(cluster, LockModeType.PESSIMISTIC_WRITE);
      cluster.setLocks(0);
      entityManager.persist(cluster);
    }
    if (!inTransaction)
      entityManager.getTransaction().commit();
    return cluster.getName();
  }

  @Override
  public boolean finishTask(final long id, final String output) {
    return ackTask(id, output, Task.TaskState.DONE, true, false) != null;
  }

  @Override
  public boolean failTask(final long id, final String reason) {
    return ackTask(id, reason, Task.TaskState.FAILED, true, false) != null;
  }

  @Override
  public int resetZombieTasks(final int zombieThresholdSeconds) {
    entityManager.getTransaction().begin();
    List<TaskEntity> runningTasks = entityManager
        .createNamedQuery("task.findAllRunning")
        .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
    List<TaskEntity> zombieTasks = runningTasks.stream()
        .filter(t -> t.getState() == Task.TaskState.RUNNING.ordinal() &&
            DateUtils.addSeconds(t.getLastAliveAt(), zombieThresholdSeconds)
        .before(new Date())).collect(Collectors.toList());
    for (TaskEntity zombieTask : zombieTasks) {
      entityManager.lock(zombieTask.getCluster(), LockModeType.PESSIMISTIC_WRITE);
      zombieTask.setState(Task.TaskState.PENDING.ordinal());
      zombieTask.getCluster().setLocks(0);
      entityManager.persist(zombieTask);
      entityManager.persist(zombieTask.getCluster());
    }
    entityManager.getTransaction().commit();
    return zombieTasks.size();
  }

  @Override
  public boolean keepTaskAlive(final long id) {
    entityManager.getTransaction().begin();
    TaskEntity taskEntity = entityManager.find(TaskEntity.class, id);
    if (taskEntity == null) {
      LOG.error("Cannot find task {}.", id);
      entityManager.getTransaction().rollback();
      return false;
    }
    taskEntity.setLastAliveAt(new Date());
    entityManager.persist(taskEntity);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public long finishTaskAndEnqueueRunningTask(final long id,
                                              final String output,
                                              final TaskBase newTaskBase,
                                              final String worker) {
    entityManager.getTransaction().begin();
    String clusterName = ackTask(id, output, Task.TaskState.DONE, false, true);
    if (clusterName == null) {
      return -1;
    }
    long newTaskId = enqueueTaskImpl(newTaskBase, clusterName, 0,
        Task.TaskState.RUNNING, worker, true);
    entityManager.getTransaction().commit();
    return newTaskId;
  }

  @Override
  public boolean finishTaskAndEnqueuePendingTask(final long id,
                                                 final String output,
                                                 final TaskBase newTaskBase,
                                                 final int runDelaySeconds) {
    entityManager.getTransaction().begin();
    String clusterName = ackTask(id, output, Task.TaskState.DONE, true, true);
    if (clusterName == null) {
      return false;
    }
    long newTaskId = enqueueTaskImpl(newTaskBase, clusterName, runDelaySeconds,
        Task.TaskState.PENDING, null, true);
    if (newTaskId == -1) {
      return false;
    }
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public List<Task> peekTasks(final String clusterName, final Integer state) {
    Query query = entityManager
        .createNamedQuery("task.peekTasks")
        .setParameter("state", state).setParameter("name", clusterName);
    List<TaskEntity> result = query.getResultList();
    return result.stream().map(
        MySQLTaskQueue::convertTaskEntityToTask).collect(Collectors.toList());
  }

  @Override
  public Task findTask(long id) {
    TaskEntity taskEntity = entityManager.find(TaskEntity.class, id);
    if (taskEntity == null) {
      LOG.error("Cannot find task {}.", id);
      return null;
    }
    return convertTaskEntityToTask(taskEntity);
  }

  @Override
  public int removeFinishedTasks(final int secondsAgo) {
    entityManager.getTransaction().begin();
    List<TaskEntity> finishedTasks = entityManager
        .createNamedQuery("task.findFinished")
        .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
    List<TaskEntity> removingTasks = finishedTasks.stream().filter(t -> DateUtils.addSeconds(t
        .getCreatedAt(), secondsAgo).before(new Date())).collect(Collectors.toList());
    removingTasks.stream().forEach(t -> entityManager.remove(t));
    entityManager.getTransaction().commit();
    return removingTasks.size();
  }

}

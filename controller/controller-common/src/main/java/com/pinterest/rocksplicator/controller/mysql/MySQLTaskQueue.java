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

import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskBase;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.bean.TaskState;
import com.pinterest.rocksplicator.controller.mysql.entity.TagEntity;
import com.pinterest.rocksplicator.controller.mysql.entity.TagId;
import com.pinterest.rocksplicator.controller.mysql.entity.TaskEntity;

import com.google.common.base.Strings;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MySQLTaskQueue extends MySQLBase implements TaskQueue {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskQueue.class);

  class MySQLTaskQueueException extends Exception {
    public MySQLTaskQueueException() { super(); }
  }

  public MySQLTaskQueue(String jdbcUrl, String dbUser, String dbPassword) {
    super(jdbcUrl, dbUser, dbPassword);
  }

  static Task convertTaskEntityToTask(TaskEntity taskEntity) {
    return new Task()
        .setId(taskEntity.getId())
        .setState(taskEntity.getState())
        .setCluster(new Cluster(taskEntity.getCluster().getNamespace(),
            taskEntity.getCluster().getName()))
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
  public boolean createCluster(final Cluster cluster) {
    TagEntity tagEntity = getEntityManager().find(TagEntity.class, new TagId(cluster));
    if (tagEntity != null) {
      LOG.error("Cluster {} is already existed", cluster);
      return false;
    }
    TagEntity newCluster = new TagEntity().setName(cluster.getName())
        .setNamespace(cluster.getNamespace());
    beginTransaction();
    getEntityManager().persist(newCluster);
    getEntityManager().getTransaction().commit();
    return true;
  }


  @Override
  public boolean lockCluster(final Cluster cluster) {
    beginTransaction();
    TagEntity tagEntity = getEntityManager().find(
        TagEntity.class, new TagId(cluster), LockModeType.PESSIMISTIC_WRITE);
    try {
      if (tagEntity == null) {
        LOG.error("Cluster {} hasn't been created", cluster);
        throw new MySQLTaskQueueException();
      }
      if (tagEntity.getLocks() == 1) {
        LOG.error("Cluster {} is already locked, cannot double lock", cluster);
        throw new MySQLTaskQueueException();
      }
    } catch (MySQLTaskQueueException e) {
      getEntityManager().getTransaction().rollback();
      return false;
    }
    tagEntity.setLocks(1);
    getEntityManager().persist(tagEntity);
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public boolean unlockCluster(final Cluster cluster) {
    beginTransaction();
    TagEntity tagEntity = getEntityManager().find(
        TagEntity.class, new TagId(cluster), LockModeType.PESSIMISTIC_WRITE);
    if (cluster == null) {
      LOG.error("Cluster {} hasn't been created", cluster);
      getEntityManager().getTransaction().rollback();
      return false;
    }
    tagEntity.setLocks(0);
    getEntityManager().persist(tagEntity);
    getEntityManager().getTransaction().commit();
    return true;
  }


  @Override
  public boolean removeCluster(final Cluster cluster) {
    beginTransaction();
    TagEntity tagEntity = getEntityManager().find(
        TagEntity.class, new TagId(cluster), LockModeType.PESSIMISTIC_WRITE);
    try {
      if (tagEntity == null) {
        LOG.error("Cluster {} hasn't been created", cluster);
        throw new MySQLTaskQueueException();
      }
      if (tagEntity.getLocks() == 1) {
        LOG.error("Cluster {} is already locked, cannot remove.", cluster);
        throw new MySQLTaskQueueException();
      }
    } catch (MySQLTaskQueueException e) {
      getEntityManager().getTransaction().rollback();
      return false;
    }
    getEntityManager().remove(tagEntity);
    getEntityManager().getTransaction().commit();
    return true;
  }

  @Override
  public Set<Cluster> getAllClusters() {
    Query query = getEntityManager().createNamedQuery("tag.findAll");
    List<Object[]> result = query.getResultList();
    Set<Cluster> clusters = new HashSet<>();
    result.stream().forEach(fields -> {
      clusters.add(new Cluster((String)fields[0], (String)fields[1]));
    });
    return clusters;
  }

  private TaskEntity enqueueTaskImpl(final TaskBase taskBase,
                                     final Cluster cluster,
                                     final int runDelaySeconds,
                                     final TaskState state,
                                     final String claimedWorker) {
    TagEntity tagEntity = getEntityManager().find(
        TagEntity.class, new TagId(cluster), LockModeType.PESSIMISTIC_WRITE);
    if (tagEntity == null) {
      LOG.error("Cluster {} is not created", cluster);
      getEntityManager().getTransaction().rollback();
      return null;
    }
    TaskEntity entity = new TaskEntity()
        .setName(taskBase.name)
        .setPriority(taskBase.priority)
        .setBody(taskBase.body)
        .setCluster(tagEntity)
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
                             final Cluster cluster,
                             final int runDelaySeconds) {
    beginTransaction();
    TaskEntity taskEntity = enqueueTaskImpl(taskBase, cluster, runDelaySeconds, TaskState
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

  private Cluster ackTask(final long id,
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
    return new Cluster(cluster.getNamespace(), cluster.getName());
  }

  @Override
  public boolean finishTask(final long id, final String output) {
    beginTransaction();
    Cluster cluster = ackTask(id, output, TaskState.DONE, true);
    getEntityManager().getTransaction().commit();
    return cluster != null;
  }

  @Override
  public boolean failTask(final long id, final String reason) {
    beginTransaction();
    Cluster cluster = ackTask(id, reason, TaskState.FAILED, true);
    getEntityManager().getTransaction().commit();
    return cluster != null;
  }

  @Override
  public int resetZombieTasks(final int zombieThresholdSeconds) {
    beginTransaction();
    List<TaskEntity> runningTasks = getEntityManager()
        .createNamedQuery("task.peekTasksWithState")
        .setParameter("state", TaskState.RUNNING.intValue())
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
    Cluster cluster = ackTask(id, output, TaskState.DONE, false);
    if (cluster == null) {
      return -1;
    }
    TaskEntity entity = enqueueTaskImpl(newTaskBase, cluster, 0, TaskState.RUNNING, worker);
    getEntityManager().getTransaction().commit();
    return entity.getId();
  }

  private boolean ackTaskAndEnqueuePendingTask(final long id,
                                               final String output,
                                               final TaskBase newTaskBase,
                                               final int runDelaySeconds,
                                               TaskState ackState) {
    beginTransaction();
    Cluster cluster = ackTask(id, output, ackState, true);
    if (cluster == null) {
      return false;
    }
    TaskEntity entity = enqueueTaskImpl(newTaskBase, cluster, runDelaySeconds,
        TaskState.PENDING, null);
    if (entity == null) {
      return false;
    }
    getEntityManager().getTransaction().commit();
    return true;
  }


  @Override
  public boolean finishTaskAndEnqueuePendingTask(final long id,
                                                 final String output,
                                                 final TaskBase newTaskBase,
                                                 final int runDelaySeconds) {
    return ackTaskAndEnqueuePendingTask(id, output, newTaskBase, runDelaySeconds, TaskState.DONE);
  }

  @Override
  public boolean failTaskAndEnqueuePendingTask(final long id,
                                               final String output,
                                               final TaskBase newTaskBase,
                                               final int runDelaySeconds) {
    return ackTaskAndEnqueuePendingTask(id, output, newTaskBase, runDelaySeconds, TaskState.FAILED);
  }

  @Override
  public List<Task> peekTasks(final Cluster cluster, final Integer state) {
    Query query;
    if (!cluster.getName().isEmpty() && !cluster.getNamespace().isEmpty() && state != null) {
      query = getEntityManager()
        .createNamedQuery("task.peekTasksWithStateFromCluster")
        .setParameter("state", state)
        .setParameter("namespace", cluster.getNamespace()).setParameter("name", cluster.getName());
    } else if (!cluster.getNamespace().isEmpty() && cluster.getName().isEmpty() && state != null) {
      query = getEntityManager()
        .createNamedQuery("task.peekTasksWithStateFromNamespace")
        .setParameter("state", state).setParameter("namespace", cluster.getNamespace());
    } else if (cluster.getNamespace().isEmpty() && cluster.getName().isEmpty() && state != null) {
      query = getEntityManager()
          .createNamedQuery("task.peekTasksWithState")
          .setParameter("state", state);
    } else if (!cluster.getName().isEmpty() && !cluster.getNamespace().isEmpty() && state == null) {
      query = getEntityManager()
          .createNamedQuery("task.peekTasksFromCluster")
          .setParameter("namespace", cluster.getNamespace()).setParameter("name", cluster.getName());
    } else if (!cluster.getNamespace().isEmpty() && cluster.getName().isEmpty() && state == null) {
      query = getEntityManager()
          .createNamedQuery("task.peekTasksFromNamespace")
          .setParameter("namespace", cluster.getNamespace());
    } else {
      query = getEntityManager()
          .createNamedQuery("task.peekAllTasks");
    }
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
  public boolean removeTask(final long id) {
    beginTransaction();
    TaskEntity task = getEntityManager().find(TaskEntity.class, id, LockModeType.PESSIMISTIC_WRITE);
    try {
      if (task == null) {
        LOG.error("Cannot find task {}.", id);
        throw new MySQLTaskQueueException();
      }
    } catch (MySQLTaskQueueException e) {
      getEntityManager().getTransaction().rollback();
      return false;
    }
    getEntityManager().remove(task);
    getEntityManager().getTransaction().commit();
    return true;
  }
 
  @Override
  public int removeFinishedTasks(final int secondsAgo, final String namespace,
                                 final String clusterName, final String taskName) {
    beginTransaction();
    List<TaskEntity> finishedTasks;
    if (Strings.isNullOrEmpty(namespace)) {
      if (Strings.isNullOrEmpty(taskName)) {
        finishedTasks = getEntityManager()
            .createNamedQuery("task.peekTasksWithState")
            .setParameter("state", TaskState.DONE.intValue())
            .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
      } else {
        finishedTasks = getEntityManager()
            .createNamedQuery("task.peekTasksWithStateAndName")
            .setParameter("state", TaskState.DONE.intValue())
            .setParameter("name", taskName)
            .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
      }
    } else {
      if (Strings.isNullOrEmpty(clusterName)) {
        if (Strings.isNullOrEmpty(taskName)) {
          finishedTasks = getEntityManager()
              .createNamedQuery("task.peekTasksWithStateFromNamespace")
              .setParameter("state", TaskState.DONE.intValue())
              .setParameter("namespace", namespace)
              .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
        } else {
          finishedTasks = getEntityManager()
              .createNamedQuery("task.peekTasksWithStateAndNameFromNamespace")
              .setParameter("state", TaskState.DONE.intValue())
              .setParameter("namespace", namespace)
              .setParameter("name", taskName)
              .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
        }
      } else {
        if (Strings.isNullOrEmpty(taskName)) {
          finishedTasks = getEntityManager()
              .createNamedQuery("task.peekTasksWithStateFromCluster")
              .setParameter("state", TaskState.DONE.intValue())
              .setParameter("namespace", namespace)
              .setParameter("name", clusterName)
              .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
        } else {
          finishedTasks = getEntityManager()
              .createNamedQuery("task.peekTasksWithStateAndNameFromCluster")
              .setParameter("state", TaskState.DONE.intValue())
              .setParameter("namespace", namespace)
              .setParameter("name", taskName)
              .setParameter("clusterName", clusterName)
              .setLockMode(LockModeType.PESSIMISTIC_WRITE).getResultList();
        }
      }
    }

    List<TaskEntity> removingTasks = finishedTasks.stream().filter(t -> DateUtils.addSeconds(t
        .getCreatedAt(), secondsAgo).before(new Date())).collect(Collectors.toList());
    removingTasks.stream().forEach(t -> getEntityManager().remove(t));
    getEntityManager().getTransaction().commit();
    return removingTasks.size();
  }
}

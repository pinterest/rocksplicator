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
import com.pinterest.rocksplicator.controller.mysql.entity.TagEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link TaskQueue} which talks to MySQL directly on all kinds of queue operations through JPA.
 */
public class MySQLTaskQueue implements TaskQueue {

  class MySQLTaskQueueException extends Exception {
    public MySQLTaskQueueException(String message) {
      super(message);
    }
  }

  private EntityManager entityManager;
  private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskQueue.class);

  public MySQLTaskQueue(EntityManager entityManager) {
    this.entityManager = entityManager;
  }

  public boolean isClusterLocked(final String clusterName) throws MySQLTaskQueueException {
    List<Integer> result = entityManager
        .createNamedQuery("tag.findLock")
        .setParameter("name",clusterName)
        .getResultList();
    if (result.isEmpty()) {
      throw new MySQLTaskQueueException("Cluster doens't exist");
    }
    Integer locks = result.get(0);
    return locks == 1;
  }

  @Override
  public boolean createCluster(final String clusterName) {
    TagEntity cluster = entityManager.find(TagEntity.class, clusterName);
    if (cluster != null) {
      LOG.error("Cluster {} is already existed", clusterName);
      return false;
    }
    TagEntity tagEntity = new TagEntity().setName(clusterName);
    entityManager.getTransaction().begin();
    entityManager.persist(tagEntity);
    entityManager.getTransaction().commit();
    return true;
  }

  @Override
  public boolean lockCluster(final String clusterName) {
    boolean locked = true;
    try {
      locked = isClusterLocked(clusterName);
    } catch (MySQLTaskQueueException e) {
      LOG.error("Cluster {} hasn't been created", clusterName);
    } finally {
      if (locked) {
        LOG.error("Cluster {} is already locked, cannot double lock", clusterName);
        return false;
      }
    }
    Query query = entityManager.createNamedQuery("tag.lock").setParameter("name", clusterName);
    try {
      entityManager.getTransaction().begin();
      query.executeUpdate();
      entityManager.getTransaction().commit();
    } catch (IllegalStateException | PersistenceException e) {
      LOG.error("Error locking cluster {}", clusterName, e);
      return false;
    }
    return true;
  }

  @Override
  public boolean unlockCluster(final String clusterName) {
    Query query = entityManager.createNamedQuery("tag.unlock").setParameter("name", clusterName);
    try {
      entityManager.getTransaction().begin();
      query.executeUpdate();
      entityManager.getTransaction().commit();
    } catch (IllegalStateException | PersistenceException e) {
      LOG.error("Error unlocking cluster {}", clusterName, e);
      return false;
    }
    return true;
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
        LOG.error("Cluster {} is locked, cannot remove", clusterName);
        return false;
      }
    }
    TagEntity cluster = entityManager.find(TagEntity.class, clusterName);
    entityManager.getTransaction().begin();
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

}

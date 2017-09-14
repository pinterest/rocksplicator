/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.mysql;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;


/**
 * Does some common operations to MySQL like transaction management.
 * @author shu (shu@pinterest.com)
 */
public abstract class MySQLBase {

  private class JDBC_CONFIGS {
    static final String PERSISTENCE_UNIT_NAME = "controller";
    static final String DRIVER_PROPERTY = "javax.persistence.jdbc.driver";
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String URL_PROPERTY = "javax.persistence.jdbc.url";
    static final String USER_PROPERTY = "javax.persistence.jdbc.user";
    static final String PASSWORD_PROPERTY = "javax.persistence.jdbc.password";
  }

  private static final Logger LOG = LoggerFactory.getLogger(MySQLBase.class);

  private ThreadLocal<EntityManager> entityManager;
  private EntityManagerFactory entityManagerFactory;

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

  public MySQLBase(String jdbcUrl, String dbUser, String dbPassword) {
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

}

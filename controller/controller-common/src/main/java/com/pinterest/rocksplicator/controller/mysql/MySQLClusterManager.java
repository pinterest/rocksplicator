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

import com.google.common.base.Strings;
import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.ClusterManager;
import com.pinterest.rocksplicator.controller.bean.HostBean;
import com.pinterest.rocksplicator.controller.mysql.entity.TagEntity;
import com.pinterest.rocksplicator.controller.mysql.entity.TagHostsEntity;
import com.pinterest.rocksplicator.controller.mysql.entity.TagId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Query;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author shu (shu@pinterest.com)
 */
public class MySQLClusterManager extends MySQLBase implements ClusterManager{

  private static final Logger LOG = LoggerFactory.getLogger(MySQLClusterManager.class);

  public MySQLClusterManager(String jdbcUrl, String dbUser, String dbPassword) {
    super(jdbcUrl, dbUser, dbPassword);
  }

  private Set<HostBean> fromHostsString(final String hostsString) {
    Set<HostBean> hostBeanList = new HashSet<>();
    if (Strings.isNullOrEmpty(hostsString)) {
      return hostBeanList;
    }
    String[] hostsArray = hostsString.split("#");
    for (String hostString : hostsArray) {
      hostBeanList.add(HostBean.fromString(hostString));
    }
    return hostBeanList;
  }

  private String toHostsString(Set<HostBean> hostBeans) {
    if (hostBeans.isEmpty()) return "";
    Set<String> hostStrings = new HashSet<String>();
    for (HostBean hostBean : hostBeans) {
      hostStrings.add(hostBean.toString());
    }
    return String.join("#", hostStrings);
  }

  private TagHostsEntity findTagHostsEntity(final Cluster cluster) {
    Query query = getEntityManager().createNamedQuery("tag_hosts.getCluster")
        .setParameter("namespace", cluster.getNamespace())
        .setParameter("name", cluster.getName());
    List<TagHostsEntity> entities = query.getResultList();
    if (entities.isEmpty()) return null;
    return entities.get(0);
  }

  private boolean persistTagHostsEntity(TagHostsEntity tagHostsEntity, boolean transactionBegan) {
    if (!transactionBegan) {
      beginTransaction();
    }
    getEntityManager().persist(tagHostsEntity);
    getEntityManager().getTransaction().commit();
    return true;
  }

  private boolean persistTagHostsEntity(TagHostsEntity tagHostsEntity) {
    persistTagHostsEntity(tagHostsEntity, false);
  }

  @Override
  public boolean createCluster(final Cluster cluster) {
    beginTransaction();
    TagHostsEntity tagHostsEntity = findTagHostsEntity(cluster);
    if (tagHostsEntity != null) {
      LOG.error("The cluster {} is already created", cluster.toString());
      return false;
    }
    tagHostsEntity = new TagHostsEntity().setCluster(
        getEntityManager().find(TagEntity.class, new TagId(cluster)));
    tagHostsEntity.setBlacklistedHosts("").setLiveHosts("");
    return persistTagHostsEntity(tagHostsEntity, true);
  }

  @Override
  public boolean registerToCluster(final Cluster cluster, final HostBean hostBean) {
    TagHostsEntity tagHostsEntity = findTagHostsEntity(cluster);
    if (tagHostsEntity == null) {
      LOG.error("Trying to register {} from non-existing cluster {}", hostBean.toString(), cluster.toString());
      return false;
    }
    Set<HostBean> hostBeanList = fromHostsString(tagHostsEntity.getLiveHosts());
    hostBeanList.add(hostBean);
    tagHostsEntity.setLiveHosts(toHostsString(hostBeanList));
    return persistTagHostsEntity(tagHostsEntity);
  }

  @Override
  public boolean unregisterFromCluster(final Cluster cluster, final HostBean hostBean) {
    TagHostsEntity tagHostsEntity = findTagHostsEntity(cluster);
    if (tagHostsEntity == null) {
      LOG.error("Trying to unregister {} from non-existing cluster {}", hostBean.toString(), cluster.toString());
      return false;
    }
    Set<HostBean> liveHostsList = fromHostsString(tagHostsEntity.getLiveHosts());
    if (!liveHostsList.contains(hostBean)) {
      LOG.error("Trying to unregister a non-live host {} from {}", hostBean.toString(), cluster.toString());
      return false;
    }
    liveHostsList.remove(hostBean);
    tagHostsEntity.setLiveHosts(toHostsString(liveHostsList));
    Set<HostBean> blacklistedHosts = fromHostsString(tagHostsEntity.getBlacklistedHosts());
    if (blacklistedHosts.contains(hostBean)) {
      blacklistedHosts.remove(hostBean);
      tagHostsEntity.setBlacklistedHosts(toHostsString(blacklistedHosts));
    }
    return persistTagHostsEntity(tagHostsEntity);
  }

  @Override
  public boolean blacklistHost(final Cluster cluster, final HostBean hostBean) {
    TagHostsEntity tagHostsEntity = findTagHostsEntity(cluster);
    if (tagHostsEntity == null) {
      LOG.error("Trying to blacklist {} to non-existing cluster {}", hostBean.toString(), cluster.toString());
      return false;
    }
    Set<HostBean> blacklistedHosts = fromHostsString(tagHostsEntity.getBlacklistedHosts());
    blacklistedHosts.add(hostBean);
    tagHostsEntity.setBlacklistedHosts(toHostsString(blacklistedHosts));
    return persistTagHostsEntity(tagHostsEntity);
  }

  @Override
  public Set<HostBean> getHosts(final Cluster cluster, boolean excludeBlacklisted) {
    TagHostsEntity tagHostsEntity = findTagHostsEntity(cluster);
    if (tagHostsEntity == null) {
      LOG.error("Trying to query an non-existing cluster {}", cluster.toString());
      return new HashSet<>();
    }
    Set<HostBean> liveHosts = fromHostsString(tagHostsEntity.getLiveHosts());
    if (!excludeBlacklisted) {
      return liveHosts;
    } else {
      Set<HostBean> blacklistedHosts = fromHostsString(tagHostsEntity.getBlacklistedHosts());
      liveHosts.removeAll(blacklistedHosts);
      return liveHosts;
    }
  }

  @Override
  public Set<HostBean> getBlacklistedHosts(final Cluster cluster) {
    TagHostsEntity tagHostsEntity = findTagHostsEntity(cluster);
    if (tagHostsEntity == null) {
      LOG.error("Trying to query an non-existing cluster {}", cluster.toString());
      return new HashSet<>();
    }
    return fromHostsString(tagHostsEntity.getBlacklistedHosts());
  }
}

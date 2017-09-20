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
import com.pinterest.rocksplicator.controller.bean.HostBean;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * @author shu (shu@pinterest.com)
 */
public class MySQLClusterManagerIntegrationTest {

  private static final String TEST_CLUSTER_NAME = "integ_test";
  private static final String TEST_CLUSTER_NAMESPACE = "integ_test_rocksdb";
  private Cluster cluster = new Cluster(TEST_CLUSTER_NAMESPACE, TEST_CLUSTER_NAME);
  private MySQLClusterManager clusterManager;
  private MySQLTaskQueue queue;

  @BeforeMethod
  protected void checkMySQLRunning() {
    try {
      this.clusterManager = new MySQLClusterManager(
          "jdbc:mysql://localhost:3306/controller_test", "root", "");
      this.queue = new MySQLTaskQueue(
          "jdbc:mysql://localhost:3306/controller_test", "root", "");
    } catch (Exception e) {
      throw new SkipException("MySQL is not running correctly");
    }
    this.queue.createCluster(cluster);
    this.clusterManager.createCluster(cluster);
  }

  @AfterMethod
  protected void CleanUp() {
    this.queue.removeCluster(cluster);
  }

  @Test
  public void testRegister() {
    HostBean hostBean = new HostBean();
    hostBean.setIp("1.2.3.4").setPort(9090).setAvailabilityZone("us-east-1a");
    clusterManager.registerToCluster(cluster, hostBean);
    Set<HostBean> live_hosts = clusterManager.getHosts(cluster, false);
    Assert.assertEquals(live_hosts.size(), 1);
    Assert.assertTrue(live_hosts.contains(hostBean));
    hostBean = new HostBean();
    hostBean.setIp("2.3.4.5").setPort(9090).setAvailabilityZone("us-west-1a");
    clusterManager.registerToCluster(cluster, hostBean);
    live_hosts = clusterManager.getHosts(cluster, false);
    Assert.assertEquals(live_hosts.size(), 2);
    Assert.assertTrue(live_hosts.contains(hostBean));
  }

  @Test
  public void testUnregister() {
    HostBean hostBean = new HostBean();
    hostBean.setIp("1.2.3.4").setPort(9090).setAvailabilityZone("us-east-1a");
    clusterManager.registerToCluster(cluster, hostBean);
    Set<HostBean> live_hosts = clusterManager.getHosts(cluster, false);
    Assert.assertEquals(live_hosts.size(), 1);
    Assert.assertTrue(live_hosts.contains(hostBean));
    clusterManager.unregisterFromCluster(cluster, hostBean);
    live_hosts = clusterManager.getHosts(cluster, false);
    Assert.assertEquals(live_hosts.size(), 0);
  }

  @Test
  public void testBlacklist() {
    HostBean hostBean = new HostBean();
    hostBean.setIp("1.2.3.4").setPort(9090).setAvailabilityZone("us-east-1a");
    clusterManager.registerToCluster(cluster, hostBean);
    Set<HostBean> live_hosts = clusterManager.getHosts(cluster, false);
    Assert.assertEquals(live_hosts.size(), 1);
    Assert.assertTrue(live_hosts.contains(hostBean));
    clusterManager.blacklistHost(cluster, hostBean);
    Set<HostBean> blacklisted = clusterManager.getBlacklistedHosts(cluster);
    Assert.assertEquals(blacklisted.size(), 1);
    Assert.assertTrue(blacklisted.contains(hostBean));
    live_hosts = clusterManager.getHosts(cluster, true);
    Assert.assertEquals(live_hosts.size(), 0);
    clusterManager.unregisterFromCluster(cluster, hostBean);
    blacklisted = clusterManager.getBlacklistedHosts(cluster);
    Assert.assertEquals(blacklisted.size(), 0);
    live_hosts = clusterManager.getHosts(cluster, false);
    Assert.assertEquals(live_hosts.size(), 0);
  }

}

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

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksplicator.controller.bean.HostBean;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BadHostsStatsKeeperTest {

  @Test
  public void testCounting() {
    BadHostsStatesKeeper badHostsStatesKeeper = new BadHostsStatesKeeper();
    Set<HostBean> inputSet = new HashSet<>();
    HostBean bean = new HostBean().setIp("1.2.3.4").setPort(9090);
    inputSet.add(bean);
    List<HostBean> emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(!emailedHosts.isEmpty());
    Assert.assertEquals(emailedHosts.get(0), bean);
    // It goes into silence mode
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    Assert.assertEquals(badHostsStatesKeeper.getBadHostStates().get(bean).consecutiveFailures, 4);
  }

  @Test
  public void testReset() {
    BadHostsStatesKeeper badHostsStatesKeeper = new BadHostsStatesKeeper();
    Set<HostBean> inputSet = new HashSet<>();
    HostBean bean = new HostBean().setIp("1.2.3.4").setPort(9090);
    inputSet.add(bean);
    Set<HostBean> emptySet = new HashSet<>();
    List<HostBean> emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(emptySet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(!emailedHosts.isEmpty());
    Assert.assertEquals(emailedHosts.get(0), bean);
  }

  @Test
  public void testExpiration() throws InterruptedException {
    BadHostsStatesKeeper badHostsStatesKeeper = new BadHostsStatesKeeper(3, 5);
    Set<HostBean> inputSet = new HashSet<>();
    HostBean bean = new HostBean().setIp("1.2.3.4").setPort(9090);
    inputSet.add(bean);
    List<HostBean> emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(!emailedHosts.isEmpty());
    Assert.assertEquals(emailedHosts.get(0), bean);
    // It goes into silence mode
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(emailedHosts.isEmpty());
    Assert.assertEquals(badHostsStatesKeeper.getBadHostStates().get(bean).consecutiveFailures, 4);
    Thread.sleep(6000);
    emailedHosts = badHostsStatesKeeper.updateStatesAndGetHostsToEmail(inputSet);
    Assert.assertTrue(!emailedHosts.isEmpty());
    Assert.assertEquals(emailedHosts.get(0), bean);
  }

}

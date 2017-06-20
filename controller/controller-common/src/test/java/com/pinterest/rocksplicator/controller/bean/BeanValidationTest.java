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

package com.pinterest.rocksplicator.controller.bean;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class BeanValidationTest {

  private Validator validator;

  @BeforeTest
  public void setUp() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @DataProvider(name = "hostData")
  public Object[][] createHostData() {
    return new Object[][] {
        { "", 8090, "us-east-1a", "may not be empty" },
        { "127.0.0.1", -1, "us-east-1a", "must be greater than or equal to 0"},
        { "127.0.0.1", 65536, "us-east-1a", "must be less than or equal to 65535"},
        { "127.0.0.1", 8090, "", "may not be empty" },
    };
  }

  @DataProvider(name = "shardData")
  public Object[][] createShardData() {
    return new Object[][] {
        { -1, Role.MASTER, "must be greater than or equal to 0" },
        {12345, null, "may not be null"},
    };
  }

  @DataProvider(name = "segmentData")
  public Object[][] createSegmentData() {
    return new Object[][] {
        {"segment", 0, "must be greater than or equal to 1" },
        {"", 3, "may not be empty"},
    };
  }

  @DataProvider(name = "hostUrlQueryData")
  public Object[][] createHostUrlQuerydata() {
    return new Object[][] {
        {"127-1-2-3-9090-us-east-1a", true, "127.1.2.3", 9090, "us-east-1a"},
        {"127-1-2-3-9090", true, "127.1.2.3", 9090, ""},
        {"127-1-2-3-us-east-1a", false, "", 0, ""},
        {"127-1-9090-us-east-1a", false, "", 0, ""}
    };
  }

  @Test(dataProvider = "hostData")
  public void testHostBean(String ip, int port, String az, String errorMsg) {
    HostBean host = new HostBean().setIp(ip).setPort(port).setAvailabilityZone(az);

    Set<ConstraintViolation<HostBean>> constraintViolations = validator.validate(host);

    Assert.assertEquals(1, constraintViolations.size());
    Assert.assertEquals(errorMsg, constraintViolations.iterator().next().getMessage());
  }

  @Test(dataProvider = "shardData")
  public void testShardBean(int id, Role role, String errorMsg) {
    ShardBean shard = new ShardBean().setId(id).setRole(role);

    Set<ConstraintViolation<ShardBean>> constraintViolations = validator.validate(shard);

    Assert.assertEquals(1, constraintViolations.size());
    Assert.assertEquals(errorMsg, constraintViolations.iterator().next().getMessage());
  }

  @Test(dataProvider = "segmentData")
  public void testSegmentBean(String name, int numShards, String errorMsg) {
    SegmentBean segment = new SegmentBean().setName(name).setNumShards(numShards);

    Set<ConstraintViolation<SegmentBean>> constraintViolations = validator.validate(segment);

    Assert.assertEquals(1, constraintViolations.size());
    Assert.assertEquals(errorMsg, constraintViolations.iterator().next().getMessage());
  }

  @Test(dataProvider = "hostUrlQueryData")
  public void testParsingHostUrl(
      String queryString, boolean result, String ip, int port, String zone) {
    HostBean hostBean = HostBean.fromUrlParam(queryString);
    Assert.assertEquals(result, hostBean != null);
    if (result) {
      Assert.assertEquals(ip, hostBean.getIp());
      Assert.assertEquals(port, hostBean.getPort());
      Assert.assertEquals(zone, hostBean.getAvailabilityZone());
    }
  }
}

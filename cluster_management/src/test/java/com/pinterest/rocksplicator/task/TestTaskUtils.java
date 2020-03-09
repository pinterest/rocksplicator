package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.TaskUtils;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;
import org.testng.annotations.Test;


@RunWith(PowerMockRunner.class)
public class TestTaskUtils {

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testBuildStatePostUrl() throws Exception {
    String configPostUrl = "http://prefix.com/clusterA?mode=overwrite";
    String expectedStatePostUrl = "http://prefix.com/clusterA_state?mode=overwrite";
    String statePostUrl = TaskUtils.buildStatePostUrl(configPostUrl);
    Assert.assertEquals(statePostUrl, expectedStatePostUrl);
  }
}

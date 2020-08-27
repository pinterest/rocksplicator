package com.pinterest.rocksplicator.monitoring.mbeans;

import org.apache.helix.TestHelper;
import org.apache.helix.monitoring.mbeans.ClusterMBeanObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

public class TestRocksplicatorMonitor {

  static Logger
      LOG = LoggerFactory.getLogger(org.apache.helix.monitoring.TestParticipantMonitor.class);

  private static final String TEST_CLUSTER = "testCluster";

  // Listener impl copied from: org/apache/helix/monitoring/TestParticipantMonitor.java
  class ParticipantRocksplicatorMonitorListener extends ClusterMBeanObserver {

    Map<String, Map<String, Object>> _beanValueMap = new HashMap<String, Map<String, Object>>();

    public ParticipantRocksplicatorMonitorListener(String domain)
        throws InstanceNotFoundException, IOException,
               MalformedObjectNameException, NullPointerException {
      super(domain);
      init();
    }

    void init() {
      try {
        // Mbeans from different test class might exist at same time and queried by current Listener
        // thus, specific the Mbean name to query
        Set<ObjectInstance> existingInstances =
            _server
                .queryMBeans(new ObjectName(_domain + ":cluster=*,instanceName=localhost"), null);
        for (ObjectInstance instance : existingInstances) {
          String mbeanName = instance.getObjectName().toString();
          // System.out.println("mbeanName: " + mbeanName);
          addMBean(instance.getObjectName());
        }
      } catch (Exception e) {
        LOG.warn("fail to get all existing mbeans in " + _domain, e);
      }
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
                                  MBeanServerNotification mbsNotification) {
      addMBean(mbsNotification.getMBeanName());
    }

    void addMBean(ObjectName beanName) {
      try {
        MBeanInfo info = _server.getMBeanInfo(beanName);
        MBeanAttributeInfo[] infos = info.getAttributes();
        _beanValueMap.put(beanName.toString(), new HashMap<String, Object>());
        for (MBeanAttributeInfo infoItem : infos) {
          Object val = _server.getAttribute(beanName, infoItem.getName());
          // System.out.println("         " + infoItem.getName() + " : " +
          // _server.getAttribute(beanName, infoItem.getName()) + " type : " + infoItem.getType());
          _beanValueMap.get(beanName.toString()).put(infoItem.getName(), val);
        }
      } catch (Exception e) {
        LOG.error("Error getting bean info, domain=" + _domain, e);
      }
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
                                    MBeanServerNotification mbsNotification) {
    }

    public void printBeanValueMap() {
      for (String beanName : _beanValueMap.keySet()) {
        System.out.println(beanName);
        System.out.println(_beanValueMap.get(beanName).toString());
      }
    }
  }

  @Test
  public void testMBeansRegisteredAfterMonitorInit()
      throws InstanceNotFoundException, MalformedObjectNameException,
             NullPointerException, IOException, InterruptedException {
    System.out.println("START " + TestHelper.getTestMethodName());
    RocksplicatorMonitor monitor = new RocksplicatorMonitor(TEST_CLUSTER, "localhost");

    int monitorNum = 0;

    ParticipantRocksplicatorMonitorListener monitorListener =
        new ParticipantRocksplicatorMonitorListener(
            RocksplicatorMonitor.ROCKSPLICATOR_REPORT_DOMAIN);

    Thread.sleep(1000);

    monitorListener.printBeanValueMap();

    // only 1 mbean found: RocksplicatorReport:ParticipantName=localhost
    Assert.assertEquals(monitorListener._beanValueMap.size(), monitorNum + 1);
    String expectedParticipantBeanName =
        "RocksplicatorReport:cluster=testCluster,instanceName=localhost";
    Assert.assertEquals(monitorListener._beanValueMap.keySet(),
        new HashSet<>(Arrays.asList(expectedParticipantBeanName)));
    Assert.assertEquals(
        (long) monitorListener._beanValueMap.get(expectedParticipantBeanName)
            .get("ConfigGeneratorCalledCount"), 0);
    Assert.assertEquals(
        (long) monitorListener._beanValueMap.get(expectedParticipantBeanName)
            .get("ConfigGeneratorNullExternalView"), 0);
    Assert.assertEquals(
        (double) monitorListener._beanValueMap.get(expectedParticipantBeanName)
            .get("MeanConfigGeneratorLatency"), 0.0);

    monitorListener.disconnect();
    System.out.println("END " + TestHelper.getTestMethodName());
  }
}
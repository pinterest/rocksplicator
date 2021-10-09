package com.pinterest.rocksplicator.monitoring.mbeans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class RocksplicatorMonitor {

  private Logger LOG = LoggerFactory.getLogger(RocksplicatorMonitor.class);

  public static final String ROCKSPLICATOR_REPORT_DOMAIN = "RocksplicatorReport";
  public static final String CLUSTER_DN_KEY = "cluster";
  public static final String INSTANCE_DN_KEY = "instanceName";

  private MBeanServer beanServer;
  private RocksplicatorSpectatorMonitor spectatorStatsMonitor;
  private String cluster;
  private String instanceName;

  public RocksplicatorMonitor(String clusterName, String instanceName) {
    this.beanServer = ManagementFactory.getPlatformMBeanServer();
    this.cluster = clusterName;
    try {
      this.instanceName = InetAddress.getByName(instanceName.split("_")[0]).getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Error getting hostname, continue to use: " + instanceName, e);
      this.instanceName = instanceName;
    }

    try {
      spectatorStatsMonitor = new RocksplicatorSpectatorMonitor(this.instanceName);

      register(spectatorStatsMonitor, getObjectName(getInstanceBeanName(instanceName)));
    } catch (Exception e) {
      LOG.warn(e.toString());
      e.printStackTrace();
      beanServer = null;
    }
  }

  public void incrementConfigGeneratorCalledCount() {
    spectatorStatsMonitor.incrementConfigGeneratorCalledCount(1);
  }

  public void incrementConfigGeneratorFailCount() {
    spectatorStatsMonitor.incrementConfigGeneratorFailCount(1);
  }

  public void incrementConfigGeneratorNullExternalView() {
    spectatorStatsMonitor.incrementConfigGeneratorNullExternalView(1);
  }

  public void reportConfigGeneratorLatency(double latency) {
    spectatorStatsMonitor
        .addLatency(RocksplicatorSpectatorMonitor.LATENCY_TYPE.CONFIG_GENERATOR_EXECUTION, latency);
  }

  private ObjectName getObjectName(String name) throws MalformedObjectNameException {
    LOG.info("Registering bean: " + name);
    return new ObjectName(String.format("%s:%s", ROCKSPLICATOR_REPORT_DOMAIN, name));
  }

  public String clusterBeanName() {
    return String.format("%s=%s", CLUSTER_DN_KEY, cluster);
  }

  private String getInstanceBeanName(String instanceName) {
    return String.format("%s,%s=%s", clusterBeanName(), INSTANCE_DN_KEY, instanceName);
  }

  private void register(Object bean, ObjectName name) {
    if (beanServer == null) {
      LOG.warn("bean server is null, skip reporting");
      return;
    }
    try {
      beanServer.unregisterMBean(name);
    } catch (Exception e1) {
      // Swallow silently
    }

    try {
      beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      LOG.warn("Could not register MBean", e);
    }
  }

  public void close() {
    try {
      unregister(getObjectName(getInstanceBeanName(instanceName)));
    } catch (MalformedObjectNameException e) {
      e.printStackTrace();
    }
  }

  private void unregister(ObjectName name) {
    if (beanServer == null) {
      LOG.warn("bean server is null, nothing to do");
      return;
    }
    try {
      beanServer.unregisterMBean(name);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

package com.pinterest.rocksplicator.monitoring.mbeans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class RocksplicatorMonitor {

  private Logger LOG = LoggerFactory.getLogger(RocksplicatorMonitor.class);

  public static final String ROCKSPLICATOR_REPORT_DOMAIN = "RocksplicatorReport";

  private MBeanServer beanServer;
  private RocksplicatorSpectatorMonitor statsMonitor;

  public RocksplicatorMonitor(String participantName) {
    beanServer = ManagementFactory.getPlatformMBeanServer();

    try {
      statsMonitor = new RocksplicatorSpectatorMonitor(participantName);

      register(statsMonitor, getObjectName(statsMonitor.getBeanName()));
    } catch (Exception e) {
      LOG.warn(e.toString());
      e.printStackTrace();
      beanServer = null;
    }
  }

  public void incrementConfigGeneratorCalledCount() {
    statsMonitor.incrementConfigGeneratorCalledCount(1);
  }

  public void incrementConfigGeneratorNullExternalView() {
    statsMonitor.incrementConfigGeneratorNullExternalView(1);
  }

  public void reportConfigGeneratorLatency(double latency) {
    statsMonitor.addLatency(RocksplicatorSpectatorMonitor.LATENCY_TYPE.CONFIG_GENERATOR_EXECUTION, latency);
  }

  private ObjectName getObjectName(String name) throws MalformedObjectNameException {
    LOG.info("Registering bean: " + name);
    return new ObjectName(String.format("%s:%s", ROCKSPLICATOR_REPORT_DOMAIN, name));
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

}

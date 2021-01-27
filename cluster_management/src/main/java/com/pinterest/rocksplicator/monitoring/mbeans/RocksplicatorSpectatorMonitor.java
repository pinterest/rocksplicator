package com.pinterest.rocksplicator.monitoring.mbeans;

import org.apache.helix.monitoring.StatCollector;

import java.util.concurrent.ConcurrentHashMap;

public class RocksplicatorSpectatorMonitor
    implements RocksplicatorSpectatorMonitorMBean {

  public enum LATENCY_TYPE {
    CONFIG_GENERATOR_EXECUTION
  }

  private final String instanceName;
  private long configGeneratorCalledCount = 0;
  private long configGeneratorFailCount = 0;
  private long configGeneratorNullExternalView = 0;

  private ConcurrentHashMap<LATENCY_TYPE, StatCollector> latencyMonitorMap =
      new ConcurrentHashMap<LATENCY_TYPE, StatCollector>();


  public RocksplicatorSpectatorMonitor(String instanceName) {
    this.instanceName = instanceName;

    latencyMonitorMap.put(LATENCY_TYPE.CONFIG_GENERATOR_EXECUTION, new StatCollector());
    reset();
  }

  public void incrementConfigGeneratorCalledCount(int count) {
    configGeneratorCalledCount += count;
  }

  public void incrementConfigGeneratorFailCount(int count) {
    configGeneratorFailCount += count;
  }

  void addLatency(LATENCY_TYPE type, double latency) {
    assert (latencyMonitorMap.containsKey(type));
    latencyMonitorMap.get(type).addData(latency);
  }

  public void incrementConfigGeneratorNullExternalView(int count) {
    configGeneratorNullExternalView += count;
  }

  @Override
  public long getConfigGeneratorCalledCount() {
    return configGeneratorCalledCount;
  }

  @Override
  public long getConfigGeneratorFailCount() {
    return configGeneratorFailCount;
  }

  @Override
  public double getMeanConfigGeneratorLatency() {
    return latencyMonitorMap.get(LATENCY_TYPE.CONFIG_GENERATOR_EXECUTION).getMean();
  }

  @Override
  public double getMaxConfigGeneratorLatency() {
    return latencyMonitorMap.get(LATENCY_TYPE.CONFIG_GENERATOR_EXECUTION).getMax();
  }

  @Override
  public double getMinConfigGeneratorLatency() {
    return latencyMonitorMap.get(LATENCY_TYPE.CONFIG_GENERATOR_EXECUTION).getMin();
  }

  @Override
  public long getConfigGeneratorNullExternalView() {
    return configGeneratorNullExternalView;
  }

  public void reset() {
    for (StatCollector statCollector : latencyMonitorMap.values()) {
      statCollector.reset();
    }
  }
}

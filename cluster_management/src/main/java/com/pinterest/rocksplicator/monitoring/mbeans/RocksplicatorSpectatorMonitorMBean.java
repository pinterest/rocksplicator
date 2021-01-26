package com.pinterest.rocksplicator.monitoring.mbeans;

public interface RocksplicatorSpectatorMonitorMBean {
  // ConfigGenerator
  public long getConfigGeneratorCalledCount();
  public long getConfigGeneratorFailCount();
  public double getMeanConfigGeneratorLatency();
  public double getMaxConfigGeneratorLatency();
  public double getMinConfigGeneratorLatency();
  public long getConfigGeneratorNullExternalView();
}

package com.pinterest.rocksplicator.shardmap;

import java.util.Objects;

public class Instance {
  private final String host;
  private final int port;
  private final String domain;
  private final String instanceId;

  public Instance(String hostWithDomainName) {
    String[] parts = hostWithDomainName.split(":");
    this.host = parts[0];
    this.port = Integer.parseInt(parts[1]);
    this.domain = parts[2];
    this.instanceId = String.format("%s_%d", this.host, this.port);
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getDomain() {
    return domain;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Instance instance = (Instance) o;
    return port == instance.port &&
        host.equals(instance.host) &&
        domain.equals(instance.domain) &&
        instanceId.equals(instance.instanceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, domain, instanceId);
  }

  @Override
  public String toString() {
    return "Instance{" +
        "host='" + host + '\'' +
        ", port=" + port +
        ", domain='" + domain + '\'' +
        ", instanceId='" + instanceId + '\'' +
        '}';
  }
}

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

package com.pinterest.rocksplicator.controller.util;

import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksplicator.controller.bean.HostBean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Factory class to generate clients to rocksdb_admin.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class AdminClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AdminClientFactory.class);

  private final Lifecycle lifecycle;
  private final LoadingCache<InetSocketAddress, TTransport> clientPool;

  /**
   * Constructor of AdminClientFactory
   *
   * @param idleTimeout socket idle timeout in seconds
   */
  public AdminClientFactory(long idleTimeout) {
    lifecycle = new Lifecycle();
    clientPool = CacheBuilder.newBuilder()
        .expireAfterAccess(idleTimeout, TimeUnit.SECONDS)
        .removalListener(lifecycle)
        .build(lifecycle);
  }

  public Admin.Client getClient(InetSocketAddress addr) throws ExecutionException {
    try {
      TTransport transport = clientPool.get(addr);
      if (!transport.isOpen()) {
        clientPool.invalidate(addr);
        transport = clientPool.get(addr);
      }
      return new Admin.Client(new TBinaryProtocol(transport));
    } catch (ExecutionException ex) {
      LOG.error("Failed to get client for {}.", addr, ex);
      throw ex;
    }
  }

  public Admin.Client getClient(HostBean host) throws ExecutionException {
    return getClient(new InetSocketAddress(host.getIp(), host.getPort()));
  }

  public void shutdown() {
    clientPool.invalidateAll();
  }

  @VisibleForTesting
  LoadingCache<InetSocketAddress, TTransport> getClientPool() {
    return clientPool;
  }

  public static class Lifecycle extends CacheLoader<InetSocketAddress, TTransport>
      implements RemovalListener<InetSocketAddress, TTransport> {

    @Override
    public TTransport load(InetSocketAddress key) throws Exception {
      TSocket sock = new TSocket(key.getHostName(), key.getPort());
      sock.setConnectTimeout(5000);
      sock.open();
      return sock;
    }

    @Override
    public void onRemoval(RemovalNotification<InetSocketAddress, TTransport> notification) {
      notification.getValue().close();
    }
  }

}

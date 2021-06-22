/// Copyright 2021 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author prem (prem@pinterest.com)
//

package com.pinterest.rocksplicator.utils;

import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionStateManager {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStateManager.class);
    private static final String LOCAL_HOST_IP = "127.0.0.1";
    final CuratorFramework zkClient;
    final String cluster;
    final String resourceName;
    final String partitionName;
    final PartitionState oldState;

    public PartitionStateManager (
      final String cluster,
      final String resourceName,
      final String partitionName,
      final CuratorFramework zkClient) {
        this.cluster = cluster;
        this.resourceName = resourceName;
        this.partitionName = partitionName;
        this.zkClient = zkClient;
        oldState = new PartitionState();
    }

    public String getStatePath() {
      return String.format("/partitionstate/%s/%s/%s",cluster,resourceName,partitionName);
    }

    public PartitionState getState()  throws Exception {
      final int max_retries = 3;
      final AtomicInteger retryCount = new AtomicInteger(0);
      LOG.error("get.state : " + oldState.toString() );
      String statePath = getStatePath();
      return RetryLoop.callWithRetry(zkClient.getZookeeperClient(), new Callable<PartitionState>() {
          @Override
          public PartitionState call() throws Exception {
            retryCount.incrementAndGet();
            try {
              LOG.error(String .format("Attempt (%d-) to get state: %s", retryCount.get(), statePath));
              zkClient.sync().forPath(statePath);
              
              String stateData = new String(zkClient.getData().forPath(statePath));
              oldState.deserialize(stateData);
              LOG.error("got.state: " + oldState);
              return oldState;

            } catch (KeeperException.NoNodeException ex) {
              // By throwing the OperationTimeoutException, we force the retry, as NoNode exception is
              // not retryable.
              if (retryCount.get() < max_retries) {
                throw new KeeperException.OperationTimeoutException();
              } else {
                LOG.error(String.format("Path (%s) doesn't exist yet: ", statePath));
                oldState.clear();
                LOG.error("unable to fetch state: " + oldState);
                return oldState;
              }
            }
          }
        }
      );
    }

    public boolean saveSeqNum(long seqNum)  throws Exception {
      PartitionState state = new PartitionState();
      state.setSeqNum(seqNum);
      return saveState(state);
    }

    public boolean saveState(PartitionState state)  throws Exception {
      LOG.error("save.state : " + state + " = " + state.serialize());
      final int max_retries = 3;
      final AtomicInteger retryCount = new AtomicInteger(0);
      byte[] bytes = state.serialize().getBytes();
      String statePath = getStatePath();
      return RetryLoop.callWithRetry(zkClient.getZookeeperClient(), new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            retryCount.incrementAndGet();
            try {
              // LOG.error(String.format("Attempt (%d) to save state: %s", retryCount.get(), statePath));
              if (zkClient.checkExists().forPath(statePath) == null) {
                  zkClient.create().creatingParentsIfNeeded().forPath(statePath, bytes);
                } else {
                  zkClient.setData().forPath(statePath, bytes);
                }
              
              return true;

            } catch (KeeperException.NoNodeException ex) {
              // LOG.error(String.format("Path (%s) doesn't exist yet", statePath));
              // By throwing the OperationTimeoutException, we force the retry, as NoNode exception is
              // not retryable.
              if (retryCount.get() < max_retries) {
                throw new KeeperException.OperationTimeoutException();
              } else {
                  LOG.error(String.format("Path (%s) doesn't exist yet", statePath));
                  return false;
              }
            }
          }
        }
      );
    }
}
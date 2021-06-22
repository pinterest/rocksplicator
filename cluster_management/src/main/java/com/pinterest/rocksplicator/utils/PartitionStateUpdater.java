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

import com.pinterest.rocksplicator.Utils;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStateUpdater extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionStateUpdater.class);
  private final AtomicBoolean running = new AtomicBoolean(false);
  HashMap<LeaderInfo, PartitionState> leaders = new HashMap<LeaderInfo, PartitionState>();
  long runCount = 0;
  final long minWaitMs = 60 * 1000; // 1 minute
  
  final CuratorFramework zkClient;
  final String cluster;
  final int adminPort;

  class LeaderInfo {
    String resource;
    String partition;

    @Override
    public boolean equals(Object o) {        
      if (this == o) return true; 
      LeaderInfo li = (LeaderInfo) o;
      return resource == li.resource && partition == li.partition;
    }

    @Override
    public int hashCode() {
      int hash = 0;
      hash += resource == null ? 0 : resource.hashCode();
      hash += partition == null ? 0 : partition.hashCode();
      return hash;
    }
  } // End of LeaderInfo

  public PartitionStateUpdater(
    CuratorFramework zkClient,
    String cluster,
    int adminPort) {
      this.zkClient = zkClient;
      this.cluster = cluster;
      this.adminPort = adminPort;
  }
  
  public synchronized boolean shutdown() {
    if (!running.get()) {
      notify();
      return false;
    }
    running.set(false);
    notify();
    return true;
  }

  public synchronized boolean addLeader(String resource, String partition) {
    LeaderInfo li = new LeaderInfo();
    li.partition = partition;
    li.resource = resource;
    if (leaders.containsKey(li)) {
      return false;
    }
    leaders.put(li, new PartitionState());
    notify();
    return true;
  }

  public synchronized boolean removeLeader(String resource, String partition) {
    LeaderInfo li = new LeaderInfo();
    li.partition = partition;
    li.resource = resource;
    boolean success = leaders.remove(li) != null;
    notify();
    return success;
  }

  public void run() {
    running.set(true);
    LOG.error("PartitionStateUpdater started");
    long waitTime = minWaitMs;
    while(running.get()) {
      synchronized(this) {
        // update the states for all leaders
        for (LeaderInfo leader : leaders.keySet()) {
          try {
            String dbName = Utils.getDbName(leader.partition);
            long localSeq = Utils.getLocalLatestSequenceNumber(dbName, adminPort);
            PartitionState lastState = leaders.get(leader);

            // check if we have already updated a higher seqnum.
            if (lastState.seqNum >= localSeq ) {
              // we dont need to update
              LOG.error(String.format("[%s] No need to update old:%d new:%d", leader.partition, lastState.seqNum, localSeq));
              continue;
            } else {
              LOG.error(String.format("[%s] Updating old:%d new:%d", leader.partition, lastState.seqNum, localSeq));
              lastState.setSeqNum(localSeq);
              PartitionStateManager stateManager = new PartitionStateManager(cluster, leader.resource, leader.partition, zkClient);
              stateManager.saveSeqNum(localSeq);
            }
          } catch(Exception e) {
            LOG.error(String.format("Exception while storing state : %s",e));
          }
        }

        // calculate wait time
        runCount++;

        if (runCount % 8 == 0) {
          waitTime = minWaitMs;
        } else {
          waitTime *= 2;
        }
        try {
          wait(waitTime);
        } catch(InterruptedException e) {
          LOG.error("Thread was interrupted");
        }
      } // synchronized
    }
    LOG.error("PartitionStateUpdater shutdown");
  }

}
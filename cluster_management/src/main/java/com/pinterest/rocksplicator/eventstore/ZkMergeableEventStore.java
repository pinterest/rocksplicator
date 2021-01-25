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
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.CodecException;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class ZkMergeableEventStore<R, E> implements MergeableReadWriteStore<R, E> {

  private final AtomicBoolean pathExists;
  private final String eventHistoryPath;
  private final String partitionLockPath;
  private final BatchMergeOperator<R> mergeOperator;
  private final Supplier<R> zeroSupplier;
  private final Codec<R, byte[]> recordCodec;
  private final CuratorFramework zkClient;
  private final Semaphore ipMutexGuard = new Semaphore(1);
  private InterProcessMutex interProcessPartitionMutex;


  public ZkMergeableEventStore(
      CuratorFramework zkClient,
      final String clusterName,
      final String resourceName,
      final String partitionName,
      final Codec<R, byte[]> recordCodec,
      final Supplier<R> zeroSupplier,
      final BatchMergeOperator<R> mergeOperator) {
    Preconditions.checkNotNull(clusterName);
    Preconditions.checkNotNull(resourceName);
    Preconditions.checkNotNull(partitionName);
    this.zkClient = Preconditions.checkNotNull(zkClient);
    this.pathExists = new AtomicBoolean(false);
    this.mergeOperator = Preconditions.checkNotNull(mergeOperator);
    this.recordCodec = Preconditions.checkNotNull(recordCodec);
    this.zeroSupplier = Preconditions.checkNotNull(zeroSupplier);

    this.eventHistoryPath =
        getLeaderEventHistoryPath(clusterName, resourceName, partitionName);
    this.partitionLockPath =
        getPartitionLockPath(clusterName, resourceName, partitionName);

    this.ipMutexGuard.acquireUninterruptibly();
    try {
      this.interProcessPartitionMutex = new InterProcessMutex(zkClient, partitionLockPath);
    } finally {
      this.ipMutexGuard.release();
    }

    this.zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
          case LOST:
          case SUSPENDED:
          case READ_ONLY:
            ipMutexGuard.acquireUninterruptibly();
            try {
              if (interProcessPartitionMutex != null && interProcessPartitionMutex
                  .isAcquiredInThisProcess()) {
                try {
                  interProcessPartitionMutex.release();
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  interProcessPartitionMutex = null;
                }
              }
            } finally {
              ipMutexGuard.release();
            }
            break;
          case CONNECTED:
          case RECONNECTED:
            ipMutexGuard.acquireUninterruptibly();
            try {
              if (interProcessPartitionMutex != null && interProcessPartitionMutex
                  .isAcquiredInThisProcess()) {
                try {
                  interProcessPartitionMutex.release();
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  interProcessPartitionMutex = null;
                }
              }
              interProcessPartitionMutex = new InterProcessMutex(zkClient, partitionLockPath);
            } finally {
              ipMutexGuard.release();
            }
            break;
        }
      }
    });
  }

  public static String getPartitionLockPath(String cluster, String resourceName,
                                            String partitionName) {
    return "/rocksplicator/eventhistory/" + cluster + "/" + resourceName + "/" + partitionName
        + "/lock";
  }

  public static String getLeaderEventHistoryPath(String cluster, String resourceName,
                                                 String partitionName) {
    return "/rocksplicator/eventhistory/" + cluster + "/" + resourceName + "/" + partitionName
        + "/eventhistory";
  }

  private void ensurePathExists() {
    while (!pathExists.get()) {
      try {
        Stat stat = zkClient.checkExists().forPath(eventHistoryPath);
        if (stat == null) {
          zkClient.create().creatingParentsIfNeeded().forPath(eventHistoryPath,
              recordCodec.encode(zeroSupplier.get()));
        }
        this.pathExists.set(true);
      } catch (CodecException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public R read() throws IOException {
    ipMutexGuard.acquireUninterruptibly();
    try {
      if (interProcessPartitionMutex == null) {
        throw new IOException("interProcessPartitionMutex couldn't be obtained");
      }
      try (Locker locker = new Locker(interProcessPartitionMutex)) {
        try {
          zkClient.sync().forPath(eventHistoryPath);
          byte[] dataFromStore = zkClient.getData().forPath(eventHistoryPath);
          if (dataFromStore == null) {
            return null;
          }
          return recordCodec.decode(dataFromStore);
        } catch (CodecException e) {
          throw new IOException(e);
        } catch (Exception e) {
          throw new IOException(e);
        }
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      ipMutexGuard.release();
    }
  }

  @Override
  public R mergeBatch(R updateRecord) throws IOException {
    ipMutexGuard.acquireUninterruptibly();
    try {
      if (interProcessPartitionMutex == null) {
        throw new IOException("interProcessPartitionMutex couldn't be obtained");
      }
      try (Locker locker = new Locker(interProcessPartitionMutex)) {
        ensurePathExists();
        try {
          zkClient.sync().forPath(eventHistoryPath);
          byte[] dataFromStore = zkClient.getData().forPath(eventHistoryPath);
          if (dataFromStore == null) {
            dataFromStore = recordCodec.encode(zeroSupplier.get());
          }
          R record = recordCodec.decode(dataFromStore);
          R mergedRecord = mergeOperator.apply(record, updateRecord);
          byte[] appendedData = recordCodec.encode(mergedRecord);
          zkClient.setData().forPath(eventHistoryPath, appendedData);
          return mergedRecord;
        } catch (CodecException e) {
          throw new IOException(e);
        } catch (Exception e) {
          throw new IOException(e);
        }
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      ipMutexGuard.release();
    }
  }


  @Override
  public void close() throws IOException {
    ipMutexGuard.acquireUninterruptibly();
    try {
      if (interProcessPartitionMutex != null && interProcessPartitionMutex
          .isAcquiredInThisProcess()) {
        interProcessPartitionMutex.release();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      interProcessPartitionMutex = null;
      ipMutexGuard.release();
    }
  }
}



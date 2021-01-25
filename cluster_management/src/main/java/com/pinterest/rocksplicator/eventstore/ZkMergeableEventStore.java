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
  private final SingleMergeOperator<R, E> singleMergeOperator;
  private final BatchMergeOperator<R> batchMergeOperator;
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
      final SingleMergeOperator<R, E> singleMergeOperator,
      final BatchMergeOperator<R> batchMergeOperator) {
    Preconditions.checkNotNull(clusterName);
    Preconditions.checkNotNull(resourceName);
    Preconditions.checkNotNull(partitionName);
    this.zkClient = Preconditions.checkNotNull(zkClient);
    this.pathExists = new AtomicBoolean(false);

    this.singleMergeOperator = Preconditions.checkNotNull(singleMergeOperator);
    this.batchMergeOperator = Preconditions.checkNotNull(batchMergeOperator);
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
  public synchronized R merge(E event) throws IOException {
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
          R mergedRecord = singleMergeOperator.apply(record, event);
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
          R mergedRecord = batchMergeOperator.apply(record, updateRecord);
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



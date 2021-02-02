package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.SimpleJsonObjectDecoder;
import com.pinterest.rocksplicator.config.ConfigNotifier;
import com.pinterest.rocksplicator.config.FileWatchers;
import com.pinterest.rocksplicator.shardmap.ShardMap;
import com.pinterest.rocksplicator.shardmap.ShardMaps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ClientShardMapLeaderEventLoggerDriver implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      ClientShardMapLeaderEventLoggerDriver.class);

  private final String clusterName;
  private final String shardMapPath;
  private final ConfigNotifier<JSONObject> notifier;
  private final ClientShardMapLeaderEventLogger clientLeaderEventLogger;
  private final String zkEventHistoryStr;
  private final CuratorFramework zkClient;
  private final ExecutorService service;
  private final Semaphore ipMutexGuard = new Semaphore(1);
  private InterProcessMutex interProcessPartitionMutex;

  public ClientShardMapLeaderEventLoggerDriver(
      final String clusterName,
      final String shardMapPath,
      final ClientShardMapLeaderEventLogger clientLeaderEventLogger,
      final String zkEventHistoryStr) {
    this.clusterName = clusterName;
    this.shardMapPath = shardMapPath;
    this.clientLeaderEventLogger = clientLeaderEventLogger;
    this.zkEventHistoryStr = zkEventHistoryStr;

    this.service = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread();
      // Ensure that this thread is a daemon thread, since
      // this thread may potentially be blocked on obtaining
      // a global lock.
      thread.setDaemon(false);
      System.out.println(thread.getName() + "from : " + Thread.currentThread().getName());
      return thread;
    });

    /**
     * This will always start in a stopped state.
     * We need to manually start the notification.
     */
    ConfigNotifier<JSONObject> localNotifier = null;
    if (shardMapPath != null && !shardMapPath.isEmpty()) {
      localNotifier = new ConfigNotifier<>(
          new SimpleJsonObjectDecoder(),
          shardMapPath,
          FileWatchers.getPollingPerSecondFileWatcher(),
          jsonObjectContext -> {
            process(jsonObjectContext);
            return null;
          });
    }
    this.notifier = localNotifier;

    this.zkClient = CuratorFrameworkFactory
        .newClient(zkEventHistoryStr, new ExponentialBackoffRetry(1000, 3));

    this.zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
          case LOST:
          case SUSPENDED:
          case READ_ONLY:
            System.out.println("Zk Disconnected : " + newState  + "from : " + Thread.currentThread().getName());
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
            System.out.println("Stopping notification"  + "from : " + Thread.currentThread().getName());
            stopNotification();
            break;
          case CONNECTED:
          case RECONNECTED:
            System.out.println("Zk Connected : " + newState  + "from : " + Thread.currentThread().getName());
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
              interProcessPartitionMutex =
                  new InterProcessMutex(zkClient,
                      getLeaderHandoffClientClusterLockPath(clusterName));
            } finally {
              ipMutexGuard.release();
            }
            System.out.println("Starting notification "  + "from : " + Thread.currentThread().getName());
            startNotification();
            System.out.println("Finished notification "  + "from : " + Thread.currentThread().getName());
            notificationRunner();
            break;
          default:
        }
      }
    });

    System.out.println("Zk Starting :"  + "from : " + Thread.currentThread().getName());
    this.zkClient.start();

    try {
      zkClient.blockUntilConnected(60, TimeUnit.SECONDS);
      System.out.println("Zk unblocked from connected :"  + "from : " + Thread.currentThread().getName());
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.out.println("Cannot connect to zk in 60 seconds"  + "from : " + Thread.currentThread().getName());
      throw new RuntimeException(e);
    }
  }

  private String getLeaderHandoffClientClusterLockPath(String clusterName) {
    return "/leadereventhistory/client-exclusion-lock/" + clusterName;
  }

  private void startNotification() {
    System.out.println("Called startNotification"  + "from : " + Thread.currentThread().getName());
    if (this.notifier == null || this.notifier.isStarted() || this.notifier.isClosed()) {
      LOGGER.error("Either the notification thread already started "
          + "or is closed and cannot be restarted anymore.");
      return;
    }
    System.out.println("Submitting notificationRunner to be run"  + "from : " + Thread.currentThread().getName());

    try {
      service.submit(() -> notificationRunner());
      System.out.println("Submitted task"  + "from : " + Thread.currentThread().getName());
    } catch (Throwable throwable) {
      System.out.println(throwable);
    }
  }

  private void stopNotification() {
    System.out.println("Called stopNotification"  + "from : " + Thread.currentThread().getName());
    if (this.notifier == null || this.notifier.isClosed() || !this.notifier.isStarted()) {
      // Cannot stop notification, as either notifier is already closed or not started yet
      return;
    }
    System.out.println("Calling stop on notifier"  + "from : " + Thread.currentThread().getName());
    this.notifier.stop();
  }

  @Override
  public synchronized void close() throws IOException {
    /**
     * Make sure no more tasks can be submited.
     */
    System.out.println("Called close "  + "from : " + Thread.currentThread().getName());
    service.shutdown();

    stopNotification();

    if (this.notifier != null && !this.notifier.isClosed()) {
      try {
        this.notifier.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    /**
     * Next close the zkClient.
     */
    this.zkClient.close();

    notifier.notifyAll();
    /**
     * Release the service.
     */
    while (!service.isTerminated()) {
      try {
        service.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void process(ConfigNotifier.Context<JSONObject> shardMapWithContext) {
    System.out.println("New Json: " + shardMapWithContext.getItem().toJSONString()  + "from : " + Thread.currentThread().getName());
    JSONObject jsonShardMap = shardMapWithContext.getItem();
    ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
    clientLeaderEventLogger
        .process(shardMap, shardMapWithContext.getNotification_received_time_millis());
  }

  private void notificationRunner() {
    System.out.println("starting notificationRunner");
    System.out.println("Obtaininig lock");
    System.out.flush();
    try (Locker locker = new Locker(interProcessPartitionMutex)) {
      System.out.println("Obtained lock");
      System.out.println("Starting the notification process");
      if (!notifier.isClosed() && !notifier.isStarted()) {
        notifier.start();
        /**
         * We must wait here until someone releases us.
         * There are multiple places where that can happen.
         * e.g.. if zk connection is lost, we must retry
         */
        // Wait in this thread until a condition is not satisfied.
        System.out.println("Going to wait for someone to tell me to break out of this thread");
      }
    } catch (Exception e) {
      LOGGER.error("Failed to release the interProcessPartitionMutex for cluster " + clusterName,
          e);
    }
  }
}

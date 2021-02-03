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
  private final Semaphore ipMutexGuard = new Semaphore(1);
  private InterProcessMutex ipClusterMutex;
  private ExecutorService service;

  public ClientShardMapLeaderEventLoggerDriver(
      final String clusterName,
      final String shardMapPath,
      final ClientShardMapLeaderEventLogger clientLeaderEventLogger,
      final String zkEventHistoryStr) throws IOException {
    this.clusterName = clusterName;
    this.shardMapPath = shardMapPath;
    this.clientLeaderEventLogger = clientLeaderEventLogger;
    this.zkEventHistoryStr = zkEventHistoryStr;

    /**
     * This will always start in a stopped state.
     * We need to manually start the notification.
     */
    ConfigNotifier<JSONObject> localNotifier = null;
    if (this.shardMapPath != null && !this.shardMapPath.isEmpty()) {
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

    this.service = Executors.newSingleThreadExecutor();

    if (this.notifier == null) {
      this.zkClient = null;
    } else {
      this.zkClient = CuratorFrameworkFactory
          .newClient(zkEventHistoryStr, new ExponentialBackoffRetry(1000, 3));

      this.zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
          switch (newState) {
            case LOST:
            case SUSPENDED:
            case READ_ONLY:
              ipMutexGuard.acquireUninterruptibly();
              try {
                if (ipClusterMutex != null && ipClusterMutex
                    .isAcquiredInThisProcess()) {
                  try {
                    ipClusterMutex.release();
                  } catch (Exception e) {
                    e.printStackTrace();
                  } finally {
                    ipClusterMutex = null;
                  }
                }
              } finally {
                ipMutexGuard.release();
              }
              stopNotification();
              break;
            case CONNECTED:
            case RECONNECTED:
              ipMutexGuard.acquireUninterruptibly();
              try {
                if (ipClusterMutex != null && ipClusterMutex
                    .isAcquiredInThisProcess()) {
                  try {
                    ipClusterMutex.release();
                  } catch (Exception e) {
                    e.printStackTrace();
                  } finally {
                    ipClusterMutex = null;
                  }
                }
                ipClusterMutex =
                    new InterProcessMutex(zkClient,
                        getLeaderHandoffClientClusterLockPath(clusterName));
              } finally {
                ipMutexGuard.release();
              }
              startNotification();
              break;
            default:
          }
        }
      });

      this.zkClient.start();

      try {
        zkClient.blockUntilConnected(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("Cannot connect to zk in 60 seconds");
        throw new RuntimeException(e);
      }
    }
  }

  private String getLeaderHandoffClientClusterLockPath(String clusterName) {
    return "/leadereventhistory/client-exclusion-lock/" + clusterName;
  }

  private synchronized void startNotification() {
    if (this.notifier == null) {
      return;
    } else {
      synchronized (notifier) {
        if (this.notifier.isStarted() || this.notifier.isClosed()) {
          LOGGER.error("Either the notification thread already started "
              + "or is closed and cannot be restarted anymore.");
          return;
        }
      }
    }

    try {
      service.submit(() -> notificationRunner());
    } catch (Throwable throwable) {
      System.out.println(throwable);
    }
  }

  private synchronized void stopNotification() {
    if (this.notifier == null) {
      return;
    }

    synchronized (notifier) {
      if (this.notifier.isClosed() || !this.notifier.isStarted()) {
        // Cannot stop notification, as either notifier is already closed or not started yet
        return;
      }
      this.notifier.stop();
      this.notifier.notifyAll();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    LOGGER.error("Closing ClientShardMapLeaderEventLoggerDriver");

    /**
     * Make sure no more tasks can be submited.
     */

    /**
     * No more events from zk can startNotification. This will also
     * release any previously held lock by any previously submitted task.
     * However some thread may be waiting for notify.
     */
    try {
      LOGGER.error("Closing zkClient for ClientShardMapLeaderEventLoggerDriver");
      this.zkClient.close();
    } catch (Throwable throwable) {
      LOGGER.error("Cannot close the zkClient", throwable);
    }

    /**
     * Stop submission of any more tasks.
     */
    try {
      LOGGER.error("Shutting down executor service for ClientShardMapLeaderEventLoggerDriver");
      service.shutdown();
    } catch (Throwable throwable) {
      LOGGER.error("Cannot shutdown executor service", throwable);
    }

    /**
     * Stop processing all the notification going forward. After this any
     * previously submitted tasks will not execute and simply return without
     * performing any task. Any previously submitted and running task that did
     * obtain a zk lock should be waiting to be notified and hence this will
     * release those threads as well.
     */
    try {
      LOGGER.error("stopping notifications for shardMap for ClientShardMapLeaderEventLoggerDriver");
      stopNotification();
    } catch (Throwable throwable) {
      LOGGER.error("Could not stop notifications", throwable);
    }

    /**
     * From now on, any execution of remaining tasks will be no-op.
     */
    try {
      if (notifier != null) {
        synchronized (notifier) {
          if (!this.notifier.isClosed()) {
            // Cannot stop notification, as either notifier is already closed or not started yet;
            if (this.notifier.isStarted()) {
              this.notifier.start();
            }
            this.notifier.close();
            this.notifier.notifyAll();
          }
        }
      }
    } catch (Throwable throwable) {
      LOGGER.error("Cannot close the notifier", throwable);
    }

    /**
     * Release the service.
     */
    LOGGER.error("Wait for executor service to terminate for ClientShardMapLeaderEventLoggerDriver");
    while (!service.isTerminated()) {
      try {
        service.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted", e);
      }
    }
    LOGGER.error("Terminated executor service for ClientShardMapLeaderEventLoggerDriver");


    /**
     * Finally cleanup the clientShardMapEventsLogger.
     */
    clientLeaderEventLogger.close();
  }

  private void process(ConfigNotifier.Context<JSONObject> shardMapWithContext) {
    JSONObject jsonShardMap = shardMapWithContext.getItem();
    ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
    clientLeaderEventLogger
        .process(shardMap, shardMapWithContext.getSrc_change_time_millis());
  }

  private void notificationRunner() {
    if (notifier == null) {
      return;
    }

    /**
     * A process is blocked obtaining the lock, until it owns a globally distributed lock.
     * This is what ensures that there is only one leader performing notifications.
     */
    try (Locker locker = new Locker(ipClusterMutex)) {
      synchronized (notifier) {
        if (!notifier.isClosed() && !notifier.isStarted()) {
          notifier.start();
          /**
           * We must wait here until someone releases us.
           * There are multiple places where that can happen.
           * e.g.. if zk connection is lost, we must retry
           */
          // Wait in this thread until a condition is not satisfied.
          // This will also release the monitor on notifier.
          notifier.wait();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to obtain the ipClusterMutex for cluster " + clusterName, e);
    }
  }
}

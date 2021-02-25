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

package com.pinterest.rocksplicator.shardmapagent;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.pinterest.rocksplicator.codecs.CodecException;
import com.pinterest.rocksplicator.codecs.ZkGZIPCompressedShardMapCodec;
<<<<<<< HEAD
import com.pinterest.rocksplicator.codecs.ZkShardMapCodec;
=======
>>>>>>> master
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.CloseableExecutorService;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is the main class
 */
public class ClusterShardMapAgent implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterShardMapAgent.class);

  private final boolean CACHE_DATA = true;
  private final boolean DO_NOT_COMPRESS = false;

  private final String shardMapDir;
  private final String tempShardMapDir;
  private final String clusterName;
  private final String zkConnectString;
  private final CuratorFramework zkShardMapClient;
  private final PathChildrenCache pathChildrenCache;
  private final ConcurrentHashMap<String, JSONObject> shardMapsByResources;
  private final ZkShardMapCodec zkShardMapCompressedCodec;
  private final ScheduledExecutorService dumperExecutorService;
  private final AtomicInteger numPendingNotifications;

  public ClusterShardMapAgent(String zkConnectString, String clusterName, String shardMapDir) {
    this.clusterName = clusterName;
    this.shardMapDir = shardMapDir;
    this.tempShardMapDir = shardMapDir + "/" + ".temp";
    this.zkConnectString = zkConnectString;

    this.zkShardMapClient = CuratorFrameworkFactory
        .newClient(this.zkConnectString,
            new BoundedExponentialBackoffRetry(
                100, 10000, 10));

    this.zkShardMapClient.start();
    try {
      this.zkShardMapClient.blockUntilConnected(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException();
    }
    this.shardMapsByResources = new ConcurrentHashMap<>();
    this.zkShardMapCompressedCodec = new ZkGZIPCompressedShardMapCodec();

    this.pathChildrenCache = new PathChildrenCache(
        zkShardMapClient,
        ZkPathUtils.getClusterShardMapParentPath(this.clusterName),
        CACHE_DATA, DO_NOT_COMPRESS,
        new CloseableExecutorService(Executors.newSingleThreadExecutor()));

    this.dumperExecutorService = Executors.newSingleThreadScheduledExecutor();

    new File(this.shardMapDir).mkdirs();
    new File(this.tempShardMapDir).mkdirs();

    this.numPendingNotifications = new AtomicInteger(0);
  }

  /**
   * Should only be called once.
   */
  public synchronized void startNotification() throws Exception {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    LOG.info(String.format("Initializing shardMap for cluster=%s", clusterName));
    this.pathChildrenCache.getListenable()
        .addListener(new PathChildrenCacheListener() {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
              throws Exception {
            switch (event.getType()) {
              case INITIALIZED: {
                List<ChildData> childrenData = event.getInitialData();
                if (childrenData != null) {
                  for (ChildData childData : childrenData) {
                    if (childData != null) {
                      addOrUpdateResourceShardMap(childData.getPath(), childData.getData());
                    }
                  }
                }
              }
              numPendingNotifications.incrementAndGet();
              countDownLatch.countDown();
              break;
              case CHILD_ADDED:
              case CHILD_UPDATED: {
                ChildData childData = event.getData();
                if (childData != null) {
                  addOrUpdateResourceShardMap(childData.getPath(), childData.getData());
                }
              }
              numPendingNotifications.incrementAndGet();
              break;
              case CHILD_REMOVED: {
                ChildData childData = event.getData();
                if (childData != null) {
                  removeResource(childData.getPath());
                }
              }
              numPendingNotifications.incrementAndGet();
              break;
            }
          }
        });

    /**
     * This will initialize the cluster data in background.
     */
    this.pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

    countDownLatch.await();
    LOG.info(String.format("Initialized shardMap for cluster=%s", clusterName));

    final CountDownLatch firstDumpComplete = new CountDownLatch(1);
    this.dumperExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        int pendingNotifications = numPendingNotifications.getAndSet(0);
        if (pendingNotifications > 0) {
          LOG.info(String.format(
              "Coalesced shardMap notifications for cluster=%s (num_events=%d) ",
              clusterName, pendingNotifications));
          try {
            writeShardMapFile();
            firstDumpComplete.countDown();
          } catch (Throwable throwable) {
            // Never allow any error to propagate to kill scheduler thread, else subsequent
            // tasks will not be scheduled.
            LOG.error("Error while dumping shardMaps", throwable);
          }
        }
      }
    }, 0, 100, TimeUnit.MILLISECONDS);
    firstDumpComplete.await();
  }

  private void addOrUpdateResourceShardMap(String resourcePath, byte[] data) {
    if (data == null) {
      return;
    }
    try {
      String[] splits = resourcePath.split("/");
      if (splits.length <= 0) {
        return;
      }
      String resourceName = splits[splits.length - 1];
      try {
        JSONObject jsonObject = zkShardMapCompressedCodec.decode(data);
        this.shardMapsByResources.put(resourceName, jsonObject);
      } catch (CodecException e) {
        LOG.error(String.format(
            "Cannot decode resourcemap cluster: %s, resource: %s",
            clusterName, resourceName), e);
      }
    } catch (Throwable throwable) {
      LOG.error(String.format(
          "Cannot split resourcePath cluster: %s, resourcePath: %s",
          clusterName, resourcePath), throwable);
    }
  }

  private void removeResource(String resourcePath) {
    try {
      String[] splits = resourcePath.split("/");
      if (splits.length <= 0) {
        return;
      }
      String resourceName = splits[splits.length - 1];
      this.shardMapsByResources.remove(resourceName);
    } catch (Throwable throwable) {
      LOG.error(String.format(
          "Cannot split resourcePath cluster: %s, resourcePath: %s",
          clusterName, resourcePath), throwable);
    }
  }

  private void writeShardMapFile() {
    try {
      Map<String, JSONObject> localCopy = new HashMap<>(this.shardMapsByResources);

      // Create a Cluster ShardMap.
      JSONObject clusterShardMap = new JSONObject();

      for (Map.Entry<String, JSONObject> entry : localCopy.entrySet()) {
        String resourceName = entry.getKey();
        JSONObject resourceMap = (JSONObject) entry.getValue().get("shard_map");
        clusterShardMap.put(resourceName, resourceMap.get(resourceName));
      }

      try {
        File tempFile = File.createTempFile(
            String.format("%s-", clusterName),
            String.format("-%d", System.currentTimeMillis()),
            new File(tempShardMapDir));

        LOG.info(String.format(
            "Dumping new shard_map for cluster=%s at file_location: %s",
            clusterName, tempFile.getPath()));

        FileWriter fileWriter = new FileWriter(tempFile);
        fileWriter.write(clusterShardMap.toString());
        fileWriter.close();

        // Now move the dumped data file to intended destination file.
        File finalDestinationFile = new File(shardMapDir, clusterName);
        LOG.info(String.format(
            "Moving shard_map file for cluster=%s from: %s -> to: %s",
            clusterName,
            tempFile.getPath(),
            finalDestinationFile.getPath()));

        try {
          Files.move(tempFile.toPath(), finalDestinationFile.toPath(), ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ae) {
          try {
            Files.move(tempFile.toPath(), finalDestinationFile.toPath(), REPLACE_EXISTING);
          } catch (Exception moveExp) {
            LOG.error(String.format(
                "Cannot replace shard_map file for cluster: %s from: %s -> to: %s",
                clusterName,
                tempFile.getPath(),
                finalDestinationFile.getPath()),
                moveExp);
          }
        }
      } catch (IOException e) {
        LOG.error(String.format("Error dumping shard_map for cluster=%s", clusterName), e);
      }
    } catch (Throwable throwable) {
      LOG.error(String.format("Error dumping shard_map for cluster=%s", clusterName), throwable);
    }
  }

  @Override
  public void close() throws IOException {
    dumperExecutorService.shutdown();

    while (!dumperExecutorService.isTerminated()) {
      try {
        dumperExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    this.pathChildrenCache.close();
    this.zkShardMapClient.close();
  }
}

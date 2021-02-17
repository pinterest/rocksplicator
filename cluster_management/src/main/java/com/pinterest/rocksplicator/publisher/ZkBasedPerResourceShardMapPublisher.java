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

package com.pinterest.rocksplicator.publisher;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.Codecs;
import com.pinterest.rocksplicator.codecs.JSONObjectCodec;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.helix.model.ExternalView;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZkBasedPerResourceShardMapPublisher implements ShardMapPublisher<JSONObject> {

  private static final int MAX_THREADS = 16;
  private static final Logger
      LOG =
      LoggerFactory.getLogger(ZkBasedPerResourceShardMapPublisher.class);

  private static final Codec<JSONObject, byte[]> baseJsonObjectCodec = new JSONObjectCodec();
  private static final Codec<JSONObject, byte[]> gzipCompressedJsonObjectCodec =
      Codecs.getCompressedCodec(baseJsonObjectCodec, CompressionAlgorithm.GZIP);

  private final String clusterName;
  private final String zkShardMapConnectString;
  private final CuratorFramework zkShardMapClient;
  private final List<ExecutorService> executorServices;

  public ZkBasedPerResourceShardMapPublisher(
      final String clusterName,
      final String zkShardMapConnectString) {
    this.clusterName = Preconditions.checkNotNull(clusterName);
    this.zkShardMapConnectString = Preconditions.checkNotNull(zkShardMapConnectString);

    this.zkShardMapClient = CuratorFrameworkFactory.newClient(this.zkShardMapConnectString,
        new BoundedExponentialBackoffRetry(100, 1000, 10));

    try {
      this.zkShardMapClient.blockUntilConnected(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    this.executorServices = Lists.newArrayListWithCapacity(MAX_THREADS);
    for (int i = 0; i < MAX_THREADS; ++i) {
      this.executorServices.add(Executors.newSingleThreadExecutor());
    }
  }

  private static String getId(String cluster, String resource) {
    StringBuilder builder = new StringBuilder()
        .append(cluster)
        .append('/')
        .append(resource);
    return builder.toString();
  }

  @Override
  public void publish(
      final Set<String> validResources,
      final List<ExternalView> externalViews,
      final JSONObject shardMap) {

    Map<String, ExternalView> externalViewMap = new HashMap<>();

    for (ExternalView externalView : externalViews) {
      if (externalView != null) {
        String resourceName = externalView.getResourceName();
        if (shardMap.containsKey(resourceName)) {
          externalViewMap.put(resourceName, externalView);
        }
      }
    }

    // Ensure for each of the resources, we have the externalView.
    for (Object resourceObj : shardMap.keySet()) {
      Preconditions.checkArgument(externalViewMap.containsKey(resourceObj));
    }

    for (Object resourceObj : shardMap.keySet()) {
      String resource = (String) resourceObj;
      JSONObject resourceConfig = (JSONObject) shardMap.get(resourceObj);
      asyncWriteToZkGZIPCompressedPerResourceShardMap(
          clusterName, resource, externalViewMap.get(resource), resourceConfig);
    }
    removeResourceFromZkGZIPCompressedPerResourceShardMap(
        clusterName,
        validResources.stream().collect(Collectors.toSet()));
  }

  private ExecutorService getExecutorService(String resourceName) {
    return this.executorServices.get(
        IntMath.mod(getId(clusterName, resourceName).hashCode(), MAX_THREADS));
  }

  /**
   * Asynchronously publish a compressed, per resource shardMap
   * to provided zk for storing shardMap. If we see that per resource
   * shardMap publishing to zk takes longer time, we have an opportunity
   * to parallelize publishing by making sure that each per resource
   * shardMap is published into specific queues and hence specific
   * executorService.
   */
  private void asyncWriteToZkGZIPCompressedPerResourceShardMap(
      final String clusterName,
      final String resourceName,
      final ExternalView externalView,
      final JSONObject perResourceShardMap) {

    final JSONObject topLevelJSONObject = new JSONObject();
    final JSONObject metaBlock = new JSONObject();

    final JSONObject sharedMapObj = new JSONObject();
    sharedMapObj.put(resourceName, perResourceShardMap);

    if (externalView.getStat() != null) {
      metaBlock.put("external_view_modified_time_ms", externalView.getStat().getModifiedTime());
      metaBlock.put("external_view_version", externalView.getStat().getVersion());
    }
    metaBlock.put("publish_to_zk_time_ms", System.currentTimeMillis());

    topLevelJSONObject.put("meta", metaBlock);
    topLevelJSONObject.put("shard_map", sharedMapObj);

    getExecutorService(resourceName).submit(new Runnable() {
      @Override
      public void run() {
        // publish to zk.
        try {
          byte[] setData = gzipCompressedJsonObjectCodec.encode(topLevelJSONObject);
          String zkPath = ZkPathUtils.getClusterResourceShardMapPath(clusterName, resourceName);
          Stat stat = zkShardMapClient.checkExists().creatingParentsIfNeeded().forPath(zkPath);
          if (stat == null) {
            zkShardMapClient.create().creatingParentsIfNeeded().forPath(zkPath, setData);
          } else {
            zkShardMapClient.setData().forPath(zkPath, setData);
          }
        } catch (Exception e) {
          LOG.error(String.format(
              "Error publishing shard_map / resource_map to zk cluster: %s, resource: %s",
              clusterName, resourceName), e);
        }
      }
    });
  }

  /**
   * Remove the resources which are no longer available in the externalView, but available in the
   * zk.
   */
  private void removeResourceFromZkGZIPCompressedPerResourceShardMap(
      final String clusterName,
      final Set<String> keepTheseResources) {
    getExecutorService(clusterName).submit(new Runnable() {
      @Override
      public void run() {
        try {
          String zkPath = ZkPathUtils.getClusterShardMapParentPath(clusterName);
          Stat stat = zkShardMapClient.checkExists().creatingParentsIfNeeded().forPath(zkPath);
          if (stat == null) {
            return;
          }
          List<String> childrenPaths = zkShardMapClient.getChildren().forPath(zkPath);

          for (String childResourcePath : childrenPaths) {
            String[] splits = childResourcePath.split("/");
            if (splits == null || splits.length <= 0) {
              continue;
            }
            String resourceName = splits[splits.length - 1];

            /**
             * This is important step. Do not remove resources that are known to be still alive
             * (active or dormant, online or offline, enabled or disabled) in helix.
             */
            if (keepTheseResources.contains(resourceName)) {
              continue;
            }

            getExecutorService(resourceName).submit(new Runnable() {
              @Override
              public void run() {
                try {
                  String zkChildPath = ZkPathUtils.getClusterResourceShardMapPath(
                      clusterName, resourceName);
                  zkShardMapClient.delete().forPath(zkChildPath);
                } catch (Exception exp) {
                  LOG.error(String.format(
                      "Couldn't remove extraneous resources from cluster=%s, resource=%s",
                      clusterName, resourceName));
                }
              }
            });
          }
        } catch (Exception exp) {
          LOG.error(String.format(
              "Couldn't remove extraneous resources from cluster=%s", clusterName));
        }
      }
    });
  }

  @Override
  public void close() throws IOException {
    // ensure the while we are closing the ConfigGenerator, we are not processing any callback
    // at the moment.
    for (ExecutorService executorService : executorServices) {
      executorService.shutdown();
    }

    for (ExecutorService executorService : executorServices) {
      while (!executorService.isTerminated()) {
        try {
          executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    zkShardMapClient.close();
  }
}

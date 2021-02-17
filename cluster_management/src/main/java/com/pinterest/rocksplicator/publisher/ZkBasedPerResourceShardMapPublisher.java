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

import com.pinterest.rocksplicator.codecs.ZkGZIPCompressedShardMapCodec;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class publishes shard_map in gzip compressed format per resource.
 * The big cluster shard_map is split per resource and is published to
 * individual znodes corresponding to a path identified by clusterName and
 * resourceName.
 */
public class ZkBasedPerResourceShardMapPublisher implements ShardMapPublisher<JSONObject> {

  private static final int MAX_THREADS = 16;
  private static final Logger LOG =
      LoggerFactory.getLogger(ZkBasedPerResourceShardMapPublisher.class);

  private final ZkGZIPCompressedShardMapCodec gzipCodec;
  private final String clusterName;
  private final String zkShardMapConnectString;
  private final CuratorFramework zkShardMapClient;
  private final List<ExecutorService> executorServices;
  private final ConcurrentHashMap<String, String> latestResourceToConfigMap;

  public ZkBasedPerResourceShardMapPublisher(
      final String clusterName,
      final String zkShardMapConnectString) {
    this.clusterName = Preconditions.checkNotNull(clusterName);
    this.zkShardMapConnectString = Preconditions.checkNotNull(zkShardMapConnectString);

    /**
     * This is fixed at the moment.
     */
    this.gzipCodec = new ZkGZIPCompressedShardMapCodec();

    this.zkShardMapClient = CuratorFrameworkFactory.newClient(this.zkShardMapConnectString,
        new BoundedExponentialBackoffRetry(100, 5000, 10));

    try {
      this.zkShardMapClient.blockUntilConnected(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    this.executorServices = Lists.newArrayListWithCapacity(MAX_THREADS);
    for (int i = 0; i < MAX_THREADS; ++i) {
      /**
       * Each of the executor service must be single threaded.
       */
      this.executorServices.add(Executors.newSingleThreadExecutor());
    }

    this.latestResourceToConfigMap = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void publish(
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
        clusterName, validResources.stream().collect(Collectors.toSet()));
  }

  private String getId(String clusterName, String resourceName) {
    StringBuilder builder = new StringBuilder()
        .append(clusterName)
        .append('/')
        .append(resourceName);
    return builder.toString();
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
   *
   * Note: Any operation on a particular resource will be sequential
   * even in async way, since any operation on a particular resource is routed
   * to specific executorservice, and each executor service is a single threaded.
   */
  private void asyncWriteToZkGZIPCompressedPerResourceShardMap(
      final String clusterName,
      final String resourceName,
      final ExternalView externalView,
      final JSONObject resourceShardMap) {

    /**
     * Make sure we push the data to zk in async fashion. This is to ensure that it doesn't
     * perform sequential blocking calls to zk which can increase the overall latency
     * of publishing pipeline.
     */
    getExecutorService(resourceName).submit(() -> {
      final String latestResourceMapStr = latestResourceToConfigMap.get(resourceName);
      final String currentResourceMapStr = resourceShardMap.toString();

      /**
       * No need to publish, if the latest known resourceMap is same as the current one
       * Note that we perform this check inside the thread run on executor service
       * for a given resource. This is in order to parallelize serialization of json object
       * which can be expensive
       **/
      if (latestResourceMapStr != null && latestResourceMapStr.equals(currentResourceMapStr)) {
        return;
      }
      latestResourceToConfigMap.put(resourceName, latestResourceMapStr);

      final JSONObject topLevelJSONObject = new JSONObject();
      final JSONObject metaBlock = new JSONObject();

      final JSONObject clusterShardMapObj = new JSONObject();
      clusterShardMapObj.put(resourceName, resourceShardMap);

      if (externalView.getStat() != null) {
        metaBlock.put("external_view_modified_time_ms", externalView.getStat().getModifiedTime());
        metaBlock.put("external_view_version", externalView.getStat().getVersion());
        metaBlock.put("shard_map_format", "json");
      }
      metaBlock.put("publish_time_ms", System.currentTimeMillis());

      topLevelJSONObject.put("meta", metaBlock);
      topLevelJSONObject.put("shard_map", clusterShardMapObj);

      try {
        byte[] serializedCompressedJson = gzipCodec.encode(topLevelJSONObject);
        String zkPath = ZkPathUtils.getClusterResourceShardMapPath(clusterName, resourceName);
        Stat stat = zkShardMapClient.checkExists().creatingParentsIfNeeded().forPath(zkPath);
        if (stat == null) {
          zkShardMapClient.create().creatingParentsIfNeeded()
              .forPath(zkPath, serializedCompressedJson);
        } else {
          zkShardMapClient.setData().forPath(zkPath, serializedCompressedJson);
        }
      } catch (Exception e) {
        LOG.error(String.format(
            "Error publishing shard_map / resource_map to zk cluster: %s, resource: %s",
            clusterName, resourceName), e);
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
    getExecutorService(clusterName).submit(() -> {
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

          getExecutorService(resourceName).submit(() -> {
            try {
              String zkChildPath = ZkPathUtils.getClusterResourceShardMapPath(
                  clusterName, resourceName);
              zkShardMapClient.delete().forPath(zkChildPath);
            } catch (Exception exp) {
              LOG.error(String.format(
                  "Couldn't remove extraneous resources from cluster=%s, resource=%s",
                  clusterName, resourceName));
            }
          });
        }
      } catch (Exception exp) {
        LOG.error(String.format(
            "Couldn't remove extraneous resources from cluster=%s", clusterName));
      }
    });
  }

  @Override
  public void close() throws IOException {
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

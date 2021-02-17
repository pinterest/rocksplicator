package com.pinterest.rocksplicator.publisher;

import com.pinterest.rocksplicator.ConfigGenerator;
import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.Codecs;
import com.pinterest.rocksplicator.codecs.JSONObjectCodec;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.helix.model.ExternalView;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZkBasedPerResourceShardMapPublisher implements ShardMapPublisher<JSONObject> {
  private final Codec<JSONObject, byte[]> baseJsonObjectCodec = new JSONObjectCodec();
  private final Codec<JSONObject, byte[]> gzipCompressedJsonObjectCodec =
      Codecs.getCompressedCodec(baseJsonObjectCodec, CompressionAlgorithm.GZIP);

  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);
  private final String clusterName;
  private final String zkShardMapConnectString;
  private final CuratorFramework zkShardMapClient;
  private final ExecutorService executorService;

  public ZkBasedPerResourceShardMapPublisher(
      final String clusterName,
      final String zkShardMapConnectString) {
    this.clusterName = clusterName;
    this.zkShardMapConnectString = zkShardMapConnectString;
    this.zkShardMapClient = CuratorFrameworkFactory.newClient(
        this.zkShardMapConnectString,
        new BoundedExponentialBackoffRetry(100, 1000, 10));
    if (this.zkShardMapClient != null) {
      this.executorService = Executors.newSingleThreadExecutor();
    } else {
      this.executorService = null;
    }

  }

  @Override
  public void publish(
      final Set<String> validResources,
      final List<ExternalView> externalViews,
      final JSONObject shardMap) {

    for (Object resourceObj : shardMap.keySet()) {
      String resource = (String) resourceObj;
      JSONObject resourceConfig = (JSONObject) shardMap.get(resourceObj);
      asyncWriteToZkGZIPCompressedPerResourceShardMap(resourceConfig, clusterName, resource);
    }
    removeResourceFromZkGZIPCompressedPerResourceShardMap(
        clusterName,
        validResources.stream().collect(Collectors.toSet()));
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
      JSONObject perResourceShardMap,
      String clusterName,
      String resourceName) {
    if (executorService != null) {
      final JSONObject perResourceShardMapObj = new JSONObject();
      perResourceShardMapObj.put(resourceName, perResourceShardMap);

      executorService.submit(new Runnable() {
        @Override
        public void run() {
          // publish to zk.
          try {
            String zkPath = ZkPathUtils.getClusterResourceShardMapPath(clusterName, resourceName);
            byte[] emptyByteArray = new byte[0];
            Stat stat = zkShardMapClient.checkExists().creatingParentsIfNeeded().forPath(zkPath);
            if (stat == null) {
              zkShardMapClient.create().creatingParentsIfNeeded().forPath(zkPath, emptyByteArray);
            }
            byte[] setData = gzipCompressedJsonObjectCodec.encode(perResourceShardMapObj);
            zkShardMapClient.setData().forPath(zkPath, setData);
          } catch (Exception e) {
            LOG.error("Error publishing shard_map / resource_map to zk", e);
          }
        }
      });
    }
  }

  /**
   * Remove the resources which are no longer available in the externalView, but available in the zk.
   */
  private void removeResourceFromZkGZIPCompressedPerResourceShardMap(
      final String clusterName,
      final Set<String> keepTheseResources) {
    if (executorService != null) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            String zkPath = ZkPathUtils.getClusterShardMapParentPath(clusterName);
            byte[] emptyByteArray = new byte[0];
            Stat stat = zkShardMapClient.checkExists().creatingParentsIfNeeded().forPath(zkPath);
            if (stat == null) {
              zkShardMapClient.create().creatingParentsIfNeeded().forPath(zkPath, emptyByteArray);
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

              try {
                String zkChildPath = ZkPathUtils.getClusterResourceShardMapPath(clusterName, resourceName);
                zkShardMapClient.delete().forPath(zkChildPath);
              } catch (Exception exp) {
                LOG.error(String.format(
                    "Couldn't remove extraneous resources from cluster=%s, resource=%s", clusterName));
              }
            }

          } catch (Exception exp) {
            LOG.error(String.format(
                "Couldn't remove extraneous resources from cluster=%s", clusterName));
          }
        }
      });
    }
  }

  @Override
  public void close() throws IOException {
    // ensure the while we are closing the ConfigGenerator, we are not processing any callback
    // at the moment.
    if (executorService != null) {
      executorService.shutdown();

      while (!executorService.isTerminated()) {
        try {
          executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      zkShardMapClient.close();
    }
  }
}

package com.pinterest.rocksplicator.codecs;

import com.pinterest.rocksplicator.eventstore.ClientShardMapLeaderEventLogger;
import com.pinterest.rocksplicator.eventstore.ClientShardMapLeaderEventLoggerImpl;
import com.pinterest.rocksplicator.eventstore.LeaderEventsLogger;
import com.pinterest.rocksplicator.shardmap.ShardMap;
import com.pinterest.rocksplicator.shardmap.ShardMaps;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientZKShardMapDriver implements Closeable {
  private final String clusterName;
  private final CuratorFramework zkClient;
  private final LeaderEventsLogger leaderEventsLogger;
  private final ClientShardMapLeaderEventLogger clientLeaderEventLogger;
  private final ExecutorService executorService;
  private PathChildrenCache pathChildrenCache;


  public ClientZKShardMapDriver(String clusterName, String zkShardClientConnectString,
                                LeaderEventsLogger leaderEventsLogger) {
    this.clusterName = clusterName;
    this.leaderEventsLogger = leaderEventsLogger;
    this.clientLeaderEventLogger = new ClientShardMapLeaderEventLoggerImpl(this.leaderEventsLogger);
    this.executorService = Executors.newSingleThreadExecutor();

    this.zkClient = CuratorFrameworkFactory
        .newClient(zkShardClientConnectString, new ExponentialBackoffRetry(1000, 3));

    this.zkClient.start();

    try {
      this.zkClient.blockUntilConnected(60, TimeUnit.SECONDS);
      this.pathChildrenCache =
          new PathChildrenCache(this.zkClient, "/shard_map/" + this.clusterName, true, false,
              new CloseableExecutorService(Executors.newSingleThreadExecutor(), true));
    } catch (InterruptedException e) {
      this.pathChildrenCache = null;
      e.printStackTrace();
    }
  }

  public void startNotification() throws Exception {
    if (pathChildrenCache == null) {
      return;
    }

    final Codec<JSONObject, byte[]> baseCodec = new JSONObjectCodec();
    final Codec<JSONObject, byte[]>
        compressedCodec =
        Codecs.getCompressedCodec(baseCodec, CompressionAlgorithm.GZIP);

    pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

    pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
          throws Exception {
        switch (event.getType()) {
          case CHILD_ADDED: {
            JSONObject jsonShardMap = compressedCodec.decode(event.getData().getData());
            ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
            clientLeaderEventLogger.process(shardMap, System.currentTimeMillis());
            String pathChanged = event.getData().getPath();
            Stat statChanged = event.getData().getStat();
            System.out.println(
                String.format("\nPathChildrenCache Path= %s Stat= %s", pathChanged, statChanged));
          }
          break;
          case CHILD_UPDATED: {
            JSONObject jsonShardMap = compressedCodec.decode(event.getData().getData());
            ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
            clientLeaderEventLogger.process(shardMap, System.currentTimeMillis());
            String pathChanged = event.getData().getPath();
            Stat statChanged = event.getData().getStat();
            System.out.println(
                String.format("\nPathChildrenCache Path= %s Stat= %s", pathChanged, statChanged));
          }
          break;
          case CHILD_REMOVED: {
            String pathChanged = event.getData().getPath();
            Stat statChanged = event.getData().getStat();
            System.out.println(
                String.format("\nPathChildrenCache Path= %s Stat= %s", pathChanged, statChanged));
          }
          break;
        }
        System.out.println(String.format("PathChildrenCache %s", event.getType()));
      }
    });
  }

  @Override
  public void close() throws IOException {
    if (pathChildrenCache != null) {
      pathChildrenCache.close();
    }
    if (zkClient != null) {
      zkClient.close();
    }

    if (executorService != null) {
      executorService.shutdown();
      while (!executorService.isTerminated()) {
        try {
          executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    if (clientLeaderEventLogger != null) {
      clientLeaderEventLogger.close();
    }
  }
}

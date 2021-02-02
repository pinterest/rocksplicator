package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.SimpleJsonObjectDecoder;
import com.pinterest.rocksplicator.config.ConfigNotifier;
import com.pinterest.rocksplicator.config.FileWatchers;
import com.pinterest.rocksplicator.shardmap.ShardMap;
import com.pinterest.rocksplicator.shardmap.ShardMaps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.json.simple.JSONObject;

import java.io.IOException;

public class ClientLeaderEventsDriver {
  private String shardMapPath;
  private ConfigNotifier<JSONObject> notifier;
  private ClientLeaderEventLogger clientLeaderEventLogger;
  private String zkEventHistoryStr;
  private CuratorFramework zkClient;

  public ClientLeaderEventsDriver(
      final String shardMapPath,
      final ClientLeaderEventLogger clientLeaderEventLogger,
      final String zkEventHistoryStr) {
    this.shardMapPath = shardMapPath;
    this.clientLeaderEventLogger = clientLeaderEventLogger;
    this.zkEventHistoryStr = zkEventHistoryStr;
    this.zkClient = CuratorFrameworkFactory
        .newClient(zkEventHistoryStr, new ExponentialBackoffRetry(1000, 3));

    this.zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
          case LOST:
          case SUSPENDED:
            close();
          default:
        }
      }
    });

    this.zkClient.start();

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

  }

  public void close() {
    /**
     * Stop the notification process first.
     */
    if (this.notifier != null) {
      try {
        this.notifier.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void process(ConfigNotifier.Context<JSONObject> shardMapWithContext) {
    JSONObject jsonShardMap = shardMapWithContext.getItem();
    ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
    clientLeaderEventLogger
        .process(shardMap, shardMapWithContext.getNotification_received_time_millis());
  }
}

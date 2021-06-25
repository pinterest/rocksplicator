package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventLogger;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisher;

import org.apache.helix.HelixManager;
import org.json.simple.JSONObject;

public class ConfigGeneratorFactory {

  private final boolean enableCurrentStatesBasedRoutingTableConfigGenerator;

  public ConfigGeneratorFactory(boolean enableCurrentStates) {
    this.enableCurrentStatesBasedRoutingTableConfigGenerator = enableCurrentStates;
  }

  public ConfigGeneratorIface createConfigGenerator(
      final String clusterName,
      final HelixManager helixManager,
      final ShardMapPublisher<JSONObject> shardMapPublisher,
      final RocksplicatorMonitor monitor,
      final ExternalViewLeaderEventLogger externalViewLeaderEventLogger) throws Exception {
    if (enableCurrentStatesBasedRoutingTableConfigGenerator) {
      CurrentStatesConfigGenerator
          generator =
          new CurrentStatesConfigGenerator(clusterName, helixManager, shardMapPublisher, monitor,
              externalViewLeaderEventLogger);
      return generator;
    } else {
      ConfigGenerator configGenerator =
          new ConfigGenerator(clusterName, helixManager, shardMapPublisher, monitor,
              externalViewLeaderEventLogger);

      /**
       * Add to the helixManager, message handlers.
       */
      helixManager.addExternalViewChangeListener(configGenerator);
      helixManager.addConfigChangeListener(configGenerator);

      return configGenerator;
    }
  }
}

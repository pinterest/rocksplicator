package com.pinterest.rocksplicator;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class DistClusterSpectatorStateModelFactory
    extends StateModelFactory<DistClusterSpectatorStateModel> {
  private final String _zkAddr;

  public DistClusterSpectatorStateModelFactory(String zkAddr) {
    _zkAddr = zkAddr;
  }

  @Override
  public DistClusterSpectatorStateModel createNewStateModel(String resourceName, String partitionKey) {
    return new DistClusterSpectatorStateModel(_zkAddr);
  }

}

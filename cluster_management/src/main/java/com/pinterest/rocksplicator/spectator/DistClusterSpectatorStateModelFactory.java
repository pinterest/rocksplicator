package com.pinterest.rocksplicator.spectator;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class DistClusterSpectatorStateModelFactory
    extends StateModelFactory<DistClusterSpectatorStateModel> {
  private final String _zkAddr;
  private final SpectatorLeadershipCallback callback;

  public DistClusterSpectatorStateModelFactory(
      SpectatorLeadershipCallback callback,
      String zkAddr) {
    _zkAddr = zkAddr;
    this.callback = callback;
  }

  @Override
  public DistClusterSpectatorStateModel createNewStateModel(String resourceName, String partitionKey) {
    return new DistClusterSpectatorStateModel(callback, _zkAddr);
  }

}

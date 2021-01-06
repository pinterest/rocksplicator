package com.pinterest.rocksplicator.spectator;

import org.apache.helix.HelixManager;

public interface SpectatorLeadershipCallback {

  void onAcquire(HelixManager helixManager);

  /**
   * May be called multiple times
   */
  void onRelease(HelixManager helixManager);
}

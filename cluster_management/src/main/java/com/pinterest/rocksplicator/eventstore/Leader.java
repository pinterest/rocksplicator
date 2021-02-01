package com.pinterest.rocksplicator.eventstore;

class Leader {
  private final String leaderInstanceId;
  private final LeaderState state;

  Leader(String leaderInstanceId, LeaderState state) {
    this.leaderInstanceId = leaderInstanceId;
    this.state = state;
  }

  public String getLeaderInstanceId() {
    return leaderInstanceId;
  }

  public LeaderState getState() {
    return state;
  }
}

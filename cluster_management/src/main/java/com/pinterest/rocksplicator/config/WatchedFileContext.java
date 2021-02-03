package com.pinterest.rocksplicator.config;

public class WatchedFileContext<R> {
  private R data;
  private long lastModifiedTimeMillis;

  public WatchedFileContext(R data, long lastModifiedTimeMillis) {
    this.data = data;
    this.lastModifiedTimeMillis = lastModifiedTimeMillis;
  }

  public R getData() {
    return data;
  }

  public long getLastModifiedTimeMillis() {
    return lastModifiedTimeMillis;
  }
}

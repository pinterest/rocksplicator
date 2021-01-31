package com.pinterest.rocksplicator.utils;

import java.util.concurrent.locks.Lock;

public class AutoCloseableLock implements AutoCloseable {

  private final Lock lock;

  public AutoCloseableLock(Lock lock) {
    this.lock = lock;
    this.lock.lock();
  }

  public static AutoCloseableLock lock(Lock lock) {
    return new AutoCloseableLock(lock);
  }

  @Override
  public void close() {
    this.lock.unlock();
  }
}

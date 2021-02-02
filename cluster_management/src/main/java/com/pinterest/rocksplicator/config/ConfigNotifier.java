package com.pinterest.rocksplicator.config;

import com.pinterest.rocksplicator.codecs.CodecException;
import com.pinterest.rocksplicator.codecs.Decoder;
import com.pinterest.rocksplicator.utils.AutoCloseableLock;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class ConfigNotifier<R> implements Closeable {

  private final Decoder<byte[], R> decoder;
  private final Lock exclusionLock = new ReentrantLock();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final WatchedFileNotificationCallback callback;
  private final FileWatcher<byte[]> fileWatcher;
  private final String filePath;

  public ConfigNotifier(Decoder<byte[], R> decoder, String filePath,
                        Function<Context<R>, Void> notifier) {
    this(decoder, filePath, FileWatchers.getPollingFileWatcher(), notifier);
  }

  public ConfigNotifier(Decoder<byte[], R> decoder, String filePath,
                        FileWatcher<byte[]> fileWatcher, Function<Context<R>, Void> notifier) {
    this.filePath = filePath;
    this.fileWatcher = fileWatcher;
    this.decoder = decoder;
    this.callback = new WatchedFileNotificationCallback(notifier);
  }

  public synchronized boolean isStarted() {
    try (AutoCloseableLock autoLock = new AutoCloseableLock(exclusionLock)) {
      return this.isStarted.get();
    }
  }

  public synchronized boolean isClosed() {
    try (AutoCloseableLock autoLock = new AutoCloseableLock(exclusionLock)) {
      return this.isClosed.get();
    }
  }

  public synchronized void start() throws IOException {
    try (AutoCloseableLock autoLock = new AutoCloseableLock(exclusionLock)) {
      if (isClosed()) {
        throw new IOException("Cannot start once closed");
      } else if (!isStarted()) {
        this.fileWatcher.addWatch(filePath, callback);
        this.isStarted.set(true);
      }
    }
  }

  public synchronized void stop() {
    try (AutoCloseableLock autoLock = new AutoCloseableLock(exclusionLock)) {
      if (!isClosed() && isStarted()) {
        this.fileWatcher.removeWatch(filePath, callback);
        this.isStarted.set(false);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try (AutoCloseableLock autoLock = new AutoCloseableLock(exclusionLock)) {
      isClosed.getAndSet(true);
    }
  }

  public static class Context<R> {

    private final R item;
    private final long src_change_time_millis;
    private final long notification_received_time_millis;
    private final long deserialization_time_in_millis;

    public Context(
        final R item,
        final long src_change_time_millis,
        final long notification_received_time_millis,
        final long deserialization_time_in_millis) {
      this.item = item;
      this.src_change_time_millis = src_change_time_millis;
      this.notification_received_time_millis = notification_received_time_millis;
      this.deserialization_time_in_millis = deserialization_time_in_millis;

    }

    public R getItem() {
      return item;
    }

    public long getSrc_change_time_millis() {
      return src_change_time_millis;
    }

    public long getNotification_received_time_millis() {
      return notification_received_time_millis;
    }

    public long getDeserialization_time_in_millis() {
      return deserialization_time_in_millis;
    }
  }

  private class WatchedFileNotificationCallback
      implements Function<WatchedFileContext<byte[]>, Void> {

    private final Function<Context<R>, Void> notifier;

    public WatchedFileNotificationCallback(Function<Context<R>, Void> notifier) {
      this.notifier = notifier;
    }

    @Override
    public Void apply(WatchedFileContext<byte[]> context) {
      try (AutoCloseableLock autoLock = new AutoCloseableLock(exclusionLock)) {
        // If the notifier is already closed, do nothing.
        if (isClosed.get()) {
          return null;
        }
        long notification_received_time_millis = System.currentTimeMillis();
        R newConfigRef = ConfigNotifier.this.decoder.decode(context.getData());
        long
            deserialization_time_in_millis =
            notification_received_time_millis - System.currentTimeMillis();
        notifier.apply(new Context<>(newConfigRef, context.getLastModifiedTimeMillis(),
            notification_received_time_millis, deserialization_time_in_millis));
      } catch (CodecException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
}

/// Copyright 2021 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.config;

import com.pinterest.rocksplicator.codecs.CodecException;
import com.pinterest.rocksplicator.codecs.Decoder;
import com.pinterest.rocksplicator.utils.AutoCloseableLock;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class ConfigNotifier<R> implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNotifier.class);

  private final Decoder<byte[], R> decoder;
  private final Lock exclusionLock = new ReentrantLock();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final WatchedFileNotificationCallback callback;
  private final FileWatcher<byte[]> fileWatcher;
  private final String filePath;

  @VisibleForTesting
  ConfigNotifier(Decoder<byte[], R> decoder, String filePath,
                 Function<Context<R>, Void> notifier) throws IOException {
    this(decoder, filePath, FileWatchers.getPollingFileWatcher(), notifier);
  }

  public ConfigNotifier(Decoder<byte[], R> decoder, String filePath,
                        FileWatcher<byte[]> fileWatcher, Function<Context<R>, Void> notifier)
      throws IOException {
    this.filePath = filePath;
    this.fileWatcher = fileWatcher;
    this.decoder = decoder;
    this.callback = new WatchedFileNotificationCallback(notifier);

    WatchedFileNotificationCallback fakeCallback = new WatchedFileNotificationCallback(
        new Function<Context<R>, Void>() {
          @Override
          public Void apply(Context<R> rContext) {
            LOGGER.warn("Successfully");
            return null;
          }
        });

    // Add the watch to ensure the file exists.
    this.fileWatcher.addWatch(filePath, fakeCallback);
    // Immediately release the watch.
    this.fileWatcher.removeWatch(filePath, fakeCallback);
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
  public synchronized void close() throws IOException {
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

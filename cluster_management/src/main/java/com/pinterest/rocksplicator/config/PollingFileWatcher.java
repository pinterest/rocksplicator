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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Class to monitor config files on local disk. Typical usage is to use the default instance
 * and have it monitor as many config files as needed.
 *
 * The class allows users to specify a watch on a file path and pass in a callback that
 * will be invoked whenever an update to the file is detected. Update detection currently
 * works by periodic polling; if the last modified time on the file is updated and the
 * content hash has changed, all watchers on that file are notified. Note that last modified
 * time is just used as a hint to determine whether to check the content and not for versioning.
 *
 * Objects of this class are thread safe.
 *
 */
public class PollingFileWatcher implements FileWatcher<byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(PollingFileWatcher.class);
  private static final HashFunction HASH_FUNCTION = Hashing.md5();
  private static final int DEFAULT_POLL_PERIOD_MILLIS = 10000;
  private static final int DEFAULT_HIGH_RESOLUTION_POLL_PERIOD_MILLIS = 1000;
  private static final int DEFAULT_EXPENSIVE_HIGH_RESOLUTION_POLL_PERIOD_MILLIS = 50;

  private static volatile PollingFileWatcher DEFAULT_INSTANCE = null;
  private static volatile PollingFileWatcher HIGH_RESOLUTION_INSTANCE = null;
  private static volatile PollingFileWatcher EXPENSIVE_HIGH_RESOLUTION_INSTANCE = null;

  // Thread safety note: only addWatch() can add new entries to this map, and that method
  // is synchronized. The reason for using a concurrent map is only to allow the watcher
  // thread to concurrently iterate over it.
  private final ConcurrentMap<String, ConfigFileInfo> watchedFileMap = Maps.newConcurrentMap();
  private final WatcherTask watcherTask;
  private final boolean relyOnTimestamp;

  @VisibleForTesting
  PollingFileWatcher(int pollPeriod, TimeUnit timeUnit) {
    this(pollPeriod, timeUnit, true);
  }

  @VisibleForTesting
  PollingFileWatcher(int pollPeriod, TimeUnit timeUnit, boolean relyOnTimestamp) {
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("PollingFileWatcher-%d").build());
    this.watcherTask = new WatcherTask();
    this.relyOnTimestamp = relyOnTimestamp;
    service.scheduleWithFixedDelay(
        watcherTask, pollPeriod, pollPeriod, timeUnit);
  }

  /**
   * Creates the default ConfigFileWatcher instance on demand.
   */
  public static PollingFileWatcher defaultPerTenSecondsWatcher() {
    if (DEFAULT_INSTANCE == null) {
      synchronized (PollingFileWatcher.class) {
        if (DEFAULT_INSTANCE == null) {
          DEFAULT_INSTANCE =
              new PollingFileWatcher(DEFAULT_POLL_PERIOD_MILLIS, TimeUnit.MILLISECONDS, true);
        }
      }
    }

    return DEFAULT_INSTANCE;
  }

  /**
   * Creates the default ConfigFileWatcher instance on demand.
   */
  public static PollingFileWatcher defaultPerSecondWatcher() {
    if (HIGH_RESOLUTION_INSTANCE == null) {
      synchronized (PollingFileWatcher.class) {
        if (HIGH_RESOLUTION_INSTANCE == null) {
          HIGH_RESOLUTION_INSTANCE = new PollingFileWatcher(
              DEFAULT_HIGH_RESOLUTION_POLL_PERIOD_MILLIS, TimeUnit.MILLISECONDS, false);
        }
      }
    }

    return HIGH_RESOLUTION_INSTANCE;
  }

  /**
   * Creates the default ConfigFileWatcher instance on demand.
   */
  public static PollingFileWatcher defaultExpensiveHighResolutionWatcher() {
    if (EXPENSIVE_HIGH_RESOLUTION_INSTANCE == null) {
      synchronized (PollingFileWatcher.class) {
        if (EXPENSIVE_HIGH_RESOLUTION_INSTANCE == null) {
          EXPENSIVE_HIGH_RESOLUTION_INSTANCE = new PollingFileWatcher(
              DEFAULT_EXPENSIVE_HIGH_RESOLUTION_POLL_PERIOD_MILLIS, TimeUnit.MILLISECONDS, false);
        }
      }
    }
    return EXPENSIVE_HIGH_RESOLUTION_INSTANCE;
  }

  private static byte[] read(File file) throws IOException {
    try (FileInputStream stream = new FileInputStream(file)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, stream.available()));
      copy(stream, out);
      return out.toByteArray();
    }
  }

  private static void copy(InputStream from, OutputStream to) throws IOException {
    byte[] buf = new byte[8192];
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
    }
  }

  /**
   * Adds a watch on the specified file. The file must exist, otherwise a FileNotFoundException
   * is returned. If the file is deleted after a watch is established, the watcher will log errors
   * but continue to monitor it, and resume watching if it is recreated.
   *
   * @param filePath path to the file to watch.
   * @param onUpdate function to call when a change is detected to the file. The entire contents
   *                 of the file will be passed in to the function. Note that onUpdate will be
   *                 called once before this call completes, which facilities initial load of data.
   *                 This callback is executed synchronously on the watcher thread - it is
   *                 important that the function be non-blocking.
   */
  @Override
  public synchronized void addWatch(String filePath,
                                    Function<WatchedFileContext<byte[]>, Void> onUpdate)
      throws IOException {
    Preconditions.checkNotNull(filePath);
    Preconditions.checkArgument(!filePath.isEmpty());
    Preconditions.checkNotNull(onUpdate);

    // Read the file and make the initial onUpdate call.
    File file = new File(filePath);
    long lastModifiedTimeMillis = file.lastModified();
    byte[] contents = read(file);
    onUpdate.apply(new WatchedFileContext<>(contents, lastModifiedTimeMillis));

    // Add the file to our map if it isn't already there, and register the new change watcher.
    ConfigFileInfo configFileInfo = watchedFileMap.get(filePath);
    if (configFileInfo == null) {
      configFileInfo = new ConfigFileInfo(
          file, HASH_FUNCTION.newHasher().putBytes(contents).hash());
      watchedFileMap.put(filePath, configFileInfo);
    }
    configFileInfo.changeWatchers.add(onUpdate);
  }

  @Override
  public synchronized void removeWatch(
      String filePath,
      Function<WatchedFileContext<byte[]>, Void> onUpdate) {
    ConfigFileInfo configFileInfo = watchedFileMap.get(filePath);
    if (configFileInfo != null) {
      configFileInfo.changeWatchers.remove(onUpdate);
    }
  }

  @VisibleForTesting
  public void runWatcherTaskNow() {
    watcherTask.run();
  }

  /**
   * Encapsulates state related to each watched config file.
   *
   * Thread safety note:
   *   1. changeWatchers is thread safe since it uses a copy-on-write array list.
   *   2. lastModifiedTimestampMillis and contentHash aren't safe to update across threads. We
   *      initialize in addWatch() at construction time, and thereafter only the watcher task
   *      thread accesses this state, so we are good.
   */
  private static class ConfigFileInfo {

    private final List<Function<WatchedFileContext<byte[]>, Void>>
        changeWatchers =
        new CopyOnWriteArrayList<>();
    private long lastModifiedTimestampMillis;
    private long lastFileLength;
    private HashCode contentHash;

    public ConfigFileInfo(File file, HashCode contentHash) {
      this.lastModifiedTimestampMillis = file.lastModified();
      this.lastFileLength = file.length();
      Preconditions.checkArgument(lastModifiedTimestampMillis > 0L);
      this.contentHash = Preconditions.checkNotNull(contentHash);
    }
  }

  /**
   * Scheduled task that periodically checks each watched file for updates, and if found to have
   * been changed, triggers notifications on all its watchers.
   *
   * Thread safety note: this task must be run in a single threaded executor; i.e. only one run
   * of the task can be active at any time.
   */
  private class WatcherTask implements Runnable {

    @Override
    public void run() {
      for (Map.Entry<String, ConfigFileInfo> entry : watchedFileMap.entrySet()) {
        String filePath = entry.getKey();
        ConfigFileInfo configFileInfo = entry.getValue();
        try {
          File file = new File(filePath);
          long lastModified = file.lastModified();
          long lastLength = file.length();
          Preconditions.checkArgument(lastModified > 0L);
          if (!relyOnTimestamp || lastModified != configFileInfo.lastModifiedTimestampMillis
              || lastLength != configFileInfo.lastFileLength) {
            if (relyOnTimestamp) {
              configFileInfo.lastModifiedTimestampMillis = lastModified;
              configFileInfo.lastFileLength = lastLength;
            }
            byte[] newContents = read(file);
            HashCode newContentHash = HASH_FUNCTION.newHasher().putBytes(newContents).hash();
            if (!newContentHash.equals(configFileInfo.contentHash)) {
              if (!relyOnTimestamp) {
                configFileInfo.lastModifiedTimestampMillis = lastModified;
                configFileInfo.lastFileLength = lastLength;
              }
              configFileInfo.contentHash = newContentHash;
              LOG.info("File {} was modified at {}, notifying watchers.", filePath, lastModified);
              for (Function<WatchedFileContext<byte[]>, Void> watchers :
                  configFileInfo.changeWatchers) {
                try {
                  watchers.apply(new WatchedFileContext<>(newContents, lastModified));
                } catch (Exception e) {
                  LOG.error(
                      "Exception in watcher callback for {}, ignoring. New file contents were: {}",
                      filePath, new String(newContents, Charsets.UTF_8), e);
                }
              }
            } else {
              LOG.info("File {} was modified at {} but content hash is unchanged.",
                  filePath, lastModified);
            }
          } else {
            LOG.debug("File {} not modified since {}", filePath, lastModified);
          }
        } catch (Exception e) {
          // We catch and log exceptions related to the update of any specific file, but
          // move on so others aren't affected. Issues can happen for example if the watcher
          // races with an external file replace operation; in that case, the next run should
          // pick up the update.
          // TODO: Consider adding a metric to track this so we can alert on failures.
          LOG.error("Config update check failed for {}", filePath, e);
        }
      }
    }
  }
}

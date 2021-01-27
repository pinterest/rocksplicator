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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Base class for tests that deal directly or indirectly with the ConfigFileWatcher.
 * Provides utility methods as well as ability to automatically cleanup temporary files created.
 */
public class ConfigTestBase {

  private final List<File> filesToCleanup = Lists.newArrayList();

  public static PollingFileWatcher createPollingFileWatcher() {
    // Note: we run the watcher task directly rather than rely on polling, to avoid test timing
    // issues. Hence we can just set a high poll period.
    return new PollingFileWatcher(1000000 /*pollPeriodSeconds*/);
  }

  public static void writeContentToFile(File file, String content) throws IOException {
    long lastModified = file.lastModified();
    FileUtils.writeStringToFile(file, content, Charsets.UTF_8.name());
    // The resolution of lastModified is 1sec, so we need to explicitly update.
    file.setLastModified(lastModified + 1000);
  }

  public static void writeRawContentToFile(File file, byte[] content) throws IOException {
    long lastModified = file.lastModified();
    FileUtils.writeByteArrayToFile(file, content);
    // The resolution of lastModified is 1sec, so we need to explicitly update.
    file.setLastModified(lastModified + 1000);
  }

  @After
  public void afterTest() {
    for (File file : filesToCleanup) {
      file.delete();
    }
    filesToCleanup.clear();
  }

  protected File createTempFile() throws IOException {
    File tmpFile = File.createTempFile("PollingFileWatcherTest", "txt");
    filesToCleanup.add(tmpFile);
    return tmpFile;
  }
}

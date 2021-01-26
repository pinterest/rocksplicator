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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.rocksplicator.codecs.LineTerminatedStringSetDecoder;

import org.junit.Test;

import java.io.File;
import java.util.Set;

public class ConfigStoreTest extends ConfigTestBase {

  @Test
  public void testConfigStore() throws Exception {
    File file = createTempFile();

    String initialContent = new StringBuilder()
        .append("first\n")
        .append("second\n")
        .toString();

    String updatedContent = new StringBuilder()
        .append("first\n")
        .append("third\n")
        .toString();

    String finalContent = new StringBuilder()
        .append("fourth\n")
        .append("fifth\n")
        .toString();

    PollingFileWatcher watcher = createPollingFileWatcher();

    // Initial content
    writeContentToFile(file, initialContent);

    ConfigStore<Set<String>>
        configStore =
        new ConfigStore<>(new LineTerminatedStringSetDecoder(), file.getAbsolutePath(), watcher);

    Set<String> initialSet = configStore.get();

    assertEquals(2, initialSet.size());
    assertTrue(initialSet.contains("first"));
    assertTrue(initialSet.contains("second"));

    // Update the content
    writeContentToFile(file, updatedContent);
    watcher.runWatcherTaskNow();

    Set<String> updatedSet = configStore.get();
    assertEquals(2, updatedSet.size());
    assertTrue(updatedSet.contains("first"));
    assertTrue(updatedSet.contains("third"));

    // Update the content
    writeContentToFile(file, finalContent);
    watcher.runWatcherTaskNow();

    Set<String> finalSet = configStore.get();
    assertEquals(2, finalSet.size());
    assertTrue(finalSet.contains("fourth"));
    assertTrue(finalSet.contains("fifth"));

  }
}

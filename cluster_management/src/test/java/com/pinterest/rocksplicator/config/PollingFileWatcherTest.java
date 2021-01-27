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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class PollingFileWatcherTest extends ConfigTestBase {

  @Test
  public void testBasicFunctionality() throws Exception {
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    File testConfigFile = createTempFile();
    final AtomicInteger watchFiredCount = new AtomicInteger(0);
    final AtomicReference<String> contentRetrieved = new AtomicReference<String>();

    // Initial update.
    writeContentToFile(testConfigFile, "test1");

    // Setup watch and ensure it fires immediately.
    configFileWatcher.addWatch(testConfigFile.getPath(), new Function<byte[], Void>() {
      @Override
      public Void apply(byte[] bytes) {
        watchFiredCount.incrementAndGet();
        contentRetrieved.set(new String(bytes, Charsets.UTF_8));
        return null;
      }
    });
    assertEquals(1, watchFiredCount.get());
    assertEquals("test1", contentRetrieved.get());

    // Update the contents and run the watcher task. Watch should fire.
    writeContentToFile(testConfigFile, "test2");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(2, watchFiredCount.get());
    assertEquals("test2", contentRetrieved.get());

    // Update the contents but keep unchanged and run the watcher task.
    // Watch should not fire.
    writeContentToFile(testConfigFile, "test2");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(2, watchFiredCount.get());

    // Update the contents and change the modified time to a time in the past.
    // The watch should still fire. This is to support the restore from backup case.
    writeContentToFile(testConfigFile, "test3");
    testConfigFile.setLastModified(testConfigFile.lastModified() - 10000);
    configFileWatcher.runWatcherTaskNow();
    assertEquals(3, watchFiredCount.get());
    assertEquals("test3", contentRetrieved.get());

    // Update the contents but keep the modified time unchanged. Watch won't fire.
    // This tests the use of modified time as an optimization.
    long lastModifiedMillis = testConfigFile.lastModified();
    writeContentToFile(testConfigFile, "test4");
    testConfigFile.setLastModified(lastModifiedMillis);
    configFileWatcher.runWatcherTaskNow();
    assertEquals(3, watchFiredCount.get());
    assertEquals("test3", contentRetrieved.get());
  }

  @Test
  public void testMultipleFiles() throws IOException {
    final AtomicInteger watchesFired = new AtomicInteger(0);
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    List<File> files = Lists.newArrayList();
    final List<AtomicReference<String>> fileContents = Lists.newArrayList();
    // Create files and set up watches. Validate initial callback.
    for (int i = 0; i < 10; i++) {
      File file = createTempFile();
      files.add(file);
      fileContents.add(new AtomicReference<String>());
      writeContentToFile(file, "initial" + i);
      final int index = i;
      configFileWatcher.addWatch(file.getPath(), new Function<byte[], Void>() {
        @Override
        public Void apply(byte[] bytes) {
          watchesFired.incrementAndGet();
          fileContents.get(index).set(new String(bytes, Charsets.UTF_8));
          return null;
        }
      });
      assertEquals(i + 1, watchesFired.get());
      assertEquals("initial" + i, fileContents.get(i).get());
    }

    // Update contents of all files and verify callbacks.
    for (int i = 0; i < 10; i++) {
      writeContentToFile(files.get(i), "update" + i);
    }
    configFileWatcher.runWatcherTaskNow();
    assertEquals(20, watchesFired.get());
    for (AtomicReference<String> sRef : fileContents) {
      assertTrue(sRef.get().startsWith("update"));
    }

    // Update contents of all files again, but now even ones have same content.
    for (int i = 0; i < 10; i++) {
      String contentPrefix = (i % 2) == 0 ? "update" : "newUpdate";
      writeContentToFile(files.get(i), contentPrefix + i);
    }
    configFileWatcher.runWatcherTaskNow();
    assertEquals(25, watchesFired.get());
    for (int i = 0; i < 10; i++) {
      String contentPrefix = (i % 2) == 0 ? "update" : "newUpdate";
      assertTrue(fileContents.get(i).get().startsWith(contentPrefix));
    }

    // Only update files at index 2 and 5.
    writeContentToFile(files.get(2), "yetAnotherUpdate");
    writeContentToFile(files.get(5), "yetAnotherUpdate");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(27, watchesFired.get());
    for (int i = 0; i < 10; i++) {
      if (i == 2 || i == 5) {
        assertEquals("yetAnotherUpdate", fileContents.get(i).get());
      } else {
        assertNotEquals("yetAnotherUpdate", fileContents.get(i).get());
      }
    }

    // Now delete file at index 0, and update the one at index 3. the index 3 update
    // should still fire.
    files.get(0).delete();
    writeContentToFile(files.get(3), "yetAnotherUpdate");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(28, watchesFired.get());
    assertEquals("yetAnotherUpdate", fileContents.get(2).get());

    // Now recreate the file at index 0, watch should fire.
    writeContentToFile(files.get(0), "recreated");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(29, watchesFired.get());
    assertEquals("recreated", fileContents.get(0).get());

  }

  @Test
  public void testMultipleWatchersOnSingleFile() throws IOException {
    final AtomicInteger watchesFired = new AtomicInteger(0);
    final List<AtomicReference<String>> fileContents = Lists.newArrayList();
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    File testConfigFile = createTempFile();
    writeContentToFile(testConfigFile, "initial");
    for (int i = 0; i < 10; i++) {
      fileContents.add(new AtomicReference<String>());
      final int index = i;
      configFileWatcher.addWatch(testConfigFile.getPath(), new Function<byte[], Void>() {
        @Override
        public Void apply(byte[] bytes) {
          watchesFired.incrementAndGet();
          fileContents.get(index).set(new String(bytes, Charsets.UTF_8));
          return null;
        }
      });
    }
    assertEquals(10, watchesFired.get());
    for (AtomicReference<String> sRef : fileContents) {
      assertEquals("initial", sRef.get());
    }

    // Now update the contents and run the watcher task. All watches should fire.
    writeContentToFile(testConfigFile, "update");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(20, watchesFired.get());
    for (AtomicReference<String> sRef : fileContents) {
      assertEquals("update", sRef.get());
    }

    // Now update the contents but keep unchanged. No watches should fire.
    writeContentToFile(testConfigFile, "update");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(20, watchesFired.get());
  }

  @Test
  public void testExceptionInWatcherCallback() throws IOException {
    final AtomicInteger watchesFired = new AtomicInteger(0);
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    File testConfigFile = createTempFile();
    writeContentToFile(testConfigFile, "initial");

    // Add a watcher that throws an exception in its callback. It should not prevent other
    // watchers from getting notified.
    configFileWatcher.addWatch(testConfigFile.getPath(), new Function<byte[], Void>() {
      @Override
      public Void apply(byte[] bytes) {
        if (new String(bytes, Charset.defaultCharset()).equals("update")) {
          throw new RuntimeException();
        }
        return null;
      }
    });

    configFileWatcher.addWatch(testConfigFile.getPath(), new Function<byte[], Void>() {
      @Override
      public Void apply(byte[] bytes) {
        watchesFired.incrementAndGet();
        return null;
      }
    });

    assertEquals(1, watchesFired.get());

    writeContentToFile(testConfigFile, "update");
    configFileWatcher.runWatcherTaskNow();
    assertEquals(2, watchesFired.get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyFilePath() throws IOException {
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    configFileWatcher.addWatch("", new Function<byte[], Void>() {
      @Override
      public Void apply(byte[] bytes) {
        return null;
      }
    });
  }

  @Test(expected = FileNotFoundException.class)
  public void testNonExistentFile() throws IOException {
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    configFileWatcher.addWatch("/foo/bar", new Function<byte[], Void>() {
      @Override
      public Void apply(byte[] bytes) {
        return null;
      }
    });
  }

  @Test(expected = NullPointerException.class)
  public void testNullCallback() throws IOException {
    PollingFileWatcher configFileWatcher = createPollingFileWatcher();
    configFileWatcher.addWatch("/tmp/foo", null);
  }
}

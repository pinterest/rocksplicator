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

package com.pinterest.rocksplicator.publisher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.helix.model.ExternalView;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Parallelizes execution of individual shardMapPublishers by executing the calls
 * into dedicated thread. It is guaranteed that the publishing for individual publisher
 * provided in the list will always be executed by same thread, and hence will be in
 * FIFO order.
 */
public class ParallelShardMapPublisher<T> implements ShardMapPublisher<T> {

  private final List<ShardMapPublisher<T>> shardMapPublishers;
  private final List<ExecutorService> executorServices;

  public ParallelShardMapPublisher(final List<ShardMapPublisher<T>> shardMapPublishers) {
    Preconditions.checkNotNull(shardMapPublishers);
    this.shardMapPublishers = shardMapPublishers;
    this.executorServices = Lists.newArrayListWithCapacity(shardMapPublishers.size());
    for (int i = 0; i < shardMapPublishers.size(); ++i) {
      this.executorServices.add(Executors.newSingleThreadExecutor());
    }
  }

  @Override
  public synchronized void publish(final Set<String> validResources,
                                   final List<ExternalView> externalViews,
                                   final T shardMap) {
    final CountDownLatch latch = new CountDownLatch(shardMapPublishers.size());
    for (int index = 0; index < shardMapPublishers.size(); ++index) {
      ShardMapPublisher<T> shardMapPublisher = shardMapPublishers.get(index);
      ExecutorService executorService = executorServices.get(index);
      executorService.submit(() -> {
        try {
          shardMapPublisher.publish(validResources, externalViews, shardMap);
        } finally {
          latch.countDown();
        }
      });
    }

    // Wait for all tasks to be completed, before returning.
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    for (ExecutorService executorService : executorServices) {
      executorService.shutdown();
    }
    for (ExecutorService executorService : executorServices) {
      while (!executorService.isTerminated()) {
        try {
          executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}

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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.ExternalView;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelShardMapPublisherTest {

  @Test
  public void testAllPublishersAreCalled() {
    Set<String> resources = ImmutableSet.of();
    List<ExternalView> externalViews = ImmutableList.of();
    JSONObject shardMapJsonObject = new JSONObject();
    shardMapJsonObject.put("resource1", new JSONObject());

    ShardMapPublisher<JSONObject> mockPublisher1 = mock(ShardMapPublisher.class);
    ShardMapPublisher<JSONObject> mockPublisher2 = mock(ShardMapPublisher.class);
    ShardMapPublisher<JSONObject> mockPublisher3 = mock(ShardMapPublisher.class);
    ShardMapPublisher<JSONObject> mockPublisher4 = mock(ShardMapPublisher.class);

    ParallelShardMapPublisher parallelShardMapPublisher = new ParallelShardMapPublisher(
        ImmutableList.of(mockPublisher1, mockPublisher2, mockPublisher3, mockPublisher4));

    parallelShardMapPublisher.publish(resources, externalViews, shardMapJsonObject);

    verify(mockPublisher1, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
    verify(mockPublisher2, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
    verify(mockPublisher3, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
    verify(mockPublisher4, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
  }

  @Test
  public void testOnePublisherDoesNotAffectOther() {
    Set<String> resources = ImmutableSet.of();
    List<ExternalView> externalViews = ImmutableList.of();
    JSONObject shardMapJsonObject = new JSONObject();
    shardMapJsonObject.put("resource1", new JSONObject());

    AtomicBoolean mockPublisher1Called = new AtomicBoolean(false);
    ShardMapPublisher<JSONObject> mockPublisher1 = spy(new ShardMapPublisher<JSONObject>() {
      @Override
      public void publish(Set<String> validResources,
                          List<ExternalView> externalViews,
                          JSONObject shardMap) {
        mockPublisher1Called.set(true);
        throw new RuntimeException();
      }
    });
    ShardMapPublisher<JSONObject> mockPublisher2 = mock(ShardMapPublisher.class);
    ShardMapPublisher<JSONObject> mockPublisher3 = mock(ShardMapPublisher.class);
    ShardMapPublisher<JSONObject> mockPublisher4 = mock(ShardMapPublisher.class);

    ParallelShardMapPublisher parallelShardMapPublisher = new ParallelShardMapPublisher(
        ImmutableList.of(mockPublisher1, mockPublisher2, mockPublisher3, mockPublisher4));

    parallelShardMapPublisher.publish(resources, externalViews, shardMapJsonObject);

    assertTrue(mockPublisher1Called.get());
    verify(mockPublisher1, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
    verify(mockPublisher2, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
    verify(mockPublisher3, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
    verify(mockPublisher4, times(1)).publish(
        resources, externalViews, shardMapJsonObject);
  }

}

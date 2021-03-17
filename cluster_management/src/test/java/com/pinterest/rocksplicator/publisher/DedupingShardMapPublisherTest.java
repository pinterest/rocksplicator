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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.ExternalView;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

public class DedupingShardMapPublisherTest {

  @Test
  public void testDeduping() {
    Set<String> resources = ImmutableSet.of();
    List<ExternalView> externalViews = ImmutableList.of();
    JSONObject shardMapJsonObject = new JSONObject();

    ShardMapPublisher<String> mockPublisher = mock(ShardMapPublisher.class);

    DedupingShardMapPublisher deDupublisher = new DedupingShardMapPublisher(mockPublisher);

    shardMapJsonObject.put("resource1", new JSONObject());

    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    verify(mockPublisher, Mockito.times(1))
        .publish(resources, externalViews, shardMapJsonObject.toString());

    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    verifyNoMoreInteractions(mockPublisher);

    shardMapJsonObject.put("resource2", new JSONObject());
    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    verify(mockPublisher, Mockito.times(1))
        .publish(resources, externalViews, shardMapJsonObject.toString());
  }
}

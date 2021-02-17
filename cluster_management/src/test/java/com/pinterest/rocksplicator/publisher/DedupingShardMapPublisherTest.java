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
    verify(mockPublisher, Mockito.times(1)).publish(resources, externalViews, shardMapJsonObject.toString());

    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    verifyNoMoreInteractions(mockPublisher);

    shardMapJsonObject.put("resource2", new JSONObject());
    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    verify(mockPublisher, Mockito.times(1)).publish(resources, externalViews, shardMapJsonObject.toString());
  }
}

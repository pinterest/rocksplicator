package com.pinterest.rocksplicator.publisher;

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

    ShardMapPublisher<String> mockPublisher = Mockito.mock(ShardMapPublisher.class);

    DedupingShardMapPublisher deDupublisher = new DedupingShardMapPublisher(mockPublisher);

    shardMapJsonObject.put("resource1", new JSONObject());

    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    Mockito.verify(mockPublisher, Mockito.times(1)).publish(resources, externalViews, shardMapJsonObject.toString());

    deDupublisher.publish(resources, externalViews, shardMapJsonObject);
    Mockito.verifyNoMoreInteractions(mockPublisher);

  }
}

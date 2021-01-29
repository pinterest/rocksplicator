package com.pinterest.rocksplicator.eventstore;

import org.apache.helix.model.ExternalView;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

public interface ExternalViewLeaderEventLogger extends Closeable {

  void process(
      final List<ExternalView> externalViews,
      final Set<String> disabledHosts,
      final long shardMapGenStartTimeMillis,
      final long shardMapPostTimeMillis);
}

package com.pinterest.rocksplicator.eventstore;

import java.io.Closeable;
import java.io.IOException;

public interface MergeableReadWriteStore<R, E> extends Closeable {
  R read() throws IOException;
  R merge(E data) throws IOException;
  R mergeBatch(R updateRecord) throws IOException;
}

package com.pinterest.rocksplicator.eventstore;

import java.util.function.BiFunction;

public interface SingleMergeOperator<R, E> extends BiFunction<R, E, R> {}

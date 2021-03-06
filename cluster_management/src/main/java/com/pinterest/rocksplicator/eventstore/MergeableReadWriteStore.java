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

package com.pinterest.rocksplicator.eventstore;

import java.io.Closeable;
import java.io.IOException;

public interface MergeableReadWriteStore<R, E> extends Closeable {

  /**
   * Read an existing data from the store. Reading a non-existing data will cause an io exception.
   */
  R read() throws IOException;

  /**
   * Provides a read-modify-write semantics operations. If the data doesn't already exist in the
   * store, then this operation de-generates to insert (write/update) operation.
   * If there already exists data in the store, this operation provides read-merge-write semantics
   * i.e.. the existing data is merged with new updateRecord argument passed in and then written
   * back to the store. If there is any issue in the reading data from underlying store, or issue
   * while writing to the store, the implemention should throw an IOException.
   */
  R merge(R updateRecord) throws IOException;
}

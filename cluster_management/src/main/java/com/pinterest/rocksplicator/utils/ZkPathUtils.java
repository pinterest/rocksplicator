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

package com.pinterest.rocksplicator.utils;

public class ZkPathUtils {

  private static final String BASE_PATH_PER_RESOURCE_SHARD_MAP
      = "/rocksplicator-shard_map/gzipped-json/byResource";

  public static String getClusterShardMapParentPath(String clusterName) {
    return String.format("%s/%s", BASE_PATH_PER_RESOURCE_SHARD_MAP, clusterName);
  }

  public static String getClusterResourceShardMapPath(String clusterName, String resourceName) {
    return String.format("%s/%s/%s", BASE_PATH_PER_RESOURCE_SHARD_MAP, clusterName, resourceName);
  }
}

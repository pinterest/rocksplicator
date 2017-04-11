/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.util;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public final class ShardUtil {

  public static final String HDFS_PATH_FORMAT = "%s/%s/%s/%05d/%s/%s";

  private ShardUtil() {
  }

  public static String getDBNameFromSegmentAndShardId(String segment, int shardId) {
    return String.format("%s%05d", segment, shardId);
  }

  public static String getHdfsPath(String hdfsDir,
                                   String clusterName,
                                   String segmentName,
                                   int shardId,
                                   String upstreamIp,
                                   String dateTimeStr) {
    return String.format(HDFS_PATH_FORMAT,
        hdfsDir, clusterName, segmentName, shardId, upstreamIp, dateTimeStr);
  }
}

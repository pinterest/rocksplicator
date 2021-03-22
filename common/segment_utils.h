/// Copyright 2016 Pinterest Inc.
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

#pragma once

#include <string>

namespace common {
/*
 * Convert a segment name to a db name
 */
std::string SegmentToDbName(const std::string& segment, const int shard_id);
/*
 * Convert a db name to segment name
 */
std::string DbNameToSegment(const std::string& db_name);

/*
 * Extract the shard id from a db name
 */
int ExtractShardId(const std::string& db_name);

/*
 * Convert a db name (test00100) to helix partition name (test_100)
 */
std::string DbNameToHelixPartitionName(const std::string& db_name);

}  // namespace common
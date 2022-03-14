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

//
// @author bol (bol@pinterest.com)
//

#pragma once

#include <string>

namespace replicator {

extern const std::string kReplicatorLatency;
extern const std::string kReplicatorOutBytes;
extern const std::string kReplicatorOutNumUpdates;
extern const std::string kReplicatorInBytes;
extern const std::string kReplicatorWriteBytes;

extern const std::string kReplicatorConnectionErrors;
extern const std::string kReplicatorRemoteApplicationExceptions;
extern const std::string kReplicatorRemoteApplicationExceptionsNotFound;

extern const std::string kReplicatorGetUpdatesSinceErrors;
extern const std::string kReplicatorGetUpdatesMissingSequence;
extern const std::string kReplicatorGetUpdatesSinceMs;
extern const std::string kReplicatorReplyUpdatesSuccessLatency;
extern const std::string kReplicatorReplyUpdatesFailureLatency;

extern const std::string kReplicatorLeaderSequenceNumbersBehind;
extern const std::string kReplicatorLeaderReset;

extern const std::string kReplicatorWriteSuccess;
extern const std::string kReplicatorWriteLeaderFailure;
extern const std::string kReplicatorWriteWaitTimedOut;
extern const std::string kReplicatorWriteToLeaderMs;
extern const std::string kReplicatorWriteSuccessResponseTime;
extern const std::string kReplicatorWriteFailureResponseTime;

extern const std::string kReplicatorPullRequests;
extern const std::string kReplicatorPullRequestsSuccess;
extern const std::string kReplicatorPullRequestsFailure;
extern const std::string kReplicatorPullRequestsNoUpdates;
extern const std::string kReplicatorPullFromNonLeader;
extern const std::string kReplicatorPullLatency;
extern const std::string kReplicatorHandleResponseFailure;
extern const std::string kReplicatorResetUpstreamOnNoUpdates;

// add value to metric_name. If db_name is not empty, add value to the per db
// metric also
void logMetric(const std::string& metric_name, int64_t value,
               const std::string& db_name = std::string());

// add value to counter_name. If db_name is not empty, add value to the per db
// counter also
void incCounter(const std::string& counter_name, uint64_t value,
                const std::string& db_name = std::string());

}  // namespace replicator

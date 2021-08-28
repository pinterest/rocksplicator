# Copyright 2016 Pinterest Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

namespace cpp2 cdc_admin
namespace java com.pinterest.cdc_admin.thrift

enum CDCAdminErrorCode {
  NOT_FOUND = 1,
  ALREADY_EXIST = 2,
  INVALID_ROLE = 3,
  INVALID_UPSTREAM = 4,
  ADMIN_ERROR = 5,
}

exception CDCAdminException {
  1: required string message,
  2: required CDCAdminErrorCode errorCode,
}

struct AddObserverRequest {
  # the db to add an observer for.
  # if this db is already being observed, an ALREADY_EXIST error is thrown
  1: required string db_name,
  2: required string upstream_ip,
  # The following are needed if the replicator zk and helix cluster will not be provided via command-line args
  3: optional string replicator_zk_cluster,
  4: optional string replicator_helix_cluster,
}

struct AddObserverResponse {
  # for future use
}

struct RemoveObserverRequest {
  # the db to remove the observer for
  1: required string db_name,
}

struct RemoveObserverResponse {
  # for future use
}

struct CheckObserverRequest {
  # the DB to check for an observer for
  1: required string db_name,
}

struct CheckObserverResponse {
  # the largest sequence number that has been published for this segment
  1: optional i64 seq_num = 0,
}

struct GetSequenceNumberRequest {
  # the db to get sequence number of the observer for
  1: required string db_name,
}

struct GetSequenceNumberResponse {
  1: required i64 seq_num,
}

service CdcAdmin {

/*
 * Ping the server for liveness.
 */
void ping()

/*
 * Add observer for a DB to the host, throw exception if it already exists
 */
AddObserverResponse addObserver(1: AddObserverRequest request)
  throws (1:CDCAdminException e)

/*
 * Check if an observer for a DB exists on a host
 */
CheckObserverResponse checkObserver(1: CheckObserverRequest request)
  throws (1:CDCAdminException e)

/*
 * Remove an observer for a DB
 */
RemoveObserverResponse removeObserver(1:RemoveObserverRequest request)
  throws (1:CDCAdminException e)

/*
 * Get the sequence number of the observer for the input db.
 * This can be used for debugging
 */
GetSequenceNumberResponse getSequenceNumber(1:GetSequenceNumberRequest request)
  throws (1:CDCAdminException e)
} (priority = 'HIGH')

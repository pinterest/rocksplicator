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

# meta data maintained for each individual DB
struct DBMetaData {
  1: required string db_name,
  # the s3 dataset (identified by s3_bucket + s3_path) that is currently being
  # hosted by this DB.
  2: optional string s3_bucket,
  3: optional string s3_path,
  4: optional i64 last_kafka_msg_timestamp_ms
}

enum AdminErrorCode {
  DB_NOT_FOUND = 1,
  DB_EXIST = 2,
  INVALID_DB_ROLE = 3,
  INVALID_UPSTREAM = 4,
  DB_ADMIN_ERROR = 5,
  DB_ERROR = 6,
}

exception CDCAdminException {
  1: required string message,
  2: required AdminErrorCode errorCode,
}

struct AddDBRequest {
  # the db to add. the db is added as slave, so upstream_ip must be provided
  # if the db exists already, a DB_EXIST error is thrown
  1: required string db_name,
  2: required string upstream_ip,
  # if set, add the db to the db_manager with the specified role. one of FOLLOWER, STANDBY
  3: optional string db_role = "STANDBY",
}

struct AddDBResponse {
  # for future use
}

struct CloseDBRequest {
  # the db to close
  1: required string db_name,
}

struct CloseDBResponse {
  # for future use
}

struct CheckDBRequest {
  # the DB to check
  1: required string db_name,
  # retrieve DB's option values for specified option names
  2: optional list<string> option_names,
  # if true, get the DBMetaData of the db
  3: optional bool include_meta,
  # get DB's properties
  4: optional list<string> property_names,
}

struct CheckDBResponse {
  # the largest sequence number of the DB
  1: optional i64 seq_num = 0,
  # if the DB is Leader or Standby
  2: optional bool is_leader = false,
}

struct ChangeDBRoleAndUpstreamRequest {
  # the db to change
  1: required string db_name,
  # the role to change to, only "FOLLOWER" and "STANDBY" are supported
  2: required string new_role,
  # the new upstream to pull updates from
  3: optional string upstream_ip,
  4: optional i16 upstream_port,
}

struct ChangeDBRoleAndUpstreamResponse {
  # for future use
}

struct GetSequenceNumberRequest {
  # the db to get sequence number for
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
 * Add the DB to the host, throw exception if it already exists
 */
AddDBResponse addDB(1: AddDBRequest request)
  throws (1:CDCAdminException e)

/*
 * Check if a DB exists on a host
 */
CheckDBResponse checkDB(1: CheckDBRequest request)
  throws (1:CDCAdminException e)

/*
 * Close a DB
 */
CloseDBResponse closeDB(1:CloseDBRequest request)
  throws (1:CDCAdminException e)

/*
 * Change the role and the upstream for the specified db
 */
ChangeDBRoleAndUpstreamResponse changeDBRoleAndUpStream(
    1:ChangeDBRoleAndUpstreamRequest request)
  throws (1:CDCAdminException e)

/*
 * Get the sequence number of the db.
 * This is useful when choosing a new MASTER from multiple SLAVEs
 */
GetSequenceNumberResponse getSequenceNumber(1:GetSequenceNumberRequest request)
  throws (1:CDCAdminException e)
} (priority = 'HIGH')

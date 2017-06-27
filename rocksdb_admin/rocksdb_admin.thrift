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

namespace cpp2 admin
namespace java com.pinterest.rocksdb_admin.thrift

enum AdminErrorCode {
  DB_NOT_FOUND = 1,
  DB_EXIST = 2,
  INVALID_DB_ROLE = 3,
  INVALID_UPSTREAM = 4,
  DB_ADMIN_ERROR = 5,
  DB_ERROR = 6,
}

exception AdminException {
  1: required string message,
  2: required AdminErrorCode errorCode,
}

struct BackupDBRequest {
  # the db to backup
  1: required string db_name,
  # the hdfs path to backup to
  2: required string hdfs_backup_dir,
  # rate limit in MB/S, a non positive value means no limit
  3: optional i32 limit_mbs = 0,
}

struct BackupDBResponse {
  # for future use
}

struct RestoreDBRequest {
  # the db to be restored
  1: required string db_name,
  # the hdfs path to restore from
  2: required string hdfs_backup_dir,
  # where to pull update from after restoring
  3: required string upstream_ip,
  4: required i16 upstream_port,
  # rate limit in MB/S, a non positive value means no limit
  5: optional i32 limit_mbs = 0,
}

struct RestoreDBResponse {
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
  # check to ensure this db exists
  1: required string db_name,
}

struct CheckDBResponse {
  # for future use
}

struct ChangeDBRoleAndUpstreamRequest {
  # the db to change
  1: required string db_name,
  # the role to change to, only "MASTER" and "SLAVE" are supported
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

struct ClearDBRequest {
  1: required string db_name,
  2: optional bool reopen_db = true,
}

struct ClearDBResponse {
  # for future use
}

struct AddS3SstFilesToDBRequest {
  1: required string db_name,
  2: required string s3_bucket,
  3: required string s3_path,
  4: optional i32 s3_download_limit_mb = 64,
}

struct AddS3SstFilesToDBResponse {
  # for future use
}

struct SetDBOptionsRequest {
  # For keys supported in this map, please refer to:
  # https://github.com/facebook/rocksdb/blob/master/util/cf_options.h#L161
  1: required map<string, string> options;
  2: required string db_name;
}

struct SetDBOptionsResponse {
  # for future use
}

service Admin {

/*
 * Ping the server for liveness.
 */
void ping()

/*
 * Create a backup on hdfs for the specified db.
 */
BackupDBResponse backupDB(1:BackupDBRequest request)
  throws (1:AdminException e)

/*
 * Restore the db from a backup directory on hdfs.
 * All existing data in this db will be wiped out before restoring.
 * The newly restored db is always SLAVE, and it will pull updates
 * from upstream_ip_port thereafter
 */
RestoreDBResponse restoreDB(1:RestoreDBRequest request)
  throws (1:AdminException e)

/*
 * Check if a DB exists on a host
 */
CheckDBResponse checkDB(1: CheckDBRequest request)
  throws (1:AdminException e)

/*
 * Close a DB
 */
CloseDBResponse closeDB(1:CloseDBRequest request)
  throws (1:AdminException e)

/*
 * Change the role and the upstream for the specified db
 */
ChangeDBRoleAndUpstreamResponse changeDBRoleAndUpStream(
    1:ChangeDBRoleAndUpstreamRequest request)
  throws (1:AdminException e)

/*
 * Get the sequence number of the db.
 * This is useful when choosing a new MASTER from multiple SLAVEs
 */
GetSequenceNumberResponse getSequenceNumber(1:GetSequenceNumberRequest request)
  throws (1:AdminException e)

/*
 * Clear the content of a DB.
 */
ClearDBResponse clearDB(1:ClearDBRequest request)
  throws (1:AdminException e)

/*
 * Add SST files from s3 to a DB.
 */
AddS3SstFilesToDBResponse addS3SstFilesToDB(1:AddS3SstFilesToDBRequest request)
  throws (1:AdminException e)

/*
 * Set mutable DB options.
 * The option map in request will be passed down to Rocksdb::DB::SetOptions().
 */
SetDBOptionsResponse setDBOptions(1:SetDBOptionsRequest request)
  throws (1:AdminException e)
}

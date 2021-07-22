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
// @author shu (shu@pinterest.com)
//

#include "common/s3util.h"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <fstream>
#include "boost/algorithm/string.hpp"
#include "boost/iostreams/stream.hpp"
#include "common/aws_s3_rate_limiter.h"
#include "common/stats/stats.h"
#include "glog/logging.h"

using std::string;
using std::vector;
using std::tuple;
using Aws::S3::Model::CopyObjectRequest;
using Aws::S3::Model::DeleteObjectRequest;
using Aws::S3::Model::GetObjectRequest;
using Aws::S3::Model::ListObjectsRequest;
using Aws::S3::Model::HeadObjectRequest;
using Aws::S3::Model::PutObjectRequest;

DEFINE_int32(direct_io_buffer_n_pages, 1,
             "Number of pages we need to set to direct io buffer");
DEFINE_bool(disable_s3_download_stream_buffer, false,
            "disable the stream buffer used by s3 downloading");


namespace {
  const std::string kS3GetObject = "s3_getobject";
  const std::string kS3GetObjectToStream = "s3_getobject_tostream";
  const std::string kS3ListObjects = "s3_listobjects";
  const std::string kS3ListObjectsItems = "s3_listobjects_items";
  const std::string kS3ListObjectsV2 = "s3_listobjectsv2";
  const std::string kS3ListObjectsV2Items = "s3_listobjectsv2_items";
  const std::string kS3ListAllObjects = "s3_listallobjects";
  const std::string kS3ListAllObjectsItems = "s3_listallobjects_items";
  const std::string kS3GetObjects = "s3_getobjects";
  const std::string kS3GetObjectMetadata = "s3_getobject_metadata";
  const std::string kS3GetObjectSizeAndModTime = "s3_getobject_sizeandmodtime";
  const std::string kS3PutObject = "s3_putobject";
  const std::string kS3GetObjectCallable = "s3_getobject_callable";
  const std::string kS3CopyObject = "s3_copyobject";
  const std::string kS3DeleteObject = "s3_deleteobject";
}

namespace common {

std::mutex S3Util::counter_mutex_;
std::uint32_t S3Util::instance_counter_(0);
const uint32_t kPageSize = getpagesize();
SDKOptions S3Util::options_;

DirectIOWritableFile::DirectIOWritableFile(const string& file_path)
    : fd_(-1)
    , file_size_(0)
    , buffer_()
    , offset_(0)
    , buffer_size_(FLAGS_direct_io_buffer_n_pages * kPageSize){
  if (posix_memalign(&buffer_, kPageSize, buffer_size_) != 0) {
    LOG(ERROR) << "Failed to allocate memaligned buffer, errno = " << errno;
    return;
  }

  int flag = O_WRONLY | O_TRUNC | O_CREAT | O_DIRECT;
  fd_ = open(file_path.c_str(), flag , S_IRUSR | S_IWUSR | S_IRGRP);
  if (fd_ < 0) {
    LOG(ERROR) << "Failed to open " << file_path << " with flag " << flag
               << ", errno = " << errno;
    free(buffer_);
    buffer_ = nullptr;
  }
}

DirectIOWritableFile::~DirectIOWritableFile() {
  if (buffer_ == nullptr || fd_ < 0) {
    return;
  }

  if (offset_ > 0) {
    if (::write(fd_, buffer_, buffer_size_) != buffer_size_) {
      LOG(ERROR) << "Failed to write last chunk, errno = " << errno;
    } else {
      ftruncate(fd_, file_size_);
    }
  }
  free(buffer_);
  close(fd_);
}

std::streamsize DirectIOWritableFile::write(const char* s, std::streamsize n) {
  if (buffer_ == nullptr || fd_ < 0) {
    return -1;
  }
  uint32_t remaining = static_cast<uint32_t>(n);
  while (remaining > 0) {
    uint32_t bytes = std::min((buffer_size_ - offset_), remaining);
    std::memcpy((char*)buffer_ + offset_, s, bytes);
    offset_ += bytes;
    remaining -= bytes;
    s += bytes;
    // flush when buffer is full
    if (offset_ == buffer_size_) {
      if (::write(fd_, buffer_, buffer_size_) != buffer_size_) {
        LOG(ERROR) << "Failed to write to DirectIOWritableFile, errno = "
                   << errno;
        return -1;
      }
      // reset offset
      offset_ = 0;
    }
  }
  file_size_ += n;
  return n;
}


GetObjectResponse S3Util::getObject(
    const string& key, const string& local_path, const bool direct_io) {
  Stats::get()->Incr(kS3GetObject);
  auto getObjectResult = sdkGetObject(key, local_path, direct_io);
  string err_msg_prefix =
    "Failed to download from " + key + " to " + local_path + " error: ";
  if (getObjectResult.IsSuccess()) {
    return GetObjectResponse(true, "");
  } else {
    return GetObjectResponse(false,
        std::move(err_msg_prefix + getObjectResult.GetError().GetMessage()));
  }
}

GetObjectResponse S3Util::getObject(const string& key, iostream* out) {
  Stats::get()->Incr(kS3GetObjectToStream);
  GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(bucket_);
  getObjectRequest.SetKey(key);
  auto getObjectResult = s3Client->GetObject(getObjectRequest);
  if (getObjectResult.IsSuccess()) {
    *out << getObjectResult.GetResult().GetBody().rdbuf();
    return GetObjectResponse(true, "");
  } else {
    string err_msg_prefix = "Failed to get " + key + ", error: ";
    return GetObjectResponse(false,
        std::move(err_msg_prefix + getObjectResult.GetError().GetMessage()));
  }
}

SdkGetObjectResponse S3Util::sdkGetObject(const string& key,
                                          const string& local_path,
                                          const bool direct_io) {
  GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(bucket_);
  getObjectRequest.SetKey(key);
  if (local_path != "") {
    if (direct_io) {
      getObjectRequest.SetResponseStreamFactory(
        [=]() {
          if (FLAGS_disable_s3_download_stream_buffer) {
            return new boost::iostreams::stream<DirectIOFileSink>(
              DirectIOFileSink(local_path), 0, 0);
          } else {
            return new boost::iostreams::stream<DirectIOFileSink>(local_path);
          }
        }
      );
    } else {
      getObjectRequest.SetResponseStreamFactory(
        [=]() {
          // "T" is just a place holder for ALLOCATION_TAG.
          return Aws::New<Aws::FStream>(
                  "T", local_path.c_str(),
                  std::ios_base::out | std::ios_base::in | std::ios_base::trunc);
        }
      );
    }
  }
  auto getObjectResult = s3Client->GetObject(getObjectRequest);
  return getObjectResult;
}

void S3Util::listObjectsHelper(const string& prefix, const string& delimiter,
                               const string& marker, vector<string>* objects,
                               string* next_marker, string* error_message) {
  ListObjectsRequest listObjectRequest;
  listObjectRequest.SetBucket(bucket_);
  listObjectRequest.SetPrefix(prefix);
  if (!delimiter.empty()) {
    listObjectRequest.SetDelimiter(delimiter);
  }
  if (!marker.empty()) {
    listObjectRequest.SetMarker(marker);
  }
  auto listObjectResult = s3Client->ListObjects(listObjectRequest);
  if (listObjectResult.IsSuccess()) {
    if (!delimiter.empty()) {
      Aws::Vector<Aws::S3::Model::CommonPrefix> contents =
        listObjectResult.GetResult().GetCommonPrefixes();
      for (const auto& object : contents) {
        objects->push_back(object.GetPrefix());
      }
    } else {
      Aws::Vector<Aws::S3::Model::Object> contents =
        listObjectResult.GetResult().GetContents();
      for (const auto& object : contents) {
        objects->push_back(object.GetKey());
      }
    }

    if (listObjectResult.GetResult().GetIsTruncated() &&
            next_marker != nullptr) {
      if (listObjectResult.GetResult().GetNextMarker().empty()) {
        // if the response is truncated but NextMarker is not set,
        // last object of response can be used as marker.
        *next_marker = objects->back();
      } else {
        *next_marker = listObjectResult.GetResult().GetNextMarker();
      }
    }
  } else {
    if (error_message != nullptr) {
      *error_message = listObjectResult.GetError().GetMessage();
    }
  }
}


ListObjectsResponse S3Util::listObjects(const string& prefix,
                                        const string& delimiter) {
  Stats::get()->Incr(kS3ListObjects);
  vector<string> objects;
  string error_message;
  listObjectsHelper(prefix, delimiter, "", &objects, nullptr, &error_message);
  Stats::get()->Incr(kS3ListObjectsItems, objects.size());
  return ListObjectsResponse(objects, error_message);
}

ListObjectsResponseV2 S3Util::listObjectsV2(const string& prefix,
                                            const string& delimiter,
                                            const string& marker) {
  Stats::get()->Incr(kS3ListObjectsV2);
  vector<string> objects;
  string error_message;
  string next_marker;
  listObjectsHelper(prefix, delimiter, marker, &objects,
                    &next_marker, &error_message);
  Stats::get()->Incr(kS3ListObjectsV2Items, objects.size());
  return ListObjectsResponseV2(
          ListObjectsResponseV2Body(objects, next_marker), error_message);
}

ListObjectsResponseV2 S3Util::listAllObjects(const string& prefix, const string& delimiter) {
  Stats::get()->Incr(kS3ListAllObjects);
  vector<string> output;
  vector<string> objects;
  string error_message;
  string marker;
  string next_marker;
  do {
    listObjectsHelper(prefix, delimiter, marker, &objects, &next_marker, &error_message);
    if (!error_message.empty()) {
      break;
    }
    output.insert(output.end(), objects.begin(), objects.end());
    objects.clear();
    marker = next_marker;
    next_marker.clear();
  } while (!marker.empty());
  Stats::get()->Incr(kS3ListAllObjectsItems, output.size());
  return ListObjectsResponseV2(ListObjectsResponseV2Body(output, next_marker), error_message);
}

GetObjectsResponse S3Util::getObjects(
    const string& prefix, const string& local_directory,
    const string& delimiter, const bool direct_io) {
  Stats::get()->Incr(kS3GetObjects);
  ListObjectsResponse list_result = listObjects(prefix);
  vector<S3UtilResponse<bool>> results;
  if (!list_result.Error().empty()) {
    return GetObjectsResponse(results, list_result.Error());
  } else {
    string formatted_dir_path = local_directory;
    if (local_directory.back() != '/') {
      formatted_dir_path += "/";
    }
    for (string object_key : list_result.Body()) {
      // sanitization check
      vector<string> parts;
      boost::split(parts, object_key, boost::is_any_of(delimiter));
      string object_name = parts[parts.size() - 1];
      if (object_name.empty()) {
        continue;
      }
      GetObjectResponse download_response =
        getObject(object_key, formatted_dir_path + object_name, direct_io);
      if (download_response.Body()) {
        results.push_back(GetObjectResponse(true, object_key));
      } else {
        results.push_back(download_response);
      }
    }
    return GetObjectsResponse(results, "");
  }
}

GetObjectMetadataResponse S3Util::getObjectMetadata(const string &key) {
  Stats::get()->Incr(kS3GetObjectMetadata);
  HeadObjectRequest headObjectRequest;
  headObjectRequest.SetBucket(bucket_);
  headObjectRequest.SetKey(key);
  map<string, string> metadata;
  Aws::StringStream ss;
  ss << uri_ << "/" << headObjectRequest.GetBucket() << "/"
     << headObjectRequest.GetKey();
  XmlOutcome headObjectOutcome = s3Client->MakeHttpRequest(
          ss.str(), headObjectRequest,HttpMethod::HTTP_HEAD);
  if (!headObjectOutcome.IsSuccess()) {
    return GetObjectMetadataResponse(
            metadata, headObjectOutcome.GetError().GetMessage());
  }
  const auto& headers = headObjectOutcome.GetResult().GetHeaderValueCollection();
  const auto& md5Iter = headers.find("etag");
  if(md5Iter != headers.end()) {
    auto md5str = md5Iter->second;
    md5str.erase(std::remove(md5str.begin(), md5str.end(), '\"'), md5str.end());
    metadata["md5"] = md5str;
  }
  const auto& contentLengthIter = headers.find("content-length");
  if(contentLengthIter != headers.end()) {
    metadata["content-length"] = contentLengthIter->second;
  }
  return GetObjectMetadataResponse(std::move(metadata), "");
}

GetObjectSizeAndModTimeResponse S3Util::getObjectSizeAndModTime(const string& key) {
  Stats::get()->Incr(kS3GetObjectSizeAndModTime);
  HeadObjectRequest headObjectRequest;
  headObjectRequest.SetBucket(bucket_);
  headObjectRequest.SetKey(key);
  map<string, uint64_t> metadata;
  auto headObjectOutcome = s3Client->HeadObject(headObjectRequest);
  if (!headObjectOutcome.IsSuccess()) {
    return GetObjectSizeAndModTimeResponse(
        std::move(metadata), headObjectOutcome.GetError().GetMessage());
  }

  metadata["size"] = headObjectOutcome.GetResult().GetContentLength();
  metadata["last-modified"] = headObjectOutcome.GetResult().GetLastModified().Millis();
  return GetObjectSizeAndModTimeResponse(std::move(metadata), "");
}

PutObjectResponse S3Util::putObject(const string& key, const string& local_path, const string& tags) {
  Stats::get()->Incr(kS3PutObject);
  PutObjectRequest object_request;
  object_request.WithBucket(bucket_).WithKey(key);
  if (!tags.empty()) {
    object_request.WithTagging(tags);
  }
  auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                                                  local_path.c_str(),
                                                  std::ios_base::in | std::ios_base::binary);
  object_request.SetBody(input_data);
  auto put_result = s3Client->PutObject(object_request);

  if (put_result.IsSuccess()) {
    return PutObjectResponse(true, "");
  } else {
    string err_msg_prefix = "Failed to upload file " + local_path + " to " + key + ", error: ";
    return PutObjectResponse(false,
                             std::move(err_msg_prefix + put_result.GetError().GetMessage()));
  }
}

Aws::S3::Model::PutObjectOutcomeCallable
S3Util::putObjectCallable(const string& key, const string& local_path) {
  Stats::get()->Incr(kS3GetObjectCallable);
  PutObjectRequest object_request;
  object_request.WithBucket(bucket_).WithKey(key);
  auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                                                  local_path.c_str(),
                                                  std::ios_base::in | std::ios_base::binary);
  object_request.SetBody(input_data);
  return s3Client->PutObjectCallable(object_request);
}

CopyObjectResponse S3Util::copyObject(const string& src, const string& target) {
  Stats::get()->Incr(kS3CopyObject);
  CopyObjectRequest copyObjectRequest;
  copyObjectRequest.SetCopySource(bucket_ + "/" + src);
  copyObjectRequest.SetBucket(bucket_);
  copyObjectRequest.SetKey(target);

  auto outcome = s3Client->CopyObject(copyObjectRequest);
  if (!outcome.IsSuccess()) {
    return CopyObjectResponse(
        false, outcome.GetError().GetMessage());
  }

  return CopyObjectResponse(true, "");
}

DeleteObjectResponse S3Util::deleteObject(const string& key) {
  Stats::get()->Incr(kS3DeleteObject);
  DeleteObjectRequest deleteObjectRequest;
  deleteObjectRequest.SetBucket(bucket_);
  deleteObjectRequest.SetKey(key);

  auto outcome = s3Client->DeleteObject(deleteObjectRequest);
  if (!outcome.IsSuccess()) {
    return DeleteObjectResponse(
        false, outcome.GetError().GetMessage());
  }

  return DeleteObjectResponse(true, "");
}

tuple<string, string> S3Util::parseFullS3Path(const string& s3_path) {
  string bucket;
  string object_path;
  string formatted;
  bool start = true;
  if (s3_path.find("s3n://") == 0) {
    formatted = s3_path.substr(6);
  } else if (s3_path.find("s3://") == 0) {
    formatted = s3_path.substr(5);
  } else {
    start = false;
  }
  if (start) {
    int index = formatted.find("/");
    bucket = formatted.substr(0, index);
    object_path = formatted.substr(index+1);
  }
  return std::make_tuple(std::move(bucket), std::move(object_path));
}

shared_ptr<S3Util> S3Util::BuildS3Util(
    const uint32_t read_ratelimit_mb,
    const string& bucket,
    const uint32_t connect_timeout_ms,
    const uint32_t request_timeout_ms,
    const uint32_t max_connections,
    const uint32_t write_ratelimit_mb) {
  Aws::Client::ClientConfiguration aws_config;
  aws_config.connectTimeoutMs = connect_timeout_ms;
  aws_config.requestTimeoutMs = request_timeout_ms;
  aws_config.maxConnections = max_connections;
  if (read_ratelimit_mb > 0) {
    aws_config.readRateLimiter =
        std::make_shared<AwsS3RateLimiter>(
            read_ratelimit_mb * 1024 * 1024);
  }
  if (write_ratelimit_mb > 0) {
    aws_config.writeRateLimiter =
        std::make_shared<AwsS3RateLimiter>(
            write_ratelimit_mb * 1024 * 1024);
  }
  return std::shared_ptr<S3Util>(
      new S3Util(bucket, aws_config, read_ratelimit_mb, write_ratelimit_mb));
}

void S3Util::TryAwsInitAPI() {
  std::lock_guard<std::mutex> guard(counter_mutex_);
  if (instance_counter_ == 0) {
    LOG(INFO) << "Aws::InitAPI";
    Aws::InitAPI(options_);
  }
  ++instance_counter_;
}

void S3Util::TryAwsShutdownAPI() {
  std::lock_guard<std::mutex> guard(counter_mutex_);
  --instance_counter_;
  if (instance_counter_ == 0) {
    LOG(INFO) << "Aws::ShutdownAPI";
    Aws::ShutdownAPI(options_);
  }
}


S3Concurrent::S3Concurrent(const int upload_MBps)
  : executor_(2),  // n_threads
    transfer_config_(&executor_) {
  S3Util::TryAwsInitAPI();
  const uint64_t MB = 1000*1000;
  const uint64_t max_heap_size = 400 * MB;
  const uint64_t buffer_size = 25 * MB;
  int max_connections = 16; // reasonable upper limit, good for 10Gbps.
  // throttle concurrency according to desired rate.
  // 2 threads allows for some file interleaving while limiting cpu exposure.
  if (upload_MBps != 0) {
    const int MBps_per_connection_est = 70;
    max_connections = std::min(max_connections, std::max(1, (int)(upload_MBps/MBps_per_connection_est)));
    rate_limiter_ = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>("S3Concurrent", upload_MBps * MB);
    client_config_.writeRateLimiter = rate_limiter_;
  }
  LOG(INFO) << "S3Concurrent() upload_MBps: " << upload_MBps
            << " max_connections: " << max_connections
            << " max_heap_size MB: " << max_heap_size / MB
            << " buffer_size MB: " << buffer_size / MB;
  int socket_timeout_ms = 3000;
  client_config_.connectTimeoutMs = socket_timeout_ms;
  client_config_.requestTimeoutMs = socket_timeout_ms;
  client_config_.maxConnections = max_connections;
  client_config_.region = "us-east-1";

  s3_client_ = std::make_shared<Aws::S3::S3Client>(client_config_);
  transfer_config_.s3Client = s3_client_;
  transfer_config_.transferBufferMaxHeapSize = max_heap_size;
  transfer_config_.bufferSize = buffer_size;
  transfer_config_.transferInitiatedCallback =
    [](const Aws::Transfer::TransferManager*,
       const std::shared_ptr<const Aws::Transfer::TransferHandle>& th) {
      LOG(INFO) << "Transfer Status = " << static_cast<int>(th->GetStatus())
                << ", File Path: " << th->GetTargetFilePath() << "\n";
    };

  transfer_config_.errorCallback =
    [](const Aws::Transfer::TransferManager*,
       const std::shared_ptr<const Aws::Transfer::TransferHandle>& th,
       const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
      LOG(ERROR)
        << "Failed to download file. s3 bucket: "  << th->GetBucketName()
        << ", s3 key: " << th->GetKey()
        << ", target_path: " << th->GetTargetFilePath()
        << ", error: " << th->GetLastError().GetMessage();
    };
  context_ = Aws::MakeShared<Aws::Client::AsyncCallerContext>("upload");
  transfer_manager_ = Aws::Transfer::TransferManager::Create(transfer_config_);
}

S3Concurrent::~S3Concurrent() {
    S3Util::TryAwsShutdownAPI();
}

bool S3Concurrent::setUploadMBps(uint64_t upload_MBps) {
  const uint64_t MB = 1000*1000;
  // only useful for restricting rate below initial rate
  if (rate_limiter_) {
    rate_limiter_->SetRate(upload_MBps * MB); // is lock guarded internally
    return true;
  }
  return false;
}

bool S3Concurrent::enqueuePutObject(const string& s3_bucket, const string& key,
                                    const string& local_path, const string& sync_group_id){
  LOG(INFO) << "enqueuePutObject " << local_path << " to " << key;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto sync_collection_ptr = handles_.find(sync_group_id);
    if (sync_collection_ptr == handles_.end()) {
      handles_[sync_group_id] = vector<std::shared_ptr<Aws::Transfer::TransferHandle>>();
    }
    handles_[sync_group_id].push_back(
        std::move(transfer_manager_->UploadFile(local_path,
                                                s3_bucket,
                                                key,
                                                "application/octet-stream",
                                                Aws::Map<Aws::String, Aws::String>(),
                                                context_)));
  }
  return true;
}

bool S3Concurrent::Sync(const string& sync_group_id) {
  uint64_t num_files_transferred = 0;
  uint64_t num_bytes_transferred = 0;
  uint64_t num_failures = 0;
  vector<std::shared_ptr<Aws::Transfer::TransferHandle>> sync_group;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto sync_group_ptr = handles_.find(sync_group_id);
    if (sync_group_ptr == handles_.end()) {
      return true;
    }
    sync_group.swap(sync_group_ptr->second);
    handles_.erase(sync_group_ptr);
  }

  for(auto &h: sync_group){
    h->WaitUntilFinished();
    if (h->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
      //LOG(INFO) << "S3Concurrent transfer success: " << h->GetTargetFilePath();
      Stats::get()->Incr(kS3PutObject);
      num_bytes_transferred += h->GetBytesTransferred();
      num_files_transferred ++;
    } else {
      LOG(ERROR) << "S3Concurrent transfer fail: " << h->GetTargetFilePath();
      failures_.push_back(h);
      num_failures++;
      LOG(ERROR)
        << "Error happened when uploading files from checkpoint to S3: "
        << h->GetLastError();
    }
  }

  LOG(INFO) << "S3Concurrent sync: " << sync_group_id
            << " transferred: " << num_files_transferred
            << " failures: " << num_failures;
  return num_files_transferred == sync_group.size();
}

void S3Concurrent::clearFailures() {
  failures_.clear();
}
}  // namespace common

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
#include "glog/logging.h"

using std::string;
using std::vector;
using std::tuple;
using Aws::S3::Model::GetObjectRequest;
using Aws::S3::Model::ListObjectsRequest;
using Aws::S3::Model::HeadObjectRequest;
using Aws::S3::Model::PutObjectRequest;

DEFINE_int32(direct_io_buffer_n_pages, 1,
             "Number of pages we need to set to direct io buffer");
DEFINE_bool(disable_s3_download_stream_buffer, false,
            "disable the stream buffer used by s3 downloading");

namespace common {

std::mutex S3Util::counter_mutex_;
std::uint32_t S3Util::instance_counter_(0);
const uint32_t kPageSize = getpagesize();

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
  vector<string> objects;
  string error_message;
  listObjectsHelper(prefix, delimiter, "", &objects, nullptr, &error_message);
  return ListObjectsResponse(objects, error_message);
}

ListObjectsResponseV2 S3Util::listObjectsV2(const string& prefix,
                                            const string& delimiter,
                                            const string& marker) {
  vector<string> objects;
  string error_message;
  string next_marker;
  listObjectsHelper(prefix, delimiter, marker, &objects,
                    &next_marker, &error_message);
  return ListObjectsResponseV2(
          ListObjectsResponseV2Body(objects, next_marker), error_message);
}

GetObjectsResponse S3Util::getObjects(
    const string& prefix, const string& local_directory,
    const string& delimiter, const bool direct_io) {
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

PutObjectResponse S3Util::putObject(const string& key, const string& local_path) {
  PutObjectRequest object_request;
  object_request.WithBucket(bucket_).WithKey(key);
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
  PutObjectRequest object_request;
  object_request.WithBucket(bucket_).WithKey(key);
  auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                                                  local_path.c_str(),
                                                  std::ios_base::in | std::ios_base::binary);
  object_request.SetBody(input_data);
  return s3Client->PutObjectCallable(object_request);
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
  SDKOptions options;
  return std::shared_ptr<S3Util>(
      new S3Util(bucket, aws_config, options, read_ratelimit_mb, write_ratelimit_mb));
}

}  // namespace common

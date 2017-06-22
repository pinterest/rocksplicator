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

#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/Object.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <fstream>
#include "boost/algorithm/string.hpp"
#include "boost/iostreams/stream.hpp"
#include "glog/logging.h"

using std::string;
using std::vector;
using std::tuple;
using Aws::S3::Model::GetObjectRequest;
using Aws::S3::Model::ListObjectsRequest;
using Aws::S3::Model::HeadObjectRequest;
using Aws::Utils::RateLimits::DefaultRateLimiter;

DEFINE_int32(direct_io_buffer_n_pages, 1,
             "Number of pages we need to set to direct io buffer");


namespace common {

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
    "Failed to download from " + key + "to " + local_path + "error: ";
  if (getObjectResult.IsSuccess()) {
    return GetObjectResponse(true, "");
  } else {
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
            return new boost::iostreams::stream<DirectIOFileSink>(local_path);
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
  auto getObjectResult = s3Client.GetObject(getObjectRequest);
  return getObjectResult;
}

ListObjectsResponse S3Util::listObjects(const string& prefix) {
  vector<string> objects;
  ListObjectsRequest listObjectRequest;
  listObjectRequest.SetBucket(bucket_);
  listObjectRequest.SetPrefix(prefix);
  auto listObjectResult = s3Client.ListObjects(listObjectRequest);
  if (listObjectResult.IsSuccess()) {
    Aws::Vector<Aws::S3::Model::Object> contents =
      listObjectResult.GetResult().GetContents();
    for (auto object : contents) {
      objects.push_back(object.GetKey());
    }
    return ListObjectsResponse(objects, "");
  } else {
    return ListObjectsResponse(
        objects, listObjectResult.GetError().GetMessage());
  }
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
  XmlOutcome headObjectOutcome = s3Client.MakeHttpRequest(
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
    const uint32_t request_timeout_ms) {
  Aws::Client::ClientConfiguration aws_config;
  aws_config.connectTimeoutMs = connect_timeout_ms;
  aws_config.requestTimeoutMs = request_timeout_ms;
  aws_config.readRateLimiter = 
    std::make_shared<DefaultRateLimiter<>>(
          read_ratelimit_mb * 1024 * 1024);
  SDKOptions options;
  Aws::InitAPI(options);
  return std::make_shared<S3Util>(bucket, aws_config, options);
}

}  // namespace common

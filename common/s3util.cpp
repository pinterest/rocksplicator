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

#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsResult.h>
#include <aws/s3/model/Object.h>

#include <iostream>
#include <vector>
#include <string>
#include <tuple>
#include <fstream>
#include "boost/algorithm/string.hpp"

using std::string;
using std::vector;
using std::tuple;
using Aws::S3::Model::GetObjectRequest;
using Aws::S3::Model::ListObjectsRequest;
using Aws::S3::Model::HeadObjectRequest;
using Aws::Utils::RateLimits::DefaultRateLimiter;

namespace common {

GetObjectResponse S3Util::getObject(
    const string& key, const string& local_path) {
  auto getObjectResult = sdkGetObject(key, local_path);
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
                                          const string& local_path) {
  GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(bucket_);
  getObjectRequest.SetKey(key);
  if (local_path != "") {
    getObjectRequest.SetResponseStreamFactory(
      [=]() {
        // "T" is just a place holder for ALLOCATION_TAG.
        return Aws::New<Aws::FStream>(
                "T", local_path.c_str(),
                std::ios_base::out | std::ios_base::in | std::ios_base::trunc);
      }
    );
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
    const string& delimiter) {
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
        getObject(object_key, formatted_dir_path + object_name);
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
  // Copied from Aws S3 Client
  Aws::StringStream ss;
  ss << this->uri_ << "/" << headObjectRequest.GetBucket() << "/"
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
  return std::make_shared<S3Util>(bucket, aws_config);
}

}  // namespace common

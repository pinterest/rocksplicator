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


// Implementation of Status Server based on microhttpd.

#include "common/stats/status_server.h"

#include <sys/select.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>

#include <folly/Uri.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <microhttpd.h>

#include <memory>
#include <set>
#include <string>
#include "common/stats/stats.h"

DEFINE_int32(
    http_status_port, 9999,
    "Port at which status information such as build info/stats is exported.");

namespace common {

namespace {

int ServeCallback(void* param, struct MHD_Connection* connection,
                  const char* url, const char* method, const char* version,
                  const char* upload_data, size_t* upload_data_size,
                  void** ptr) {
  /********* BOILERPLATE code **********/
  static int dummy;
  struct MHD_Response* response;
  int ret;

  if (0 != strcmp(method, "GET")) return MHD_NO; /* unexpected method */
  if (&dummy != *ptr) {
    /* The first time only the headers are valid,
       do not respond in the first round... */
    *ptr = &dummy;
    return MHD_YES;
  }
  if (0 != *upload_data_size) return MHD_NO; /* upload data in a GET!? */
  *ptr = NULL;                               /* clear context pointer */

  StatusServer* server = reinterpret_cast<StatusServer*>(param);
  auto str = server->GetPageContent(url);
  response = MHD_create_response_from_data(str.size(),
                                           const_cast<char*>(str.c_str()),
                                           MHD_NO, MHD_YES);
  ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
  MHD_destroy_response(response);
  return ret;
}
}  // namespace

StatusServer* StatusServer::StartStatusServer(
    EndPointToOPMap op_map, std::set<std::string> extra_stats_endpoints) {
  static StatusServer server(FLAGS_http_status_port, std::move(op_map),
                             std::move(extra_stats_endpoints));
  return &server;
}

void StatusServer::StartStatusServerOrDie(
    EndPointToOPMap op_map, std::set<std::string> extra_stats_endpoints) {
  static StatusServer server(FLAGS_http_status_port, std::move(op_map),
                             std::move(extra_stats_endpoints));
  CHECK(server.Serving());
}

StatusServer::StatusServer(uint16_t port, EndPointToOPMap op_map,
                           std::set<std::string> extra_stats_endpoints)
    : port_(port), d_(nullptr), op_map_(std::move(op_map)),
      extra_stats_endpoints_(std::move(extra_stats_endpoints)) {

  extra_stats_endpoints_.emplace("/rocksdb_info.txt");
  // prevent infinite recursion...
  extra_stats_endpoints_.erase("/stats.txt");
  // Text
  op_map_.emplace("/stats.txt", [this]
      (const std::vector<std::pair<std::string, std::string>>* ) {
    std::string ret = common::Stats::get()->DumpStatsAsText();

    for (const auto& endpoint_name : extra_stats_endpoints_) {
      auto itor = op_map_.find(endpoint_name);
      if (itor != op_map_.end()) {
        ret += itor->second(nullptr);
      }
    }
    return ret;
  });

  // dump_heap
  op_map_.emplace("/dump_heap",
                  [] (const std::vector<std::pair<std::string, std::string>>*) {
                    auto ret = mallctl("prof.dump", nullptr, nullptr, nullptr, 0);
                    return strerror(ret);
                  });

  op_map_.emplace("/gflags.txt",
                  [] (const std::vector<std::pair<std::string, std::string>>*) {
                    std::stringstream ss;
                    ss << "flag:value [default_value]" << std::endl;
                    std::vector<google::CommandLineFlagInfo> flag_infos;
                    google::GetAllFlags(&flag_infos);
                    for (const auto& flag_info : flag_infos) {
                      ss << flag_info.name << ":" << flag_info.current_value << " ["
                         << flag_info.default_value << "]" << std::endl;
                    }
                    return ss.str();
                  });

  if (!this->Serve()) {
    LOG(INFO) << "Failed to start status server at " << port;
    LOG(INFO) << strerror(errno);
  }
}

std::string StatusServer::GetPageContent(const std::string& end_point) {
  // Add dummy url to allow it to be parsed by folly:Uri.
  folly::Uri u("http://blah.blah" + end_point);
  auto params = u.getQueryParams();

  auto iter = op_map_.find(u.path().toStdString());
  if (iter == op_map_.end()) {
    return  "Unsupported http path.\n";
  }

  std::vector<std::pair<std::string, std::string>> args;
  for (const auto& p : params) {
    args.push_back(std::make_pair(p.first.toStdString(),
                                  p.second.toStdString()));
  }

  return iter->second(&args);
}

void StatusServer::Stop() {
  if (d_) {
    MHD_stop_daemon(d_);
  }
}

bool StatusServer::Serve() {
  LOG(INFO) << "Starting status server at " << port_;
  d_ = MHD_start_daemon(MHD_USE_POLL_INTERNALLY, port_, NULL, NULL,
                        &ServeCallback, this, MHD_OPTION_END);
  return (d_ != nullptr);
}

bool StatusServer::Serving() {
  return (d_ != nullptr);
}
}  // namespace common

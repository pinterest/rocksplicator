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
#include <folly/FileUtil.h>
#include <folly/Format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <microhttpd.h>

#include <functional>
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
  *ptr = nullptr;                            /* clear context pointer */

  StatusServer* server = reinterpret_cast<StatusServer*>(param);
  StatusServer::Arguments args;
  MHD_get_connection_values(
      connection,
      MHD_GET_ARGUMENT_KIND,
      [] (void* cls, enum MHD_ValueKind kind, const char* key, const char* value) {
        if (!(key && value)) {
          return MHD_NO;
        }
        reinterpret_cast<StatusServer::Arguments*>(cls)->emplace_back(key, value);
        return MHD_YES;
      },
      &args);
  auto str = server->GetPageContent(url, &args);
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
      (const Arguments*) {
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
                  [] (const Arguments*) {
                    static const char* kMallctlProfDumpNamespace = "prof.dump";
                    static const std::string kHeapTmpFileTemplate = "/tmp/pprof.heap.{}";

                    std::string tmp_file = folly::sformat(kHeapTmpFileTemplate, static_cast<int>(getpid()));
                    const char* tmp_file_c_str = tmp_file.c_str();
                    auto ret = mallctl(kMallctlProfDumpNamespace, nullptr, nullptr, &tmp_file_c_str, sizeof(const char*));
                    if (ret) {
                      return std::string(strerror(ret));
                    }

                    LOG(INFO) << "Heap dump succeeded via mallctl! : " << tmp_file;
                    std::string prof;
                    folly::readFile(tmp_file.c_str(), prof);
                    std::remove(tmp_file.c_str());
                    return prof;
                  });

  op_map_.emplace("/gflags.txt",
                  [] (const Arguments*) {
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

std::string StatusServer::GetPageContent(const std::string& end_point, Arguments* args) {
  // Add dummy url to allow it to be parsed by folly:Uri.
  // Note: folly:Uri is not thread-safe!
  folly::Uri u("http://blah.blah" + end_point);

#if __GNUC__ >= 8
  auto iter = op_map_.find(u.path());
#else
  auto iter = op_map_.find(u.path().toStdString());
#endif
  if (iter == op_map_.end()) {
    return "Unsupported http path: " + end_point + "!\n";
  }

  auto params = u.getQueryParams();
  for (const auto& p : params) {
#if __GNUC__ >= 8
    args->emplace_back(p.first, p.second);
#else
    args->emplace_back(p.first.toStdString(), p.second.toStdString());
#endif
  }

  return iter->second(args);
}

void StatusServer::Stop() {
  if (d_) {
    MHD_stop_daemon(d_);
  }
}

bool StatusServer::Serve() {
  LOG(INFO) << "Starting status server at " << port_;
  d_ = MHD_start_daemon(MHD_USE_POLL_INTERNALLY, port_, nullptr, nullptr,
                        &ServeCallback, this, MHD_OPTION_END);
  return (d_ != nullptr);
}

bool StatusServer::Serving() {
  return (d_ != nullptr);
}
}  // namespace common

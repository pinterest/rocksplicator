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


/**
 * An StatusServer based on libmicrohttpd.
 * http://www.gnu.org/software/libmicrohttpd/
 *
 * Used for exporting stats, deploy commit etc.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

struct MHD_Daemon;

namespace common {

class Stats;

class StatusServer {
 public:
  /*
   * \brief Endpoint/function name to std::function mapping.
   *
   * Stores the mapping of an end-point/function to the the
   * actual std::function to be executed when requested.
   *
   * The std::function takes as input the list of arguments to
   * the function in the form of a vector of [key, value] pairs
   * (arrays can be passed in as delimited values) and returns a
   * std::string.
   */
  using EndPointToOPMap =
      std::map<std::string, std::function<std::string(
              const std::vector<std::pair<std::string, std::string>>* )>>;

  /*!
   * \brief Instantiate a StatusServer.
   *
   * The end-point handler in the EndPointToOPMap is an
   * std::function that take as input the list of arguments to
   * the function in the form of a vector of [key, value] pairs
   * (arrays can be passed in as delimited values) and returns a
   * std::string.
   * @param extra_stats_endpoints For endpoints in this list, their string
   * result will be appended to the regular stats.txt endpoint result. Note:
   * These endpoints' format need to confirm to the existing stats.txt format,
   * so that the same stats collector can consume it. They will be listed in
   * the counter section.
   * Metrics format(there should be 2 empty space at the beginning of each line):
   *   metrics_name1: metrics_value1
   *   metrics_name2 tag_name2=tag_value12 metrics_value2
   */
  static StatusServer* StartStatusServer(EndPointToOPMap op_map = EndPointToOPMap(),
                                         std::set<std::string> extra_stats_endpoints = {});
  // Instantiate a StatusServer or Die.
  static void StartStatusServerOrDie(
      EndPointToOPMap op_map = EndPointToOPMap(),
      std::set<std::string> extra_stats_endpoints = {});

  /*!
   * \brief Executes the function corresponding to the given end_point.
   * @param end_point The requested end point, including the query.
   * @return The result of the funciton call.
   */
  std::string GetPageContent(const std::string& end_point);

  // Stop the HTTP serving daemon. Not currently used.
  // Add to shutdown hook when available.
  void Stop();

  // Check if the service has started successfully
  bool Serving();

 private:
  StatusServer(uint16_t port, EndPointToOPMap op_map,
               std::set<std::string> extra_stats_endpoints);

  // Start the server and return true if the serve starts successfully.
  bool Serve();

  const uint16_t port_;

  MHD_Daemon* d_;

  EndPointToOPMap op_map_;
  std::set<std::string> extra_stats_endpoints_;
};
}  // namespace common

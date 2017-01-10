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
   */
  static void StartStatusServer(EndPointToOPMap op_map = EndPointToOPMap());
  // Instantiate a StatusServer or Die.
  static void StartStatusServerOrDie(
      EndPointToOPMap op_map = EndPointToOPMap());

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
  StatusServer(uint16_t port, EndPointToOPMap op_map);

  // Start the server and return true if the serve starts successfully.
  bool Serve();

  const uint16_t port_;

  MHD_Daemon* d_;

  EndPointToOPMap op_map_;
};
}  // namespace common

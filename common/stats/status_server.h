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

struct MHD_Daemon;

namespace common {

class Stats;

class StatusServer {
 public:
  using EndPointToOPMap =
      std::map<std::string, std::function<std::string(void)>>;

  // Instantiate a StatusServer.
  static void StartStatusServer(EndPointToOPMap op_map = EndPointToOPMap());
  // Instantiate a StatusServer or Die.
  static void StartStatusServerOrDie(
      EndPointToOPMap op_map = EndPointToOPMap());

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

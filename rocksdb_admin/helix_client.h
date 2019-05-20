/// Copyright 2018 Pinterest Inc.
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
// @author bol (bol@pinterest.com)
//

#pragma once

#include <string>

namespace admin {

/*
 * Join a Helix managed cluster.
 *
 * @param zk_connect_str The zk cluster. e.g., "host1:port,host2:port,host3:port"
 * @param cluster The cluster name to join.
 * @param state_model_type The Helix state model to use. e.g., "OnlineOffline"
 * @param domain This is for topology aware cluster management.
 *               e.g., "az=us-east-1a,pg=placement_group1"
 * @param class_path The class path for cluster_management jar.
 * @param config_post_url The url to post shard config
 * @param disable_spectator whether we should disable spectator on this participant
 *
 * @note the function will intentionally crash the calling process if it couldn't
 *  join the cluster successfully.
 */
void JoinCluster(const std::string& zk_connect_str,
                 const std::string& cluster,
                 const std::string& state_model_type,
                 const std::string& domain,
                 const std::string& class_path,
                 const std::string& config_post_url,
                 bool disable_spectator = false);

}  // namespace admin

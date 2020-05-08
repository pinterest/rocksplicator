//
// Created by Gopal Rajpurohit on 5/8/20.
//

#pragma once

#include <map>
#include <string>

namespace kafka {
typedef std::map<std::string, std::pair<std::string, bool>> ConfigMap;

static bool read_conf_file (
  const std::string &conf_file,
  const std::shared_ptr<ConfigMap> &configMap);

} // namespace kafka

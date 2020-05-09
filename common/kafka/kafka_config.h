//
// Created by Gopal Rajpurohit on 5/8/20.
//

#pragma once

#include <map>
#include <string>
#include <memory>

namespace kafka {
typedef std::map<std::string, std::pair<std::string, bool>> ConfigMap;

bool read_conf_file (
  std::string &conf_file,
  std::shared_ptr<ConfigMap> configMap,
  bool compatibility_flag);

} // namespace kafka

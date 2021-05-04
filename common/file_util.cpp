#include "file_util.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

namespace filesystem = boost::filesystem;

namespace common {

void FileUtil::touch(const std::string& path) {
  try {
    filesystem::ofstream file(path);
    file.close();
  } catch (std::exception& e) {
    throw e;
  }
}

std::string FileUtil::getSuccessFilePath(const std::string& base_dir) {
  if (!filesystem::exists(base_dir)) {
    throw std::runtime_error(
        "Failed to get success file path since base dir not exist, " +
        base_dir);
  }
  if (base_dir.back() != '/') {
    return base_dir + "/_SUCCESS";
  } else {
    return base_dir + "_SUCCESS";
  }
}

std::string FileUtil::createSuccessFile(const std::string& base_dir) {
  std::string path = FileUtil::getSuccessFilePath(base_dir);
  touch(path);
  return path;
}
}  // namespace common
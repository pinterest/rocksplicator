#include "file_util.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>

namespace filesystem = boost::filesystem;

namespace common {

void FileUtil::touch(const std::string& path) {
  try {
    std::ofstream file(path);
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
  return base_dir + ((base_dir.back() == '/') ? "_SUCCESS" : "/_SUCCESS");
}

std::string FileUtil::createSuccessFile(const std::string& base_dir) {
  std::string path = FileUtil::getSuccessFilePath(base_dir);
  touch(path);
  return path;
}

std::string FileUtil::createFileWithContent(const std::string& base_dir,
                                            const std::string& filename,
                                            const std::string& content) {
  std::string path =
      base_dir + ((base_dir.back() == '/') ? filename : ("/" + filename));
  std::ofstream file(path);
  file << content;
  file.close();
  return path;
}

void FileUtil::readFileToString(const std::string& path, std::string* content) {
  const std::ifstream input_stream(path);
  if (input_stream.fail()) {
    throw std::runtime_error("Failed to open file: " + path);
  }
  std::stringstream buffer;
  buffer << input_stream.rdbuf();
  content->assign(buffer.str());
}
}  // namespace common

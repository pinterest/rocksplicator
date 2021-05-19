#include "file_util.h"

#include <fstream>
#include <sstream>

namespace common {

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

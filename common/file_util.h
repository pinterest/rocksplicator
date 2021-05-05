
#include <string>

namespace common {
class FileUtil {
 public:
  // create an empty file at local file system at absoluate path
  static void touch(const std::string& path);

  static std::string getSuccessFilePath(const std::string& base_dir);

  // Create an empty _SUCCESS file; the file path is "<baseDir>/_SUCCESS".
  static std::string createSuccessFile(const std::string& base_dir);

  // create file at path: "<baseDir>/filename"
  static std::string createFileWithContent(const std::string& base_dir,
                                           const std::string& filename,
                                           const std::string& content);

  static void readFileToString(const std::string& path, std::string* content);
};
}  // namespace common

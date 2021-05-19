
#include <string>

namespace common {
class FileUtil {
 public:
  // create file at path: "<baseDir>/filename"
  static std::string createFileWithContent(const std::string& base_dir,
                                           const std::string& filename,
                                           const std::string& content);

  static void readFileToString(const std::string& path, std::string* content);
};
}  // namespace common

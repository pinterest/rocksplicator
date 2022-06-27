#include "rocksdb_admin/utils.h"
#include "boost/filesystem.hpp"


namespace admin {

// Will remove and create a directory
// returns false if any of these operations failed.
bool ClearAndCreateDir(std::string dir_path, std::string* err) {
  boost::system::error_code create_err;
  boost::system::error_code remove_err;
  boost::filesystem::remove_all(dir_path, remove_err);
  if (remove_err) {
    *err = remove_err.message();
    return false;
  }
  boost::filesystem::create_directories(dir_path, create_err);
  if(create_err) {
      *err = create_err.message();
      return false;
  }
  return true;
}

}
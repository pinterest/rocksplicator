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
// @author ghan (ghan@pinterest.com)
//

#include "common/rocksdb_env_s3.h"

#include <string>

#include "boost/filesystem.hpp"
#include "common/rocksdb_glogger/rocksdb_glogger.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "rocksdb/status.h"

DECLARE_bool(s3_direct_io);

namespace rocksdb {

// open a file for sequential reading
Status S3Env::NewSequentialFile(const std::string& fname,
                                std::unique_ptr<SequentialFile>* result,
                                const EnvOptions& options) {
  assert(s3_util_ != nullptr);
  result->reset();

  // We read first from local storage and then from S3.
  auto local_full_path = local_directory_ + GetRelativePath(fname);
  auto st = posix_env_->NewSequentialFile(local_full_path, result, options);

  if (!st.ok()) {
    // If file doesnt exist in local, we copy the file to the local storage from S3
    size_t slash = local_full_path.find_last_of('/');
    assert(slash != std::string::npos);
    boost::system::error_code create_err;
    boost::filesystem::create_directories(local_full_path.substr(0, slash), create_err);
    if (create_err) {
      LOG(ERROR) << "Cant create dir in local: " << local_full_path;
      return Status::IOError();
    }

    auto resp = s3_util_->getObject(fname, local_full_path, FLAGS_s3_direct_io);
    if (!resp.Error().empty()) {
      LOG(ERROR) << "Error happened when copying the file from S3 to local: "
                 << resp.Error();
      return Status::IOError();
    }
  }

  // we successfully copied the file, try opening it locally now
  return posix_env_->NewSequentialFile(local_full_path, result, options);
}

// open a file for random reading
Status S3Env::NewRandomAccessFile(const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result,
                                  const EnvOptions& options) {
  assert(s3_util_ != nullptr);
  result->reset();

  // We read first from local storage and then from S3.
  auto local_full_path = local_directory_ + GetRelativePath(fname);
  auto st = posix_env_->NewRandomAccessFile(local_full_path, result, options);

  if (!st.ok()) {
    // If file doesnt exist in local, we copy the file to the local storage from S3
    size_t slash = local_full_path.find_last_of('/');
    assert(slash != std::string::npos);
    boost::system::error_code create_err;
    boost::filesystem::create_directories(local_full_path.substr(0, slash), create_err);
    if (create_err) {
      LOG(ERROR) << "Cant create dir in local: " << local_full_path;
      return Status::IOError();
    }

    auto resp = s3_util_->getObject(fname, local_full_path, FLAGS_s3_direct_io);
    if (!resp.Error().empty()) {
      LOG(ERROR) << "Error happened when copying the file from S3 to local: "
                 << resp.Error();
      return Status::IOError();
    }
  }

  // we successfully copied the file, try opening it locally now
  return posix_env_->NewRandomAccessFile(local_full_path, result, options);
}

// S3WritableFile is a wrapper for writing a file in S3. We will perform the
// operations in a local tmp file and upload to S3 when closing the file.
class S3WritableFile : public WritableFile {

 public:
  S3WritableFile(const std::string& local_fname,
                 const std::string& s3_fname,
                 const EnvOptions& options,
                 S3Env* env,
                 std::shared_ptr<common::S3Util> s3_util) :
      fname_(local_fname),
      s3_fname_(s3_fname),
      env_(env),
      s3_util_(s3_util) {
    auto local_env = env_->GetBaseEnv();
    auto s = local_env->NewWritableFile(local_fname, &local_file_, options);
    if (!s.ok()) {
      LOG(ERROR) << "Error happened when creating writable file in local: " << s.ToString();
      status_ = s;
    }
  }

  virtual ~S3WritableFile() {
    if (local_file_ != nullptr) {
      Close();
    }
  }

  virtual Status Append(const Slice& data) {
    assert(status_.ok());
    // write to the temporary local file
    return local_file_->Append(data);
  }

  Status PositionedAppend(const Slice& data, uint64_t offset) {
    return local_file_->PositionedAppend(data, offset);
  }

  Status Truncate(uint64_t size) {
    return local_file_->Truncate(size);
  }

  Status Fsync() {
    return local_file_->Fsync();
  }

  bool IsSyncThreadSafe() const {
    return local_file_->IsSyncThreadSafe();
  }

  bool use_direct_io() const {
    return local_file_->use_direct_io();
  }

  size_t GetRequiredBufferAlignment() const {
    return local_file_->GetRequiredBufferAlignment();
  }

  uint64_t GetFileSize() {
    return local_file_->GetFileSize();
  }

  size_t GetUniqueId(char* id, size_t max_size) const {
    return local_file_->GetUniqueId(id, max_size);
  }

  Status InvalidateCache(size_t offset, size_t length) {
    return local_file_->InvalidateCache(offset, length);
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) {
    return local_file_->RangeSync(offset, nbytes);
  }

  Status Allocate(uint64_t offset, uint64_t len) {
    return local_file_->Allocate(offset, len);
  }

  virtual Status Flush() {
    return local_file_->Flush();
  }

  virtual Status Sync() {
    return local_file_->Sync();
  }

  virtual Status status() {
    return status_;
  }

  virtual Status Close() {
    if (local_file_ == nullptr) {  // already closed
      return status_;
    }
    assert(status_.ok());

    // close local file
    auto st = local_file_->Close();
    if (!st.ok()) {
      LOG(ERROR) << "Error happened when closing the writable file in local: "
                 << st.ToString();
      return st;
    }
    local_file_.reset();

    return Status::OK();
  }

 private:
  const std::string fname_;
  const std::string s3_fname_;
  S3Env* env_;
  std::shared_ptr<common::S3Util> s3_util_;
  Status status_;
  std::unique_ptr<WritableFile> local_file_;
};

Status S3Env::NewWritableFile(const std::string& fname,
                              std::unique_ptr<WritableFile>* result,
                              const EnvOptions& options) {
  assert(s3_util_ != nullptr);
  result->reset();

  std::unique_ptr<S3WritableFile> f(
      new S3WritableFile(local_directory_ + GetRelativePath(fname), fname, options, this, s3_util_));
  auto s = f->status();
  if (!s.ok()) {
    LOG(ERROR) << "Error happened when creating S3WritableFile: " << s.ToString();;
    return s;
  }

  result->reset(dynamic_cast<WritableFile*>(f.release()));
  return Status::OK();
}

class S3Directory : public Directory {
 public:
  explicit S3Directory(S3Env* env, const std::string& name) : env_(env){
    status_ = env_->GetBaseEnv()->NewDirectory(name, &posix_dir_);
  }

  ~S3Directory() {}

  virtual Status Fsync() {
    if (!status_.ok()) {
      return status_;
    }
    return posix_dir_->Fsync();
  }

  virtual Status status() { return status_; }

 private:
  S3Env* env_;
  Status status_;
  std::unique_ptr<Directory> posix_dir_;
};

// S3 has no concepts of directories, so we just have to forward the request to
// posix_env_ under the S3Directory
Status S3Env::NewDirectory(const std::string& name,
                           std::unique_ptr<Directory>* result) {
  result->reset(nullptr);

  auto local_dir_path = local_directory_ + GetRelativePath(name);
  // create new object.
  std::unique_ptr<S3Directory> d(new S3Directory(this, local_dir_path));

  // Check if the path exists in local dir
  if (!d->status().ok()) {
    LOG(ERROR) << "Unable to create local dir: " << local_dir_path;
    return d->status();
  }

  result->reset(d.release());
  return Status::OK();
}

Status S3Env::FileExists(const std::string& fname) {
  // We read first from local storage and then from S3
  auto st = posix_env_->FileExists(local_directory_ + GetRelativePath(fname));
  if (st.IsNotFound()) {
    auto resp = s3_util_->listObjects(fname);
    if (!resp.Error().empty() || resp.Body().empty()) {
      return Status::NotFound();
    }
  }

  return Status::OK();
}

inline std::string ensure_ends_with_pathsep(const std::string& s) {
  if (!s.empty() && s.back() != '/') {
    return s + '/';
  }
  return s;
}

Status S3Env::GetChildren(const std::string& path,
                          std::vector<std::string>* result) {
  auto formated_path = ensure_ends_with_pathsep(path);
  // fetch all files using the given path as the key prefix in S3
  auto resp = s3_util_->listAllObjects(formated_path);
  if (!resp.Error().empty()) {
    LOG(ERROR) << "Error happened when fetching files from S3: "
               << resp.Error() << " under path: " << formated_path;
    return Status::IOError();
  }

  // trim the file name
  for (auto& v : resp.Body().objects) {
    // the path should be a prefix of the fetched value
    if (v.find(formated_path) == 0) {
      result->push_back(v.substr(formated_path.size()));
    }
  }

  return Status::OK();
}

Status S3Env::DeleteFile(const std::string& fname) {
  // first delete the file from s3
  auto resp = s3_util_->deleteObject(fname);
  if (!resp.Error().empty()) {
    LOG(ERROR) << "Error happened when deleting file from S3: "
               << resp.Error();
    return Status::IOError();
  }

  // Do the delete from local filesystem too and ignore the error
  posix_env_->DeleteFile(local_directory_ + GetRelativePath(fname));
  return Status::OK();
}

// S3 has no concepts of directories, so we just have to forward the request to
// posix_env_
Status S3Env::CreateDir(const std::string& name) {
  return posix_env_->CreateDir(local_directory_ + GetRelativePath(name));
};

// S3 has no concepts of directories, so we just have to forward the request to
// posix_env_
Status S3Env::CreateDirIfMissing(const std::string& name) {
  return posix_env_->CreateDirIfMissing(local_directory_ + GetRelativePath(name));
};

// S3 has no concepts of directories, so we just have to forward the request to
// posix_env_
Status S3Env::DeleteDir(const std::string& name) {
  return posix_env_->DeleteDir(local_directory_ + GetRelativePath(name));
};

Status S3Env::GetFileSize(const std::string& fname, uint64_t* size) {
  Status st;
  auto local_path = local_directory_ + GetRelativePath(fname);
  if (posix_env_->FileExists(local_path).ok()) {
    st = posix_env_->GetFileSize(local_path, size);
  } else {
    st = Status::NotFound();
    auto meta_resp = s3_util_->getObjectSizeAndModTime(fname);
    if (!meta_resp.Error().empty()) {
      LOG(ERROR) << "Error happened when querying object size from S3: "
                 << meta_resp.Error();
    }

    const auto& size_itr = meta_resp.Body().find("size");
    if (size_itr != meta_resp.Body().end()) {
      *size = size_itr->second;
      st = Status::OK();
    }
  }

  return st;
}

Status S3Env::GetFileModificationTime(const std::string& fname,
                                      uint64_t* file_mtime) {
  Status st;
  auto local_path = local_directory_ + GetRelativePath(fname);
  if (posix_env_->FileExists(local_path).ok()) {
    st = posix_env_->GetFileModificationTime(local_path, file_mtime);
  } else {
    st = Status::NotFound();
    auto meta_resp = s3_util_->getObjectSizeAndModTime(fname);
    if (!meta_resp.Error().empty()) {
      LOG(ERROR) << "Error happened when querying object ModTime from S3: "
                 << meta_resp.Error();
    }

    const auto& mtime_itr = meta_resp.Body().find("last-modified");
    if (mtime_itr != meta_resp.Body().end()) {
      *file_mtime = mtime_itr->second;
      st = Status::OK();
    }
  }

  return st;
}

// The rename is not atomic. S3 does not support renaming natively, so
// we perform the renaming in local and then upload the updated file to S3.
Status S3Env::RenameFile(const std::string& src, const std::string& target) {
  auto local_src_path = local_directory_ + GetRelativePath(src);
  auto local_target_path = local_directory_ + GetRelativePath(target);
  Status st = posix_env_->RenameFile(local_src_path, local_target_path);
  if (!st.ok()) {
    LOG(ERROR) << "Error happened when renaming local file: " << src;
    return st;
  }
  boost::filesystem::path path(local_target_path);
  bool is_dir = boost::filesystem::is_directory(boost::filesystem::path(local_target_path));
  if (is_dir) {
    std::vector<std::string> files;
    st = posix_env_->GetChildren(local_target_path, &files);
    if (!st.ok()) {
      LOG(ERROR) << "Error happened when GetChildren for renaming file: " << src;
      return st;
    }
    for (const auto& file : files) {
      if (file == "." || file == "..") {
        continue;
      }
      auto copy_resp = s3_util_->putObject(target + file, local_target_path + file);
      if (!copy_resp.Error().empty()) {
        LOG(ERROR) << "Error happened when uploading file to S3: "
                   << copy_resp.Error();
        return Status::IOError();
      }
    }
  } else {
    auto copy_resp = s3_util_->putObject(target, local_target_path);
    if (!copy_resp.Error().empty()) {
      LOG(ERROR) << "Error happened when uploading file to S3: "
                 << copy_resp.Error();
      return Status::IOError();
    }
  }

  return Status::OK();
}

Status S3Env::LockFile(const std::string& fname, FileLock** lock) {
  // there isn's a very good way to atomically check and create
  // a file via libs3
  *lock = nullptr;
  return Status::OK();
}

Status S3Env::UnlockFile(FileLock* lock) {
  return Status::OK();
}

Status S3Env::NewLogger(const std::string& fname, std::shared_ptr<Logger>* result) {
  common::RocksdbGLogger* logger = new common::RocksdbGLogger();
  result->reset(logger);
  return Status::OK();
}

}  // namespace rocksdb

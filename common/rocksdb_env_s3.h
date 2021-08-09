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

#pragma once

#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include <mutex>
#include <pthread.h>

#include "common/s3util.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {

class S3FatalException : public std::exception {
 public:
  explicit S3FatalException(const std::string& s) : what_(s) { }

  virtual ~S3FatalException() noexcept { }

  virtual const char* what() const noexcept {
    return what_.c_str();
  }

 private:
  const std::string what_;
};

/**
 * The S3 environment for rocksdb. This class overrides all the
 * file/dir access methods and delegates the thread-mgmt methods to the
 * default posix environment, which is similar as rocksdb::HdfsEnv. The S3Env
 * could be only used for the rocksdb backup/restore process by rocksdb::BackupEngine,
 * and can't be used as an env in the purpose of creating the rocksdb instance in the cloud.
 * During the process,  all the file are immutable, so there are file operations like read,
 * copy and delete but no modification. The implementation is very straight-forward, for the
 * backup, it will first backup files to a local dir and then upload to s3. And for restore,
 * it will first download latest backup to a local dir from s3, then perform the restore.
 */
class S3Env : public Env {

 public:
  explicit S3Env(const std::string& s3_key_prefix,
                 const std::string& local_directory,
                 std::shared_ptr<common::S3Util> s3_util) :
      s3_key_prefix_(s3_key_prefix),
      local_directory_(local_directory),
      s3_util_(std::move(s3_util)) {
    posix_env_ = Env::Default();
  }

  virtual ~S3Env() {}

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override;

  Status FileExists(const std::string& fname) override;

  Status GetChildren(const std::string& path,
                     std::vector<std::string>* result) override;

  Status DeleteFile(const std::string& fname) override;

  Status CreateDir(const std::string& name) override;

  Status CreateDirIfMissing(const std::string& name) override;

  Status DeleteDir(const std::string& name) override;

  Status GetFileSize(const std::string& fname, uint64_t* size) override;

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override;

  Status RenameFile(const std::string& src, const std::string& target) override;

  Status LinkFile(const std::string& /*src*/,
                  const std::string& /*target*/) override {
    return Status::NotSupported(); // not supported
  }

  Status LockFile(const std::string& fname, FileLock** lock) override;

  Status UnlockFile(FileLock* lock) override;

  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override;

  void Schedule(void (*function)(void* arg), void* arg, Priority pri = LOW,
                void* tag = nullptr,
                void (*unschedFunction)(void* arg) = 0) override {
    posix_env_->Schedule(function, arg, pri, tag, unschedFunction);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return posix_env_->UnSchedule(tag, pri);
  }

  void StartThread(void (*function)(void* arg), void* arg) override {
    posix_env_->StartThread(function, arg);
  }

  void WaitForJoin() override { posix_env_->WaitForJoin(); }

  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return posix_env_->GetThreadPoolQueueLen(pri);
  }

  Status GetTestDirectory(std::string* path) override {
    return posix_env_->GetTestDirectory(path);
  }

  uint64_t NowMicros() override { return posix_env_->NowMicros(); }

  void SleepForMicroseconds(int micros) override {
    posix_env_->SleepForMicroseconds(micros);
  }

  Status GetHostName(char* name, uint64_t len) override {
    return posix_env_->GetHostName(name, len);
  }

  Status GetCurrentTime(int64_t* unix_time) override {
    return posix_env_->GetCurrentTime(unix_time);
  }

  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    return posix_env_->GetAbsolutePath(db_path, output_path);
  }

  void SetBackgroundThreads(int number, Priority pri = LOW) override {
    posix_env_->SetBackgroundThreads(number, pri);
  }

  int GetBackgroundThreads(Priority pri = LOW) override {
    return posix_env_->GetBackgroundThreads(pri);
  }

  void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
    posix_env_->IncBackgroundThreadsIfNeeded(number, pri);
  }

  std::string TimeToString(uint64_t number) override {
    return posix_env_->TimeToString(number);
  }

  static uint64_t gettid() {
    assert(sizeof(pthread_t) <= sizeof(uint64_t));
    return (uint64_t)pthread_self();
  }

  uint64_t GetThreadID() const override { return S3Env::gettid(); }

  Env* GetBaseEnv() { return posix_env_; }

 private:
  // s3 object key prefix
  const std::string s3_key_prefix_;
  // Local directory to temporarily store the files downloaded from/uploading to S3
  const std::string local_directory_;
  // S3 util used for accessing AWS S3
  std::shared_ptr<common::S3Util> s3_util_;
  // This object is derived from Env, but not from
  // posixEnv. We have posixEnv as an encapsulated
  // object here so that we can use posix timers,
  // posix threads, etc.
  Env* posix_env_;

  inline std::string GetRelativePath(
      const std::string &absolute_path = "") const {
    assert(absolute_path.size() > s3_key_prefix_.size() + 1);
    return absolute_path.substr(s3_key_prefix_.size() + 1);
  }

};

}  // namespace rocksdb

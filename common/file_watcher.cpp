/// Copyright 2016 Pinterest Inc.
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
// @author bol (bol@pinterest.com)
//

#include "common/file_watcher.h"
#include "common/stats/stats.h"

#include <folly/FileUtil.h>
#include <folly/SpookyHashV2.h>
#include <folly/ThreadName.h>
#include <sys/inotify.h>

#include <string>

#include "boost/filesystem.hpp"
#include "gflags/gflags.h"

/*
 * TODO(shu): Commented temporarily.
 * Reason: In Pinability scorer shared object
 * we want to use file watcher to watch model files. The flag will
 * be defined twice since we both static and dynamic link file watcher.
 * Since this flag is not being set actively, making it a
 * hard coded number for now.
 */
// DEFINE_int32(recheck_removed_file_interval_ms, 2 * 1000,
//              "The interval for checking removed files");
const int kRecheckRemovedFileIntervalMs = 2000;

namespace common {
FileWatcher* FileWatcher::Instance() {
  static FileWatcher instance;
  return &instance;
}

FileWatcher::FileWatcher()
    : evb_()
    , fd_(::inotify_init())
    , handler_(&evb_, fd_, this)
    , thread_()
    , file_names_()
    , states_() {
  CHECK(fd_ != -1) << "Failed to inotify_init()";

  CHECK(handler_.registerHandler(folly::EventHandler::EventFlags::READ |
                                 folly::EventHandler::EventFlags::PERSIST));
  thread_ = std::thread([this] {
      if (!folly::setThreadName("FileWatcher")) {
        LOG(ERROR) << "Failed to setThreadName for FileWatcher thread";
      }
      this->evb_.loopForever();
      LOG(INFO) << "Stopping FileWatcher thread...";
    });
}

FileWatcher::~FileWatcher() {
  handler_.unregisterHandler();
  evb_.terminateLoopSoon();
  thread_.join();
  ::close(fd_);
}

bool FileWatcher::AddFile(const std::string& file_name,
                          std::function<void(std::string)> cb) {
  bool ret = true;
  evb_.runInEventBaseThreadAndWait([&ret, &file_name, &cb, this] {
      auto watch_fd = RegisterFile(file_name);
      if (watch_fd == -1) {
        ret = false;
        return;
      }

      uint64_t hash;
      cb(ReadFileAndHash(file_name, &hash));

      file_names_.emplace(watch_fd, file_name);
      states_.emplace(file_name, State(std::move(cb), hash, watch_fd));
    });

  return ret;
}

bool FileWatcher::RemoveFile(const std::string& file_name) {
  bool ret = true;
  evb_.runInEventBaseThreadAndWait([&ret, &file_name, this] {
      auto itor = states_.find(file_name);
      if (itor == states_.end()) {
        LOG(ERROR) << file_name << " is not being watched";
        ret = false;
        return;
      }

      const auto& state = itor->second;
      if (state.watch_fd != -1 &&
          ::inotify_rm_watch(fd_, state.watch_fd) == -1) {
        LOG(ERROR) << " Failed to inotify_rm_watch() with errno " << errno;
        ret = false;
        return;
      }

      file_names_.erase(state.watch_fd);
      states_.erase(itor);
    });

  return ret;
}

void FileWatcher::ReadAndProcessEvents() {
  const int kBufLength = 1024 * (sizeof(struct inotify_event) + 16);
  static char buf[kBufLength];

  auto length = ::read(fd_, buf, kBufLength);
  if (length < 0) {
    LOG(ERROR) << "Failed to read() inotify fd with errno " << errno;
    return;
  }

  ProcessEvents(buf, length);
}

void FileWatcher::ProcessEvents(const char* buf, int sz) {
  // It is promised that there will be no partial or truncated inotify_event
  // structs.
  int i = 0;
  while (i < sz) {
    auto event = reinterpret_cast<const struct inotify_event*>(buf + i);
    i += sizeof(struct inotify_event) + event->len;

    auto file_name_itor = file_names_.find(event->wd);
    if (file_name_itor == file_names_.end()) {
      // this is possible as clients could have removed it from FileWatcher
      continue;
    }
    auto state_itor = states_.find(file_name_itor->second);
    CHECK(state_itor != states_.end()) << "Cound't find state for file "
                                       << file_name_itor->second;

    if (event->mask & (IN_IGNORED | IN_UNMOUNT | IN_MOVE_SELF)) {
      // A watched file has been removed or unmounted, we will need to
      // periodically try to re-register it.
      LOG(ERROR) << "File removed or unmounted: " << file_name_itor->second;
      state_itor->second.watch_fd = -1;
      file_names_.erase(file_name_itor);
      ScheduleRegisterMonitoredFile(state_itor->first);

      continue;
    }

    if ((event->mask & IN_CLOSE_WRITE) == 0) {
      LOG(ERROR) << "Events other than IN_CLOSE_WRITE returned " << event->mask;
      continue;
    }

    auto& state = state_itor->second;
    CheckFileAndCallback(state_itor->first, &state);
  }
}

std::string FileWatcher::ReadFileAndHash(const std::string& file_name,
                                         uint64_t* hash) {
  CHECK(hash);
  std::string content;
  if (!folly::readFile(file_name.c_str(), content)) {
    LOG(ERROR) << "Failed to read file " << file_name << " with errno "
               << errno << " : " << strerror(errno);
    content.clear();
  }

  *hash = folly::hash::SpookyHashV2::Hash64(content.data(), content.size(), 0);
  return content;
}

int FileWatcher::RegisterFile(const std::string& file_name,
                              const bool check_dup) {
  if (check_dup && states_.count(file_name) == 1) {
    LOG(ERROR) << file_name << " is already being watched, RemoveFile() "
               << "first if you want to change its callback.";
    return -1;
  }

  auto watch_fd = ::inotify_add_watch(fd_, file_name.c_str(),
                                      IN_CLOSE_WRITE | IN_MOVE_SELF);
  if (watch_fd == -1) {
    LOG(ERROR) << "Failed to inotify_add_watch() with errno " << errno <<
        " : " << strerror(errno);
  }

  return watch_fd;
}

void FileWatcher::ScheduleRegisterMonitoredFile(const std::string& file_name) {
  LOG(INFO) << "Schedule registering file " << file_name << " in "
            << kRecheckRemovedFileIntervalMs << " ms.";
  try {
    evb_.runAfterDelay([this, file_name] {
      auto itor = states_.find(file_name);
      if (itor == states_.end()) {
        LOG(ERROR) << file_name << " has been removed, cancel registering";
        return;
      }

      auto watch_fd = RegisterFile(file_name, false);
      if (watch_fd == -1) {
        ScheduleRegisterMonitoredFile(file_name);
        return;
      }

      auto& state = itor->second;
      CheckFileAndCallback(file_name, &state);

      file_names_.emplace(watch_fd, std::move(file_name));
      state.watch_fd = watch_fd;
    },
    kRecheckRemovedFileIntervalMs);
  } catch (const std::system_error& err) {
    LOG(ERROR) << "Failed to schedule registering file: " << file_name
               << std::endl << err.what();
    states_.erase(file_name);
  }
}

void FileWatcher::CheckFileAndCallback(const std::string& file_name,
                                       State* state) {
  CHECK(state);
  uint64_t new_hash;
  auto content = ReadFileAndHash(file_name, &new_hash);
  if (new_hash != state->current_hash) {
    LOG(INFO) << "File change detected for " << file_name;
    common::Stats::get()->Incr("file_change_detected file_name=" +
      boost::filesystem::path(file_name).filename().string());
    state->current_hash = new_hash;
    state->cb(std::move(content));
  }
}
}  // namespace common

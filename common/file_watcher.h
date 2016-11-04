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

#pragma once

#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>

namespace common {
// FileWatcher is designed mainly for monitoring config file changes.
class FileWatcher {
 public:
  // All public interfaces of FileWatcher are thread safe.
  static FileWatcher* Instance();

  /*
   * Add a file to be watched.
   *
   * Once a full modification detected, cb will be called with the content of
   * the file after modification. When a file is first added, cb will be called
   * immediately with the current content of file.
   *
   * A full modification means the fd opened for writing the file has been
   * closed. This is detected by monitoring the IN_CLOSE_WRITE event of inotify
   * API. We don't use the IN_MODIFY event. Because we don't want to have partly
   * modified file content.
   *
   * The file must exist, otherwise a false will be returned. If the file is
   * deleted after a watch is established, the watcher will log errors but
   * continue to monitor it, and resume watching if it is recreated.
   *
   * @return true on success
   *
   * @note currently FileWatcher allows only one callback per file. If needs
   * arise, we can easily extend it to support multiple callbacks per file.
   */
  virtual bool AddFile(const std::string& file_name,
               std::function<void(std::string)> cb);

  /*
   * Remove a file from the file list being watched.
   * @return true on success
   */
  virtual bool RemoveFile(const std::string& file_name);

  /*
   * A custom deleter that does nothing. This should be used when the singleton
   * pointer is wrapped with shared_ptr.
   * Example usage:
   *   std::shared_ptr<FileWatcher> watcher(FileWatcher::Instance(),
   *                                        FileWatcher::Deleter());
   *
   * TODO(zhiyuan): This is for the backward compatibility with the FileWatcher
   * in the vision repo, which returns a shared_ptr. In the future we should
   * remove this and use FileWatcher as a singleton raw pointer directly.
   */
  struct Deleter {
    void operator()(void const *) const {
    }
  };

 protected:
  FileWatcher();
  virtual ~FileWatcher();

 private:
  struct State {
    State(std::function<void(std::string)>&& cb_arg,
          uint64_t hash_arg,
          int watch_fd_arg)
        : cb(std::move(cb_arg))
        , current_hash(hash_arg)
        , watch_fd(watch_fd_arg) {
    }
    std::function<void(std::string)> cb;
    uint64_t current_hash;
    int watch_fd;
  };

  void ReadAndProcessEvents();
  void ProcessEvents(const char* buf, int sz);
  std::string ReadFileAndHash(const std::string& file_name, uint64_t* hash);
  int RegisterFile(const std::string& file_name, const bool check_dup = true);
  void ScheduleRegisterMonitoredFile(const std::string& file_name);
  void CheckFileAndCallback(const std::string& file_name, State* state);

  struct INotifyHandler : public folly::EventHandler {
    INotifyHandler(folly::EventBase* evb, int fd, FileWatcher* watcher)
        : folly::EventHandler(evb, fd)
        , watcher_(watcher) {
    }

    void handlerReady(uint16_t events) noexcept override {
      if ((events & folly::EventHandler::EventFlags::READ) == 0) {
        LOG(ERROR) << "Unexpected inotify event " << events;
        return;
      }

      watcher_->ReadAndProcessEvents();
    }

    FileWatcher* watcher_;
  };

  // event base driving the watcher
  folly::EventBase evb_;

  // fd for the inotify instance
  int fd_;

  // handler for inotify events. handler_ must be defined after evb_ and fd_.
  // because we initilize handler_ in the initialization list, which takes evb_
  // and fd_ as its constructor inputs.
  INotifyHandler handler_;

  // internal thread driving the event base
  std::thread thread_;

  // mapping from watch fd to file name
  std::unordered_map<int, std::string> file_names_;

  // mapping from file name to states
  std::unordered_map<std::string, State> states_;
};
}  // namespace common

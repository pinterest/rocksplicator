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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "folly/futures/Promise.h"
#include "folly/io/async/EventBase.h"
#include "folly/io/async/SSLContext.h"
#include "folly/SocketAddress.h"
#include "thrift/lib/cpp/async/TAsyncSocket.h"
#include "thrift/lib/cpp/async/TAsyncSSLSocket.h"
#include "thrift/lib/cpp/transport/THeader.h"
#include "thrift/lib/cpp2/async/HeaderClientChannel.h"
#include "thrift/lib/cpp2/protocol/BinaryProtocol.h"

DECLARE_int32(channel_cleanup_min_interval_seconds);

DECLARE_int32(channel_max_checking_size);

DECLARE_int32(channel_send_timeout_ms);

DECLARE_int32(default_thrift_client_pool_threads);

DECLARE_int32(min_channel_create_interval_seconds);

DECLARE_int32(tcp_user_timeout_ms);

DECLARE_bool(channel_enable_snappy);

namespace common {

/*
 * ThriftClientPool maintains a pool of channels to remote services.
 * Users may get thrift client object from the pool, and use it to communicate
 * with remote services.
 * Internally each pool has (owns or shares with others) N IO threads and N
 * event bases. Each IO Thread drives one event base. A pool can have at most N
 * connections to a destination.
 * IO threads will be used in a round-robin way for creating new client.
 *
 * If a shared_ptr to a folly::SSLContext is provided, the clientpool will make
 * a TAsyncSSLSocket using this context whenever getChannelFor() is called.
 * The caller is responsible for safely modifying the context and keeping it
 * valid.
 *
 * ThriftClientPool is designed to be used as a shared global object. i.e.,
 * create a pool and use it for the entire process life.
 *
 * // Example usage 1 (create a pool of 8 IO threads, which are created and
 * owned by this pool)
 * ThriftClientPool<T> pool(8);
 * auto client = pool.getClient(ip, port);
 * client->call_some_func();
 *
 * // Example usage 2 (create a pool of clients for service A by sharing the
 * // underlying IO threads and event bases with an existing pool)
 * auto pool_for_service_A = pool.shareIOThreads<A>();
 * auto client_for_A = pool_for_service_A.getClient(...);
 *
 * // Example usage 3 (create a new pool by using the event bases, which are
 * driven by some other threads)
 * const std::vector<folly::EventBase*> evbs = ...;
 * ThriftClientPool<T> pool(evbs);
 * auto client = pool.getClient();
 *
 * // Example usage 4, Create a client using TBinaryProtocol instead of
 * // default THeaderProtocol.
 * ThriftClientPool<T, true> pool(8);
 *
 */
template <typename T, bool USE_BINARY_PROTOCOL = false>
class ThriftClientPool {
 private:
  struct ClientStatusCallback
      : public apache::thrift::CloseCallback
      , public apache::thrift::async::TAsyncSocket::ConnectCallback {
    ClientStatusCallback(const folly::SocketAddress& addr)
      : is_good(true)
      , create_time(time(nullptr))
      , peer_addr(addr) {}

    void channelClosed() override {
      LOG_EVERY_N(INFO, 1000) << peer_addr << " connection closed after "
                              << elapsedTime() << " seconds";

      is_good.store(false);
    }

    void connectSuccess() noexcept override {
      LOG_EVERY_N(INFO, 1000) << peer_addr << " connection established after "
                              << elapsedTime() << " seconds";
    }

    void connectError(const apache::thrift::transport::TTransportException& ex)
        noexcept override {
      LOG(ERROR) << peer_addr << " ConnectError: " << ex.what()
                 << " after " << elapsedTime() << " seconds";

      is_good.store(false);
    }

    time_t elapsedTime() const {
      return time(nullptr) - create_time;
    }

    std::atomic<bool> is_good;
    const time_t create_time;
    const folly::SocketAddress peer_addr;
  };

  struct EventLoop {
    // the event base driving this loop.
    folly::EventBase* evb_;

    // the thread driving this loop.
    std::unique_ptr<std::thread> thread_;

    // last time cleanup was done
    time_t last_cleanup_time_;

    // a map from destinations to channels
    std::unordered_map<
      folly::SocketAddress,
      std::pair<std::weak_ptr<apache::thrift::HeaderClientChannel>,
                std::unique_ptr<ClientStatusCallback>>> channels_;


    // Create an IO thread and an event base for this loop.
    EventLoop() {
      auto evb = std::make_unique<folly::EventBase>();
      thread_ = std::make_unique<std::thread>([evb = evb.get()] {
          static std::atomic<int> thread_id(0);
          auto name =
            folly::stringPrintf("client-io-%d", thread_id.fetch_add(1));
          if (!folly::setThreadName(name)) {
            LOG(ERROR) << "Failed to set thread name for thrift IO thread";
          }
          LOG(INFO) << "Started " << folly::demangle(typeid(T).name())
                    << " thrift client IO thread with name "
                    << name;

          evb->loopForever();
        });

      evb_ = evb.release();
      last_cleanup_time_ = time(nullptr);
    }

    explicit EventLoop(folly::EventBase* evb)
        : evb_(evb)
        , thread_(nullptr)
        , last_cleanup_time_(time(nullptr)) {
    }

    ~EventLoop() {
      // if thread_ is not nullptr, *this owns evb_ and thread_.
      if (thread_) {
        // Pending callbacks won't be called.
        // Thus it is client's responsibility to release resources
        evb_->terminateLoopSoon();
        thread_->join();
        delete evb_;
      }
    }

    // If aggressively is set to true, a new channel will be created
    // immediately if there is no existing good channel for the addr
    //
    // a nullptr or bad channel can be returned if it's too soon to create a new
    // channel and aggressively is set to false
    std::shared_ptr<apache::thrift::HeaderClientChannel>
    getChannelFor(const folly::SocketAddress& addr,
                  const uint32_t connect_timeout_ms,
                  const std::atomic<bool>** is_good,
                  const bool aggressively,
                  const std::shared_ptr<folly::SSLContext>& ssl_ctx) {
      std::shared_ptr<apache::thrift::HeaderClientChannel> channel;
      auto itor = channels_.find(addr);
      bool should_new_channel = false;

      if (itor == channels_.end()) {
        // no such channel yet
        should_new_channel = true;
      } else {
        channel = itor->second.first.lock();
        const bool channel_good = (channel && channel->getTransport()->good());
        const bool too_soon =
          (itor->second.second->create_time +
           FLAGS_min_channel_create_interval_seconds > time(nullptr));
        // we only want to create a new channel if the current channel is not
        // good for use and it's not too soon to create a new one or we want to
        // be aggressive
        if (!channel_good && (!too_soon || aggressively)) {
          should_new_channel = true;
        }
      }

      if (should_new_channel) {
        std::shared_ptr<apache::thrift::async::TAsyncSocket> socket;
        if (ssl_ctx == nullptr) {
          socket = apache::thrift::async::TAsyncSocket::newSocket(evb_);
        } else {
          socket = apache::thrift::async::TAsyncSSLSocket::newSocket(ssl_ctx, evb_);
        }
        auto cb = std::make_unique<ClientStatusCallback>(addr);
        socket->connect(cb.get(), addr, connect_timeout_ms);

#ifdef TCP_USER_TIMEOUT
        // TCP_USER_TIMEOUT is not supported by Ubuntu 12.04.
        if (FLAGS_tcp_user_timeout_ms > 0) {
          unsigned int timeout_ms = FLAGS_tcp_user_timeout_ms;
          const auto ret =
            socket->setSockOpt(IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout_ms);

          if (ret == 0) {
            LOG_EVERY_N(INFO, 1000) << "Set TCP_USER_TIMEOUT to " << timeout_ms
                                    << " ms for " << addr;
          } else {
            LOG(ERROR) << "Failed to set TCP_USER_TIMEOUT to " << timeout_ms
                       << " ms with errno " << errno << " : "
                       << strerror(errno);
          }
        }
#endif

        channel = apache::thrift::HeaderClientChannel::newChannel(socket);
        if (FLAGS_channel_send_timeout_ms > 0) {
          channel->setTimeout(FLAGS_channel_send_timeout_ms);
        }
        if (FLAGS_channel_enable_snappy) {
          channel->setTransform(apache::thrift::transport::THeader::SNAPPY_TRANSFORM);
        }
        if (USE_BINARY_PROTOCOL) {
          channel->setProtocolId(apache::thrift::protocol::T_BINARY_PROTOCOL);
          channel->setClientType(THRIFT_FRAMED_DEPRECATED);
        }

        if (is_good) {
          *is_good = &cb->is_good;
        }
        channel->setCloseCallback(cb.get());
        channels_[addr] =
          std::pair<std::weak_ptr<apache::thrift::HeaderClientChannel>,
                    std::unique_ptr<ClientStatusCallback>>(
            channel, std::move(cb));
      } else {
        if (is_good) {
          *is_good = &(itor->second.second->is_good);
        }
      }

      return channel;
    }

    void cleanupStaleChannels(const folly::SocketAddress& addr) {
      // cleanup stale entries if it hasn't been done for a period of time.
      auto now = time(nullptr);
      if (last_cleanup_time_ + FLAGS_channel_cleanup_min_interval_seconds
          < now) {
        last_cleanup_time_ = now;
        auto itor = channels_.find(addr);
        int n = 0;
        while (itor != channels_.end() &&
               n++ < FLAGS_channel_max_checking_size) {
          // We don't cleanup !good() live channels here. Otherwise we
          // will need to upgrade it to a shared_ptr. We expect clients
          // won't keep a !good() channels for a long period of time.
          if (itor->second.first.use_count() == 0) {
            itor = channels_.erase(itor);
          } else {
            ++itor;
          }
        }
      }
    }

    // no copy
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    EventLoop& operator=(EventLoop&& el) {
      evb_ = el.evb_;
      thread_ = std::move(el.thread_);
      last_cleanup_time_ = el.last_cleanup_time_;
      channels_ = std::move(el.channels_);

      return *this;
    }

    EventLoop(EventLoop&& el) {
      *this = std::move(el);
    }
  };

 public:
  // Create a new pool of n_io_threads IO threads. The threads are owned by the
  // pool.
  explicit ThriftClientPool(
      const uint16_t n_io_threads =
          static_cast<uint16_t>(FLAGS_default_thrift_client_pool_threads),
      const std::shared_ptr<folly::SSLContext>* ssl_ctx = nullptr)
      : event_loops_(n_io_threads), ssl_ctx_(ssl_ctx) {
    CHECK_GT(n_io_threads, 0);
  }

  // Create a new pool by using the evbs, which are supposed to be driven by
  // some other threads. It's users' responsibility to ensure that evbs and
  // the threads driving them outlive the pool object.
  explicit ThriftClientPool(
      const std::vector<folly::EventBase*>& evbs,
      const std::shared_ptr<folly::SSLContext>* ssl_ctx = nullptr)
      : event_loops_(), ssl_ctx_(ssl_ctx) {
    CHECK(!evbs.empty());
    event_loops_.reserve(evbs.size());
    for (const auto& evb : evbs) {
      event_loops_.emplace_back(evb);
    }
  }

  // Create a new pool of clients of type U, which share the same IO threads
  // and event bases with *this
  template <typename U>
  std::unique_ptr<ThriftClientPool<U>> shareIOThreads(
      const std::shared_ptr<folly::SSLContext>* ssl_ctx = nullptr) const {
    std::vector<folly::EventBase*> evbs;
    evbs.reserve(event_loops_.size());
    for (const auto& event_loop : event_loops_) {
      evbs.push_back(event_loop.evb_);
    }

    return std::make_unique<ThriftClientPool<U>>(evbs, ssl_ctx);
  }

  // Get unique_ptr pointing to a thrift client object of type T, which can be
  // used to talk to ip:port.
  // It's users' responsibility to ensure that *this outlives the returned
  // client.
  // @param is_good is an optional out parameter indicating if the underlying
  // channel is good. It is guaranteed to be alive until the returned client
  // is released
  // If aggressively is set to true, a new channel will be created
  // immediately if there is no existing good channel for the addr
  //
  // @note a nullptr will be returned if a channel couldn't be obtained.
  auto getClient(const folly::SocketAddress& addr,
                 const uint32_t connect_timeout_ms = 0,
                 const std::atomic<bool>** is_good = nullptr,
                 const bool aggressively = true) {
    auto idx = nextEvbIdx_.fetch_add(1) % event_loops_.size();

    // We can't use lambda for std::unique_ptr deleter. Otherwise, we won't be
    // able to do "client = getClient()", where client was previously created.
    // Because lambda doesn't have copy assignment operator. And gcc
    // happens not implement move assignment operator for lambdas.
    struct Deleter {
      explicit Deleter(folly::EventBase* evb) : evb_(evb) {}

      void operator()(T* t) {
        // We have to wait for it to avoid memory leak.
        evb_->runInEventBaseThreadAndWait([t] {
            delete t;
          });
      }

      folly::EventBase* evb_;
    };

    Deleter deleter(event_loops_[idx].evb_);
    std::unique_ptr<T, Deleter> client(nullptr, deleter);
    auto ssl_ctx =
        ssl_ctx_ == nullptr ? nullptr : std::atomic_load_explicit(ssl_ctx_, std::memory_order_acquire);
    event_loops_[idx].evb_->runInEventBaseThreadAndWait(
        [&client, &event_loop = event_loops_[idx], &addr, &is_good,
         connect_timeout_ms, aggressively, ssl_ctx = std::move(ssl_ctx)] () mutable {
          auto channel = event_loop.getChannelFor(addr, connect_timeout_ms,
                                                  is_good, aggressively, std::move(ssl_ctx));

          event_loop.cleanupStaleChannels(addr);

          // The underlying folly::AsyncSocket has to be created/released on the
          // same IO thread to avoid race condition on its internal states. So
          // we need to release client on the corresponding IO thread too.
          // assert(eventBase_ == nullptr || eventBase_->isInEventBaseThread());
          // was placed in folly code base to ensure that.
          //
          // This must be called after cleanupStaleChannels above. Otherwise
          // there will be a race between ~ThriftClientPool() and getClient()
          // for pools which don't own the underlying IO threads.
          if (channel) {
            client.reset(new T(channel));
          }
        });
    return client;
  }

  // Similar to getClient() above
  auto getClient(const std::string& ip, const uint16_t port,
                 const uint32_t connect_timeout_ms = 0,
                 const std::atomic<bool>** is_good = nullptr,
                 const bool aggressively = true) {
    return getClient(folly::SocketAddress(ip, port), connect_timeout_ms,
                     is_good, aggressively);
  }

  // no copy or move
  ThriftClientPool(const ThriftClientPool&) = delete;
  ThriftClientPool(ThriftClientPool&&) = delete;
  ThriftClientPool& operator=(const ThriftClientPool&) = delete;
  ThriftClientPool& operator=(ThriftClientPool&&) = delete;

 private:
  std::vector<EventLoop> event_loops_;
  const std::shared_ptr<folly::SSLContext>* ssl_ctx_;

  // The evb to be used for the next new client
  static std::atomic<uint32_t> nextEvbIdx_;
};
template <typename T, bool USE_BINARY_PROTOCOL>
std::atomic<uint32_t> ThriftClientPool<T, USE_BINARY_PROTOCOL>::nextEvbIdx_ {0};
}  // namespace common

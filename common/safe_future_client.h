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
// @author bol (bol@pinterest.com)
//

#pragma once

#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp/TApplicationException.h>

#include <string>

#include "common/global_cpu_executor.h"
#include "common/timer.h"
#include "folly/futures/Future.h"
#include "folly/futures/Promise.h"
#include "folly/io/async/EventBase.h"
#if __GNUC__ >= 8
#include "folly/executors/task_queue/BlockingQueue.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#else
#include "wangle/concurrent/CPUThreadPoolExecutor.h"
#endif

namespace common {

/*
 * Request traits for RequestType that are used by SafeFutureClient
 */
template <typename RequestType> struct RequestTraits;
/*
  An example specialization is as follow.

template <> struct RequestTraits<::realpin::thrift::RealPinRequest> {
  using response_type = ::realpin::thrift::RealPinResponse;

  using call_func_type = void(::realpin::thrift::RealPinAsyncClient::*)(
      apache::thrift::RpcOptions&,
      std::unique_ptr<apache::thrift::RequestCallback>,
      const ::realpin::thrift::RealPinRequest&);

  using recv_func_type =
      void(*)(response_type&, apache::thrift::ClientReceiveState&);

  using exception_type = ::realpin::thrift::RealPinException;

  static const call_func_type call_func;

  static const recv_func_type recv_func;

  static const std::string request_latency_metric_name;

  static const std::string response_processing_metric_name;

  static const bool process_response_in_io_thread = false;
};
*/


/*
 * SafeFutureClient is a wrapper of AsyncClient generated by FBThrift, which has
 * two nice features.
 * 1. It is thread safe (AsyncClient is not), which is useful for implementing
 *    speculative failover
 * 2. Its template member function future_call() is useful for building generic
 *    logics. Such as request routing
 */
template <typename ClientType>
class SafeFutureClient {
 public:
  explicit SafeFutureClient(std::shared_ptr<ClientType> client)
    : client_(std::move(client)) {}

  template <typename T>
  folly::Future<typename RequestTraits<T>::response_type> future_call(
      const apache::thrift::RpcOptions& rpc_options,
      std::shared_ptr<T> request) {
    folly::Promise<typename RequestTraits<T>::response_type> p;
    auto f = p.getFuture();
    auto evb = dynamic_cast<apache::thrift::HeaderClientChannel*>(
      client_->getChannel())->getEventBase();
    common::Stats::get()->AddMetric(
      evb_queue_size_metrics_name, evb->getNotificationQueueSize());
    auto timer = std::make_unique<common::Timer>(jump_in_evb_ms_metrics_name);

    evb->runInEventBaseThread([rpc_options = rpc_options, request,
                               p = std::move(p), client = client_,
                               timer = std::move(timer)] () mutable {
      timer.reset(
        new common::Timer(RequestTraits<T>::request_latency_metric_name));

      auto op = [p = std::move(p), timer = std::move(timer)] (
          apache::thrift::ClientReceiveState&& ws) mutable {
        if (RequestTraits<T>::process_response_in_io_thread) {
          timer.reset(nullptr);
          process_response<T>(&p, &ws);
        } else {
          timer.reset(new common::Timer(executor_pending_ms_metrics_name));
          auto process_response_op = [p = std::move(p), ws = std::move(ws),
                                      timer = std::move(timer)] () mutable {
            timer.reset(nullptr);
            process_response<T>(&p, &ws);
          };

          try {
            common::getGlobalCPUExecutor()->add(std::move(process_response_op));
#if __GNUC__ >= 8
          } catch (const folly::QueueFullException& e) {
#else
          } catch (const wangle::QueueFullException& e) {
#endif
            // If add() throws, process_response_op is ok to use here.
            // Remember that std::move() is just a typecast that doesn't
            // actually move anything. And Executor won't consume it if add()
            // fails
            process_response_op();
            LOG(ERROR) << "Failed to enqueue process_response into global cpu "
                       << " pool, did it in IO thread";
            common::Stats::get()->Incr(executor_enqueue_fail_times);
          }

          auto stats = common::getGlobalCPUExecutor()->getPoolStats();
          common::Stats::get()->AddMetric(
            executor_pending_task_count_metrics_name, stats.pendingTaskCount);
        }
      };

      (*client.*RequestTraits<T>::call_func)(
        rpc_options, make_callback(std::move(op)), *request);
    });

    return f;
  }

 private:
  template <typename F>
  struct Callback : public apache::thrift::RequestCallback {
    explicit Callback(F&& callback)
      : callback_(std::forward<F>(callback)) {}

    void replyReceived(apache::thrift::ClientReceiveState&& state) override {
      callback_(std::move(state));
    }

    void requestError(apache::thrift::ClientReceiveState&& state) override {
      callback_(std::move(state));
    }

    void requestSent() override {}

    F callback_;
  };

  template <typename F>
  static std::unique_ptr<Callback<F>> make_callback(F&& f) {
    return std::make_unique<Callback<F>>(std::forward<F>(f));
  }

  template <typename T>
  static void process_response(
      folly::Promise<typename RequestTraits<T>::response_type>* p,
      apache::thrift::ClientReceiveState* recvState) {
    common::Timer timer(RequestTraits<T>::response_processing_metric_name);

    if (recvState->isException()) {
#if __GNUC__ >= 8
      p->setException(recvState->exception());
#else
      p->setException(recvState->exceptionWrapper());
#endif
      return;
    }

    auto ex = folly::try_and_catch<std::exception,
                                   apache::thrift::TApplicationException,
                                   typename RequestTraits<T>::exception_type>([&p, &recvState]() {
       typename RequestTraits<T>::response_type response;
       RequestTraits<T>::recv_func(response, *recvState);
       p->setValue(std::move(response));
    });

    if (ex) {
      p->setException(std::move(ex));
    }
  }

  std::shared_ptr<ClientType> client_;
  static const std::string executor_enqueue_fail_times;
  static const std::string executor_pending_ms_metrics_name;
  static const std::string executor_pending_task_count_metrics_name;
  static const std::string jump_in_evb_ms_metrics_name;
  static const std::string evb_queue_size_metrics_name;
};

template <typename ClientType>
const std::string
SafeFutureClient<ClientType>::executor_enqueue_fail_times =
  "executor_enqueue_fail_times";

template <typename ClientType>
const std::string
SafeFutureClient<ClientType>::executor_pending_ms_metrics_name =
  "executor_pending_ms";

template <typename ClientType>
const std::string
SafeFutureClient<ClientType>::executor_pending_task_count_metrics_name =
  "executor_pending_task_count";

template <typename ClientType>
const std::string SafeFutureClient<ClientType>::jump_in_evb_ms_metrics_name =
  "jump_in_evb_latency_ms";

template <typename ClientType>
const std::string SafeFutureClient<ClientType>::evb_queue_size_metrics_name =
  "evb_queue_size";

}  // namespace common

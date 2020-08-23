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


/**
 * Stats class for recording metrics/counters. Uses folly's stats libraries
 * to record thread local stats to avoid locking. As the thread local stats
 * become stale (upto 1 seconds old), they are flushed into a global stats
 * which involves some locking. While retrieving stats, the global stats
 * object is consulted. The stats (counters/metrics) are organized in 1 second
 * buckets holding upto 1 minute (60 buckets) worth of data. For metrics,
 * we are using a histogram with bucket size of 1 and a range from 1 to 200
 * to save space. Mostly we use metrics for latency tracking for which the above
 * buckets and range is good enough. The stats can be off by 1 bucket (1 in 60).
 *
 * Usage:
 *
 * Stats::get()->Incr(counter, 100);
 * Stats::get()->AddMetric(metric, 45);
 *
 * Register callbacks so the Incr / AddMetric is called periodically when
 * refreshing threadlocal stats.
 * Stats::get()->RegisterIncr(counter, std::function<uint64_t()>);
 * Stats::get()->RegisterAddMetric(metric, std::function<int64_t()>);
 *
 * Register callbacks to set the value of a stat (eg. export a map's size).
 * Stats::get()->RegisterGauge(gauge, std::function<uint64_t()>);
 *
 * Stats::get()->GetCounter(counter)->GetTotal();
 * Stats::get()->GetCounter(counter)->GetLastMinute();
 *
 * Stats::get()->GetMetric(metric)->GetPercentileTotal(90);
 * Stats::get()->GetMetric(metric)->GetPercentileLastMinute(90);
 *
 * "counter" and "metric" above can be string names for dynamic stats (whose
 * stat names
 * are determined at run time) or uint32_t idx for pre-defined stats (whose stat
 * names are determined at compile time).
 * pre-defined stats is more performant than dynamic stats. Thus you may want to
 * use
 * pre-defined stats if your service use stats extensively and stat names are
 * pre-defined
 * at compile time.
 *
 * Pre-defined stat names (if any) must be passed to Stats::init() before any
 * other Stats
 * functions are called.
 *
 * Dynamic stats and pre-defined stats can be used together. i.e., some stats
 * are pre-defined,
 * while others are dynamic.
 */

#pragma once

#if __GNUC__ >= 8
#include <folly/concurrency/CacheLocality.h>
#else
#include <folly/detail/CacheLocality.h>
#endif

#include <folly/ThreadLocal.h>
#include <folly/stats/Histogram.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <folly/stats/TimeseriesHistogram.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace common {

class LocalStats;

class Stats {
 public:
  // Counter update functions.
  void Incr(const uint32_t counter, uint64_t value = 1);
  void Incr(const std::string& counter, uint64_t value = 1);

  // Metric update functions.
  void AddMetric(const uint32_t metric, int64_t value);
  void AddMetric(const std::string& metric, int64_t value);

  // Register callback for Counter / Metric / Gauge so they will be reported
  // periodically by the Stats class.
  void RegisterIncr(const std::string& counter, std::function<uint64_t()>);
  void RegisterAddMetric(const std::string& metric, std::function<int64_t()>);
  void RegisterGauge(const std::string& gauge, std::function<uint64_t()>);

  // Returns the singleton stats instance.
  static Stats* get();

  // set the counter and metric name vector.
  // this function must be called before get() is used.
  static void init(const std::vector<std::string>* counter_names,
                   const std::vector<std::string>* metric_names);

  struct MultiLevelTimeSeriesWrapper {
    folly::MultiLevelTimeSeries<uint64_t> timeseries;
    std::mutex m;

    MultiLevelTimeSeriesWrapper(
        const folly::MultiLevelTimeSeries<uint64_t>& timeseries_arg)
        : timeseries(timeseries_arg), m() {}
  };

  struct TimeseriesHistogramWrapper {
    folly::TimeseriesHistogram<int64_t> ts_histogram;
    std::mutex m;

    TimeseriesHistogramWrapper(
        const folly::TimeseriesHistogram<int64_t>& ts_histogram_arg)
        : ts_histogram(ts_histogram_arg), m() {}
  };

  class Counter {
   public:
    explicit Counter(MultiLevelTimeSeriesWrapper* ts_wrapper_arg);
    uint64_t GetTotal();
    uint64_t GetLastMinute();

   private:
    MultiLevelTimeSeriesWrapper* ts_wrapper_;
  };

  class Metric {
   public:
    explicit Metric(TimeseriesHistogramWrapper* hist_wrapper_arg);

    int64_t GetPercentileLastMinute(double pct);
    int64_t GetPercentileTotal(double pct);
    int64_t GetSumLastMinute();
    int64_t GetSumTotal();
    int64_t GetAverageLastMinute();
    int64_t GetAverageTotal();
    int64_t GetCountLastMinute();
    int64_t GetCountTotal();

   private:
    TimeseriesHistogramWrapper* hist_wrapper_;
  };

  class Gauge {
   public:
    explicit Gauge(std::atomic<uint64_t>* value);
    uint64_t GetValue();

   private:
    std::atomic<uint64_t>* value_;
  };

  // Returns the corresponding Stats::Metric object, if the metric is not found,
  // nullptr is returned.
  std::unique_ptr<Metric> GetMetric(const uint32_t metric);
  std::unique_ptr<Metric> GetMetric(const std::string& metric);
  // Returns the corresponding Stats::Counter object, if the counter is not
  // found, nullptr is returned.
  std::unique_ptr<Counter> GetCounter(const uint32_t counter);
  std::unique_ptr<Counter> GetCounter(const std::string& counter);

  // Returns the corresponding Stats::Gauge object, if the gauge is not
  // found, nullptr is returned.
  std::unique_ptr<Gauge> GetGauge(const std::string& gauge);

  // Dumps all the stats in the form of text (formatted similar to how java's
  // ostrich library dumps stats).
  std::string DumpStatsAsText();

  // Set the number of seconds per min, used for test to reduce testing time
  static void SetSecondsPerMin(uint32_t n) { nSecondsPerMin_.store(n); }

 private:
  friend class LocalStats;

  // Used by the LocalStats class to flush thread local metrics to global stats.
  // Not meant for regular usage.
  void FlushMetric(const uint32_t metric,
                   const folly::Histogram<int64_t>& histogram,
                   std::chrono::seconds time_epoch_seconds);
  void FlushMetric(const std::string& metric,
                   const folly::Histogram<int64_t>& histogram,
                   std::chrono::seconds time_epoch_seconds);
  void FlushCounter(const uint32_t counter, uint64_t sum,
                    std::chrono::seconds time_epoch_seconds);
  void FlushCounter(const std::string& counter, uint64_t sum,
                    std::chrono::seconds time_epoch_seconds);
  void FlushGauge(const std::string& counter, uint64_t value);

  Stats();
  ~Stats();

  LocalStats* GetLocalStats();

  // Interval at which thread local stats are flushed out.
  const std::chrono::milliseconds flush_interval_;

  // Thread local stats.
  folly::ThreadLocalPtr<LocalStats, Stats> local_stats_;

  std::vector<std::unique_ptr<TimeseriesHistogramWrapper>> histograms_;
  std::vector<std::unique_ptr<MultiLevelTimeSeriesWrapper>> timeseries_;

  std::unordered_map<std::string, std::unique_ptr<TimeseriesHistogramWrapper>>
      histogram_map_;
  std::mutex lock_histogram_map_;
  std::unordered_map<std::string, std::unique_ptr<MultiLevelTimeSeriesWrapper>>
      timeseries_map_;
  std::mutex lock_timeseries_map_;
  std::unordered_map<std::string, std::unique_ptr<std::atomic<uint64_t>>> gauges_map_;
  std::mutex lock_gauges_map_;

  static std::atomic<uint32_t> nSecondsPerMin_;
  static std::atomic<const std::vector<std::string>*> counter_names_;
  static std::atomic<const std::vector<std::string>*> metric_names_;

  std::thread flush_thread_;
  std::atomic<bool> should_stop_;

  // Callbacks registerd for pulling periodically
  std::mutex lock_counter_callbacks_;
  std::mutex lock_metrics_callbacks_;
  std::mutex lock_gauge_callbacks_;
  std::unordered_map<std::string, std::function<uint64_t()>> counter_callbacks_;
  std::unordered_map<std::string, std::function<int64_t()>> metrics_callbacks_;
  std::unordered_map<std::string, std::function<uint64_t()>> gauge_callbacks_;
};

}  // namespace common

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


#include "common/stats/stats.h"

#include <boost/format.hpp>
#include <folly/Memory.h>
#include <folly/stats/BucketedTimeSeries-defs.h>
#include <folly/stats/MultiLevelTimeSeries-defs.h>
#include <folly/stats/Histogram-defs.h>
#include <folly/stats/TimeseriesHistogram-defs.h>
#include <folly/String.h>
#include <cstdlib>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using folly::Histogram;
using folly::MultiLevelTimeSeries;
using std::chrono::duration_cast;
using std::chrono::seconds;
using std::chrono::system_clock;
using std::lock_guard;
using std::mutex;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace common {

namespace {

const int64_t kMinMetricValue = 0;
const int64_t kMaxMetricValue = 200;
const int64_t kFlushIntervalMS = 250;

inline seconds GetTimeSinceEpochSeconds() {
  return duration_cast<seconds>(system_clock::now().time_since_epoch());
}

unique_ptr<Stats::TimeseriesHistogramWrapper> GetTimeseriesHistogramWrapper(
    uint32_t nSecondsPerMin) {
  std::vector<seconds> intervals { seconds(nSecondsPerMin), seconds(0) };

  folly::TimeseriesHistogram<int64_t> ts_histogram(
      1, kMinMetricValue, kMaxMetricValue,
      MultiLevelTimeSeries<int64_t>(60, intervals.size(), intervals.data()));

  return folly::make_unique<Stats::TimeseriesHistogramWrapper>(ts_histogram);
}

unique_ptr<Stats::MultiLevelTimeSeriesWrapper> GetMultiLevelTimeSeriesWrapper(
    uint32_t nSecondsPerMin) {
  std::vector<seconds> intervals { seconds(nSecondsPerMin), seconds(0) };

  MultiLevelTimeSeries<uint64_t> timeseries(60, intervals.size(),
                                            intervals.data());

  return folly::make_unique<Stats::MultiLevelTimeSeriesWrapper>(timeseries);
}

uint32_t GetArraySize(const std::vector<string>* array) {
  if (array) {
    return array->size();
  }

  return 0;
}

}  // namespace

std::atomic<uint32_t> Stats::nSecondsPerMin_ { 60 };

std::atomic<const std::vector<string>*> Stats::counter_names_ { nullptr };

std::atomic<const std::vector<string>*> Stats::metric_names_ { nullptr };


/** Thread local stats object. Periodically flushed to global stats. */
class LocalStats {
 public:
  LocalStats(uint32_t num_counters, uint32_t num_metrics,
             int64_t min_metric_value, int64_t max_metric_value);

  // Counter update methods.
  void Incr(const uint32_t counter, uint64_t value = 1);
  void Incr(const string& counter, uint64_t value = 1);

  // Metric update methods.
  void AddMetric(const uint32_t metric, int64_t value);
  void AddMetric(const string& metric, int64_t value);

  void FlushAll();

 private:
  // Helper structs for storing last flushing time along with the histogram/
  // counter.
  struct HistogramWrapper {
    unique_ptr<Histogram<int64_t>> histogram;
    mutex m;

    HistogramWrapper(int64_t min_value, int64_t max_value)
        : histogram(new Histogram<int64_t>(1, min_value, max_value)) {}
  };

  struct Counter {
    std::atomic<uint64_t> sum;

    explicit Counter(uint64_t n) : sum(n) {}
  };

  // Flush the thread local stats to global stats.
  void FlushCounter(const uint32_t counter, seconds time_epoch_seconds);
  void FlushCounter(std::pair<const string, unique_ptr<Counter>>* counter,
                    seconds time_epoch_seconds);
  void FlushMetric(const uint32_t metric, seconds time_epoch_seconds);
  void FlushMetric(
      std::pair<const string, unique_ptr<HistogramWrapper>>* metric,
      seconds time_epoch_seconds);

  const int64_t min_metric_value_;
  const int64_t max_metric_value_;

  std::vector<unique_ptr<HistogramWrapper>> histograms_;
  std::vector<unique_ptr<Counter>> counters_;

  std::unordered_map<string, unique_ptr<HistogramWrapper>> histogram_map_;
  mutex lock_histogram_map_;
  std::unordered_map<string, unique_ptr<Counter>> counter_map_;
  mutex lock_counter_map_;

  // Global stats object.
  Stats* stats_;
};

LocalStats::LocalStats(uint32_t num_counters, uint32_t num_metrics,
                       int64_t min_metric_value, int64_t max_metric_value)
    : min_metric_value_(min_metric_value),
      max_metric_value_(max_metric_value),
      histograms_(),
      counters_(),
      histogram_map_(),
      lock_histogram_map_(),
      counter_map_(),
      lock_counter_map_(),
      stats_(Stats::get()) {
  for (uint32_t i = 0; i < num_metrics; ++i) {
    histograms_.emplace_back(folly::make_unique<HistogramWrapper>(
        min_metric_value_, max_metric_value_));
  }

  for (uint32_t i = 0; i < num_counters; ++i) {
    counters_.emplace_back(folly::make_unique<Counter>(0));
  }
}

void LocalStats::Incr(const uint32_t counter, uint64_t value) {
  if (LIKELY(counter < counters_.size())) {
    counters_[counter]->sum.fetch_add(value);
  }
}

void LocalStats::Incr(const string& counter, uint64_t value) {
  // only the thread who is recording stats can modify the structure of
  // counter_map_.
  // Thus we don't need to do any synchronizations when reading it.
  auto it = counter_map_.find(counter);
  if (LIKELY(it != counter_map_.end())) {
    it->second->sum.fetch_add(value);
    return;
  }

  lock_guard<mutex> g(lock_counter_map_);
  counter_map_.emplace(counter, folly::make_unique<Counter>(value));
}

void LocalStats::AddMetric(const uint32_t metric, int64_t value) {
  if (LIKELY(metric < histograms_.size())) {
    lock_guard<mutex> g(histograms_[metric]->m);
    histograms_[metric]->histogram->addValue(value);
  }
}

void LocalStats::AddMetric(const string& metric, int64_t value) {
  // only the thread who is recording stats can modify the structure of
  // histogram_map_.
  // Thus we don't need to do any synchronizations when reading it.
  auto it = histogram_map_.find(metric);
  if (LIKELY(it != histogram_map_.end())) {
    lock_guard<mutex> g(it->second->m);
    it->second->histogram->addValue(value);
    return;
  }

  auto hw = folly::make_unique<HistogramWrapper>(min_metric_value_,
                                                 max_metric_value_);
  hw->histogram->addValue(value);

  lock_guard<mutex> g(lock_histogram_map_);
  histogram_map_.emplace(metric, std::move(hw));
}

void LocalStats::FlushAll() {
  auto now = GetTimeSinceEpochSeconds();
  for (uint32_t counter = 0; counter < counters_.size(); ++counter) {
    FlushCounter(counter, now);
  }

  {
    lock_guard<mutex> g(lock_counter_map_);
    for (auto& counter : counter_map_) {
      FlushCounter(&counter, now);
    }
  }

  for (uint32_t metric = 0; metric < histograms_.size(); ++metric) {
    FlushMetric(metric, now);
  }

  {
    lock_guard<mutex> g(lock_histogram_map_);
    for (auto& histogram : histogram_map_) {
      FlushMetric(&histogram, now);
    }
  }
}

void LocalStats::FlushCounter(const uint32_t counter,
                              seconds time_epoch_seconds) {
  auto sum = counters_[counter]->sum.exchange(0);

  // Flush to global stats.
  if (sum > 0) {
    stats_->FlushCounter(counter, sum, time_epoch_seconds);
  }
}

void LocalStats::FlushCounter(
    std::pair<const string, unique_ptr<Counter>>* counter,
    seconds time_epoch_seconds) {
  auto sum = counter->second->sum.exchange(0);

  if (sum > 0) {
    stats_->FlushCounter(counter->first, sum, time_epoch_seconds);
  }
}

void LocalStats::FlushMetric(const uint32_t metric,
                             seconds time_epoch_seconds) {
  auto histogram_ptr = folly::make_unique<Histogram<int64_t>>(
      1, min_metric_value_, max_metric_value_);

  {
    lock_guard<mutex> g(histograms_[metric]->m);
    histograms_[metric]->histogram.swap(histogram_ptr);
  }

  // Flush to global stats.
  stats_->FlushMetric(metric, *histogram_ptr, time_epoch_seconds);
}

void LocalStats::FlushMetric(
    std::pair<const string, unique_ptr<HistogramWrapper>>* metric,
    seconds time_epoch_seconds) {
  auto histogram_ptr = folly::make_unique<Histogram<int64_t>>(
      1, min_metric_value_, max_metric_value_);

  {
    lock_guard<mutex> g(metric->second->m);
    metric->second->histogram.swap(histogram_ptr);
  }

  // Flush to global stats.
  stats_->FlushMetric(metric->first, *histogram_ptr, time_epoch_seconds);
}

Stats* Stats::get() {
  static Stats instance;
  return &instance;
}

void Stats::init(const std::vector<string>* counter_names,
                 const std::vector<string>* metric_names) {
  counter_names_.store(counter_names);
  metric_names_.store(metric_names);
}

Stats::Stats() : flush_interval_(kFlushIntervalMS)
               , should_stop_(false) {
  auto num_metrics = GetArraySize(metric_names_.load());
  if (num_metrics > 0) {
    for (uint32_t i = 0; i < num_metrics; ++i) {
      histograms_.emplace_back(
          GetTimeseriesHistogramWrapper(nSecondsPerMin_.load()));
    }
  }

  auto num_counters = GetArraySize(counter_names_.load());
  if (num_counters > 0) {
    for (uint32_t i = 0; i < num_counters; ++i) {
      timeseries_.emplace_back(
          GetMultiLevelTimeSeriesWrapper(nSecondsPerMin_.load()));
    }
  }

    // Start flush thread.
  flush_thread_ = thread([this] {
    while (!should_stop_) {
      sleep_for(this->flush_interval_);
      {
        lock_guard<mutex> g(lock_counter_callbacks_);
        for (auto &registered_counters : counter_callbacks_) {
          Incr(registered_counters.first, registered_counters.second());
        }
      }
      {
        lock_guard<mutex> g(lock_metrics_callbacks_);
        for (auto& registered_metrics : metrics_callbacks_) {
          AddMetric(registered_metrics.first, registered_metrics.second());
        }
      }
      // Note that this object blocks creation of new thread local objects until
      // it is destroyed.
      auto accessor = this->local_stats_.accessAllThreads();
      for (auto it = accessor.begin(); it != accessor.end(); ++it) {
        it->FlushAll();
      }
    }
  });
}

Stats::~Stats() {
  should_stop_.store(true);
  flush_thread_.join();
}

void Stats::Incr(const uint32_t counter, uint64_t value) {
  GetLocalStats()->Incr(counter, value);
}

void Stats::Incr(const string& counter, uint64_t value) {
  GetLocalStats()->Incr(counter, value);
}

void Stats::AddMetric(const uint32_t metric, int64_t value) {
  GetLocalStats()->AddMetric(metric, value);
}

void Stats::AddMetric(const string& metric, int64_t value) {
  GetLocalStats()->AddMetric(metric, value);
}

void Stats::RegisterIncr(const std::string &counter,
                         std::function<uint64_t()> callback) {
  lock_guard<mutex> g(lock_counter_callbacks_);
  counter_callbacks_.emplace(counter, std::move(callback));
}


void Stats::RegisterAddMetric(const std::string &metric,
                              std::function<int64_t()> callback) {
  lock_guard<mutex> g(lock_metrics_callbacks_);
  metrics_callbacks_.emplace(metric, std::move(callback));
}

LocalStats* Stats::GetLocalStats() {
  auto ptr = local_stats_.get();
  if (UNLIKELY(ptr == nullptr)) {
    local_stats_.reset(new LocalStats(GetArraySize(counter_names_.load()),
                                      GetArraySize(metric_names_.load()),
                                      kMinMetricValue, kMaxMetricValue));
    ptr = local_stats_.get();
  }

  return ptr;
}

void Stats::FlushMetric(const uint32_t metric,
                        const Histogram<int64_t>& histogram,
                        seconds time_epoch_seconds) {
  if (LIKELY(metric < histograms_.size())) {
    lock_guard<mutex> g(histograms_[metric]->m);
    histograms_[metric]->ts_histogram.addValues(time_epoch_seconds, histogram);
  }
}

void Stats::FlushMetric(const string& metric,
                        const Histogram<int64_t>& histogram,
                        seconds time_epoch_seconds) {
  // only the flush thread can modify the structure of histogram_map_.
  // Thus we don't need to do any synchronizations when reading it.
  auto it = histogram_map_.find(metric);
  if (LIKELY(it != histogram_map_.end())) {
    lock_guard<mutex> g(it->second->m);
    it->second->ts_histogram.addValues(time_epoch_seconds, histogram);
    return;
  }

  auto thw = GetTimeseriesHistogramWrapper(nSecondsPerMin_.load());
  thw->ts_histogram.addValues(time_epoch_seconds, histogram);

  lock_guard<mutex> g(lock_histogram_map_);
  histogram_map_.emplace(metric, std::move(thw));
}

void Stats::FlushCounter(const uint32_t counter, uint64_t sum,
                         seconds time_epoch_seconds) {
  if (LIKELY(counter < timeseries_.size())) {
    lock_guard<mutex> g(timeseries_[counter]->m);
    timeseries_[counter]->timeseries.addValue(time_epoch_seconds, sum);
  }
}

void Stats::FlushCounter(const string& counter, uint64_t sum,
                         seconds time_epoch_seconds) {
  // only the flush thread can modify the structure of counter_map_.
  // Thus we don't need to do any synchronizations when reading it.
  auto it = timeseries_map_.find(counter);
  if (LIKELY(it != timeseries_map_.end())) {
    lock_guard<mutex> g(it->second->m);
    it->second->timeseries.addValue(time_epoch_seconds, sum);
    return;
  }

  auto mtsw = GetMultiLevelTimeSeriesWrapper(nSecondsPerMin_.load());
  mtsw->timeseries.addValue(time_epoch_seconds, sum);

  lock_guard<mutex> g(lock_timeseries_map_);
  timeseries_map_.emplace(counter, std::move(mtsw));
}

Stats::Counter::Counter(Stats::MultiLevelTimeSeriesWrapper* ts_wrapper_arg)
    : ts_wrapper_(ts_wrapper_arg) {}

uint64_t Stats::Counter::GetTotal() {
  lock_guard<mutex> l(ts_wrapper_->m);
  ts_wrapper_->timeseries.update(GetTimeSinceEpochSeconds());
  return ts_wrapper_->timeseries.sum(1);
}

uint64_t Stats::Counter::GetLastMinute() {
  lock_guard<mutex> l(ts_wrapper_->m);
  ts_wrapper_->timeseries.update(GetTimeSinceEpochSeconds());
  return ts_wrapper_->timeseries.sum(0);
}

Stats::Metric::Metric(TimeseriesHistogramWrapper* hist_wrapper_arg)
    : hist_wrapper_(hist_wrapper_arg) {}

int64_t Stats::Metric::GetPercentileTotal(double pct) {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.getPercentileEstimate(pct, 1);
}

int64_t Stats::Metric::GetPercentileLastMinute(double pct) {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.getPercentileEstimate(pct, 0);
}

int64_t Stats::Metric::GetSumTotal() {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.sum(1);
}

int64_t Stats::Metric::GetSumLastMinute() {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.sum(0);
}

int64_t Stats::Metric::GetAverageTotal() {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.avg(1);
}

int64_t Stats::Metric::GetAverageLastMinute() {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.avg(0);
}

int64_t Stats::Metric::GetCountTotal() {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.count(1);
}

int64_t Stats::Metric::GetCountLastMinute() {
  lock_guard<mutex> l(hist_wrapper_->m);
  hist_wrapper_->ts_histogram.update(GetTimeSinceEpochSeconds());
  return hist_wrapper_->ts_histogram.count(0);
}

unique_ptr<Stats::Metric> Stats::GetMetric(const uint32_t metric) {
  if (UNLIKELY(metric >= histograms_.size())) {
    return nullptr;
  }

  return folly::make_unique<Stats::Metric>(histograms_[metric].get());
}

unique_ptr<Stats::Metric> Stats::GetMetric(const string& metric) {
  lock_guard<mutex> g(lock_histogram_map_);
  auto it = histogram_map_.find(metric);
  if (UNLIKELY(it == histogram_map_.end())) {
    return nullptr;
  }

  return folly::make_unique<Stats::Metric>(it->second.get());
}

unique_ptr<Stats::Counter> Stats::GetCounter(const uint32_t counter) {
  if (UNLIKELY(counter >= timeseries_.size())) {
    return nullptr;
  }

  return folly::make_unique<Stats::Counter>(timeseries_[counter].get());
}

unique_ptr<Stats::Counter> Stats::GetCounter(const string& counter) {
  lock_guard<mutex> g(lock_timeseries_map_);
  auto it = timeseries_map_.find(counter);
  if (UNLIKELY(it == timeseries_map_.end())) {
    return nullptr;
  }

  return folly::make_unique<Stats::Counter>(it->second.get());
}

namespace {
/**
 * Total names have __TOTAL appended to the stats name.
 * Given "stats", we want "stats__TOTAL"
 *
 * This is a bit trickier with tags however as the RAW_NAME will be of the form:
 * "stats key1=value1 key2=value2"
 *
 * For this input we will want the output:
 * "stats__TOTAL key1=value1 key2=value2"
 *
 * To do this, we assume that the stats name will always come first before
 * tags.
 */
string GetTotalName(const string& raw_name) {
  vector<string> raw_name_parts;
  folly::split(" ", raw_name, raw_name_parts);
  raw_name_parts.at(0) += "__TOTAL";
  return folly::join(" ", raw_name_parts);
}
}  // namespace

string Stats::DumpStatsAsText() {
  stringstream output;
  output << "gauges:\n";
  output << "labels:\n";
  output << "metrics:\n";
  auto metric_logger =
      [&output](TimeseriesHistogramWrapper * thw, const string & metric_name) {
    Metric metric(thw);
    output << boost::format("  %1%: (average=%2%, count=%3%, maximum=%4%, "
                            "minimum=%5%, p50=%6%, p90=%7%, p99=%8%, "
                            "p999=%9%, p9999=%10%, sum=%11%)\n") % metric_name %
                  metric.GetAverageLastMinute() % metric.GetCountLastMinute() %
                  metric.GetPercentileLastMinute(100) %
                  metric.GetPercentileLastMinute(0) %
                  metric.GetPercentileLastMinute(50) %
                  metric.GetPercentileLastMinute(90) %
                  metric.GetPercentileLastMinute(99) %
                  metric.GetPercentileLastMinute(99.9) %
                  metric.GetPercentileLastMinute(99.99) %
                  metric.GetSumLastMinute();
    output
        << boost::format("  %1%: (average=%2%, count=%3%, maximum=%4%, "
                         "minimum=%5%, p50=%6%, p90=%7%, p99=%8%, "
                         "p999=%9%, p9999=%10%, sum=%11%)\n") %
               GetTotalName(metric_name) %
               metric.GetAverageTotal() % metric.GetCountTotal() %
               metric.GetPercentileTotal(100) % metric.GetPercentileTotal(0) %
               metric.GetPercentileTotal(50) % metric.GetPercentileTotal(90) %
               metric.GetPercentileTotal(99) % metric.GetPercentileTotal(99.9) %
               metric.GetPercentileTotal(99.99) % metric.GetSumTotal();
  };

  for (uint32_t i = 0; i < histograms_.size(); ++i) {
    metric_logger(histograms_[i].get(), (*metric_names_.load())[i]);
  }

  {
    lock_guard<mutex> g(lock_histogram_map_);
    for (auto& histogram : histogram_map_) {
      metric_logger(histogram.second.get(), histogram.first);
    }
  }

  output << "counters:\n";
  auto counter_logger = [&output](MultiLevelTimeSeriesWrapper * mltsw,
                                  const string & counter_name) {
    Counter counter(mltsw);
    output << boost::format("  %1%: %2%\n") % counter_name %
                  counter.GetLastMinute();
    output << boost::format("  %1%: %2%\n") % GetTotalName(counter_name) %
                  counter.GetTotal();
  };

  for (uint32_t i = 0; i < timeseries_.size(); ++i) {
    counter_logger(timeseries_[i].get(), (*counter_names_.load())[i]);
  }

  {
    lock_guard<mutex> g(lock_timeseries_map_);
    for (auto& timeseries : timeseries_map_) {
      counter_logger(timeseries.second.get(), timeseries.first);
    }
  }

  return output.str();
}
}  // namespace common

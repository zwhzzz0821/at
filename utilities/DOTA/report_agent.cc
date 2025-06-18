//
// Created by jinghuan on 5/24/21.
//

#include "rocksdb/utilities/report_agent.h"

#include <cassert>
#include <mutex>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/DOTA_tuner.h"
#include "rocksdb/utilities/TetrisTuner.h"
#include "rocksdb/utilities/zipfian_predictor.h"
#include "trace_replay/block_cache_tracer.h"

namespace ROCKSDB_NAMESPACE {
ReporterAgent::~ReporterAgent() {
  {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    stop_cv_.notify_all();
  }
  reporting_thread_.join();
}
void ReporterAgent::InsertNewTuningPoints(ChangePoint point) {
  std::cout << "can't use change point @ " << point.change_timing
            << " Due to using default reporter" << std::endl;
};
Status update_db_options(
    DBImpl* running_db_,
    std::unordered_map<std::string, std::string>* new_db_options,
    bool* applying_changes, Env* /*env*/) {
  *applying_changes = true;
  Status s = running_db_->SetDBOptions(*new_db_options);
  free(new_db_options);
  *applying_changes = false;
  return s;
}

Status update_cf_options(
    DBImpl* running_db_,
    std::unordered_map<std::string, std::string>* new_cf_options,
    bool* applying_changes, Env* /*env*/) {
  *applying_changes = true;
  Status s = running_db_->SetOptions(*new_cf_options);
  free(new_cf_options);
  *applying_changes = false;
  return s;
}
void ReporterAgent::DetectAndTuning(int /*secs_elapsed*/) {}
Status ReporterAgent::ReportLine(int secs_elapsed,
                                 int total_ops_done_snapshot) {
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_);
  auto s = report_file_->Append(report);
  return s;
}

void print_hex(const std::string& s) {
  for (unsigned char c : s) {
    printf("%02x ", c);  // 每个字节以两位十六进制形式输出
  }
  printf("\n");
}

void ReporterAgent::UpdateSeqScore(Slice* key, Slice* value) {
  if (key == nullptr) {
    return;  // no key, do nothing
  }
  if (seq_ascending_ == 0 && !last_key_.empty()) {
    if (key->compare(last_key_) < 0) {
      seq_ascending_ = 2;  // descending
    } else if (key->compare(last_key_) > 0) {
      seq_ascending_ = 1;  // ascending
    } else {
      key_num_in_seq_++;  // same key, just increase the count
    }
    sequnencial_key_num_++;
  } else {
    if (seq_ascending_ == 1) {
      if (key->compare(last_key_) < 0) {
        seq_ascending_ = 2;
      } else {
        sequnencial_key_num_++;
      }
    } else if (seq_ascending_ == 2) {
      if (key->compare(last_key_) > 0) {
        seq_ascending_ = 1;  // reset
      } else {
        sequnencial_key_num_++;
      }
    }
  }
  key_num_in_seq_++;
  last_key_ = key->ToString();
  current_metrics_.seq_score_ = static_cast<double>(sequnencial_key_num_) /
                                static_cast<double>(key_num_in_seq_);
}

void ReporterAgent::UpdateRwRatioScore(OperationType op_type, Slice* key,
                                       Slice* value) {
  if (key == nullptr) {
    return;  // no key or value, do nothing
  }
  if (op_type == kWrite || op_type == kUpdate || op_type == kDelete ||
      op_type == kMerge) {
    if (value == nullptr) {  // delete opt
      write_opt_size_sum_ += key->size();
    } else {
      write_opt_size_sum_ += key->size() + value->size();
    }
  } else if (op_type == kRead || op_type == kSeek) {
    // read option
    read_opt_size_sum_ += key->size() + value->size();
  } else {
    // other option, do nothing
  }

  if (write_opt_size_sum_ == 0) {
    current_metrics_.rw_ratio_score_ =
        std::numeric_limits<double>::max();  // means only read
  } else {
    current_metrics_.rw_ratio_score_ =
        static_cast<double>(read_opt_size_sum_) / write_opt_size_sum_;
  }
}

void ReporterAgent::UpdateDistributionScore(Slice* key, Slice* value) {
  // TODO
  std::string string_key = key->ToString();
  if (key_distribution_map_.size() > 1000) {
    key_distribution_map_.clear();
  }
  key_distribution_map_[string_key]++;
}

void ReporterAgent::AccessOpLatency(uint64_t latency, OperationType op_type) {
  mutex_.lock();
  if (op_type == kRead || op_type == kSeek || op_type == kWrite ||
      op_type == kUpdate || op_type == kDelete) {
    if (op_latency_list_.size() < window_size_) {
      op_latency_list_.emplace_back(latency);
    } else {
      op_latency_list_.erase(op_latency_list_.begin());
      op_latency_list_.emplace_back(latency);
    }
  }
  mutex_.unlock();
}

LatencySpike ReporterTetris::DetectLatencySpike() {
  mutex_.lock();
  if (op_latency_list_.size() < window_size_) {
    mutex_.unlock();
    return kNoSpike;
  }
  auto avg_latency =
      std::accumulate(op_latency_list_.begin(), op_latency_list_.end(), 0.0) /
      op_latency_list_.size();
  auto std_latency = std::sqrt(
      std::accumulate(op_latency_list_.begin(), op_latency_list_.end(), 0.0,
                      [avg_latency](double sum, uint64_t x) {
                        return sum + (x - avg_latency) * (x - avg_latency);
                      }) /
      op_latency_list_.size());
  const auto small_spike_threshold =
      avg_latency + k_small_multiplier_ * std_latency;
  const auto big_spike_threshold =
      avg_latency + k_big_multiplier_ * std_latency;
  if (op_latency_list_.back() > small_spike_threshold &&
      op_latency_list_.back() < kMicrosInSecond /*<= 1s*/) {
    op_latency_list_.back() = kSmallSpike;
    mutex_.unlock();
    return kSmallSpike;
  } else if (op_latency_list_.back() >= big_spike_threshold &&
             op_latency_list_.back() >= kMicrosInSecond) {
    op_latency_list_.back() = kBigSpike;
    mutex_.unlock();
    return kBigSpike;
  }
  mutex_.unlock();
  return kNoSpike;
}

void ReporterTetris::AutoTune() {
  LatencySpike latency_spike = DetectLatencySpike();
  // TODO: auto tune the system
  std::vector<ChangePoint> change_points;
  // change memtable size according to the seq score
  if (last_latency_spike_ != kNoSpike ||
      (last_latency_spike_ == kNoSpike &&
       latency_spike != last_latency_spike_)) {
    tuner_->AutoTuneByMetric(current_metrics_, change_points, latency_spike);
    if (!change_points.empty()) {
      last_latency_spike_ = latency_spike;
    }
  }
  // test not change option
  for (const auto& point : change_points) {
    // 将输出改为写入文件
    std::string log_line = "change point: " + point.ToString() + "\n";
    if (tune_log_file_ != nullptr) {
      tune_log_file_->Append(log_line);
      tune_log_file_->Flush();
    }
  }
  ApplyChangePointsInstantly(&change_points);
}

void ReporterTetris::ApplyChangePointsInstantly(
    std::vector<ChangePoint>* points) {
  std::unordered_map<std::string, std::string>* new_cf_options;
  std::unordered_map<std::string, std::string>* new_db_options;

  new_cf_options = new std::unordered_map<std::string, std::string>();
  new_db_options = new std::unordered_map<std::string, std::string>();
  if (points->empty()) {
    return;
  }

  for (auto point : *points) {
    if (point.db_width) {
      new_db_options->emplace(point.opt, point.value);
    } else {
      new_cf_options->emplace(point.opt, point.value);
    }
  }
  points->clear();
  Status s;
  if (!new_db_options->empty()) {
    //    std::thread t();
    std::thread t(update_db_options, db_ptr, new_db_options, &applying_changes,
                  env_);
    t.detach();
  }
  if (!new_cf_options->empty()) {
    std::thread t(update_cf_options, db_ptr, new_cf_options, &applying_changes,
                  env_);
    t.detach();
  }
}

const TetrisMetrics& ReporterAgent::GetMetrics() const {
  return current_metrics_;
}
void ReporterAgent::UpdateMetric(int secs_elapsed) {
  assert(false);
  // No metrics to update in this reporter
}
void ReporterAgentWithTuning::DetectChangesPoints(int sec_elapsed) {
  std::vector<ChangePoint> change_points;
  if (applying_changes) {
    return;
  }
  tuner->DetectTuningOperations(sec_elapsed, &change_points);
  ApplyChangePointsInstantly(&change_points);
}

void ReporterAgentWithTuning::DetectAndTuning(int secs_elapsed) {
  if (secs_elapsed % tuning_gap_secs_ == 0) {
    DetectChangesPoints(secs_elapsed);
    //    this->running_db_->immutable_db_options().job_stats->clear();
    last_metrics_collect_secs = secs_elapsed;
  }
  if (tuning_points.empty() ||
      tuning_points.front().change_timing < secs_elapsed) {
    return;
  } else {
    PopChangePoints(secs_elapsed);
  }
}

Status ReporterAgentWithTuning::ReportLine(int secs_elapsed,
                                           int total_ops_done_snapshot) {
  auto opt = this->running_db_->GetOptions();

  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) +
                       "," + std::to_string(opt.write_buffer_size >> 20) + "," +
                       std::to_string(opt.max_background_jobs);
  auto s = report_file_->Append(report);
  return s;
}
void ReporterAgentWithTuning::UseFEATTuner(bool TEA_enable, bool FEA_enable) {
  tuner.release();
  tuner.reset(new FEAT_Tuner(options_when_boost, running_db_, &last_report_,
                             &total_ops_done_, env_, tuning_gap_secs_,
                             TEA_enable, FEA_enable));
};

Status SILK_pause_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->PauseBackgroundWork();
  *stopped = true;
  return s;
}

Status SILK_resume_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->ContinueBackgroundWork();
  *stopped = false;
  return s;
}

void ReporterAgentWithTuning::ApplyChangePointsInstantly(
    std::vector<ChangePoint>* points) {
  std::unordered_map<std::string, std::string>* new_cf_options;
  std::unordered_map<std::string, std::string>* new_db_options;

  new_cf_options = new std::unordered_map<std::string, std::string>();
  new_db_options = new std::unordered_map<std::string, std::string>();
  if (points->empty()) {
    return;
  }

  for (auto point : *points) {
    if (point.db_width) {
      new_db_options->emplace(point.opt, point.value);
    } else {
      new_cf_options->emplace(point.opt, point.value);
    }
  }
  points->clear();
  Status s;
  if (!new_db_options->empty()) {
    //    std::thread t();
    std::thread t(update_db_options, running_db_, new_db_options,
                  &applying_changes, env_);
    t.detach();
  }
  if (!new_cf_options->empty()) {
    std::thread t(update_cf_options, running_db_, new_cf_options,
                  &applying_changes, env_);
    t.detach();
  }
}

ReporterAgentWithTuning::ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                                                 const std::string& fname,
                                                 uint64_t report_interval_secs,
                                                 uint64_t dota_tuning_gap_secs)
    : ReporterAgent(env, fname, report_interval_secs, DOTAHeader()),
      options_when_boost(running_db->GetOptions()) {
  tuning_points = std::vector<ChangePoint>();
  tuning_points.clear();
  std::cout << "using reporter agent with change points." << std::endl;
  if (running_db == nullptr) {
    std::cout << "Missing parameter db_ to apply changes" << std::endl;
    abort();
  } else {
    running_db_ = running_db;
  }
  this->tuning_gap_secs_ = std::max(dota_tuning_gap_secs, report_interval_secs);
  this->last_metrics_collect_secs = 0;
  this->last_compaction_thread_len = 0;
  this->last_flush_thread_len = 0;
  tuner.reset(new DOTA_Tuner(options_when_boost, running_db_, &last_report_,
                             &total_ops_done_, env_, tuning_gap_secs_));
  tuner->ResetTuner();
  this->applying_changes = false;
}

inline double average(std::vector<double>& v) {
  assert(!v.empty());
  return accumulate(v.begin(), v.end(), 0.0) / v.size();
}

void ReporterAgentWithTuning::PopChangePoints(int secs_elapsed) {
  std::vector<ChangePoint> valid_point;
  for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
    if (it->change_timing <= secs_elapsed) {
      if (running_db_ != nullptr) {
        valid_point.push_back(*it);
      }
      tuning_points.erase(it--);
    }
  }
  ApplyChangePointsInstantly(&valid_point);
}

void ReporterWithMoreDetails::DetectAndTuning(int secs_elapsed) {
  //  report_file_->Append(",");
  //  ReportLine(secs_elapsed, total_ops_done_);
  secs_elapsed++;
  assert(secs_elapsed > -1e9);
}

Status ReporterAgentWithSILK::ReportLine(int secs_elapsed,
                                         int total_ops_done_snapshot) {
  // copy from SILK https://github.com/theoanab/SILK-USENIXATC2019
  // //check the current bandwidth for user operations
  long cur_throughput = (total_ops_done_snapshot - last_report_);
  long cur_bandwidth_user_ops_MBPS =
      cur_throughput * FLAGS_value_size / 1000000;

  // SILK TESTING the Pause compaction work functionality
  if (!pausedcompaction &&
      cur_bandwidth_user_ops_MBPS > FLAGS_SILK_bandwidth_limitation * 0.75) {
    // SILK Consider this a load peak
    //    running_db_->PauseCompactionWork();
    //    pausedcompaction = true;
    pausedcompaction = true;
    std::thread t(SILK_pause_compaction, running_db_, &pausedcompaction);
    t.detach();

  } else if (pausedcompaction && cur_bandwidth_user_ops_MBPS <=
                                     FLAGS_SILK_bandwidth_limitation * 0.75) {
    std::thread t(SILK_resume_compaction, running_db_, &pausedcompaction);
    t.detach();
  }

  long cur_bandiwdth_compaction_MBPS =
      FLAGS_SILK_bandwidth_limitation -
      cur_bandwidth_user_ops_MBPS;  // measured 200MB/s SSD bandwidth on XEON.
  if (cur_bandiwdth_compaction_MBPS < 10) {
    cur_bandiwdth_compaction_MBPS = 10;
  }
  if (abs(prev_bandwidth_compaction_MBPS - cur_bandiwdth_compaction_MBPS) >=
      10) {
    auto opt = running_db_->GetOptions();
    opt.rate_limiter.get()->SetBytesPerSecond(cur_bandiwdth_compaction_MBPS *
                                              1000 * 1000);
    prev_bandwidth_compaction_MBPS = cur_bandiwdth_compaction_MBPS;
  }
  // Adjust the tuner from SILK before reporting
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) +
                       "," + std::to_string(cur_bandwidth_user_ops_MBPS);
  auto s = report_file_->Append(report);
  return s;
}

ReporterAgentWithSILK::ReporterAgentWithSILK(DBImpl* running_db, Env* env,
                                             const std::string& fname,
                                             uint64_t report_interval_secs,
                                             int32_t value_size,
                                             int32_t bandwidth_limitation)
    : ReporterAgent(env, fname, report_interval_secs) {
  std::cout << "SILK enabled, Disk bandwidth has been set to: "
            << bandwidth_limitation << std::endl;
  running_db_ = running_db;
  this->FLAGS_value_size = value_size;
  this->FLAGS_SILK_bandwidth_limitation = bandwidth_limitation;
}

Status ReporterTetris::ReportLine(int secs_elapsed,
                                  int total_ops_done_snapshot) {
  auto opt = this->db_ptr->GetOptions();

  // //TODO: append all metrics to the report file
  // std::string report = std::to_string(secs_elapsed) + "," +
  //                      std::to_string(total_ops_done_snapshot - last_report_)
  //                      + "," + std::to_string(opt.write_buffer_size >> 20) +
  //                      "," + std::to_string(opt.max_background_jobs);
  // auto s = report_file_->Append(report);
  return Status::OK();
}

};  // namespace ROCKSDB_NAMESPACE

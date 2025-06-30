//
// create by yukimi on 2025/06/16
//
#ifndef TETRIS_TUNER_H
#define TETRIS_TUNER_H

#include <cstdint>
#include <iomanip>
#include <mutex>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/DOTA_tuner.h"

namespace ROCKSDB_NAMESPACE {

static std::string CastTimeStampToDateString(uint64_t time_stamp) {
  std::time_t t = static_cast<std::time_t>(time_stamp / 1000000);
  std::tm tm_time;

#if defined(_WIN32) || defined(_WIN64)
  localtime_s(&tm_time, &t);
#else
  localtime_r(&t, &tm_time);
#endif

  std::ostringstream oss;
  oss << std::put_time(&tm_time, "%Y_%m_%d_%H:%M:%S");
  return oss.str();
}

enum LatencySpike : uint8_t { kNoSpike = 0, kSmallSpike, kBigSpike };

struct TetrisMetrics {
  double throughput_;        // MB/s
  double p99_read_latency_;  // ms
  double p999_read_latency_;
  double p99_write_latency_;  // ms
  double p999_write_latency_;
  double write_amplification_;
  double read_write_ratio_;
  double io_intensity_;
  uint64_t compaction_granularity_;
  double key_value_size_distribution_;  // not implemented yet
  double cpu_usage_;                    // %
  double mem_usage_;                    // %
  double memtable_size_;                // MB
  uint64_t bloom_filter_size_;          // MB
  double seq_score_ = 0;                // sequential score
  double rw_ratio_score_ = 0;           // read write ratio score
  double zipfian_score_;                // distribution score
  uint64_t update_time_ = 0;
  std::string ToString() {
    return "TetrisMetrics: update_time=" + std::to_string(update_time_) + "," +
           "throughput=" + std::to_string(throughput_) + "," +
           "p99_read_latency=" + std::to_string(p99_read_latency_) + "," +
           "p999_read_latency=" + std::to_string(p999_read_latency_) + "," +
           "p99_write_latency=" + std::to_string(p99_write_latency_) + "," +
           "p999_write_latency=" + std::to_string(p999_write_latency_) + "," +
           "write_amplification=" + std::to_string(write_amplification_) + "," +
           "read_write_ratio=" + std::to_string(read_write_ratio_) + "," +
           "io_intensity=" + std::to_string(io_intensity_) + "," +
           "compaction_granularity=" + std::to_string(compaction_granularity_) +
           "," + "key_value_size_distribution=" +
           std::to_string(key_value_size_distribution_) + "," +
           "cpu_usage=" + std::to_string(cpu_usage_) + "," +
           "mem_usage=" + std::to_string(mem_usage_) + "," +
           "memtable_size=" + std::to_string(memtable_size_) + "," +
           "bloom_filter_size=" + std::to_string(bloom_filter_size_) + "," +
           "seq_score=" + std::to_string(seq_score_) + "," +
           "rw_ratio_score=" + std::to_string(rw_ratio_score_) + "," +
           "zipfian_predictor_result=" + std::to_string(zipfian_score_);
  }
};
class TetrisTuner {
 public:
  TetrisTuner(DBImpl* db_ptr, Env* env, uint64_t create_time)
      : db_ptr_(db_ptr), env_(env) {
    current_opt_ = db_ptr->GetOptions();
    last_tune_time_ = env_->NowMicros();
    std::string fname =
        "tune_option_" + CastTimeStampToDateString(create_time) + ".log";
    Status s = env_->NewWritableFile(fname, &tune_log_file_, EnvOptions());
    if (!s.ok()) {
      std::cout << "打开tune_option.log失败: " << s.ToString() << std::endl;
    }
  }
  TetrisTuner() = delete;
  ~TetrisTuner() = default;
  void AutoTuneByMetric(TetrisMetrics current_metric,
                        std::vector<ChangePoint>& change_points,
                        LatencySpike& latency_spike);

 private:
  void ReportTuneLine(LatencySpike latency_spike,
                      std::vector<ChangePoint>& change_points);
  void UpdateCurrentOptions();
  void TuneWhenSmallSpike(const TetrisMetrics& current_metric,
                          std::vector<ChangePoint>& change_points);
  void TuneWhenBigSpike(const TetrisMetrics& current_metric,
                        std::vector<ChangePoint>& change_points);
  void ResetToDefault(std::vector<ChangePoint>& change_points);
  void TuneWriteBufferSize(const std::string& target_value,
                           std::vector<ChangePoint>& change_points);
  void TuneMaxBufferNumber(const std::string& target_value,
                           std::vector<ChangePoint>& change_points);
  void TuneLevel0FileNumCompactionTrigger(
      const std::string& target_value, std::vector<ChangePoint>& change_points);
  void TuneMaxBackgroundJobs(const std::string& target_value,
                             std::vector<ChangePoint>& change_points);
  void TuneCacheIndexAndFilterBlocks(const std::string& target_value,
                                     std::vector<ChangePoint>& change_points);
  void TuneLevel0StopWritesTrigger(const std::string& target_value,
                                   std::vector<ChangePoint>& change_points);
  void TuneLevel0SlowDownWritesTrigger(const std::string& target_value,
                                       std::vector<ChangePoint>& change_points);
  void TuneFileNumCompactionTrigger(const std::string& target_value,
                                    std::vector<ChangePoint>& change_points);
  void TuneBlockSize(const std::string& target_value,
                     std::vector<ChangePoint>& change_points);
  void TuneMaxBytesForLevelBase(const std::string& target_value,
                                std::vector<ChangePoint>& change_points);
  void TuneCompactionReadaheadSize(const std::string& target_value,
                                   std::vector<ChangePoint>& change_points);
  void TuneMaxBackGroundCompactions(const std::string& target_value,
                                    std::vector<ChangePoint>& change_points);
  DBImpl* db_ptr_;
  Options current_opt_;
  Env* env_;
  Version* version_;
  VersionStorageInfo* vfs_;
  ColumnFamilyData* cfd_;
  uint64_t last_tune_time_ = 0;
  std::unique_ptr<WritableFile> tune_log_file_;  // 调优日志文件
  std::mutex mutex_;                             // 使用普通互斥锁

  static constexpr double kSeqThreshold = 0.7;
  static constexpr double kRandomThreshold = 0.4;
  static constexpr uint64_t max_memtable_size = 1ull << 30;
  static constexpr uint64_t min_memtable_size = 64ull << 20;
  static constexpr double kMemUsageThresholdLower = 60;
  static constexpr double kMemUsageThresholdUpper = 80;
  static constexpr double kReadWriteRatioThreshold = 0.5;
  static constexpr int kMaxWriteBufferNumberUpper = 4;
  static constexpr int kMaxWriteBufferNumberLower = 1;
  static constexpr uint64_t kWriteBufferSizeUpper = 1024 * 1024 * 1024;
  static constexpr uint64_t kWriteBufferSizeLower = 32 * 1024 * 1024;
  static constexpr uint64_t kWriteBufferSizeMinusFactor = 64 * 1024 * 1024;
  static constexpr uint64_t kWriteBufferSizePlusFactor = 64 * 1024 * 1024;
  static constexpr int kMaxBackgroundJobsUpper = 16;
  static constexpr int kMaxBackgroundJobsLower = 1;
  static constexpr int kMaxBackgroundCompactionsUpper = 12;
  static constexpr int kMaxBackgroundCompactionsLower = 1;
  static constexpr uint64_t kCompactionReadaheadSizeUpper = 128 * 1024 * 1024;
  static constexpr uint64_t kCompactionReadaheadSizeLower = 2 * 1024 * 1024;
  static constexpr uint64_t kCompactionGranularityThreshold = 500;
  static constexpr uint64_t kLevel0FileNumCompactionTriggerLower = 2;
  static constexpr uint64_t kLevel0FileNumCompactionTriggerUpper = 10;
  static constexpr double kZipfianThreshold = 0.8;

  // 默认参数值
  static constexpr uint64_t kDefaultWriteBufferSize = 67108864;  // 64MB
  static constexpr int kDefaultMaxBackgroundFlushes = 1;
  static constexpr int kDefaultMaxBackgroundJobs = 2;
  static constexpr int kDefaultMaxBackgroundCompactions = -1;
  static constexpr uint64_t kDefaultCompactionReadaheadSize = 2097152;  // 2MB
  static constexpr int kDefaultLevel0SlowdownWritesTrigger = 20;
  static constexpr int kDefaultLevel0StopWritesTrigger = 36;
  static constexpr int kDefaultLevel0FileNumCompactionTrigger = 4;
  static constexpr int kDefaultMaxWriteBufferNumber = 2;

  static constexpr uint64_t kMicrosInSecond = 10000000;  // 10秒
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // TETRIS_TUNER_H
//
// create by yukimi on 2025/06/16
//
#ifndef TETRIS_TUNER_H
#define TETRIS_TUNER_H

#include <cstdint>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/DOTA_tuner.h"
#include "zipfian_predictor.h"

namespace ROCKSDB_NAMESPACE {
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
  double key_value_size_distribution_;               // not implemented yet
  double cpu_usage_;                                 // %
  double mem_usage_;                                 // %
  double memtable_size_;                             // MB
  uint64_t bloom_filter_size_;                       // MB
  double seq_score_ = 0;                             // sequential score
  double rw_ratio_score_ = 0;                        // read write ratio score
  ZipfianPredictionResult zipfian_predictor_result;  // distribution score
  uint64_t update_time_ = 0;
  std::string ToString() {
    return "TetrisMetrics: update_time=" + std::to_string(update_time_) + "\n" +
           "throughput=" + std::to_string(throughput_) + "\n" +
           "p99_read_latency=" + std::to_string(p99_read_latency_) + "\n" +
           "p999_read_latency=" + std::to_string(p999_read_latency_) + "\n" +
           "p99_write_latency=" + std::to_string(p99_write_latency_) + "\n" +
           "p999_write_latency=" + std::to_string(p999_write_latency_) + "\n" +
           "write_amplification=" + std::to_string(write_amplification_) +
           "\n" + "read_write_ratio=" + std::to_string(read_write_ratio_) +
           "\n" + "io_intensity=" + std::to_string(io_intensity_) + "\n" +
           "compaction_granularity=" + std::to_string(compaction_granularity_) +
           "\n" + "key_value_size_distribution=" +
           std::to_string(key_value_size_distribution_) + "\n" +
           "cpu_usage=" + std::to_string(cpu_usage_) + "\n" +
           "mem_usage=" + std::to_string(mem_usage_) + "\n" +
           "memtable_size=" + std::to_string(memtable_size_) + "\n" +
           "bloom_filter_size=" + std::to_string(bloom_filter_size_) + "\n" +
           "seq_score=" + std::to_string(seq_score_) + "\n" +
           "rw_ratio_score=" + std::to_string(rw_ratio_score_) + "\n" +
           "zipfian_predictor_result=" + zipfian_predictor_result.ToString() +
           "\n";
  }
};
class TetrisTuner {
 public:
  TetrisTuner(DBImpl* db_ptr) : db_ptr_(db_ptr) {
    current_opt_ = db_ptr->GetOptions();
  }
  void AutoTuneByMetric(const TetrisMetrics& current_metric,
                        std::vector<ChangePoint>& change_points);

 private:
  void UpdateCurrentOptions();
  void TuneWriteBufferSize(const TetrisMetrics& current_metric,
                           std::vector<ChangePoint>& change_points);
  void TuneMaxBufferNumber(const TetrisMetrics& current_metric,
                           std::vector<ChangePoint>& change_points);
  void TuneLevel0FileNumCompactionTrigger(
      const TetrisMetrics& current_metric,
      std::vector<ChangePoint>& change_points);
  void TuneMaxBackgroundJobs(const TetrisMetrics& current_metric,
                             std::vector<ChangePoint>& change_points);
  void TuneCacheIndexAndFilterBlocks(const TetrisMetrics& current_metric,
                                     std::vector<ChangePoint>& change_points);
  void TuneLevel0StopWritesTrigger(const TetrisMetrics& current_metric,
                                   std::vector<ChangePoint>& change_points);
  void TuneLevel0SlowDownWritesTrigger(const TetrisMetrics& current_metric,
                                       std::vector<ChangePoint>& change_points);
  void TuneFileNumCompactionTrigger(const TetrisMetrics& current_metric,
                                    std::vector<ChangePoint>& change_points);
  void TuneBlockSize(const TetrisMetrics& current_metric,
                     std::vector<ChangePoint>& change_points);
  void TuneMaxBytesForLevelBase(const TetrisMetrics& current_metric,
                                std::vector<ChangePoint>& change_points);
  void TuneCompactionReadaheadSize(const TetrisMetrics& current_metric,
                                   std::vector<ChangePoint>& change_points);
  void TuneMaxBackGroundCompactions(const TetrisMetrics& current_metric,
                                    std::vector<ChangePoint>& change_points);
  DBImpl* db_ptr_;
  Options current_opt_;
  static constexpr uint64_t max_memtable_size = 512ull << 20;
  static constexpr uint64_t min_memtable_size = 64ull << 20;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // TETRIS_TUNER_H
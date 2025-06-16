//
// create by yukimi on 2025/06/16
//
#include <vector>

#include "rocksdb/utilities/DOTA_tuner.h"
#include "rocksdb/utilities/TetrisTuner.h"
namespace ROCKSDB_NAMESPACE {
void TetrisTuner::AutoTuneByMetric(const TetrisMetrics& current_metric,
                                   std::vector<ChangePoint>& change_points) {
  UpdateCurrentOptions();
  TuneWriteBufferSize(current_metric, change_points);
  TuneBlockSize(current_metric, change_points);
  TuneCacheIndexAndFilterBlocks(current_metric, change_points);
  TuneLevel0SlowDownWritesTrigger(current_metric, change_points);
  TuneLevel0StopWritesTrigger(current_metric, change_points);
  TuneLevel0FileNumCompactionTrigger(current_metric, change_points);
  TuneFileNumCompactionTrigger(current_metric, change_points);
  TuneCompactionReadaheadSize(current_metric, change_points);
  TuneMaxBackGroundCompactions(current_metric, change_points);
  TuneMaxBufferNumber(current_metric, change_points);
  TuneMaxBytesForLevelBase(current_metric, change_points);
  TuneMaxBackgroundJobs(current_metric, change_points);
}

void TetrisTuner::UpdateCurrentOptions() {
  current_opt_ = db_ptr_->GetOptions();
}

void TetrisTuner::TuneWriteBufferSize(const TetrisMetrics& current_metric,
                                      std::vector<ChangePoint>& change_points) {
  // TODO:
  ChangePoint cp;
  cp.opt = "write_buffer_size";
  cp.db_width = false;
  uint64_t memtable_size = current_opt_.write_buffer_size;
  if (current_metric.seq_score_ <= 0.4) {
    // random workload
    memtable_size = std::min(memtable_size << 1, max_memtable_size);
  } else if (current_metric.seq_score_ >= 0.7) {
    // sequencial workload
    memtable_size = std::max(memtable_size >> 1, min_memtable_size);
  }
  if (memtable_size != current_opt_.write_buffer_size) {
    change_points.emplace_back(std::move(cp));
  }
}
void TetrisTuner::TuneMaxBufferNumber(const TetrisMetrics& current_metric,
                                      std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneLevel0FileNumCompactionTrigger(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneMaxBackgroundJobs(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneCacheIndexAndFilterBlocks(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneLevel0StopWritesTrigger(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneLevel0SlowDownWritesTrigger(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneFileNumCompactionTrigger(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneBlockSize(const TetrisMetrics& current_metric,
                                std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneMaxBytesForLevelBase(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneCompactionReadaheadSize(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
void TetrisTuner::TuneMaxBackGroundCompactions(
    const TetrisMetrics& current_metric,
    std::vector<ChangePoint>& change_points) {
  // TODO:
}
}  // namespace ROCKSDB_NAMESPACE
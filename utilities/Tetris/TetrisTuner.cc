//
// create by yukimi on 2025/06/16
//
#include "rocksdb/utilities/TetrisTuner.h"

#include <cstdint>
#include <vector>

#include "rocksdb/utilities/DOTA_tuner.h"
#include "trace_replay/block_cache_tracer.h"
namespace ROCKSDB_NAMESPACE {
void TetrisTuner::AutoTuneByMetric(const TetrisMetrics& current_metric,
                                   std::vector<ChangePoint>& change_points,
                                   LatencySpike& latency_spike) {
  uint64_t now_tune_time = env_->NowMicros();
  if (now_tune_time - last_tune_time_ < kMicrosInSecond ||
      latency_spike == LatencySpike::kNoSpike) {
    return;
  }
  last_tune_time_ = now_tune_time;
  UpdateCurrentOptions();
  if (latency_spike == LatencySpike::kSmallSpike) {
    std::cout << "ksmallspike : " << std::endl;
    TuneWhenSmallSpike(current_metric, change_points);
  } else if (latency_spike == LatencySpike::kBigSpike) {
    TuneWhenBigSpike(current_metric, change_points);
  }
}

void TetrisTuner::UpdateCurrentOptions() {
  current_opt_ = db_ptr_->GetOptions();
  version_ =
      db_ptr_->GetVersionSet()->GetColumnFamilySet()->GetDefault()->current();
  vfs_ = version_->storage_info();
  cfd_ = version_->cfd();
}

void TetrisTuner::TuneWhenSmallSpike(const TetrisMetrics& current_metric,
                                     std::vector<ChangePoint>& change_points) {
  if (cfd_->imm()->NumNotFlushed() >= current_opt_.max_write_buffer_number) {
    if (current_metric.read_write_ratio_ < kReadWriteRatioThreshold) {
      uint64_t max_write_buffer_number = current_opt_.max_write_buffer_number;
      uint64_t write_buffer_size = current_opt_.write_buffer_size;
      if (max_write_buffer_number > kMaxWriteBufferNumberLower) {
        max_write_buffer_number--;
        TuneMaxBufferNumber(std::to_string(max_write_buffer_number),
                            change_points);
      }
      if (write_buffer_size > kWriteBufferSizeLower) {
        write_buffer_size =
            std::max(write_buffer_size - kWriteBufferSizeMinusFactor,
                     kWriteBufferSizeLower);
        TuneWriteBufferSize(std::to_string(write_buffer_size), change_points);
      }
      if (current_metric.seq_score_ >= 0.7) {
        // 顺序写入场景，不合并memtable
        // TuneMinWriteBufferNumberToMerge("1", change_points);
      } else {
        // 随机写入场景，合并memtable
        // TuneMinWriteBufferNumberToMerge("2", change_points);
      }

      // 内存压力大且写为主，禁用cache_index_and_filter_blocks
      if (current_opt_.table_factory) {
        // not support dynamic change
        // TuneCacheIndexAndFilterBlocks("false", change_points);
      }
    } else {
      // 内存压力大且读为主，启用cache_index_and_filter_blocks
      if (current_opt_.table_factory) {
        // not support dynamic change
        // TuneCacheIndexAndFilterBlocks("true", change_points);
      }
    }

    // 内存压力大且L0层满
    if (vfs_->NumLevelFiles(0) > current_opt_.level0_slowdown_writes_trigger) {
      // 增加max_background_jobs
      if (current_opt_.max_background_jobs < kMaxBackgroundJobsUpper) {
        TuneMaxBackgroundJobs(
            std::to_string(current_opt_.max_background_jobs + 1),
            change_points);
      }

      // 增加compaction_readahead_size
      uint64_t readahead_size = current_opt_.compaction_readahead_size;
      if (readahead_size < kCompactionReadaheadSizeUpper) {
        readahead_size =
            std::min(readahead_size * 2, kCompactionReadaheadSizeUpper);
        TuneCompactionReadaheadSize(std::to_string(readahead_size),
                                    change_points);
      }
    } else if (vfs_->NumLevelFiles(0) >
               current_opt_.level0_stop_writes_trigger) {
      // 增加max_background_jobs
      if (current_opt_.max_background_jobs < kMaxBackgroundJobsUpper) {
        TuneMaxBackgroundJobs(
            std::to_string(current_opt_.max_background_jobs + 1),
            change_points);
      }

      // 增加max_background_compactions
      if (current_opt_.max_background_compactions <
          kMaxBackgroundCompactionsUpper) {
        TuneMaxBackGroundCompactions(
            std::to_string(current_opt_.max_background_compactions + 1),
            change_points);
      }

      // 增加compaction_readahead_size
      uint64_t readahead_size = current_opt_.compaction_readahead_size;
      if (readahead_size < kCompactionReadaheadSizeUpper) {
        readahead_size =
            std::min(readahead_size * 2, kCompactionReadaheadSizeUpper);
        TuneCompactionReadaheadSize(std::to_string(readahead_size),
                                    change_points);
      }
    }

    // Bloom Filter调整 - 随机读取场景，加强Bloom Filter
    if (current_metric.read_write_ratio_ > 0.5 &&
        current_metric.seq_score_ < 0.4) {
      // not support dynamic change
      // TuneBloomBitsPerKey("10", change_points);

      // 调整block_size
      // not support dynamic change
      // if (current_opt_.table_factory) {
      //   auto* bbto = static_cast<BlockBasedTableOptions*>(
      //       current_opt_.table_factory->GetOptions());
      //   if (bbto) {
      //     size_t block_size = bbto->block_size;
      //     size_t new_block_size =
      //         std::min<size_t>(block_size + 4 * 1024, 64 * 1024);
      //     if (new_block_size != block_size) {
      //       TuneBlockSize(std::to_string(new_block_size), change_points);
      //     }
      //   }
      // }
    } else {
      // not support dynamic change
      // TuneBloomBitsPerKey("5", change_points);
    }
  } else if (cfd_->imm()->NumNotFlushed() >=
             current_opt_.max_write_buffer_number * 2) {
    // 内存压力更大时调整flush线程数
    // not support dynamic change
    // if (current_opt_.max_background_flushes < 4) {
    //   TuneMaxBackgroundFlushes(
    //       std::to_string(current_opt_.max_background_flushes + 1),
    //       change_points);
    // }
  }
  if (current_metric.read_write_ratio_ <= kReadWriteRatioThreshold) {
    if (current_metric.seq_score_ >= kSeqThreshold) {
      TuneLevel0SlowDownWritesTrigger("30", change_points);
      TuneLevel0StopWritesTrigger("50", change_points);
      TuneLevel0FileNumCompactionTrigger("8", change_points);
    } else if (current_metric.seq_score_ <= kRandomThreshold) {
      // 随机写入场景，严格限制L0文件数
      TuneLevel0SlowDownWritesTrigger("16", change_points);
      TuneLevel0StopWritesTrigger("24", change_points);
      TuneLevel0FileNumCompactionTrigger("2", change_points);
    }
  }
}

void TetrisTuner::TuneWhenBigSpike(const TetrisMetrics& current_metric,
                                   std::vector<ChangePoint>& change_points) {
  // 如果有太多compaction造成延迟峰值
  if (vfs_->estimated_compaction_needed_bytes() >
      current_opt_.soft_pending_compaction_bytes_limit) {
    // 增加max_background_jobs
    if (current_opt_.max_background_jobs < kMaxBackgroundJobsUpper) {
      int new_jobs = std::min<int>(kMaxBackgroundJobsUpper,
                                   current_opt_.max_background_jobs * 2);
      TuneMaxBackgroundJobs(std::to_string(new_jobs), change_points);
    }

    // 读多的场景增加预读
    if (current_metric.read_write_ratio_ > 0.5) {
      uint64_t readahead_size = current_opt_.compaction_readahead_size;
      if (readahead_size < kCompactionReadaheadSizeUpper) {
        readahead_size =
            std::min(readahead_size * 2, kCompactionReadaheadSizeUpper);
        TuneCompactionReadaheadSize(std::to_string(readahead_size),
                                    change_points);
      }
    }
  }

  // 如果是突发写入造成延迟峰值 根据write rate判断？
  if (current_metric.p99_write_latency_ > 100) {
    // 减小max_write_buffer_number
    if (current_opt_.max_write_buffer_number > kMaxWriteBufferNumberLower) {
      int new_buffer_num = std::max<int>(
          kMaxWriteBufferNumberLower, current_opt_.max_write_buffer_number / 2);
      TuneMaxBufferNumber(std::to_string(new_buffer_num), change_points);
    }

    // 减小write_buffer_size
    uint64_t write_buffer_size = current_opt_.write_buffer_size;
    if (write_buffer_size > kWriteBufferSizeLower) {
      write_buffer_size =
          std::max(write_buffer_size / 2, kWriteBufferSizeLower);
      TuneWriteBufferSize(std::to_string(write_buffer_size), change_points);
    }

    // 增加flush线程数
    // if (current_opt_.max_background_flushes < 4) {
    //   int new_flushes = std::min(4, current_opt_.max_background_flushes * 2);
    //   TuneMaxBackgroundFlushes(std::to_string(new_flushes), change_points);
    // }
  }
}

void TetrisTuner::TuneWriteBufferSize(const std::string& target_value,
                                      std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.write_buffer_size) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "write_buffer_size";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneMaxBufferNumber(const std::string& target_value,
                                      std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.max_write_buffer_number) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "max_write_buffer_number";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneLevel0FileNumCompactionTrigger(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.level0_file_num_compaction_trigger) ==
      target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "level0_file_num_compaction_trigger";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneMaxBackgroundJobs(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.max_background_jobs) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "max_background_jobs";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneCacheIndexAndFilterBlocks(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  ChangePoint cp;
  cp.opt = "cache_index_and_filter_blocks";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneLevel0StopWritesTrigger(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.level0_stop_writes_trigger) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "level0_stop_writes_trigger";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneLevel0SlowDownWritesTrigger(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.level0_slowdown_writes_trigger) ==
      target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "level0_slowdown_writes_trigger";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneFileNumCompactionTrigger(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.level0_slowdown_writes_trigger) ==
      target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "num_levels";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneBlockSize(const std::string& target_value,
                                std::vector<ChangePoint>& change_points) {
  ChangePoint cp;
  cp.opt = "block_size";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneMaxBytesForLevelBase(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.max_bytes_for_level_base) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "max_bytes_for_level_base";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneCompactionReadaheadSize(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.compaction_readahead_size) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "compaction_readahead_size";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

void TetrisTuner::TuneMaxBackGroundCompactions(
    const std::string& target_value, std::vector<ChangePoint>& change_points) {
  if (std::to_string(current_opt_.max_background_compactions) == target_value) {
    return;
  }
  ChangePoint cp;
  cp.opt = "max_background_compactions";
  cp.db_width = false;
  cp.value = target_value;
  change_points.emplace_back(std::move(cp));
}

}  // namespace ROCKSDB_NAMESPACE
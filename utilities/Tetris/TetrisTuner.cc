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
  std::lock_guard<std::mutex> lock(mutex_);

  uint64_t now_tune_time = env_->NowMicros();
  if (now_tune_time - last_tune_time_ < 10 * kMicrosInSecond) {
    return;
  }
  last_tune_time_ = now_tune_time;

  UpdateCurrentOptions();

  if (latency_spike == LatencySpike::kSmallSpike) {
    TuneWhenSmallSpike(current_metric, change_points);
  } else if (latency_spike == LatencySpike::kBigSpike) {
    TuneWhenBigSpike(current_metric, change_points);
  } else {
    // 没有延迟尖峰，性能稳定时，回到默认值
    ResetToDefault(change_points);
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
  // 内存压力大时限制MemTable数量, 减小memtable
  if (cfd_->imm()->NumNotFlushed() >= current_opt_.max_write_buffer_number &&
      cfd_->imm()->NumNotFlushed() < current_opt_.max_write_buffer_number * 2) {
    // write为主
    if (current_metric.read_write_ratio_ < kReadWriteRatioThreshold) {
      // 减少max_write_buffer_number
      uint64_t max_write_buffer_number = current_opt_.max_write_buffer_number;
      if (max_write_buffer_number > kMaxWriteBufferNumberLower) {
        max_write_buffer_number--;
        TuneMaxBufferNumber(std::to_string(max_write_buffer_number),
                            change_points);
      }
      // 增加write_buffer_size
      uint64_t write_buffer_size = current_opt_.write_buffer_size;
      if (write_buffer_size < kWriteBufferSizeUpper) {
        write_buffer_size =
            std::min(write_buffer_size + kWriteBufferSizePlusFactor,
                     kWriteBufferSizeUpper);
        TuneWriteBufferSize(std::to_string(write_buffer_size), change_points);
      }

      // 顺序写入场景判断
      if (current_metric.seq_score_ >= kSeqThreshold) {
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
      // read为主
      // 减小write_buffer_size
      uint64_t write_buffer_size = current_opt_.write_buffer_size;
      if (write_buffer_size > kWriteBufferSizeLower) {
        write_buffer_size =
            std::max(write_buffer_size - kWriteBufferSizeMinusFactor,
                     kWriteBufferSizeLower);
        TuneWriteBufferSize(std::to_string(write_buffer_size), change_points);
      }

      // 内存压力大且读为主，启用cache_index_and_filter_blocks
      if (current_opt_.table_factory) {
        // not support dynamic change
        // TuneCacheIndexAndFilterBlocks("true", change_points);
      }
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

    // 增加level0_file_num_compaction_trigger
    uint64_t file_num_trigger = current_opt_.level0_file_num_compaction_trigger;
    if (file_num_trigger < kLevel0FileNumCompactionTriggerUpper) {
      file_num_trigger++;
      TuneLevel0FileNumCompactionTrigger(std::to_string(file_num_trigger),
                                         change_points);
    }

    // 内存压力大且L0层满
    if (vfs_->NumLevelFiles(0) > current_opt_.level0_slowdown_writes_trigger &&
        vfs_->NumLevelFiles(0) <= current_opt_.level0_stop_writes_trigger) {
      // 增加max_background_jobs
      if (current_opt_.max_background_jobs < kMaxBackgroundJobsUpper) {
        TuneMaxBackgroundJobs(
            std::to_string(current_opt_.max_background_jobs + 1),
            change_points);
      }
    }
    // 内存压力大且L0层阻塞
    else if (vfs_->NumLevelFiles(0) > current_opt_.level0_stop_writes_trigger) {
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
    }
  } else {
    // 低内存压力
    // L0层满
    if (vfs_->NumLevelFiles(0) > current_opt_.level0_slowdown_writes_trigger &&
        vfs_->NumLevelFiles(0) <= current_opt_.level0_stop_writes_trigger) {
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
    // L0层阻塞
    else if (vfs_->NumLevelFiles(0) > current_opt_.level0_stop_writes_trigger) {
      // 增加max_background_jobs
      if (current_opt_.max_background_jobs < kMaxBackgroundJobsUpper) {
        int new_jobs = std::min<int>(kMaxBackgroundJobsUpper,
                                     current_opt_.max_background_jobs * 2);
        TuneMaxBackgroundJobs(std::to_string(new_jobs), change_points);
      }

      // 增加max_background_compactions
      if (current_opt_.max_background_compactions <
          kMaxBackgroundCompactionsUpper) {
        int new_compactions =
            std::min<int>(kMaxBackgroundCompactionsUpper,
                          current_opt_.max_background_compactions * 2);
        TuneMaxBackGroundCompactions(std::to_string(new_compactions),
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
  }

  // LSM调整，负载模式变化调整
  if (current_metric.read_write_ratio_ <= kReadWriteRatioThreshold) {
    // 顺序写入场景，放宽L0限制，延迟合并
    if (current_metric.seq_score_ >= kSeqThreshold) {
      TuneLevel0SlowDownWritesTrigger("30", change_points);
      TuneLevel0StopWritesTrigger("50", change_points);
      TuneLevel0FileNumCompactionTrigger("8", change_points);
    }
    // 随机写入场景，严格限制L0文件数，更激进合并
    else if (current_metric.seq_score_ <= kRandomThreshold) {
      TuneLevel0SlowDownWritesTrigger("16", change_points);
      TuneLevel0StopWritesTrigger("24", change_points);
      TuneLevel0FileNumCompactionTrigger("2", change_points);
    }
  } else if (current_metric.read_write_ratio_ > kReadWriteRatioThreshold) {
    // 读为主场景
    if (current_metric.seq_score_ <= kRandomThreshold) {
      // 读随机场景
      TuneLevel0SlowDownWritesTrigger("24", change_points);
      TuneLevel0StopWritesTrigger("36", change_points);
      TuneLevel0FileNumCompactionTrigger("4", change_points);

      // 如果L0层满，增加预读大小
      if (vfs_->NumLevelFiles(0) >
          current_opt_.level0_slowdown_writes_trigger) {
        uint64_t readahead_size = current_opt_.compaction_readahead_size;
        if (readahead_size < kCompactionReadaheadSizeUpper) {
          readahead_size =
              std::min(readahead_size * 2, kCompactionReadaheadSizeUpper);
          TuneCompactionReadaheadSize(std::to_string(readahead_size),
                                      change_points);
        }
      }

      // 随机读取场景，加强Bloom Filter
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
      // 顺序读场景
      // not support dynamic change
      // TuneBloomBitsPerKey("5", change_points);
    }
  }
  if (current_metric.read_write_ratio_ < 0.2) {
    TuneMaxBytesForLevelBase("67108864", change_points);
  } else if (current_metric.read_write_ratio_ > 0.2 &&
             current_metric.read_write_ratio_ < 0.8) {
    TuneMaxBytesForLevelBase("1073741824", change_points);
  } else if (current_metric.read_write_ratio_ > 0.8) {
    TuneMaxBytesForLevelBase("17179869184", change_points);
  }
}

void TetrisTuner::TuneWhenBigSpike(const TetrisMetrics& current_metric,
                                   std::vector<ChangePoint>& change_points) {
  // 如果有太多compaction造成延迟峰值
  (void)current_metric;
  if (vfs_->estimated_compaction_needed_bytes() >
      current_opt_.soft_pending_compaction_bytes_limit) {
    // 增加max_background_jobs
    if (current_opt_.max_background_jobs < kMaxBackgroundJobsUpper) {
      int new_jobs = std::min<int>(kMaxBackgroundJobsUpper,
                                   current_opt_.max_background_jobs * 2);
      TuneMaxBackgroundJobs(std::to_string(new_jobs), change_points);
    }

    // 增加compaction_readahead_size
    uint64_t readahead_size = current_opt_.compaction_readahead_size;
    if (readahead_size < kCompactionReadaheadSizeUpper) {
      readahead_size =
          std::min(readahead_size * 2, kCompactionReadaheadSizeUpper);
      TuneCompactionReadaheadSize(std::to_string(readahead_size),
                                  change_points);
    }

    // 增加max_write_buffer_number
    uint64_t max_write_buffer_number = current_opt_.max_write_buffer_number;
    if (max_write_buffer_number < kMaxWriteBufferNumberUpper) {
      max_write_buffer_number =
          std::min(max_write_buffer_number * 2,
                   static_cast<uint64_t>(kMaxWriteBufferNumberUpper));
      TuneMaxBufferNumber(std::to_string(max_write_buffer_number),
                          change_points);
    }
  }

  // 如果是突发写入造成延迟峰值
  // if (current_metric.p99_write_latency_ > 100) {
  //   // 减小max_write_buffer_number
  //   if (current_opt_.max_write_buffer_number > kMaxWriteBufferNumberLower) {
  //     int new_buffer_num = std::max<int>(
  //         kMaxWriteBufferNumberLower, current_opt_.max_write_buffer_number /
  //         2);
  //     TuneMaxBufferNumber(std::to_string(new_buffer_num), change_points);
  //   }

  //   // 减小write_buffer_size
  //   uint64_t write_buffer_size = current_opt_.write_buffer_size;
  //   if (write_buffer_size > kWriteBufferSizeLower) {
  //     write_buffer_size =
  //         std::max(write_buffer_size / 2, kWriteBufferSizeLower);
  //     TuneWriteBufferSize(std::to_string(write_buffer_size), change_points);
  //   }

  //   // 增加flush线程数
  //   // if (current_opt_.max_background_flushes < 4) {
  //   //   int new_flushes = std::min(4, current_opt_.max_background_flushes *
  //   2);
  //   //   TuneMaxBackgroundFlushes(std::to_string(new_flushes),
  //   change_points);
  //   // }

  //   // 增加level0_file_num_compaction_trigger
  //   uint64_t file_num_trigger =
  //   current_opt_.level0_file_num_compaction_trigger; if (file_num_trigger <
  //   kLevel0FileNumCompactionTriggerUpper) {
  //     file_num_trigger =
  //         std::min(file_num_trigger * 2,
  //         kLevel0FileNumCompactionTriggerUpper);
  //     TuneLevel0FileNumCompactionTrigger(std::to_string(file_num_trigger),
  //                                        change_points);
  //   }
  // }
}

void TetrisTuner::ResetToDefault(std::vector<ChangePoint>& change_points) {
  // 没有延迟尖峰，性能稳定时，回到默认值
  if (current_opt_.write_buffer_size != kDefaultWriteBufferSize) {
    TuneWriteBufferSize(std::to_string(kDefaultWriteBufferSize), change_points);
  }

  // max_background_flushes默认值
  // not support dynamic change
  // if (current_opt_.max_background_flushes != kDefaultMaxBackgroundFlushes) {
  //   TuneMaxBackgroundFlushes(std::to_string(kDefaultMaxBackgroundFlushes),
  //                          change_points);
  // }

  // max_background_jobs默认值
  if (current_opt_.max_background_jobs != kDefaultMaxBackgroundJobs) {
    TuneMaxBackgroundJobs(std::to_string(kDefaultMaxBackgroundJobs),
                          change_points);
  }

  // max_background_compactions默认值
  if (current_opt_.max_background_compactions !=
      kDefaultMaxBackgroundCompactions) {
    TuneMaxBackGroundCompactions(
        std::to_string(kDefaultMaxBackgroundCompactions), change_points);
  }

  // compaction_readahead_size默认值
  if (current_opt_.compaction_readahead_size !=
      kDefaultCompactionReadaheadSize) {
    TuneCompactionReadaheadSize(std::to_string(kDefaultCompactionReadaheadSize),
                                change_points);
  }

  // level0_slowdown_writes_trigger默认值
  if (current_opt_.level0_slowdown_writes_trigger !=
      kDefaultLevel0SlowdownWritesTrigger) {
    TuneLevel0SlowDownWritesTrigger(
        std::to_string(kDefaultLevel0SlowdownWritesTrigger), change_points);
  }

  // level0_stop_writes_trigger默认值
  if (current_opt_.level0_stop_writes_trigger !=
      kDefaultLevel0StopWritesTrigger) {
    TuneLevel0StopWritesTrigger(std::to_string(kDefaultLevel0StopWritesTrigger),
                                change_points);
  }

  // level0_file_num_compaction_trigger默认值
  if (current_opt_.level0_file_num_compaction_trigger !=
      kDefaultLevel0FileNumCompactionTrigger) {
    TuneLevel0FileNumCompactionTrigger(
        std::to_string(kDefaultLevel0FileNumCompactionTrigger), change_points);
  }

  // max_write_buffer_number默认值
  if (current_opt_.max_write_buffer_number != kDefaultMaxWriteBufferNumber) {
    TuneMaxBufferNumber(std::to_string(kDefaultMaxWriteBufferNumber),
                        change_points);
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
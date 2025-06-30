//
// Created by jinghuan on 5/24/21.
//

#ifndef ROCKSDB_REPORTER_H
#define ROCKSDB_REPORTER_H
#include <fcntl.h>
#include <sys/types.h>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "monitoring/histogram.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/DOTA_tuner.h"
#include "rocksdb/utilities/TetrisTuner.h"
#include "system_metric_getter.h"
#include "zipfian_predictor.h"
namespace ROCKSDB_NAMESPACE {

typedef std::vector<double> LSM_STATE;
class ReporterAgent;
struct ChangePoint;
class DOTA_Tuner;

struct SystemScores;

class ReporterWithMoreDetails;
class ReporterAgentWithTuning;
class ReporterAgentWithSILK;

enum DistributionType : unsigned char { kFixed = 0, kUniform, kNormal };

enum BenchSeqType : int { kSeq = 0, kRandom = 1, kSeqUnknown = 2 };

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kAllOpLatency,
  kOthers
};

constexpr std::string_view LatencySpikeToString(LatencySpike latency_spike) {
  switch (latency_spike) {
    case kNoSpike:
      return "kNoSpike";
    case kSmallSpike:
      return "kSmallSpike";
    case kBigSpike:
      return "kBigSpike";
    default:
      return "unknown";
  }
}
typedef std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                           std::hash<unsigned char>>* HistogramMapPtr;

class ReporterAgent {
 private:
  std::string header_string_;

  void UpdateSeqScore(Slice* key);
  void UpdateRwRatioScore(OperationType op_type, Slice* key, Slice* value);
  void UpdateDistributionScore(Slice* key);

 public:
  static std::string Header() { return "secs_elapsed,interval_qps"; }

  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs,
                std::string header_string = Header(),
                DBImpl* tmp_db_ptr = nullptr)
      : header_string_(header_string),
        env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    if (tmp_db_ptr != nullptr) {
      db_ptr = reinterpret_cast<DBImpl*>(tmp_db_ptr);
    }
    header_string = ReporterAgent::Header() + ",avg_lantency" +
                    ",interval_write_operation" + ",interval_read_operation" +
                    ",interval_flush_write_bytes" +
                    ",interval_compaction_count" + ",immutables" +
                    ",total_mem_size"
                    ",l0_fils" +
                    ",all_sst_size" + ",live_data_size" + ",pending_bytes";
    creat_time_ = env_->NowMicros();
    std::string tmp_fname =
        "report_" + CastTimeStampToDateString(creat_time_) + ".csv";
    auto s = env_->NewWritableFile(tmp_fname, &report_file_, EnvOptions());
    std::string tmp_fname_2 =
        "tune_time_" + CastTimeStampToDateString(creat_time_) + ".log";
    auto s2 = env_->NewWritableFile(tmp_fname_2, &tune_time_log_, EnvOptions());
    if (!s2.ok()) {
      std::cout << "Failed to open tune_time.log" << std::endl;
    }
    if (s.ok()) {
      s = report_file_->Append(header_string + "\n");
      //      std::cout << "opened report file" << std::endl;
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }
    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }
  virtual ~ReporterAgent();

  // thread safe
  void ReportFinishedOps(int64_t num_ops,
                         OperationType op_type = OperationType::kOthers,
                         Slice* key = nullptr, Slice* value = nullptr) {
    total_ops_done_.fetch_add(num_ops);
    if (key != nullptr) {
      mutex_.lock();
      total_key_num_ += 1;
      UpdateSeqScore(key);
      if (value != nullptr) {
        UpdateRwRatioScore(op_type, key, value);
      }
      UpdateDistributionScore(key);
      mutex_.unlock();
      // update latency for per op
    }
  }
  void AccessOpLatency(uint64_t lantency,
                       OperationType op_type = OperationType::kOthers);
  virtual void InsertNewTuningPoints(ChangePoint point);

 public:
  void SetHist(HistogramMapPtr hist) { hist_ = hist; }

 protected:
  virtual void DetectAndTuning(int secs_elapsed);
  virtual Status ReportLine(int secs_elapsed, int total_ops_done_snapshot);
  virtual const TetrisMetrics& GetMetrics() const;
  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  ROCKSDB_NAMESPACE::port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  uint64_t total_key_num_ = 0;
  std::string last_key_;
  std::unordered_map<std::string, uint64_t> key_distribution_map_;
  std::unique_ptr<WritableFile> tune_time_log_;
  uint64_t sequnencial_key_num_ = 0;  // number of sequential keys
  uint64_t key_num_in_seq_ = 0;
  uint8_t seq_ascending_ =
      0;  // 1 if the last key is ascending, 2 if descending, 0 no order now
  uint64_t read_opt_size_sum_ = 0;
  uint64_t write_opt_size_sum_ = 0;
  std::vector<uint64_t> op_latency_list_;  // sliding window of window_size_ ops
  static constexpr uint64_t window_size_ = 100;
  static constexpr double kSmallSpikeThreshold = 1;
  static constexpr double kBigSpikeThreshold = 2;
  std::condition_variable stop_cv_;
  HistogramMapPtr hist_;
  bool stop_;
  TetrisMetrics current_metrics_;
  uint64_t time_started;
  uint64_t creat_time_;
  int last_interval_read_count_;
  int last_interval_write_count_;
  uint64_t last_compaction_count_;
  static const uint64_t key_num_threshold_ = 1000;
  DBImpl* db_ptr;
  Version* version;
  Options current_opt;
  ColumnFamilyData* cfd;
  VersionStorageInfo* vfs;
  uint64_t read_count_ = 0;
  uint64_t write_count_ = 0;
  void UpdateSystemInfo() {
    assert(db_ptr != nullptr);
    current_opt = db_ptr->GetOptions();
    version =
        db_ptr->GetVersionSet()->GetColumnFamilySet()->GetDefault()->current();
    cfd = version->cfd();
    vfs = version->storage_info();
  }
  void SleepAndReport() {
    time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      //      auto secs_elapsed = env_->NowMicros();
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      DetectAndTuning(secs_elapsed);
      auto s = this->ReportLine(secs_elapsed, total_ops_done_snapshot);
      s = report_file_->Append("\n");
      if (s.ok()) {
        s = report_file_->Flush();
      }

      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }
};

class ReporterAgentWithSILK : public ReporterAgent {
 private:
  bool pausedcompaction = false;
  long prev_bandwidth_compaction_MBPS = 0;
  int FLAGS_value_size = 1000;
  int FLAGS_SILK_bandwidth_limitation = 350;
  DBImpl* running_db_;

 public:
  ReporterAgentWithSILK(DBImpl* running_db, Env* env, const std::string& fname,
                        uint64_t report_interval_secs, int32_t FLAGS_value_size,
                        int32_t bandwidth_limitation);
  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;
  const TetrisMetrics& GetMetrics() const override {
    assert(false);
    return current_metrics_;
  }
};

class ReporterAgentWithTuning : public ReporterAgent {
 private:
  std::vector<ChangePoint> tuning_points;
  DBImpl* running_db_;
  const Options options_when_boost;
  uint64_t last_metrics_collect_secs;
  uint64_t last_compaction_thread_len;
  uint64_t last_flush_thread_len;
  std::map<std::string, void*> string_to_attributes_map;
  std::unique_ptr<DOTA_Tuner> tuner;
  static std::string DOTAHeader() {
    return ReporterAgent::Header() + ",avg_lantency" +
           ",interval_write_operation" + ",interval_read_operation" +
           ",interval_flush_write_bytes" + ",interval_compaction_count" +
           ",immutables" +
           ",total_mem_size"
           ",l0_fils" +
           ",all_sst_size" + ",live_data_size" + ",pending_bytes";
  }
  int tuning_gap_secs_;
  std::map<std::string, std::string> parameter_map;
  std::map<std::string, int> baseline_map;
  const int thread_num_upper_bound = 12;
  const int thread_num_lower_bound = 2;
  bool applying_changes = false;

 public:
  const static unsigned long history_lsm_shape =
      10;  // Recorded history lsm shape, here we record 10 secs
  std::deque<LSM_STATE> shape_list;
  const size_t default_memtable_size = 64 << 20;
  const float threashold = 0.5;
  ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs,
                          uint64_t dota_tuning_gap_secs = 1);
  DOTA_Tuner* GetTuner() { return tuner.get(); }
  void ApplyChangePointsInstantly(std::vector<ChangePoint>* points);

  void DetectChangesPoints(int sec_elapsed,
                           std::unique_ptr<WritableFile>& tune_time_log);

  void PopChangePoints(int secs_elapsed);

  static bool thread_idle_cmp(std::pair<size_t, uint64_t> p1,
                              std::pair<size_t, uint64_t> p2) {
    return p1.second < p2.second;
  }

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;
  void UseFEATTuner(bool TEA_enable, bool FEA_enable);
  //  void print_useless_thing(int secs_elapsed);
  void DetectAndTuning(int secs_elapsed) override;
  enum CongestionStatus {
    kCongestion,
    kReachThreshold,
    kUnderThreshold,
    kIgnore
  };

  struct BatchSizeScore {
    double l0_stall;
    double memtable_stall;
    double pending_bytes_stall;
    double flushing_congestion;
    double read_amp_score;
    std::string ToString() {
      std::stringstream ss;
      ss << "l0 stall: " << l0_stall << " memtable stall: " << memtable_stall
         << " pending bytes: " << pending_bytes_stall
         << " flushing congestion: " << flushing_congestion
         << " reading performance score: " << read_amp_score;
      return ss.str();
    }
    std::string Differences() { return "batch size"; }
  };
  struct ThreadNumScore {
    double l0_stall;
    double memtable_stall;
    double pending_bytes_stall;
    double flushing_congestion;
    double thread_idle;
    std::string ToString() {
      std::stringstream ss;
      ss << "l0 stall: " << l0_stall << " memtable stall: " << memtable_stall
         << " pending bytes: " << pending_bytes_stall
         << " flushing congestion: " << flushing_congestion
         << " thread_idle: " << thread_idle;
      return ss.str();
    }
  };

  TuningOP VoteForThread(ThreadNumScore& scores);
  TuningOP VoteForMemtable(BatchSizeScore& scores);

};  // end ReporterWithTuning
typedef ReporterAgent DOTAAgent;
class ReporterWithMoreDetails : public ReporterAgent {
 private:
  std::string detailed_header() {
    return ReporterAgent::Header() + ",avg_lantency" +
           ",interval_write_operation" + ",interval_read_operation" +
           ",interval_flush_write_bytes" + ",interval_compaction_count" +
           ",immutables" +
           ",total_mem_size"
           ",l0_fils" +
           ",all_sst_size" + ",live_data_size" + ",pending_bytes";
  }

 public:
  ReporterWithMoreDetails(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs)
      : ReporterAgent(env, fname, report_interval_secs, detailed_header()) {
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to record more details" << std::endl;
      abort();

    } else {
      db_ptr = reinterpret_cast<DBImpl*>(running_db);
    }
  }

  void DetectAndTuning(int secs_elapsed) override;

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override {
    UpdateSystemInfo();
    auto read_hist = hist_->find(kRead);
    auto write_hist = hist_->find(kWrite);
    int read_count = 0;
    int write_count = 0;
    if (read_hist != hist_->end()) {
      read_count = read_hist->second->num();
    }
    if (write_hist != hist_->end()) {
      write_count = write_hist->second->num();
    }
    last_interval_read_count_ = read_count - read_count_;
    last_interval_write_count_ = write_count - write_count_;
    read_count_ = read_count;
    write_count_ = write_count;

    int interval_read_operation = last_interval_read_count_;
    int interval_write_operation = last_interval_write_count_;
    int compaction_count =
        static_cast<int>(cfd->internal_stats()->GetCompactionCount());
    int interval_compaction_count = compaction_count - last_compaction_count_;
    uint64_t flush_write_bytes =
        cfd->ioptions()->stats->getTickerCount(FLUSH_WRITE_BYTES);
    double avg_lantency = 0;
    if (hist_->find(kAllOpLatency) != hist_->end()) {
      const auto& hist = hist_->find(kAllOpLatency);
      avg_lantency = hist->second->Percentile(50);
    }
    auto opt = this->db_ptr->GetOptions();

    int l0_files = vfs->NumLevelFiles(0);
    uint64_t total_mem_size = 0;
    //    uint64_t active_mem = 0;
    db_ptr->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
    //    db_ptr->GetIntProperty("rocksdb.cur-size-active-mem-table",
    //    &active_mem);

    uint64_t compaction_pending_bytes =
        vfs->estimated_compaction_needed_bytes();
    uint64_t live_data_size = vfs->EstimateLiveDataSize();
    uint64_t all_sst_size = 0;
    int immutable_memtables = cfd->imm()->NumNotFlushed();
    for (int i = 0; i < vfs->num_levels(); i++) {
      all_sst_size += vfs->NumLevelBytes(i);
    }

    last_compaction_count_ = compaction_count;
    std::string report =
        std::to_string(secs_elapsed) + "," +
        std::to_string(total_ops_done_snapshot - last_report_) + "," +
        std::to_string(avg_lantency) + "," +
        std::to_string(interval_write_operation) + "," +
        std::to_string(interval_read_operation) + "," +
        std::to_string(flush_write_bytes) + "," +
        std::to_string(interval_compaction_count) + "," +
        std::to_string(immutable_memtables) + "," +
        std::to_string(total_mem_size) + "," + std::to_string(l0_files) + "," +
        std::to_string(all_sst_size) + "," + std::to_string(live_data_size) +
        "," + std::to_string(compaction_pending_bytes);
    auto s = report_file_->Append(report);
    return s;
  }

  const TetrisMetrics& GetMetrics() const override {
    assert(false);  // no use
    return current_metrics_;
  }
};

class ReporterTetris : public ReporterAgent {
 private:
  static std::string Tetris_header() {
    return ReporterAgent::Header() + ",avg_lantency" +
           ",interval_write_operation" + ",interval_read_operation" +
           ",interval_flush_write_bytes" + ",interval_compaction_count" +
           ",immutables" +
           ",total_mem_size"
           ",l0_fils" +
           ",all_sst_size" + ",live_data_size" + ",pending_bytes";
  }
  std::unique_ptr<TetrisTuner> tuner_;
  std::unique_ptr<WritableFile> metrics_file_;
  std::unique_ptr<WritableFile> z_score_file_;
  bool enable_tetris_ = false;
  bool applying_changes = false;
  LatencySpike last_latency_spike_ = kNoSpike;

  void ApplyChangePointsInstantly(std::vector<ChangePoint>* points);
  void DetectAndTuning(int secs_elapsed) override {
    uint64_t tune_start_time = env_->NowMicros();
    // This reporter does not support tuning now
    // just update the system info
    UpdateMetric(secs_elapsed);
    // detect lantency spike
    if (enable_tetris_) {
      AutoTune(tune_time_log_, tune_start_time);
    }
    // when test, dont print
    if (metrics_file_ != nullptr) {
      metrics_file_->Append(current_metrics_.ToString() + "\n");
      metrics_file_->Flush();
    }
  }

  double GetReadLantency(double percentile = 50) {
    OperationType op_type = kRead;
    if (hist_ == nullptr) {
      std::cout << "No histogram for read operation" << std::endl;
      return 0.0;
    }
    auto hist = hist_->find(op_type);
    if (hist == hist_->end()) {
      return 0.0;
    }
    auto latency = hist->second->Percentile(percentile);
    if (latency < 0) {
      return 0.0;
    }
    return latency / 1000.0;  // convert to ms
  }
  double GetWriteLantency(double percentile = 50) {
    OperationType op_type = kWrite;
    if (hist_ == nullptr) {
      return 0.0;
    }
    auto hist = hist_->find(op_type);
    if (hist == hist_->end()) {
      std::cout << "No histogram for write operation" << std::endl;
      return 0.0;
    }
    auto latency = hist->second->Percentile(percentile);
    if (latency < 0) {
      std::cout << "No p99 latency for write operation" << std::endl;
      return 0.0;
    }
    return latency / 1000.0;  // convert to ms
  }
  double GetReadWriteRatio() {
    OperationType read_op = kRead;
    OperationType write_op = kWrite;
    if (hist_ == nullptr) {
      return 0.0;
    }
    auto read_hist = hist_->find(read_op);
    auto write_hist = hist_->find(write_op);
    int read_count = 0;
    int write_count = 0;
    if (read_hist != hist_->end()) {
      read_count = read_hist->second->num();
    }
    if (write_hist != hist_->end()) {
      write_count = write_hist->second->num();
    }
    last_interval_read_count_ = read_count - read_count_;
    last_interval_write_count_ = write_count - write_count_;
    read_count_ = read_count;
    write_count_ = write_count;
    if (last_interval_write_count_ + last_interval_read_count_ == 0) {
      return 0.0;  // avoid division by zero
    }
    return static_cast<double>(last_interval_read_count_) /
           (last_interval_write_count_ +
            last_interval_read_count_);  // read/write ratio
  }
  uint64_t GetFilterSize() {
    std::shared_ptr<const TableProperties> tp;
    cfd->current()->GetAggregatedTableProperties(&tp);
    if (tp == nullptr) {
      std::cout << "No table properties found" << std::endl;
      return 0;
    }
    return tp->filter_size;
  }
  uint64_t GetMemtableSize() {
    std::string out;
    db_ptr->GetProperty("rocksdb.cur-size-all-mem-tables", &out);
    return std::stoull(out);  // in bytes
  }
  uint64_t GetCompactionGranularity() {
    if (cfd->ioptions()->stats == nullptr) {
      std::cout << "No stats available for compaction granularity" << std::endl;
      return 0;
    }
    HistogramData data;
    cfd->ioptions()->stats->histogramData(
        Histograms::NUM_FILES_IN_SINGLE_COMPACTION, &data);
    return data.sum;
  }
  double GetThroughtput(int secs_elapsed, int total_ops_done_snapshot) {
    if (secs_elapsed == 0) {
      return total_ops_done_snapshot;
    }
    return static_cast<double>(total_ops_done_snapshot) /
           secs_elapsed;  // ops/sec
  }
  /*
   * Detect latency spike
   * return true if latency spike is detected
   * return false if latency spike is not detected
   */
  LatencySpike DetectLatencySpike();
  /*
   * Auto tune the system
   */
  void AutoTune(std::unique_ptr<WritableFile>& tune_time_log,
                uint64_t start_tune_time);

 public:
  ReporterTetris(DBImpl* running_db, Env* env, const std::string& fname,
                 uint64_t report_interval_secs, bool enable_tetris)
      : ReporterAgent(env, fname, report_interval_secs, Tetris_header()) {
    this->enable_tetris_ = enable_tetris;
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to record more details" << std::endl;
      abort();

    } else {
      db_ptr = reinterpret_cast<DBImpl*>(running_db);
      std::string tmp_fname =
          "metrics_" + CastTimeStampToDateString(creat_time_) + ".log";
      Status s = env_->NewWritableFile(tmp_fname, &metrics_file_, EnvOptions());
      if (!s.ok()) {
        std::cout << "打开metrics.log失败: " << s.ToString() << std::endl;
      }
      tmp_fname = "z_score_" + CastTimeStampToDateString(creat_time_) + ".log";
      s = env_->NewWritableFile(tmp_fname, &z_score_file_, EnvOptions());
      if (!s.ok()) {
        std::cout << "打开z_score.log失败: " << s.ToString() << std::endl;
      }
      tuner_ = std::make_unique<TetrisTuner>(running_db, env, creat_time_);
    }
  }
  ~ReporterTetris() = default;
  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;

  const TetrisMetrics& GetMetrics() const override { return current_metrics_; }

  void UpdateMetric(int secs_elapsed) {
    UpdateSystemInfo();
    TetrisMetrics metrics;
    metrics.update_time_ = env_->NowMicros() - time_started;
    // throughput
    metrics.throughput_ = GetThroughtput(secs_elapsed, total_ops_done_.load());
    // P99 latency
    metrics.p99_read_latency_ = GetReadLantency(99);
    metrics.p99_write_latency_ = GetWriteLantency(99);
    // P99.9 latency
    metrics.p999_read_latency_ = GetReadLantency(99.9);
    metrics.p999_write_latency_ = GetWriteLantency(99.9);
    // write amplification
    metrics.write_amplification_ =
        cfd->internal_stats()->GetWriteAmplification();
    // read/write ratio
    metrics.read_write_ratio_ = GetReadWriteRatio();
    // IO intensity
    metrics.io_intensity_ = cfd->internal_stats()->GetIOIntensity();
    // compaction granularity
    metrics.compaction_granularity_ = GetCompactionGranularity();
    // key-value size distribution TODO
    read_opt_size_sum_ = 0;   // reset read op
    write_opt_size_sum_ = 0;  // reset write op
    // CPU usage 从proc中获取
    metrics.cpu_usage_ = getCpuUsage();
    // MEM usage 从proc中获取
    metrics.mem_usage_ = getMemoryUsage();
    // memtable size getProperty
    metrics.memtable_size_ =
        static_cast<double>(GetMemtableSize()) / (1024 * 1024);  // MB
    // bloom filter size
    metrics.bloom_filter_size_ = GetFilterSize();
    // seq score
    metrics.seq_score_ = current_metrics_.seq_score_;
    // rw ratio
    metrics.rw_ratio_score_ = current_metrics_.read_write_ratio_;
    // zipfian
    mutex_.lock();
    metrics.zipfian_score_ = PredictZipfian(key_distribution_map_);
    mutex_.unlock();
    // finished
    current_metrics_ = std::move(metrics);
  }
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_REPORTER_H

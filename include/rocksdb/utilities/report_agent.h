//
// Created by jinghuan on 5/24/21.
//

#ifndef ROCKSDB_REPORTER_H
#define ROCKSDB_REPORTER_H
#include <fcntl.h>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "monitoring/histogram.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/utilities/DOTA_tuner.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "system_metric_getter.h"
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

struct DistributionScore {
  double zipfian_score_ = 0;  // Zipfian distribution score
  double slope_ = 0;
  double r_squared_ = 0;  // R-squared value of the linear regression
  double expect_slope_ = 0;
}

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
  double distribution_score_ = 0;       // distribution score

  std::string ToString() {
    return "TetrisMetrics: throughput=" + std::to_string(throughput_) + "\n" +
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
           "distribution_score=" + std::to_string(distribution_score_) + "\n";
  }
};

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
  kOthers
};

typedef std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                           std::hash<unsigned char>>* HistogramMapPtr;

class ReporterAgent {
 private:
  std::string header_string_;

  void UpdateSeqScore(Slice* key, Slice* value);
  void UpdateRwRatioScore(OperationType op_type, Slice* key, Slice* value);
  void UpdateDistributionScore(Slice* key, Slice* value);

 public:
  static std::string Header() { return "secs_elapsed,interval_qps"; }

  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs,
                std::string header_string = Header())
      : header_string_(header_string),
        env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());

    if (s.ok()) {
      s = report_file_->Append(header_string_ + "\n");
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
      total_key_num_ += 1;
      UpdateSeqScore(key, value);
      UpdateRwRatioScore(op_type, key, value);
      UpdateDistributionScore(key, value);
    }
  }
  void ReportFinishedOps(int64_t num_ops, std::vector<Slice*> keys,
                         std::vector<Slice*> values,
                         OperationType op_type = OperationType::kOthers) {
    assert(op_type == OperationType::kSeek);  // TODO
  }
  virtual void InsertNewTuningPoints(ChangePoint point);

 public:
  void SetHist(HistogramMapPtr hist) { hist_ = hist; }

 protected:
  virtual void DetectAndTuning(int secs_elapsed);
  virtual Status ReportLine(int secs_elapsed, int total_ops_done_snapshot);
  virtual void UpdateMetric(int secs_elapsed);
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
  Slice last_key_;
  uint64_t sequnencial_key_num_ = 0;  // number of sequential keys
  uint64_t key_num_in_seq_ = 0;
  uint8_t seq_ascending_ =
      0;  // 1 if the last key is ascending, 2 if descending, 0 no order now
  uint64_t read_opt_size_sum_ = 0;
  uint64_t write_opt_size_sum_ = 0;
  std::condition_variable stop_cv_;
  HistogramMapPtr hist_;
  bool stop_;
  TetrisMetrics current_metrics_;
  uint64_t time_started;
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
      std::cout << "Reporting at " << env_->NowMicros() - time_started
                << " micros" << std::endl;
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
  void UpdateMetric(int secs_elapsed) override {
    assert(false);
    // No metrics to update in this reporter
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
  bool applying_changes;
  static std::string DOTAHeader() {
    return "secs_elapsed,interval_qps,batch_size,thread_num";
  }
  int tuning_gap_secs_;
  std::map<std::string, std::string> parameter_map;
  std::map<std::string, int> baseline_map;
  const int thread_num_upper_bound = 12;
  const int thread_num_lower_bound = 2;

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

  void DetectChangesPoints(int sec_elapsed);

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
  DBImpl* db_ptr;
  std::string detailed_header() {
    return ReporterAgent::Header() + ",immutables" + ",total_mem_size" +
           ",l0_files" + ",all_sst_size" + ",live_data_size" + ",pending_bytes";
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
    auto opt = this->db_ptr->GetOptions();

    //    current_opt = db_ptr->GetOptions();
    auto version =
        db_ptr->GetVersionSet()->GetColumnFamilySet()->GetDefault()->current();
    auto cfd = version->cfd();
    auto vfs = version->storage_info();
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

    std::string report =
        std::to_string(secs_elapsed) + "," +
        std::to_string(total_ops_done_snapshot - last_report_) + "," +
        std::to_string(immutable_memtables) + "," +
        std::to_string(total_mem_size) + "," + std::to_string(l0_files) + "," +
        std::to_string(all_sst_size) + "," + std::to_string(live_data_size) +
        "," + std::to_string(compaction_pending_bytes);
    //    std::cout << report << std::endl;
    auto s = report_file_->Append(report);
    return s;
  }

  const TetrisMetrics& GetMetrics() const override {
    assert(false);  // no use
    return current_metrics_;
  }

  void UpdateMetric(int secs_elapsed) override {
    assert(false);  // no use
  }
};

class ReporterTetris : public ReporterAgent {
 private:
  DBImpl* db_ptr;
  static std::string Tetris_header() {
    return ReporterAgent::Header() + ",throughput" + ",P99 latency" +
           ",P99.9 latency" + ",write amplification" + ",read/write ratio" +
           ",IO intensity" + ",Seq/Random" + ",Zipfian/uniform" +
           ",key-value size distribution" + ",CPU usage" + ",MEM usage" +
           ",IO bandwidth" + ",memtable size" + ",bloom filter size";
  }
  Options current_opt;
  Version* version;
  ColumnFamilyData* cfd;
  VersionStorageInfo* vfs;
  uint64_t read_count_ = 0;
  uint64_t write_count_ = 0;

  void DetectAndTuning(int secs_elapsed) override {
    // This reporter does not support tuning now
    // just update the system info
    UpdateMetric(secs_elapsed);
    std::cout << current_metrics_.ToString() << std::endl;
  }

  void UpdateSystemInfo() {
    current_opt = db_ptr->GetOptions();
    version =
        db_ptr->GetVersionSet()->GetColumnFamilySet()->GetDefault()->current();
    cfd = version->cfd();
    vfs = version->storage_info();
  }

  double GetReadLantency(double percentile = 50) {
    OperationType op_type = kRead;
    auto hist = hist_->find(op_type);
    if (hist == hist_->end()) {
      std::cout << "No histogram for read operation" << std::endl;
      return 0.0;
    }
    auto latency = hist->second->Percentile(percentile);
    if (latency < 0) {
      std::cout << "No p99 latency for read operation" << std::endl;
      return 0.0;
    }
    return latency / 1000.0;  // convert to ms
  }
  double GetWriteLantency(double percentile = 50) {
    OperationType op_type = kWrite;
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
    int last_interval_read_count = read_count - read_count_;
    int last_interval_write_count = write_count - write_count_;
    read_count_ = read_count;
    write_count_ = write_count;
    if (last_interval_write_count == 0) {
      return 0.0;  // avoid division by zero
    }
    return static_cast<double>(last_interval_read_count) /
           last_interval_write_count;  // read/write ratio
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
      return 0.0;
    }
    return static_cast<double>(total_ops_done_snapshot - last_report_) /
           secs_elapsed;  // ops/sec
  }

 public:
  ReporterTetris(DBImpl* running_db, Env* env, const std::string& fname,
                 uint64_t report_interval_secs)
      : ReporterAgent(env, fname, report_interval_secs, Tetris_header()) {
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to record more details" << std::endl;
      abort();

    } else {
      db_ptr = reinterpret_cast<DBImpl*>(running_db);
    }
  }

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;

  const TetrisMetrics& GetMetrics() const override { return current_metrics_; }

  void UpdateMetric(int secs_elapsed) override {
    UpdateSystemInfo();
    TetrisMetrics metrics;
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
    // CPU usage 从proc中获取
    metrics.cpu_usage_ = getCpuUsage();
    // MEM usage 从proc中获取
    metrics.mem_usage_ = getMemoryUsage();
    // memtable size getProperty
    metrics.memtable_size_ =
        static_cast<double>(GetMemtableSize()) / (1024 * 1024);  // MB
    // bloom filter size
    metrics.bloom_filter_size_ = GetFilterSize();

    // finished
    current_metrics_ = metrics;
  }
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_REPORTER_H

//
// created by yukimi on 2025/06/13
//

#ifndef ZIPIFIAN_PREDICTOR_H
#define ZIPIFIAN_PREDICTOR_H

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>

namespace ROCKSDB_NAMESPACE {

struct LinearRegressionResult {
  double slope;
  double intercept;
};

struct ZipfianPredictionResult {
  double zipfian_score;
  double slope;
  double r_squared;
  double expected_slope;
  std::string ToString() const {
    return "Zipfian Score: " + std::to_string(zipfian_score) +
           ", Slope: " + std::to_string(slope) +
           ", R-squared: " + std::to_string(r_squared) +
           ", Expected Slope: " + std::to_string(expected_slope);
  }
};

// --- Helper Functions ---

static inline LinearRegressionResult LinearRegression(
    const std::vector<double>& x, const std::vector<double>& y) {
  if (x.empty() || y.empty() || x.size() != y.size()) {
    return {0.0, 0.0};
  }

  size_t n = x.size();
  double sum_x = std::accumulate(x.begin(), x.end(), 0.0);
  double sum_y = std::accumulate(y.begin(), y.end(), 0.0);
  double sum_xy = 0.0;
  double sum_x2 = 0.0;

  for (size_t i = 0; i < n; ++i) {
    sum_xy += x[i] * y[i];
    sum_x2 += x[i] * x[i];
  }

  double denominator = n * sum_x2 - sum_x * sum_x;
  if (denominator == 0) {
    return {0.0, 0.0};
  }

  double slope = (n * sum_xy - sum_x * sum_y) / denominator;
  double intercept = (sum_y * sum_x2 - sum_x * sum_xy) / denominator;

  return {slope, intercept};
}

static inline double CalculateRSquared(const std::vector<double>& actual,
                                       const std::vector<double>& predicted) {
  if (actual.empty() || predicted.empty() ||
      actual.size() != predicted.size()) {
    return 0.0;
  }

  size_t n = actual.size();
  double mean_actual = std::accumulate(actual.begin(), actual.end(), 0.0) / n;

  double ss_total = 0.0;
  for (double val : actual) {
    ss_total += std::pow(val - mean_actual, 2);
  }

  double ss_residual = 0.0;
  for (size_t i = 0; i < n; ++i) {
    ss_residual += std::pow(actual[i] - predicted[i], 2);
  }

  if (ss_total == 0) {
    // 如果所有实际值都相同，SS_total 为 0。
    // 如果预测值也完全相同且等于实际值，则 R² 为 1。
    // 否则，根据定义，R² 可能无意义或为负无穷。这里返回 0 或 1 取决于预测精度。
    return (ss_residual == 0) ? 1.0 : 0.0;
  }

  return 1.0 - (ss_residual / ss_total);
}

// --- Main Function: predict_Zipfian ---

static inline double PredictZipfian(
    const std::unordered_map<std::string, uint64_t>
        key_access_counts /*use copy to avoid data race*/) {
  if (key_access_counts.empty() || key_access_counts.size() == 1) {
    return 0;
  }
  uint64_t total_accesses = std::accumulate(
      key_access_counts.begin(), key_access_counts.end(), 0LL,
      [](uint64_t sum, const std::pair<std::string, uint64_t>& p) {
        return sum + p.second;
      });
  if (total_accesses == 0) {
    std::cerr << "Error: Total accesses cannot be zero." << std::endl;
    return 0;
  }

  std::vector<double> freq_list;
  freq_list.reserve(key_access_counts.size());
  for (const auto& [str, count] : key_access_counts) {
    freq_list.emplace_back(static_cast<double>(count) / total_accesses);
  }
  std::sort(freq_list.begin(), freq_list.end(),
            [](const double& a, const double& b) -> bool { return a > b; });

  double zipfian_score = 0;
  double freq_sum = 0;
  uint64_t all = freq_list.size();
  for (size_t i = 0; i < freq_list.size(); i++) {
    freq_sum += freq_list[i];
    // freq_sum >= (all - i) / all
    if (freq_sum * all >= (all - i)) {
      zipfian_score = static_cast<double>(all - i) / all;
      break;
    }
  }
  return zipfian_score;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // ZIPIFIAN_PREDICTOR_H
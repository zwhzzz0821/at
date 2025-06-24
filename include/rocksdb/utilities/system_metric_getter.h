//
// Created by yukimi on 2025/06/11
//

#ifndef ROCKSDB_SYSTEM_METRIC_GETTER_H
#define ROCKSDB_SYSTEM_METRIC_GETTER_H

#include <unistd.h>  // for usleep

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

// Structure to hold CPU times
struct CpuTimes {
  long long user;
  long long nice;
  long long system;
  long long idle;
  long long iowait;
  long long irq;
  long long softirq;
  long long steal;
  long long guest;
  long long guest_nice;

  long long getTotalJiffies() const {
    return user + nice + system + idle + iowait + irq + softirq + steal +
           guest + guest_nice;
  }

  long long getIdleJiffies() const {
    return idle + iowait;  // Including iowait as part of idle time
  }
};

// Function to read CPU times from /proc/stat
static inline bool readCpuTimes(CpuTimes& times) {
  std::ifstream file("/proc/stat");
  if (!file.is_open()) {
    std::cerr << "Error: Could not open /proc/stat" << std::endl;
    return false;
  }

  std::string line;
  std::getline(file, line);  // Read the first line (aggregate CPU)
  file.close();

  std::stringstream ss(line);
  std::string cpuStr;
  ss >> cpuStr >> times.user >> times.nice >> times.system >> times.idle >>
      times.iowait >> times.irq >> times.softirq >> times.steal >>
      times.guest >> times.guest_nice;

  if (cpuStr != "cpu") {
    std::cerr << "Error: Unexpected format in /proc/stat" << std::endl;
    return false;
  }
  return true;
}

static CpuTimes prevTimes;

// Function to get CPU usage percentage
static inline double getCpuUsage() {
  CpuTimes currTimes;

  if (!readCpuTimes(currTimes)) return -1.0;

  long long prevTotal = prevTimes.getTotalJiffies();
  long long prevIdle = prevTimes.getIdleJiffies();
  long long currTotal = currTimes.getTotalJiffies();
  long long currIdle = currTimes.getIdleJiffies();

  prevTimes = currTimes;
  long long totalDiff = currTotal - prevTotal;
  long long idleDiff = currIdle - prevIdle;

  if (totalDiff == 0) {
    return 0.0;  // Avoid division by zero, can happen if sampling too fast or
                 // system is stuck
  }

  return 100.0 * (static_cast<double>(totalDiff - idleDiff) / totalDiff);
}

// Function to get memory usage percentage
static inline double getMemoryUsage() {
  std::ifstream file("/proc/meminfo");
  if (!file.is_open()) {
    std::cerr << "Error: Could not open /proc/meminfo" << std::endl;
    return -1.0;
  }

  std::string line;
  long long memTotal = 0, memFree = 0, buffers = 0, cached = 0;

  while (std::getline(file, line)) {
    std::stringstream ss(line);
    std::string key;
    long long value;
    std::string unit;

    ss >> key >> value >> unit;

    if (key == "MemTotal:") {
      memTotal = value;
    } else if (key == "MemFree:") {
      memFree = value;
    } else if (key == "Buffers:") {
      buffers = value;
    } else if (key == "Cached:") {
      cached = value;
    }
  }
  file.close();

  if (memTotal == 0) {
    std::cerr << "Error: MemTotal not found or zero in /proc/meminfo"
              << std::endl;
    return -1.0;
  }

  // Used memory = Total - Free - Buffers - Cached (these are considered
  // "available" for new programs)
  long long usedMemory = memTotal - memFree - buffers - cached;

  return 100.0 * (static_cast<double>(usedMemory) / memTotal);
}

#endif
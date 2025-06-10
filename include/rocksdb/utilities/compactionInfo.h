//
// create by yukimi on 2025/06/10
//

#ifndef ROCKSDB_UTILITIES_COMPACTION_INFO_H
#define ROCKSDB_UTILITIES_COMPACTION_INFO_H

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <regex>
namspace ROCKSDB_NAMESPACE {
class CompactionInfo {
public:
    // 存储每层的信息
    struct LevelStat {
        int num_files;        // 文件数
        double size_mb;       // 大小 (MB)
        double score;         // 评分
        double read_gb;       // 读带宽 (GB)
        double write_gb;      // 写带宽 (GB)
        double comp_sec;      // Compaction 时间 (秒)
        double avg_sec;       // 平均 Compaction 时间 (秒)
        int key_in;           // Key In 数量
        int key_drop;         // Key Drop 数量
    };

    // 构造函数，通过解析查询的 getProperty 的结果填充信息
    explicit CompactionInfo(const std::string& query_result) {
        parseCompactionStats(query_result);
        parseDbStats(query_result);
    }

    // 获取每一层的 compaction 信息
    const std::unordered_map<int, LevelStat>& getLevelStats() const {
        return level_stats_;
    }

    // 获取数据库的统计信息
    const std::string& getDbStats() const {
        return db_stats_;
    }

private:
    // 存储每一层的 compaction 信息
    std::unordered_map<int, LevelStat> level_stats_;

    // 存储数据库的统计信息
    std::string db_stats_;

    // 解析 Compaction Stats 部分
    void parseCompactionStats(const std::string& query_result) {
        std::regex level_regex(R"((L\d+)\s+(\d+)/(\d+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d]+)\s+([\d]+))");
        std::smatch match;
        std::string::const_iterator search_start(query_result.cbegin());

        // 查找每一层的 compaction 数据
        while (std::regex_search(search_start, query_result.cend(), match, level_regex)) {
            LevelStat stat;
            stat.num_files = std::stoi(match[2].str());
            stat.size_mb = std::stod(match[4].str());
            stat.score = std::stod(match[5].str());
            stat.read_gb = std::stod(match[6].str());
            stat.write_gb = std::stod(match[9].str());
            stat.comp_sec = std::stod(match[13].str());
            stat.avg_sec = std::stod(match[14].str());
            stat.key_in = std::stoi(match[15].str());
            stat.key_drop = std::stoi(match[16].str());

            int level = std::stoi(match[1].str().substr(1));  // 提取 L0, L1, L2, ...
            level_stats_[level] = stat;

            search_start = match.suffix().first;
        }
    }

    // 解析 DB Stats 部分
    void parseDbStats(const std::string& query_result) {
        std::regex db_stats_regex(R"((Cumulative writes:\s*[\d]+.*?stall\s+micros:\s*[\d]+))");
        std::smatch match;

        // 查找 DB Stats
        if (std::regex_search(query_result, match, db_stats_regex)) {
            db_stats_ = match.str();
        }
    }
};
}
#endif  // ROCKSDB_UTILITIES_COMPACTION_INFO_H
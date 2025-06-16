import re
import matplotlib.pyplot as plt
import os

def parse_log_data(log_file_path):
    """
    解析日志文件，提取所有 TetrisMetrics 块中的数据。

    Args:
        log_file_path (str): 日志文件的路径。

    Returns:
        list[dict]: 包含每个 TetrisMetrics 块解析数据的字典列表。
                    每个字典包含所有提取的指标。
    """
    parsed_metrics = []

    # 使用 (?s) 使得 '.' 可以匹配换行符
    # 匹配整个 TetrisMetrics 块，并捕获所有关键数值
    # 注意：这里的正则表达式捕获了所有你列出的字段
    # ([^=\s]+)=([\d.-]+) 是一个通用模式，用于捕获 '字段名=数值' 的形式
    # 但由于字段名已知且顺序固定，我们可以更精确地匹配
    tetris_block_pattern = re.compile(
        r"TetrisMetrics: update_time=(\d+)\s+"
        r"throughput=([\d.]+)\s+"
        r"p99_read_latency=([\d.]+)\s+"
        r"p999_read_latency=([\d.]+)\s+"
        r"p99_write_latency=([\d.]+)\s+"
        r"p999_write_latency=([\d.]+)\s+"
        r"write_amplification=([\d.]+)\s+"
        r"read_write_ratio=([\d.]+)\s+"
        r"io_intensity=([\d.]+)\s+"
        r"compaction_granularity=(\d+)\s+"
        r"key_value_size_distribution=([\d.]+)\s+"
        r"cpu_usage=([\d.]+)\s+"
        r"mem_usage=([\d.]+)\s+"
        r"memtable_size=([\d.]+)\s+"
        r"bloom_filter_size=(\d+)\s+"
        r"seq_score=([\d.]+)\s+"
        r"rw_ratio_score=([\d.]+)\s+"
        r"zipfian_predictor_result=Zipfian Score: ([\d.]+), Slope: ([-?\d.]+), R-squared: ([-?\d.]+), Expected Slope: ([-?\d.]+)",
        re.DOTALL # 允许 . 匹配包括换行符在内的任何字符
    )

    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            log_content = f.read()

        # 使用 finditer 查找所有匹配项
        matches = tetris_block_pattern.finditer(log_content)

        for match in matches:
            # 提取捕获组的值
            # 索引从1开始，因为组0是整个匹配的字符串
            metrics = {
                "update_time": int(match.group(1)),
                "throughput": float(match.group(2)),
                "p99_read_latency": float(match.group(3)),
                "p999_read_latency": float(match.group(4)),
                "p99_write_latency": float(match.group(5)),
                "p999_write_latency": float(match.group(6)),
                "write_amplification": float(match.group(7)),
                "read_write_ratio": float(match.group(8)),
                "io_intensity": float(match.group(9)),
                "compaction_granularity": int(match.group(10)),
                "key_value_size_distribution": float(match.group(11)),
                "cpu_usage": float(match.group(12)),
                "mem_usage": float(match.group(13)),
                "memtable_size": float(match.group(14)),
                "bloom_filter_size": int(match.group(15)),
                "seq_score": float(match.group(16)),
                "rw_ratio_score": float(match.group(17)),
                "zipfian_score": float(match.group(18)),
                "zipfian_slope": float(match.group(19)),
                "zipfian_r_squared": float(match.group(20)),
                "zipfian_expected_slope": float(match.group(21))
            }
            parsed_metrics.append(metrics)

    except FileNotFoundError:
        print(f"错误: 文件 '{log_file_path}' 未找到。请检查路径。")
    except Exception as e:
        print(f"解析文件时发生错误: {e}")

    return parsed_metrics

def plot_throughput_over_time(parsed_data):
    """
    根据解析的数据绘制时间-吞吐量图表。

    Args:
        parsed_data (list[dict]): 由 parse_log_data 函数返回的解析数据列表。
    """
    if not parsed_data:
        print("没有数据可用于绘图。")
        return

    # 提取时间戳和吞吐量
    update_times = [item['update_time'] for item in parsed_data]
    throughputs = [item['throughput'] for item in parsed_data]

    # 创建图表
    plt.figure(figsize=(12, 6)) # 设置图表大小
    plt.plot(update_times, throughputs, marker='o', linestyle='-', color='b', markersize=4)

    # 设置图表标题和轴标签
    plt.xlabel("update_time", fontsize=12)
    plt.ylabel("Throughput (ops)", fontsize=12)
    plt.title("time - throughput", fontsize=14)

    # 启用网格
    plt.grid(True, linestyle='--', alpha=0.7)

    # 自动调整X轴标签，防止重叠
    plt.gcf().autofmt_xdate()

    # 显示图表
    plt.tight_layout() # 调整布局以避免标签重叠
    plt.savefig("time-th.png")
    plt.show()

if __name__ == "__main__":
    # 请将 'your_log_file.log' 替换为你的实际日志文件路径
    # 示例: log_file = "/path/to/your/db_bench_log.txt"
    log_file = "tune.log"

    # 为了方便测试，如果文件不存在，我们创建一个模拟文件
    if not os.path.exists(log_file):
        print(f"日志文件 '{log_file}' 不存在，正在创建一个模拟文件用于演示。")
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("""
write buffer size: 67108864
No histogram for read operation
No histogram for read operation
TetrisMetrics: update_time=1234
throughput=0.000000
p99_read_latency=0.000000
p999_read_latency=0.000000
p99_write_latency=0.039425
p999_write_latency=0.057417
write_amplification=0.000000
read_write_ratio=0.000000
io_intensity=0.000000
compaction_granularity=0
key_value_size_distribution=0.000000
cpu_usage=6.012658
mem_usage=25.222879
memtable_size=2.009735
bloom_filter_size=0
seq_score=0.326577
rw_ratio_score=0.000000
zipfian_predictor_result=Zipfian Score: 0.000000, Slope: 0.000000, R-squared: -35.098782, Expected Slope: -1.000000

write buffer size: 67108864
No histogram for read operation
No histogram for read operation
TetrisMetrics: update_time=1235
throughput=100.500000
p99_read_latency=1.230000
p999_read_latency=2.450000
p99_write_latency=0.040000
p999_write_latency=0.060000
write_amplification=0.100000
read_write_ratio=0.800000
io_intensity=0.500000
compaction_granularity=1024
key_value_size_distribution=0.000000
cpu_usage=10.000000
mem_usage=30.000000
memtable_size=4.000000
bloom_filter_size=1
seq_score=0.500000
rw_ratio_score=0.700000
zipfian_predictor_result=Zipfian Score: 0.500000, Slope: -0.800000, R-squared: 0.850000, Expected Slope: -1.000000

TetrisMetrics: update_time=1238
throughput=150.120000
p99_read_latency=0.800000
p999_read_latency=1.500000
p99_write_latency=0.035000
p999_write_latency=0.050000
write_amplification=0.080000
read_write_ratio=0.900000
io_intensity=0.600000
compaction_granularity=2048
key_value_size_distribution=0.000000
cpu_usage=12.500000
mem_usage=35.000000
memtable_size=6.000000
bloom_filter_size=2
seq_score=0.600000
rw_ratio_score=0.800000
zipfian_predictor_result=Zipfian Score: 0.800000, Slope: -0.950000, R-squared: 0.920000, Expected Slope: -1.000000
            """)

    # 1. 解析日志文件
    parsed_data = parse_log_data(log_file)

    # 2. 打印解析出的数据 (可选，用于调试)
    # for item in parsed_data:
    #     print(item)

    # 3. 绘制时间-吞吐量图表
    plot_throughput_over_time(parsed_data)

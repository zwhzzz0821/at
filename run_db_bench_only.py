#!/usr/bin/env python3
import os
import subprocess
import time
import shutil
import sys
import logging
import psutil

class RocksDBBenchmarkAnalyzer:
    DEFAULT_NUM_OPERATIONS = 100000000
    DEFAULT_KEY_SIZE = 16
    DEFAULT_VALUE_SIZE = 1024
    DEFAULT_CACHE_SIZE = 0
    DEFAULT_READ_WRITE_PERCENT = 50
    DEFAULT_THREADS = 1
    BENCHMARK_TIMEOUT = 10000  # 10000s timeout

    def __init__(self):
        # Configuration
        self.DB_BENCH_PATH = os.path.abspath("./db_bench")
        self.OUTPUT_DIR = os.path.abspath("outputexpfinal") #SILKfromADOC
        self.DATA_DIR = os.path.abspath("dataexpfinal")
        self.SYSTEM_STATS_INTERVAL = 5  # seconds
      
        # Setup directories
        self.setup_directories()

        # Benchmark parameters
        self.THREADS_LIST = [1]  # Thread counts to test
        self.BATCH_SIZES = [1]  # Batch sizes to test, default is 1.
        self.BENCHMARK_TYPES = ["readrandom"]  # Benchmark types, "fillrandom", "readrandom", "readrandomwriterandom", "mixgraph", "fillseq"
        self.VALUE_SIZES = [1024]  # Value sizes to test
        self.KEY_SIZES = [16]  # Key sizes to test
        self.READ_WRITE_PERCENTS = [50, 20, 80]  # Read/write ratios,50, 20, 80
        self.MAX_BACKGROUND_JOBS = [2]  # Background job counts，default is 2.
        
        # Zipfian distribution control
        self.USE_ZIPFIAN = False  # Set to False to disable Zipfian distribution
        self.ZIPFIAN_PARAMS = {
            'keyrange_num': 1,  # 几组dist
            'keyrange_dist_a': 0.8,  # 第一个范围（20%的键）获得80%的访问
            'keyrange_dist_b': 0.7,
            'keyrange_dist_c': 0.5,
            'keyrange_dist_d': 0.0025
        }

        self.FEA =  None
        self.TEA =  None
        self.SILK =  None

    def setup_directories(self):
        """Ensure output directories exist without deleting existing content"""
        try:
            os.makedirs(self.OUTPUT_DIR, exist_ok=True)
            os.makedirs(self.DATA_DIR, exist_ok=True)
        except Exception as e:
            print(f"[CRITICAL] Failed to setup directories: {str(e)}")
            sys.exit(1)

    def get_flag(self, value, true_flag, false_flag):
        if value is True:
            return [true_flag]
        elif value is False:
            return [false_flag]
        else:
            return []  # 参数为 None 时，不添加该命令行参数

    def run_db_bench(self, benchmark_type: str, db_path: str, **kwargs) -> bool:
        params = {
            'num': self.DEFAULT_NUM_OPERATIONS,
            'key_size': self.DEFAULT_KEY_SIZE,
            'value_size': self.DEFAULT_VALUE_SIZE,
            'cache_size': self.DEFAULT_CACHE_SIZE,
            'use_existing_db': 0,
            'readwritepercent': self.DEFAULT_READ_WRITE_PERCENT,
            'threads': self.DEFAULT_THREADS,
            'batch_size': 1,
            'max_background_jobs': 2
        }
        params.update(kwargs)
        
        # 确保使用绝对路径
        abs_db_path = os.path.abspath(db_path)
        os.makedirs(abs_db_path, exist_ok=True)

        # 生成测试名称
        test_name_parts = [
            benchmark_type,
            f"t{params['threads']}",
            f"b{params['batch_size']}",
            f"v{params['value_size']}",
            f"k{params['key_size']}",
            f"j{params['max_background_jobs']}"
        ]
        if benchmark_type == "readrandomwriterandom":
            test_name_parts.append(f"p{params['readwritepercent']}")
        test_name = "_".join(test_name_parts)

        # 构建基础命令参数列表
        base_cmd = [
            self.DB_BENCH_PATH,
            f"--db={abs_db_path}",
            f"--num={params['num']}",
            f"--key_size={params['key_size']}",
            f"--value_size={params['value_size']}",
            f"--statistics=1",
            f"--histogram=1",
            f"--cache_size={params['cache_size']}",
            "--compression_type=none",
            f"--threads={params['threads']}",
            "--report_interval_seconds=1",
            "--benchmark_write_rate_limit=0",
            f"--max_background_jobs={params['max_background_jobs']}"
        ]

        # 添加功能标志
        base_cmd.extend(self.get_flag(self.FEA, "--FEA_enable=true", "--FEA_enable=false"))
        base_cmd.extend(self.get_flag(self.TEA, "--TEA_enable=true", "--TEA_enable=false"))
        base_cmd.extend(self.get_flag(self.SILK, "--SILK_triggered=true", "--SILK_triggered=false"))

        # 处理预填充数据库逻辑
        if benchmark_type in ["readrandom", "readrandomwriterandom"]:
            fill_db_path = f"{abs_db_path}_fill"
            print(f"[INFO] 预填充数据库路径: {fill_db_path}")

            # 检查是否需要预填充
            if not os.path.exists(fill_db_path):
                print("[INFO] 开始预填充数据库...")
                os.makedirs(fill_db_path, exist_ok=True)
                
                # 构建预填充命令
                fill_cmd = base_cmd.copy()
                fill_cmd[1] = f"--db={fill_db_path}"  # 更新数据库路径
                fill_cmd.extend([
                    "--benchmarks=fillrandom",
                    "--disable_wal=1"
                ])
                
                # 移除可能存在的冲突参数
                fill_cmd = [param for param in fill_cmd if not param.startswith("--use_existing_db=")]
                fill_cmd.append("--use_existing_db=0")  # 确保使用_existing_db=0
                
                # 执行预填充
                fill_log_file = os.path.join(self.OUTPUT_DIR, f"prefill_{test_name}.log")
                try:
                    with open(fill_log_file, 'w') as fill_log:
                        print(f"[INFO] 执行预填充命令: {' '.join(fill_cmd)}")
                        fill_proc = subprocess.Popen(
                            fill_cmd,
                            stdout=fill_log,
                            stderr=subprocess.STDOUT,
                            text=True,
                            bufsize=1
                        )
                        fill_proc.wait()
                    
                    if fill_proc.returncode != 0:
                        print(f"[ERROR] 预填充失败，返回码: {fill_proc.returncode}")
                        shutil.rmtree(fill_db_path, ignore_errors=True)
                        return False
                    
                    print("[INFO] 预填充完成")
                except Exception as e:
                    print(f"[ERROR] 预填充过程中出错: {str(e)}")
                    shutil.rmtree(fill_db_path, ignore_errors=True)
                    return False

            # 使用预填充的数据库
            base_cmd = [param for param in base_cmd if not param.startswith("--use_existing_db=")]
            base_cmd[1] = f"--db={fill_db_path}"
            base_cmd.append("--use_existing_db=1") 

        if benchmark_type in ["fillseq", "fillrandom"]:
            base_cmd.extend([
                f"--benchmarks={benchmark_type}",
                "--disable_wal=1"
            ])
        elif benchmark_type == "readrandom":
            base_cmd.extend([
                "--benchmarks=readrandom"
            ])
        elif benchmark_type == "readrandomwriterandom":
            base_cmd.extend([
                "--benchmarks=readrandomwriterandom",
                f"--readwritepercent={params['readwritepercent']}"
            ])

        # 添加批处理大小参数
        if 'batch_size' in params:
            base_cmd.append(f"--batch_size={params['batch_size']}")

        if benchmark_type != "readrandomwriterandom":
            params_to_include = {
                k: v for k, v in params.items() 
                if k not in ['readwritepercent', 'num', 'use_existing_db']
            }
        else:
            params_to_include = {
                k: v for k, v in params.items() 
                if k not in ['num', 'use_existing_db']
            }
        
        if self.USE_ZIPFIAN:
            params_to_include['zipfian'] = 'on'
        
        param_str = "_".join(f"{k}={v}" for k, v in params_to_include.items())
        log_file = os.path.join(self.OUTPUT_DIR, f"{benchmark_type}_{param_str}.log")

        print(f"\n[INFO] 开始执行 {benchmark_type} 测试...")
        print(f"[INFO] 命令: {' '.join(base_cmd)}")
        print(f"[INFO] 日志文件: {log_file}")

        try:
            with open(log_file, 'w') as f:
                f.write(f"=== 测试命令 ===\n{' '.join(base_cmd)}\n\n")
                f.flush()
                
                start_time = time.time()
                proc = subprocess.Popen(
                    base_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    encoding='utf-8',
                    errors='replace'
                )
                
                for line in proc.stdout:
                    f.write(line)
                    f.flush()
                    if "Iteration" in line or "Cumulative statistics" in line:
                        print(line.strip())
                
                proc.wait(timeout=self.BENCHMARK_TIMEOUT)
                elapsed = time.time() - start_time
                
                f.write(f"\n=== 运行时间 ===\n{elapsed:.2f} 秒\n")
                f.write(f"=== 返回码 ===\n{proc.returncode}\n")
                
                if proc.returncode == 0:
                    print(f"[SUCCESS] 测试成功完成，耗时 {elapsed:.2f} 秒")
                else:
                    print(f"[ERROR] 测试失败，返回码 {proc.returncode}")
                
                return proc.returncode == 0
                
        except subprocess.TimeoutExpired:
            print("[ERROR] 测试超时")
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            return False
            
        except Exception as e:
            print(f"[ERROR] 测试过程中出错: {str(e)}")
            if proc.poll() is None:
                proc.terminate()
            return False
            
    def run_benchmark_suite(self):
        """Run all combinations of configured benchmarks"""
        print(f"\n=== Starting Benchmark Suite ===")
        print(f"Output directory: {self.OUTPUT_DIR}")
        print(f"Zipfian distribution: {'ENABLED' if self.USE_ZIPFIAN else 'DISABLED'}")
        print(f"FEA: {'ENABLED' if self.FEA else 'DISABLED'}")
        print(f"TEA: {'ENABLED' if self.TEA else 'DISABLED'}")
        print(f"SILK: {'ENABLED' if self.SILK else 'DISABLED'}")

        for benchmark_type in self.BENCHMARK_TYPES:
            for threads in self.THREADS_LIST:
                for batch_size in self.BATCH_SIZES:
                    for value_size in self.VALUE_SIZES:
                        for key_size in self.KEY_SIZES:
                            for max_bg_jobs in self.MAX_BACKGROUND_JOBS:
                                if benchmark_type == "readrandomwriterandom":
                                    for read_write_percent in self.READ_WRITE_PERCENTS:
                                        test_name = f"{benchmark_type}_t{threads}_b{batch_size}_v{value_size}_k{key_size}_j{max_bg_jobs}_p{read_write_percent}"
                                        if self.USE_ZIPFIAN:
                                            test_name += "_zipfian"
                                        db_path = os.path.join(self.DATA_DIR, test_name)
                                        
                                        print(f"\nRunning {test_name}...")
                                        self.run_db_bench(
                                            benchmark_type=benchmark_type,
                                            db_path=db_path,
                                            threads=threads,
                                            batch_size=batch_size,
                                            value_size=value_size,
                                            key_size=key_size,
                                            readwritepercent=read_write_percent,
                                            max_background_jobs=max_bg_jobs
                                        )
                                else:
                                    test_name = f"{benchmark_type}_t{threads}_b{batch_size}_v{value_size}_k{key_size}_j{max_bg_jobs}"
                                    if self.USE_ZIPFIAN:
                                        test_name += "_zipfian"
                                    db_path = os.path.join(self.DATA_DIR, test_name)
                                    
                                    print(f"\nRunning {test_name}...")
                                    self.run_db_bench(
                                        benchmark_type=benchmark_type,
                                        db_path=db_path,
                                        threads=threads,
                                        batch_size=batch_size,
                                        value_size=value_size,
                                        key_size=key_size,
                                        max_background_jobs=max_bg_jobs
                                    )

        print("\n=== Benchmark Suite Complete ===")
        print(f"All raw outputs saved to: {self.OUTPUT_DIR}")

if __name__ == "__main__":
    analyzer = RocksDBBenchmarkAnalyzer()
    analyzer.run_benchmark_suite()
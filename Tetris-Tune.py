# 如果发生了参数调整, 10s之后再调
# 不可调参数：
# cache_index_and_filter_blocks
# min_write_buffer_number_to_merge
# bloom_bits_per_key
################### Case 1 ###########################################
    # latency > StdDev * 2
    if detected small spikes:
        ##########Memory调整###########
        # 内存压力大时限制MemTable数量, 减小memtable
        #    mem_usage > 0.6 * total_mem:
        if immutable MemTable count > = max_write_buffer_number && immutable MemTable count < max_write_buffer_number * 2:
            if read-to-write ratio < 0.2 # write为主
                # decrease max_write_buffer_number - 1 范围[2, 4]
                increase write_buffer_size + 64MB  范围为[32MB, 1024MB]
                # if sequential_write:
                #     min_write_buffer_number_to_merge = 1 # 不合并, 触发flush前需要合并的memtable数量
                # else:
                #     min_write_buffer_number_to_merge=2 # 合并
            #     cache_index_and_filter_blocks=false
            # else: # read为主
            #     cache_index_and_filter_blocks=true
            else:
                decrease write_buffer_size - 64MB
        elif immutable MemTable count > = max_write_buffer_number * 2:
            # increase max_background_flushes + 1 范围[1, 4]
            level0_file_num_compaction_trigger + 1 范围[2, 10]
            ##########线程调整###########
            # 内存压力大且L0层满
            if the number of SSTable in L0 > level0_slowdown_writes_trigger && the number of SSTable in L0 <= level0_stop_writes_trigger:
                increase max_background_jobs + 1 范围[1, 16]
            # 内存压力大且L0层阻塞
            elif the number of SSTable in L0 > level0_stop_writes_trigger:
                increase max_background_jobs + 1
                increase max_background_compactions + 1 范围[1, 12]
        else: # 低内存压力
            # L0层满
            if the number of SSTable in L0 > level0_slowdown_writes_trigger && the number of SSTable in L0 <= level0_stop_writes_trigger:
                increase max_background_jobs + 1 范围[1, 16]
                increase max_background_compactions + 1 范围[1, 12]
                increase compaction_readahead_size * 2 范围为 [2MB, 128MB]
            # L0层阻塞
            elif the number of SSTable in L0 > level0_stop_writes_trigger:
                increase max_background_jobs * 2
                increase max_background_compactions * 2 范围[1, 12]
                increase compaction_readahead_size * 2 范围为 [2MB, 128MB]

        ##########LSM调整, 负载模式变化###########
        # 顺序写入场景，放宽L0限制，延迟合并
        if sequential_write:
            level0_slowdown_writes_trigger = 30  # 默认20
            level0_stop_writes_trigger = 50      # 默认36
            level0_file_num_compaction_trigger = 8
        elif random_write:
            # 随机写入场景，严格限制L0文件数, 更激进合并
            level0_slowdown_writes_trigger = 16
            level0_stop_writes_trigger = 24
            level0_file_num_compaction_trigger = 2
        elif readrandom:
            level0_slowdown_writes_trigger = 24
            level0_stop_writes_trigger = 36
            level0_file_num_compaction_trigger = 4
            if the number of SSTable in L0 > level0_slowdown_writes_trigger:
                increase compaction_readahead_size * 2 范围为 [2MB, 128MB]

        # ##########read BF调整###########
        # # 随机读取场景，加强Bloom Filter
        # if random_read:
        #     bloom_bits_per_key=10  # 提高过滤精度
        #     block_size + 4KB 范围[4KB, 64Kb]
        # else:
        #     bloom_bits_per_key = 5   # 默认值

################### Case 2 ###########################################
    # latency > StdDev * 10
    elif detected large latency spikes:
        if too many compactions (hard_pending_compaction_bytes_limit, soft_pending_compaction_bytes_limit): 
            increase max_background_jobs * 2
            increase compaction_readahead_size * 2 范围[2MB, 128MB]
            increase max_write_buffer_number * 2 范围[2, 4]

        if burst writes (检查write rate):
            decrease max_write_buffer_number / 2 范围[2, 4]
            decrease write_buffer_size / 2 范围为[32MB, 1024MB]
            increase max_background_flushes * 2 范围[1, 4]
            increase level0_file_num_compaction_trigger * 2
    else: # 没有延迟尖峰，性能稳定时，回到默认值
        write_buffer_size = 67108864  # 64MB
        max_background_flushes = 1
        max_background_jobs = 2
        max_background_compactions = -1
        compaction_readahead_size = 2097152  # 2MB
        level0_slowdown_writes_trigger = 20  
        level0_stop_writes_trigger = 36      
        level0_file_num_compaction_trigger = 4
        max_write_buffer_number = 2

        


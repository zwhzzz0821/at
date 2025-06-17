    如果发生了参数调整, 1min之后再调

################### Case 1 ###########################################
    if detected small spikes:
        ##########Memory调整###########
        # 内存压力大时限制MemTable数量, 减小memtable
        if mem_usage > 0.6 * total_mem:
            if read-to-write ratio < 0.2 # write为主
                decrease max_write_buffer_number - 1 范围[1, 4]
                decrease write_buffer_size - 64MB  范围为[32MB, 1024MB]
                if sequential_write:
                    min_write_buffer_number_to_merge = 1 # 不合并
                else:
                    min_write_buffer_number_to_merge=2 # 合并
                cache_index_and_filter_blocks=false
            else: # read为主
                cache_index_and_filter_blocks=true

        elif mem_usage > 0.8 * total_mem:
           increase max_background_flushes + 1 范围[1, 4]

        ##########线程调整###########
        # 内存压力大且L0层满
        if mem_usage > 0.6 * total_mem && the number of SSTable in L0 > level0_slowdown_writes_trigger:
            increase max_background_jobs + 1 范围[1, 16]
            increase compaction_readahead_size * 2 范围为 [2MB, 128MB]
        # 内存压力大且L0层阻塞
        elif mem_usage > 0.6 * total_mem && the number of SSTable in L0 > level0_stop_writes_trigger:
            increase max_background_jobs + 1 
            increase max_background_compactions + 1 范围[1, 12]
            increase compaction_readahead_size * 2 范围为 [2MB, 128MB]

        ##########LSM调整###########
        # 顺序写入场景，放宽L0限制
        if sequential_write:
            level0_slowdown_writes_trigger = 30  # 默认20
            level0_stop_writes_trigger = 50      # 默认36
            level0_file_num_compaction_trigger = 8
        else:
            # 随机写入场景，严格限制L0文件数
            level0_slowdown_writes_trigger=16
            level0_stop_writes_trigger=24
            level0_file_num_compaction_trigger=2

        ##########read BF调整###########
        # 随机读取场景，加强Bloom Filter
        if random_read:
            bloom_bits_per_key=10  # 提高过滤精度
            block_size + 4KB 范围[4KB, 64Kb]
        else:
            bloom_bits_per_key = 5   # 默认值

################### Case 2 ###########################################
    if detected large latency spikes:
        if too many compactions: 
            increase max_background_jobs * 2
            if read-to-write ratio > 0.5:
                increase compaction_readahead_size *2 范围[2MB, 128MB]

        if burst writes (检查write rate):
            decrease max_write_buffer_number / 2 范围[1, 4]
            decrease write_buffer_size / 2 范围为[32MB, 1024MB]
            increase max_background_flushes * 2 范围[1, 4]



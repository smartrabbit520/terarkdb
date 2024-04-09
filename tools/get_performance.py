import re
import os
import sys
import pandas as pd

# data_dir="/mnt/nvme1n1/xq/mlsm/database_comparison/terarkdb_2024.03.18_ycsb_c"
data_dir = sys.argv[1]

if len(sys.argv) >= 3:
    benchmark_log_name = sys.argv[2]
else:
    benchmark_log_name = "benchmark_ycsb_a.t1.s1.log"

# Create a dictionary to store the performance metrics
performance_metrics = {
    'flush_write': [],
    'write_rate': [],
    # 'blob_file_count': [],
    'blob_size_log': [],
    'garbage_size_log': [],
    'space_amp_log': [],
    'compaction_write_GB': [],
    'compaction_write_rate': [],
    'compaction_read_GB': [],
    'compaction_read_rate': [],
    'compaction_time': [],
    'lsm_size': [],
    'read_gb': [],
    'write_gb': [],
    'write_amp': [],
    'write_microsecond_median': [],
    'write_microsecond_average': [],
    'read_microsecond_median': [],
    'read_microsecond_average': []
}

def read_performance(benchmark_log_path):
    # print("Current benchmark_log_path:", benchmark_log_path)
    global performance_metrics
    with open(benchmark_log_path, 'r') as file:
        benchmark_log = file.readlines()
        
    # Reverse the benchmark_log
    benchmark_log.reverse()


    ### part 1

    pattern = r"^Cumulative writes: .*"

    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = "Cumulative writes: 25M writes, 25M keys, 25M commit groups, 1.0 writes per commit group, ingest: 29.64 GB, 9.97 MB/s"

    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)
    # Flush/Compaction  read/ write ：FlushCP GroupFlushInPool
    flush_write = numbers[4]

    # Write rate
    write_rate=numbers[5]


    ### part 2

    # pattern = r"^Cumulative compaction: .*"
    pattern = r"^Cumulative compaction:(?!.*write-lsm).*"
    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = "Cumulative compaction: 111.44 GB write, 37.50 MB/s write, 89.54 GB read, 30.13 MB/s read, 316.1 seconds"

    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)

    # Compaction write GB 
    compaction_write_GB = numbers[0]
    # Compaction write rate
    compaction_write_rate = numbers[1]
    # Compaction read GB 
    compaction_read_GB = numbers[2]
    # Compaction read rate
    compaction_read_rate = numbers[3]
    # Compaction time 
    compaction_time = numbers[4]


    ### part 3

    pattern = r"^ Sum .*"

    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = " Sum    128/0   14.39 GB        14.39 GB   0.0     75.5    11.6     63.9      56.5      33.4     -7.4       0.0   2.5    299.5    356.6       258       387    0.667    233M    18M"

    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)

    # total-tree size
    total_size = numbers[2]
    # Read GB: rocksdb:4 terarkdb: 5
    read_gb = numbers[5]
    # Write GB: rocksdb:7 terarkdb: 8
    write_gb = numbers[8]
    # Write amp: rocksdb:10 terarkdb: 12
    write_amp = numbers[12]
    
    
    ### part 4
    pattern = r"^ L-1 .*"

    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = " L-1    111/0   12.96 GB        12.96 GB  -1.0     20.5     0.0     20.5       0.0       4.3    -20.5       0.0  -1.0    385.3     80.3        54        38    1.432     17M      0"
    
    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)
    
    # Blob size
    blob_size_log = numbers[3]
    
    # Lsm-tree size
    lsm_size = float(total_size) - float(blob_size_log)
    
    # 减出来会有很多余数，故保留两位小数
    lsm_size = round(lsm_size, 2)
    
    
    ### part 5
    
    pattern = r"^Microseconds per write:"
    # Find the first occurrence of a line starting with "Microseconds per write" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    if text:
        # Get the index of the line
        index = benchmark_log.index(text)

        # Get the next two lines
        # Since I have reversed the order, here it should be index-2 and index
        lines = benchmark_log[index-2:index]

        # write microsecond median
        numbers = re.findall(r'\d+\.?\d*', lines[0])
        write_microsecond_median = numbers[1]
        
        # write microsecond average               
        numbers = re.findall(r'\d+\.?\d*', lines[1])
        write_microsecond_average = numbers[1]
    else:
        print("Microseconds per write not found")
    
    
    ### part 6
    
    pattern = r"^Microseconds per read:"
    # Find the first occurrence of a line starting with "Microseconds per read" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    if text:
        # Get the index of the line
        index = benchmark_log.index(text)
        
        # Get the next two lines
        # Since I have reversed the order, here it should be index-2 and index
        lines = benchmark_log[index-2:index]

        # read microsecond median
        numbers = re.findall(r'\d+\.?\d*', lines[0])
        read_microsecond_median = numbers[1]
        
        # read microsecond average
        numbers = re.findall(r'\d+\.?\d*', lines[1])
        read_microsecond_average = numbers[1]
        
        performance_metrics['read_microsecond_median'].append(read_microsecond_median)
        performance_metrics['read_microsecond_average'].append(read_microsecond_average)
    else:
        if 'read_microsecond_median' in performance_metrics:
            del performance_metrics['read_microsecond_median']
            del performance_metrics['read_microsecond_average']
        print("Microseconds per read not found")
    
    ### part 7
    # caclulate the user space and space amp
    
    # workload_path = "/mnt/nvme1n1/zt/YCSB-C/data/workloada-load-10000000-50000000.log_run.formated"
    # keys = set()
    # with open(workload_path, 'r') as file:
    #     lines = file.readlines()
    #     for line in lines:
    #         key = line.split()[1]
    #         keys.add(key)
    
    # 单位：字节
    # user_space = len(keys) * 1000 + sum([len(key) for key in keys])
        
    user_space = 5628895880 # /mnt/nvme1n1/zt/YCSB-C/data/workloada-load-10000000-100000000.log_run.formated
    # user_space = 5604959453 # /mnt/nvme1n1/zt/YCSB-C/data/workloada-load-10000000-50000000.log_run.formated
    
    # Convert blob_size_log to numeric value
    blob_size_numeric = float(blob_size_log) * 1000000000
    
    # Space amp
    space_amp_log = round(blob_size_numeric / user_space, 2)
    
    # garbage size
    garbage_size_log = blob_size_numeric - user_space
    
    # Convert garbage_size_log to actual value based on unit
    garbage_size_numeric = float(garbage_size_log)
    garbage_size_unit = 'B'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'K'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'M'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'G'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'T'
        
    if garbage_size_unit == 'G':
        garbage_size_log = f"{garbage_size_numeric:.2f}"
    else:
        garbage_size_log = f"{garbage_size_numeric:.2f}{garbage_size_unit}"
    
    
    performance_metrics['flush_write'].append(flush_write)
    performance_metrics['write_rate'].append(write_rate)
    performance_metrics['blob_size_log'].append(blob_size_log)
    performance_metrics['garbage_size_log'].append(garbage_size_log)
    performance_metrics['space_amp_log'].append(space_amp_log)
    performance_metrics['compaction_write_GB'].append(compaction_write_GB)
    performance_metrics['compaction_write_rate'].append(compaction_write_rate)
    performance_metrics['compaction_read_GB'].append(compaction_read_GB)
    performance_metrics['compaction_read_rate'].append(compaction_read_rate)
    performance_metrics['compaction_time'].append(compaction_time)
    performance_metrics['lsm_size'].append(lsm_size)
    performance_metrics['read_gb'].append(read_gb)
    performance_metrics['write_gb'].append(write_gb)
    performance_metrics['write_amp'].append(write_amp)
    performance_metrics['write_microsecond_median'].append(write_microsecond_median)
    performance_metrics['write_microsecond_average'].append(write_microsecond_average)



print("Current data_dir:", data_dir)
dirs=os.listdir(data_dir)

# delete the dirs that not start with "with_gc"
dirs = [d for d in dirs if os.path.isdir(os.path.join(data_dir, d))]
blob_gc_ratio = [name.split('_')[3] for name in dirs]
value_size = [name.split('_')[6] for name in dirs]

for data_with_param_dir in dirs:
    print("Current data_with_param_dir:", data_with_param_dir)
    benchmark_log_path = os.path.join(data_dir, data_with_param_dir, benchmark_log_name)
    read_performance(benchmark_log_path)
    
# Create a DataFrame from the performance metrics dictionary
df = pd.DataFrame(performance_metrics, index=blob_gc_ratio)
df.insert(0, 'blob_gc_ratio', blob_gc_ratio)
df.insert(1, 'value_size', value_size)
df = df.sort_values('blob_gc_ratio')
print(df)

# Output to data_dir
output_file = os.path.join(data_dir, "performance_metrics.csv")
df.to_csv(output_file)
print("Output to", output_file)

#!/bin/bash

function call_run_blob() {
  local wal_dir num_keys db_dir output_dir enable_blob_file enable_blob_gc
  wal_dir=$1
  num_keys=$2
  db_dir=$3
  output_dir=$4
  enable_blob_file=${5:1}
  enable_blob_gc=${6:-1}
  local value_size=${7:-1}
  local blob_gc_ratio=${8:-0.3}
  local read_ycsb_file=${9:-"error"}
  local run_workload_name=${10:-"error"}
  local read_systor_files=${11:-"error"}
  local write_buffer_size=$((100 * 1024 * 1024))
  local target_blob_file_size=$((256 * 1024 * 1024))
  echo "db_dir: $db_dir"

  COMPRESSION_TYPE=none BLOB_COMPRESSION_TYPE=none WAL_DIR=$wal_dir \
   NUM_KEYS=$num_keys DB_DIR=$db_dir \
   OUTPUT_DIR=$output_dir ENABLE_BLOB_FILES=$enable_blob_file \
   VALUE_SIZE=$value_size \
   READ_YCSB_FILE=$read_ycsb_file \
   ENABLE_BLOB_GC=$enable_blob_gc  \
   TARGET_BLOB_FILE_SIZE=$target_blob_file_size \
   WRITE_BUFFER_SIZE=$write_buffer_size NUM_THREADS=1 \
   READ_SYSTOR_FILES=$read_systor_files \
   RUN_WORKLOAD_NAME=$run_workload_name \
   BLOB_GC_RATIO=$blob_gc_ratio \
   ./run_blob_bench.sh
}

now_time=$(date +"%Y-%m-%d-%H:%M:%S")

num_keys=5000000
enable_blob_file=1
enable_blob_gc=true
read_ycsb_files=(
  "/mnt/nvme0n1/YCSB-C/data/workloada_50M_0.2_zipfian.log_run.formated"
  # "/mnt/nvme0n1/YCSB-C/data/workloada_50M_0.5_zipfian.log_run.formated"
  "/mnt/nvme0n1/YCSB-C/data/workloada_50M_0.9_zipfian.log_run.formated"
  "/mnt/nvme0n1/YCSB-C/data/workloada_100M_0.2_zipfian.log_run.formated"
  # "/mnt/nvme0n1/YCSB-C/data/workloada_100M_0.5_zipfian.log_run.formated"
  # "/mnt/nvme0n1/YCSB-C/data/workloada_100M_0.9_zipfian.log_run.formated"
)
value_sizes=(4096 1024)
blob_gc_ratios=(0.2 0.25)
# value_sizes=(4096)
# blob_gc_ratios=(0.2)
read_systor_files="/mnt/nvme0n1/YCSB-C/data/systor_write_50M_once_twice_2_3.csv"
run_workload_name="systor"


workload_info="systor_write_50M_once_twice_2_3"
db_info=terarkdb_${now_time}_${workload_info}
db_dir=/mnt/nvme0n1/xq/mlsm/database_comparison/${db_info}
git_result_dir=/mnt/nvme0n1/xq/git_result/rocksdb_kv_sep/result/${db_info}

# if db_dir not exist, create it
if [ ! -d "$db_dir" ]; then
  mkdir -p "$db_dir"
fi

for value_size in ${value_sizes[@]} ; do {
  echo "value_size: $value_size"
  
  for blob_gc_ratio in "${blob_gc_ratios[@]}" ; do {
    echo $blob_gc_ratio
    with_gc_dir=${db_dir}/blob_gc_ratio_${blob_gc_ratio}_value_size_${value_size}
    log_path=${git_result_dir}/blob_gc_ratio_${blob_gc_ratio}_value_size_${value_size}
    log_file_name=${log_path}/log.txt

    # for value_size in "${value_sizes[@]}" ; do
    #   with_gc_dir=${db_dir}/value_size_${value_size}

    # if log_path not exist, create it
    echo "log_path:"
    echo $log_path
    if [ ! -d "$log_path" ]; then
      mkdir -p "$log_path"
    fi

    call_run_blob  $with_gc_dir $num_keys $with_gc_dir $with_gc_dir $enable_blob_file \
      $enable_blob_gc $value_size $blob_gc_ratio "1" $run_workload_name $read_systor_files | tee -a $log_file_name

    echo cp ${with_gc_dir}/benchmark_systor.t1.s1.log $log_path
    cp ${with_gc_dir}/benchmark_systor.t1.s1.log $log_path
    echo cp ${with_gc_dir}/LOG $log_path
    cp ${with_gc_dir}/LOG $log_path

    find $with_gc_dir -type f -name "*.sst" -delete
  } done
} done
python3 ./get_performance.py $db_dir "benchmark_systor.t1.s1.log"
cp ${db_dir}/performance_metrics.csv ${git_result_dir}

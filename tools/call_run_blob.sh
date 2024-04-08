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
  local write_buffer_size=$((100 * 1024 * 1024))
  # echo "db_dir: $db_dir"
  COMPRESSION_TYPE=none BLOB_COMPRESSION_TYPE=none WAL_DIR=$wal_dir \
   NUM_KEYS=$num_keys DB_DIR=$db_dir \
   OUTPUT_DIR=$output_dir ENABLE_BLOB_FILES=$enable_blob_file \
   VALUE_SIZE=$value_size \
   ENABLE_BLOB_GC=$enable_blob_gc  \
   WRITE_BUFFER_SIZE=$write_buffer_size NUM_THREADS=1 \
   BLOB_GC_RATIO=$blob_gc_ratio \
   ./run_blob_bench.sh
}

now_time=$(date +"%Y-%m-%d-%H:%M:%S")
db_info=terarkdb_${now_time}_ycsb_a_100M_0.99_adaptive_sst_file_size
db_dir=/mnt/nvme0n1/xq/mlsm/database_comparison/${db_info}
num_keys=5000000
enable_blob_file=1
enable_blob_gc=true
# value_sizes=(1024 4096 16384 65536)
value_sizes=(4096 1024)
# value_size=1024
git_result_dir=/mnt/nvme1n1/xq/git_result/rocksdb_kv_sep/result/${db_info}
blob_gc_ratios=(0.2 0.4 0.6 0.8 1.0 0.0)

for blob_gc_ratio in "${blob_gc_ratios[@]}" ; do
for value_size in "${value_sizes[@]}" ; do
  echo $blob_gc_ratio
  with_gc_dir=${db_dir}/blob_gc_ratio_${blob_gc_ratio}_value_size_${value_size}
  log_path=${git_result_dir}/blob_gc_ratio_${blob_gc_ratio}_value_size_${value_size}
  log_file_name=${log_path}/log.txt

  # for value_size in "${value_sizes[@]}" ; do
  #   with_gc_dir=${db_dir}/value_size_${value_size}

  # if log_path not exist, create it
  if [ ! -d "$log_path" ]; then
    mkdir -p "$log_path"
  fi

  call_run_blob  $with_gc_dir $num_keys $with_gc_dir $with_gc_dir $enable_blob_file \
    $enable_blob_gc $value_size $blob_gc_ratio | tee -a $log_file_name

  echo cp ${with_gc_dir}/benchmark_ycsb_a.t1.s1.log $log_path
  cp ${with_gc_dir}/benchmark_ycsb_a.t1.s1.log $log_path
  echo cp ${with_gc_dir}/LOG $log_path
  cp ${with_gc_dir}/LOG $log_path

  find $with_gc_dir -type f -name "*.sst" -delete
done
done

python3 ./get_performance.py $db_dir "benchmark_ycsb_a.t1.s1.log"
cp ${db_dir}/performance_metrics.csv ${git_result_dir}

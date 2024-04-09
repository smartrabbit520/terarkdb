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
  local write_buffer_size=$((100 * 1024 * 1024))
  # echo "db_dir: $db_dir"
  COMPRESSION_TYPE=none BLOB_COMPRESSION_TYPE=none WAL_DIR=$wal_dir \
   NUM_KEYS=$num_keys DB_DIR=$db_dir \
   OUTPUT_DIR=$output_dir ENABLE_BLOB_FILES=$enable_blob_file \
   VALUE_SIZE=$value_size \
   READ_YCSB_FILE=$read_ycsb_file \
   ENABLE_BLOB_GC=$enable_blob_gc  \
   WRITE_BUFFER_SIZE=$write_buffer_size NUM_THREADS=1 \
   BLOB_GC_RATIO=$blob_gc_ratio \
   ./run_blob_bench.sh
}

now_time=$(date +"%Y-%m-%d-%H:%M:%S")
now_time=2024-04-09-15:29:46

num_keys=5000000
enable_blob_file=1
enable_blob_gc=true
read_ycsb_files=("/mnt/nvme0n1/YCSB-C/data/workloada_1024kb_100GB_0.9_zipfian.log_run.formated" "/mnt/nvme0n1/YCSB-C/data/workloada_4096kb_100GB_0.9_zipfian.log_run.formated" "/mnt/nvme0n1/YCSB-C/data/workloada_16384kb_100GB_0.9_zipfian.log_run.formated" "/mnt/nvme0n1/YCSB-C/data/workloada_65536kb_100GB_0.9_zipfian.log_run.formated")
value_sizes=(1024 4096 16384 65536)
# value_sizes=(4096)
# value_size=1024
# blob_gc_ratios=(0.2 0.4)
# blob_gc_ratios=(0.6 0.8)
blob_gc_ratios=(1.0 0.0)
# blob_gc_ratios=(0.2 0.4 0.6 0.8 1.0 0.0)

# for value_size in "${value_sizes[@]}" ; do
for ((i=0; i<${#read_ycsb_files[@]}; i++)); do
  value_size=${value_sizes[$i]}
  read_ycsb_file=${read_ycsb_files[$i]}
  db_info=terarkdb_${now_time}_ycsb_a_${value_size}kb_100GB_0.9_zipfian_adaptive_sst_file_size
  db_dir=/mnt/nvme0n1/xq/mlsm/database_comparison/${db_info}
  git_result_dir=/mnt/nvme0n1/xq/git_result/rocksdb_kv_sep/result/${db_info}
for blob_gc_ratio in "${blob_gc_ratios[@]}" ; do
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
    echo "create:"
    echo $log_path
  fi

  call_run_blob  $with_gc_dir $num_keys $with_gc_dir $with_gc_dir $enable_blob_file \
    $enable_blob_gc $value_size $blob_gc_ratio $read_ycsb_file | tee -a $log_file_name

  echo cp ${with_gc_dir}/benchmark_ycsb_a.t1.s1.log $log_path
  cp ${with_gc_dir}/benchmark_ycsb_a.t1.s1.log $log_path
  echo cp ${with_gc_dir}/LOG $log_path
  cp ${with_gc_dir}/LOG $log_path

  find $with_gc_dir -type f -name "*.sst" -delete
done
python3 ./get_performance.py $db_dir "benchmark_ycsb_a.t1.s1.log"
cp ${db_dir}/performance_metrics.csv ${git_result_dir}
done


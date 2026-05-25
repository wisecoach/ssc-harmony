#!/bin/bash

TIME_FOR_FINISH=150


function set_expr_env() {
  env_name=$1
  value=$2
  env_file="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.env"
  sed -i "s/^$env_name=.*$/$env_name=$value/" "$env_file"
}

DATA_HANDLER() {
    cd ../ssc-cli/data_process
    conda run -n data_process python sscc_log_handle/data_handler.py "$@"
    cd -
}

function test_delay() {
  set_expr_env 'EXPERIMENT_TYPE' 'delay'

  set_expr_env 'DELAY' 5
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type delay

  set_expr_env 'DELAY' 15
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type delay

  set_expr_env 'DELAY' 20
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type delay
}

function test_shard() {
  set_expr_env 'EXPERIMENT_TYPE' 'shard_num'
  set_expr_env 'SHARD_NUM' 32
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type shard_num

  set_expr_env 'SHARD_NUM' 16
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type shard_num

  set_expr_env 'SHARD_NUM' 8
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type shard_num

  set_expr_env 'SHARD_NUM' 2
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER --experiment-type shard_num
}

function test_cross_call() {
  set_expr_env 'EXPERIMENT_TYPE' 'cross_call_cnt'
  set_expr_env 'CROSS_CNT' 0
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 1
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 2
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 3
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 4
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 5
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 6
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 7
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 8
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 9
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt

  set_expr_env 'CROSS_CNT' 10
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
  DATA_HANDLER --experiment-type cross_call_cnt
}

function test_throughput() {
  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 25
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 50
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 75
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 100
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 150
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 200
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 250
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER

  set_expr_env 'EXPERIMENT_TYPE' 'throughput'
  set_expr_env 'RATE' 300
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER
}

function test_single() {
  . test/dev_deploy.sh
  test
  test/test.sh
  sleep $TIME_FOR_FINISH
  download_log
  DATA_HANDLER
}

# 用法: source auto_test.sh && test_single [experiment-type]
# 示例: source auto_test.sh && test_single delay

#!/bin/bash

unset -v progdir
case "${0}" in
*/*) progdir="${0%/*}" ;;
*) progdir=. ;;
esac

PROJECT_ROOT="${progdir}/.."
USER=$(whoami)
OS=$(uname -s)

. "${PROJECT_ROOT}/scripts/setup_bls_build_flags.sh"

declare -A harmony_pids
declare -A harmony_exit_codes

function cleanup() {
  "${progdir}/kill_node.sh"
}

function build() {
  if [[ "${NOBUILD}" != "true" ]]; then
    pushd ${PROJECT_ROOT}
    export GO111MODULE=on
    if [[ "$OS" == "Darwin" ]]; then
      # MacOS doesn't support static build
      scripts/go_executable_build.sh -S
    else
      # Static build on Linux platform
      scripts/go_executable_build.sh -s
    fi
    popd
  fi
}

function setup() {
  # Setup blspass file
  mkdir -p ${PROJECT_ROOT}/.hmy
  if [[ ! -f "${PROJECT_ROOT}/.hmy/blspass.txt" ]]; then
    touch "${PROJECT_ROOT}/.hmy/blspass.txt"
  fi

  # Kill nodes if any
  cleanup

  # Note that the binarys only works on MacOS & Linux
  build

  # Create a tmp folder for logs
  mkdir -p "${log_folder}"
  LOG_FILE=${log_folder}/r.log
}

function launch_bootnode() {
  echo "launching boot node ..."
  ${DRYRUN} ${PROJECT_ROOT}/bin/bootnode -port 19875 -max_conn_per_ip 100 -force_public true >"${log_folder}"/bootnode.log 2>&1 | tee -a "${LOG_FILE}" &
  sleep 1
  BN_MA=$(grep "BN_MA" "${log_folder}"/bootnode.log | awk -F\= ' { print $2 } ')
  echo "bootnode launched." + " $BN_MA"
}

function simple_launch_shard() {
    shard_num=$1
    shard=$1
    shard_size=$2
    validator=$2
    ssc=$3
    delay=$4

    env=local
    config="./test/configs/${env}/shard=${shard}_validator=${validator}_ssc=${ssc}_delay=${delay}/launch_config_${env}.txt"
    launch_bootnode

    unset -v base_args
    declare -a base_args args
    declare -A per_shard_cnt
    for ((i=0; i<$shard_num; i++)); do
        per_shard_cnt[$i]=0
    done

    if ${VERBOSE}; then
      verbosity=5
    else
      verbosity=3
    fi

    base_args=(--log_folder "${log_folder}" --min_peers "${MIN}" --bootnodes "${BN_MA}" "--network_type=$NETWORK" --blspass file:"${PROJECT_ROOT}/.hmy/blspass.txt" "--dns=false" "--verbosity=${verbosity}" "--p2p.security.max-conn-per-ip=100")
    sleep 2

    echo $PWD
    while read -r addr ethAddrHex bls_key shard_id ip port submitterKeyPath commitRollbackKeyPath; do
        if [[ "$shard_id" -ge "${shard_num}" ]]; then
          echo "shard_num ${shard_num} is full, skipping node ${i}"
          continue
        fi

        if [[ "${per_shard_cnt[$shard_id]}" -ge "${shard_size}" ]]; then
          echo "shard ${shard_id} is full, skipping node ${i}"
          continue
        fi

        ((per_shard_cnt[$shard_id]=per_shard_cnt[$shard_id]+1))
        echo "Processing node: ${addr} ${bls_key} ${shard_id} ${ip} ${port}"
        echo "shard_id=$shard_id, cnt=${per_shard_cnt[$shard_id]}, num=${shard_num}"

        mode='validator'
        node_config="test/configs/${env}/shard=${shard}_validator=${validator}_ssc=${ssc}_delay=${delay}/default_config_${env}.toml"

        args=("${base_args[@]}" --ip "${ip}" --port "${port}" --key "/tmp/${ip}-${port}.key" --db_dir "${PROJECT_ROOT}/db/db-${ip}-${port}" "--broadcast_invalid_tx=false" \
          --shard_num "${shard_num}" --shard_size "${shard_size}" --run.shard "${shard_id}" --log.verb 4)
        if [[ -z "$ip" || -z "$port" || "$ip" == "#" ]]; then
          echo "skip empty line or node or comment"
          continue
        fi

        if [[ $EXPOSEAPIS == "true" ]]; then
          args=("${args[@]}" "--http.ip=0.0.0.0" "--ws.ip=0.0.0.0")
        fi

        # Setup BLS key for i-th localnet node
        if [[ ! -e "$bls_key" ]]; then
          args=("${args[@]}" --blskey_file "BLSKEY")
        elif [[ -f "$bls_key" ]]; then
          args=("${args[@]}" --blskey_file "${PROJECT_ROOT}/${bls_key}")
          args=("${args[@]}" --ssc.bls-key-path "${PROJECT_ROOT}/${bls_key}")
          args=("${args[@]}" --ssc.self-addr-hex "${ethAddrHex}")
          args=("${args[@]}" --ssc.submitter-key-path "${submitterKeyPath}")
          args=("${args[@]}" --ssc.commit-rollback-key-path "${PROJECT_ROOT}/${commitRollbackKeyPath}")
        elif [[ -d "$bls_key" ]]; then
          args=("${args[@]}" --blsfolder "${PROJECT_ROOT}/${bls_key}")
        else
          echo "skipping unknown node"
          continue
        fi

        # Setup node config for i-th localnet node
        if [[ -f "$node_config" ]]; then
          echo "node ${i} configuration is loaded from: ${node_config}"
          args=("${args[@]}" --config "${node_config}")
        fi

        # Setup flags for i-th node based on config
        case "${mode}" in
        explorer)
          args=("${args[@]}" "--node_type=explorer" "--shard_id=${shard_id}" "--http.rosetta=true" "--run.archive")
          ;;
        archival)
          args=("${args[@]}" --is_archival --run.legacy)
          ;;
        leader)
          args=("${args[@]}" --is_leader --run.legacy)
          ;;
        external)
          ;;
        client)
          args=("${args[@]}" --run.legacy)
          ;;
        validator)
          args=("${args[@]}" --run.legacy "--rpc.debug=true")
          ;;
        esac

        echo "begin to work: dryrun: ${DRYRUN}" "bin: ${PROJECT_ROOT}/bin/harmony" "${args[@]}"

        # Start the node
        ${DRYRUN} "${PROJECT_ROOT}/bin/harmony" "${args[@]}" >> "${log_folder}/log-${port}.log" 2>&1 &

        local pid=$!
        harmony_pids["$pid"]="node_$port"
        harmony_exit_codes["$pid"]="running"
        echo "实例 node_$port 已启动，PID: $pid"

    done <<< "$(cat "${config}")"
}

# 使用wait并行监控
monitor_with_wait() {
    echo "开始监控 ${#harmony_pids[@]} 个Harmony进程..."
    local total_count=${#harmony_pids[@]}
    local i=0

    echo "初始监控列表: ${!harmony_pids[@]}, iteration=$i"
    while [ ${#harmony_pids[@]} -gt 0 ]; do
        i=$((i+1))
        # 创建临时数组用于安全删除
        local pids_to_remove=()

        for pid in "${!harmony_pids[@]}"; do

            # 检查进程是否仍在运行
            if ! kill -0 "$pid" 2>/dev/null; then
                echo "进程 $pid 已结束，尝试获取退出码..."

                wait $pid
                exit_code=$?

                instance_name="${harmony_pids[$pid]}"
                harmony_exit_codes["$pid"]="$exit_code"

                echo "$(date): ${instance_name} (PID: $pid) 已停止，退出码: $exit_code"

                # 标记要删除的PID
                pids_to_remove+=("$pid")
            fi
        done

        # 安全删除已结束的进程
        for pid in "${pids_to_remove[@]}"; do
            unset harmony_pids["$pid"]
            echo "从监控列表中移除 PID: $pid"
        done

        # 显示仍在运行的进程数量
        local running_count=${#harmony_pids[@]}
        if [ $running_count -gt 0 ]; then
            echo "=== 第 ${i} 次检查 === $(date): 仍有 $running_count/$total_count 个进程运行中"
            sleep 10
        else
            echo "所有进程都已停止监控"
        fi
    done

    echo "=== 所有Harmony进程都已停止 ==="
    echo "总共监控了 $total_count 个进程"
}

generate_exit_report() {
    echo "=== Harmony进程退出报告 ==="
    echo "启动时间: $(date)"
    echo "总进程数: ${#harmony_exit_codes[@]}"
    echo ""

    local success_count=0
    local error_count=0

    for pid in "${!harmony_exit_codes[@]}"; do
        instance_name="${harmony_pids[$pid]}"
        exit_code="${harmony_exit_codes[$pid]}"

        if [ "$exit_code" = "0" ]; then
            status="成功"
            ((success_count++))
        else
            status="失败(代码:$exit_code)"
            ((error_count++))
        fi

        echo "实例: ${instance_name}, PID: $pid, 状态: $status"
    done

    echo ""
    echo "总结: 成功 $success_count, 失败 $error_count"
    echo "报告生成时间: $(date)"
}

trap cleanup SIGINT SIGTERM

function usage() {
  local ME=$(basename $0)

  echo "
USAGE: $ME [OPTIONS] config_file_name [extra args to node]

   -h             print this help message
   -D duration    test run duration (default: $DURATION)
   -m min_peers   minimal number of peers to start consensus (default: $MIN)
   -s shards      number of shards (default: $SHARDS)
   -n             dryrun mode (default: $DRYRUN)
   -N network     network type (default: $NETWORK)
   -B             don't build the binary
   -v             verbosity in log (default: $VERBOSE)
   -e             expose WS & HTTP ip (default: $EXPOSEAPIS)

This script will build all the binaries and start harmony and based on the configuration file.

EXAMPLES:

   $ME local_config.txt
"
  exit 0
}

DURATION=60000
MIN=3
SHARDS=2
DRYRUN=
NETWORK=localnet
VERBOSE=false
NOBUILD=false
EXPOSEAPIS=false

while getopts "hD:m:s:nBN:ve" option; do
  case ${option} in
  h) usage ;;
  D) DURATION=$OPTARG ;;
  m) MIN=$OPTARG ;;
  s) SHARDS=$OPTARG ;;
  n) DRYRUN=echo ;;
  B) NOBUILD=true ;;
  N) NETWORK=$OPTARG ;;
  v) VERBOSE=true ;;
  e) EXPOSEAPIS=true ;;
  *) usage ;;
  esac
done

shift $((OPTIND - 1))

ORG_ROOT=$PROJECT_ROOT/../

if [ -f "$ORG_ROOT/.env" ] ; then
  source $ORG_ROOT/.env
else
  echo "No .env file found in ${ORG_ROOT}"
  exit 1
fi

shard=${SHARD_NUM:-4}
validator=${VALIDATOR:-4}
ssc=${SSC:-1}
delay=${DELAY:-5}
rate=${RATE:-1}
exam="shard=${shard}_validator=${validator}_ssc=${ssc}_delay=${delay}_rate=${rate}"
log_folder="${PROJECT_ROOT}/tmp_log/$exam"

echo "perform exam: $exam"

shift 1 || usage
unset -v extra_args
declare -a extra_args

setup
simple_launch_shard $shard $validator $ssc $delay
#monitor_with_wait
#generate_exit_report

#cleanup || true

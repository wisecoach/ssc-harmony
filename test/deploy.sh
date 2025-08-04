#!/bin/bash
set -eo pipefail

unset -v progdir
case "${0}" in
*/*) progdir="${0%/*}" ;;
*) progdir=. ;;
esac

ROOT="${progdir}/.."
USER=$(whoami)
OS=$(uname -s)

. "${ROOT}/scripts/setup_bls_build_flags.sh"

function cleanup() {
  "${progdir}/kill_node.sh"
}

function build() {
  if [[ "${NOBUILD}" != "true" ]]; then
    pushd ${ROOT}
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
  mkdir -p ${ROOT}/.hmy
  if [[ ! -f "${ROOT}/.hmy/blspass.txt" ]]; then
    touch "${ROOT}/.hmy/blspass.txt"
  fi

  # Kill nodes if any
  cleanup

  # Note that the binarys only works on MacOS & Linux
  build

  # Create a tmp folder for logs
  t=$(date +"%Y%m%d-%H%M%S")
  log_folder="${ROOT}/tmp_log/log-$t"
  mkdir -p "${log_folder}"
  LOG_FILE=${log_folder}/r.log
}

function launch_bootnode() {
  echo "launching boot node ..."
  ${DRYRUN} ${ROOT}/bin/bootnode -port 19875 -max_conn_per_ip 100 -force_public true >"${log_folder}"/bootnode.log 2>&1 | tee -a "${LOG_FILE}" &
  sleep 1
  BN_MA=$(grep "BN_MA" "${log_folder}"/bootnode.log | awk -F\= ' { print $2 } ')
  echo "bootnode launched." + " $BN_MA"
}

function simple_launch_shard() {
    env=${3-local}
    config=./test/configs/launch_config_${env}.txt
    launch_bootnode

    shard_num=$1
    shard_size=$2

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

    base_args=(--log_folder "${log_folder}" --min_peers "${MIN}" --bootnodes "${BN_MA}" "--network_type=$NETWORK" --blspass file:"${ROOT}/.hmy/blspass.txt" "--dns=false" "--verbosity=${verbosity}" "--p2p.security.max-conn-per-ip=100")
    sleep 2

    echo $PWD
    while read -r addr bls_key shard_id ip port; do
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
        node_config='test/configs/default_config.toml'

        args=("${base_args[@]}" --ip "${ip}" --port "${port}" --key "/tmp/${ip}-${port}.key" --db_dir "${ROOT}/db/db-${ip}-${port}" "--broadcast_invalid_tx=false" --shard_num "${shard_num}" --shard_size "${shard_size}" --run.shard "${shard_id}")
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
          args=("${args[@]}" --blskey_file "${ROOT}/${bls_key}")
          # 同时设置ssc.bls-key-file
          args=("${args[@]}" --ssc.bls-key-path "${ROOT}/${bls_key}")
        elif [[ -d "$bls_key" ]]; then
          args=("${args[@]}" --blsfolder "${ROOT}/${bls_key}")
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

        echo "begin to work: dryrun: ${DRYRUN}" "bin: ${ROOT}/bin/harmony" "${args[@]}" "${extra_args[@]}"

        # Start the node
        ${DRYRUN} "${ROOT}/bin/harmony" "${args[@]}" "${extra_args[@]}" 2>&1 | tee -a "${LOG_FILE}" &
    done <<< "$(cat "${config}")"
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

config=$1
shift 1 || usage
unset -v extra_args
declare -a extra_args
extra_args=("$@")

setup
#launch_localnet
#launch_same_account_shard_net 2 5
simple_launch_shard 4 5 local
sleep "${DURATION}"
cleanup || true

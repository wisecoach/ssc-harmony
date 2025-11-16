#!/usr/bin/env bash

SERVERS=("zjnu@10.7.95.200" "zjnu@10.7.95.201" "zjnu@10.7.95.202" "zjnu@10.7.95.203")
WORK_DIR="/home/zjnu/go/src/github.com/harmony-one/ssc-harmony"
ROOT=$WORK_DIR
t=$(date +"%Y%m%d-%H%M%S")
log_folder="${ROOT}/tmp_log/log-$t"
LOG_FILE="$log_folder/r.log"
#RECOMPILE=false
RECOMPILE=true
MIN=3
NETWORK=exprnet

function upload_for_shards() {
    file=$1
    for SERVER in "${SERVERS[@]}"; do
        echo "Upload to $SERVER: $file"
        scp -r -P 10022 $file "$SERVER:$file"
    done
}

function call_for_shard() {
    shard_id=$1
    cmd=$2
    ssh -p 10022 "${SERVERS[$shard_id]}" "cd $WORK_DIR && $cmd"
}

function call_for_shards() {
    cmd=$1
    for SERVER in "${SERVERS[@]}"; do
        echo "Executing on $SERVER: $cmd"
        ssh -p 10022 "$SERVER" "cd $WORK_DIR && $cmd"
    done
}

function clean() {
    call_for_shards "./test/kill_node.sh; rm -rf tmp_log* 2> /dev/null; rm *.rlp 2> /dev/null; rm -rf .dht* 2> /dev/null"
}

function preset() {
    call_for_shards "mkdir -p ${log_folder}; touch $LOG_FILE"

    if ${RECOMPILE}; then
      scripts/go_executable_build.sh -S || exit 1


      # upload binary to servers
      for SERVER in "${SERVERS[@]}"; do
          echo "Uploading binary to $SERVER"
          scp -P 10022 ./bin/harmony "$SERVER:$WORK_DIR/bin/"
          scp -P 10022 ./bin/bootnode "$SERVER:$WORK_DIR/bin/"
      done

      # upload binary to servers
      for SERVER in "${SERVERS[@]}"; do
          echo "Uploading binary to $SERVER"
          scp -P 10022 ./bin/harmony "$SERVER:$WORK_DIR/bin/"
          scp -P 10022 ./bin/bootnode "$SERVER:$WORK_DIR/bin/"
      done
    fi
}

function launch_bootnode() {
  cmd="nohup bin/bootnode -ip 0.0.0.0 -port 19875 -max_conn_per_ip 100 -force_public true > ${log_folder}/bootnode.log 2>&1 | tee -a ${LOG_FILE} &"
  echo "launching boot node ... cmd: $cmd"
  call_for_shard 0 "$cmd" &
  sleep 1
  echo "bootnode launched."
}

function deploy() {
    shard_num=$1
    shard=$1
    shard_size=$2
    validator=$2
    ssc=$3
    delay=$4
    env="dev"

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

    base_args=(--log_folder "${log_folder}" --min_peers "${MIN}" "--network_type=$NETWORK" --blspass file:"${ROOT}/.hmy/blspass.txt" "--dns=false" "--p2p.security.max-conn-per-ip=100")
    sleep 2

    echo $PWD
    while read -r addr ethAddrHex bls_key shard_id ip port; do
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
        node_config="test/configs/${env}/default_config_${env}.toml"

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
          args=("${args[@]}" --ssc.bls-key-path "${ROOT}/${bls_key}")
          args=("${args[@]}" --ssc.self-addr-hex "${ethAddrHex}")
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

        cmd="nohup ${ROOT}/bin/harmony ${args[@]} ${extra_args[@]} > /dev/null &"
        echo "begin to work: $cmd"
        call_for_shard $shard_id "$cmd" &

    done <<< "$(cat "${config}")"
}

function download_log() {
    echo "Downloading logs from all servers..."
    mkdir -p "${log_folder}/logs"
    for SERVER in "${SERVERS[@]}"; do
        echo "Downloading logs from $SERVER"
        scp -r -P 10022 "$SERVER:$WORK_DIR/tmp_log/*" "logs/"
    done
    echo "Logs downloaded to ${log_folder}/logs"
}

function test() {
    shard=$1
    validator=$2
    ssc=$3
    delay=$4
    clean
    preset
    deploy $shard $validator $ssc $delay
}

shard=${1-4}
validator=${2-4}
ssc=${3-1}
delay=${4-5}

test $shard $validator $ssc $delay

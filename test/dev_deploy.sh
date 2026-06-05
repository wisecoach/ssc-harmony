#!/usr/bin/env bash

WORK_DIR=$PWD
PROJECT_ROOT=$WORK_DIR
ORG_ROOT=$WORK_DIR/../
REMOTE_BASE="/home/zjnu/go/src/github.com/harmony-one"

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
cross_cnt=${CROSS_CNT:-1}
[[ $cross_cnt -eq -1 ]] && cc_suffix="" || cc_suffix="_cc=$cross_cnt"
validator_per_node=${VALIDATOR_PER_NODE:-4}
exam="shard=${shard}_validator=${validator}_ssc=${ssc}_delay=${delay}_rate=${rate}${cc_suffix}_vpn=${validator_per_node}"
log_folder="${PROJECT_ROOT}/tmp_log/$exam"
LOG_FILE="$log_folder/r.log"
RECOMPILE=true
MIN=3
NETWORK=exprnet
needed_servers=$((shard * validator / validator_per_node))
selected_servers=$(jq -r --argjson n "$needed_servers" '.servers[0:$n][]' $ORG_ROOT/dev_servers.json)
readarray -t SERVERS <<< "$selected_servers"

client_num=${CLIENT_NUM:-1}
selected_clients=$(jq -r --argjson n "$client_num" '.servers[0:$n][]' $ORG_ROOT/dev_clients.json)
readarray -t CLIENTS <<< "$selected_clients"

function upload_for_servers() {
    file=$1
    for SERVER in "${SERVERS[@]}"; do
        echo "Upload to $SERVER: $file"
        scp -r -P 10022 $file "$SERVER:$file"
    done
}

function call_for_servers() {
    cmd=$1
    for SERVER in "${SERVERS[@]}"; do
        echo "Executing on $SERVER: $cmd"
        ssh -p 10022 "$SERVER" "cd $WORK_DIR && $cmd"
    done
}

function call_for_server() {
    index=$1
    cmd=$2
    ssh -p 10022 "${SERVERS[$index]}" "cd $WORK_DIR && $cmd"
}

function call_for_validator() {
    index=$1
    cmd=$2
    server_index=$(($index / $validator_per_node))
    ssh -p 10022 "${SERVERS[$server_index]}" "cd $WORK_DIR && $cmd"
}

function clean() {
    call_for_servers "./test/kill_node.sh; rm -rf tmp_log* 2> /dev/null; rm *.rlp 2> /dev/null; rm -rf .dht* 2> /dev/null; mkdir -p ${log_folder}; touch $LOG_FILE"
}

function preset() {
    if ${RECOMPILE}; then
      scripts/go_executable_build.sh -S || exit 1

      # upload binary to servers
      for SERVER in "${SERVERS[@]}"; do
          echo "Uploading binary to $SERVER"
          rsync -av -e "ssh -p 10022" ./bin "$SERVER:$WORK_DIR/"
      done
    fi
}

function launch_bootnode() {
  for SERVER in "${SERVERS[@]}"; do
    echo "Launching bootnode on $SERVER"
    ssh -n -f -T -p 10022 "$SERVER" "
      cd $WORK_DIR &&
      nohup bin/bootnode -ip 0.0.0.0 -port 19875 -max_conn_per_ip 100 -force_public true \
        > ${log_folder}/bootnode.log 2>&1 < /dev/null &
    "
  done
}

function deploy() {
    shard_num=$1
    shard=$1
    shard_size=$2
    validator=$2
    ssc=$3
    delay=$4
    env="dev"

    config="./test/configs/${env}/shard=${shard}_validator=${validator}_ssc=${ssc}_delay=${delay}_vpn=${validator_per_node}/launch_config_${env}.txt"
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

    base_args=(--log_folder "${log_folder}" --min_peers "${MIN}" "--network_type=$NETWORK" --blspass file:"${PROJECT_ROOT}/.hmy/blspass.txt" "--dns=false" "--p2p.security.max-conn-per-ip=100")
    sleep 2

    validator_index=0
    echo "using launch config: "
    mapfile -t lines < "${config}"
    for line in "${lines[@]}"; do
        echo "process config line: $line"
        # 跳过空行或注释
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

        read -r addr ethAddrHex bls_key shard_id ip port submitterKeyPath commitRollbackKeyPath <<< "$line"
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
        node_config="test/configs/${env}/shard=${shard}_validator=${validator}_ssc=${ssc}_delay=${delay}_vpn=${validator_per_node}/default_config_${env}.toml"

        bootnode="/ip4/10.7.95.202/tcp/19875/p2p/Qmc1V6W7BwX8Ugb42Ti8RnXF1rY5PF7nnZ6bKBryCgi6cv"
        args=("${base_args[@]}" --ip "${ip}" --port "${port}" --key "/tmp/${ip}-${port}.key" --db_dir "${PROJECT_ROOT}/db/db-${ip}-${port}" \
          "--broadcast_invalid_tx=false" --shard_num "${shard_num}" --shard_size "${shard_size}" --run.shard "${shard_id} --bootnodes=$bootnode")
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
          echo "node ${ip} configuration is loaded from: ${node_config}"
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

        cmd="ulimit -n 65535 && nohup ${PROJECT_ROOT}/bin/harmony ${args[@]} >> "${log_folder}/log-${port}.log" 2>&1 &"
        echo "begin to work: $cmd"
        call_for_validator $validator_index "$cmd" < /dev/null &

        # Start CPU monitoring (one per server, batch-mode top at 2s interval)
        if [ $(($validator_index % $validator_per_node)) -eq 0 ]; then
            monitor_cmd='nohup top -b -d 2 >"'"${log_folder}"'/cpu-monitor.log" 2>&1 &'
            ssh -p 10022 "${SERVERS[$(($validator_index / $validator_per_node))]}" "cd $WORK_DIR && $monitor_cmd" < /dev/null &
        fi

        validator_index=$(($validator_index + 1))
    done
}

function download_log() {
    echo "Downloading logs from all servers..."
    mkdir -p "${log_folder}/logs"
    for SERVER in "${SERVERS[@]}"; do
        echo "Downloading logs from $SERVER"
        scp -r -P 10022 "$SERVER:$WORK_DIR/tmp_log/*" "$ORG_ROOT/logs/harmony-sscc"
    done
    echo "Logs downloaded to ${log_folder}/logs"
}

function test() {
    clean
    preset
    deploy $shard $validator $ssc $delay
}

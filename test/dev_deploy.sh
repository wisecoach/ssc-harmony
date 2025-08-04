#!/usr/bin/env bash

SERVERS=("zjnu@10.7.95.200" "zjnu@10.7.95.201" "zjnu@10.7.95.202" "zjnu@10.7.95.203")
WORK_DIR="/home/zjnu/ssc-harmony"

function call_for_shard() {
    shard_id=$1
    cmd=$2
    ssh -p 10022 "$SERVERS[$shard_id]" "cd $WORK_DIR && $cmd"
}

function call_for_shards() {
    cmd=$1
    for SERVER in "${SERVERS[@]}"; do
        echo "Executing on $SERVER: $cmd"
        ssh -p 10022 "$SERVER" "cd $WORK_DIR && $cmd"
    done
}

function clean() {
    call_for_shards "./test/kill_node.sh"
    call_for_shards "rm -rf tmp_log* 2> /dev/null"
    call_for_shards "rm *.rlp 2> /dev/null"
    call_for_shards "rm -rf .dht* 2> /dev/null"
}

function preset() {
    scripts/go_executable_build.sh -S || exit 1

    # upload binary to servers
    for SERVER in "${SERVERS[@]}"; do
        echo "Uploading binary to $SERVER"
        scp -P 10022 ./bin/harmony "$SERVER:$WORK_DIR/bin/"
        scp -P 10022 ./bin/bootnode "$SERVER:$WORK_DIR/bin/"
    done
}

function launch_bootnode() {
  echo "launching boot node ..."
  call_for_shard 0 'bin/bootnode -port 19875 -max_conn_per_ip 100 -force_public true >"${log_folder}"/bootnode.log 2>&1 | tee -a "${LOG_FILE}" &'
  sleep 1
  BN_MA=$(grep "BN_MA" "${log_folder}"/bootnode.log | awk -F\= ' { print $2 } ')
  echo "bootnode launched."
}

function deploy() {
    env=${3-dev}
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

        echo "begin to work:" "bin: ${ROOT}/bin/harmony" "${args[@]}" "${extra_args[@]}"

        call_for_shard $shard_id '"${ROOT}/bin/harmony" "${args[@]}" "${extra_args[@]}" 2>&1 | tee -a "${LOG_FILE}" &'

    done <<< "$(cat "${config}")"
}

function test() {
    clean
    preset
    deploy 4 5 dev
}
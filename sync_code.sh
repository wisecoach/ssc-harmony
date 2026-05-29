#!/bin/bash

function uploadCode() {
  rsync -av -e "ssh -p 10022" \
    --exclude='.dht-127.0.0.1-19875' \
    --exclude='tmp_log' \
    --exclude='.idea' \
    --exclude='bin' \
    --exclude='db' \
    --exclude='.vscode' \
    --exclude='.gitignore' \
    --exclude='.gitmodules' \
    --exclude='.travis.yml' \
    --exclude='.editorconfig' \
    --exclude='.golangci.yml' \
    --exclude='.goreleaser.yml' \
    --exclude='.golangci.yml' \
    --exclude='.goreleaser.yml' \
    --exclude='.gitignore' \
    --exclude="logs" \
    --exclude='output' \
    ./ $1:/home/zjnu/go/src/github.com/harmony-one/harmony-sscc
}

function uploadCodes() {
  uploadCode zjnu@10.7.95.199
  shard=32
  selected_servers=$(jq -r --argjson n "$shard" '.servers[0:$n][]' ../dev_servers.json)
  readarray -t SERVERS <<< "$selected_servers"
  for server in "${SERVERS[@]}"; do
    echo "Uploading code to $server"
    uploadCode $server
  done
}

function cleanCode() {
  ssh $1 -p 10022 "rm -rf /home/zjnu/go/src/github.com/harmony-one/harmony-sscc/.hmy"
  ssh $1 -p 10022 "rm -rf /home/zjnu/go/src/github.com/harmony-one/harmony-sscc/test/configs"
}

function cleanCodes() {
  cleanCode zjnu@10.7.95.199
  shard=32
  selected_servers=$(jq -r --argjson n "$shard" '.servers[0:$n][]' ../dev_servers.json)
  readarray -t SERVERS <<< "$selected_servers"
  for server in "${SERVERS[@]}"; do
    echo "Cleaning code from $server"
    cleanCode $server
  done
}

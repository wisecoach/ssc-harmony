#!/usr/bin/env bash

shard=${1-4}
validator=${2-4}
ssc=${3-1}
delay=${4-5}

./test/kill_node.sh
rm -rf tmp_log* 2> /dev/null
rm *.rlp 2> /dev/null
rm -rf .dht* 2> /dev/null
scripts/go_executable_build.sh -S || exit 1  # d
./test/deploy.sh -B -D 600000  $shard $validator $ssc $delay

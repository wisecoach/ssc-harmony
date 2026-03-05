#!/bin/bash

source ../.env

filter=$1
cd tmp_log/shard="$SHARD_NUM"_validator="$VALIDATOR"_ssc="$SSC"_delay="$DELAY"_rate="$RATE"
ls | grep "log-" | xargs cat | grep -n "$1"
cd -

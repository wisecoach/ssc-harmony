#!/bin/bash

cd ../ssc-cli/cli
node deploySSCCTest.js
cd -
cd ../ssc-cli/cli-py
./simulate_sscc_remote.sh
cd -

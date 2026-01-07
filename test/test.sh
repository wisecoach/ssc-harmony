#!/bin/bash

cd ../ssc-cli/cli
node deploySSCTest.js
cd -
cd ../ssc-cli/cli-py
#./simulate_ssc_remote.sh
python simulate_ssc.py
cd -

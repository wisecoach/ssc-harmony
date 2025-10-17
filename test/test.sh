#!/bin/bash

node ../ssc-cli/cli/deploySSCTest.js
cd ../ssc-cli/cli-py
python simulate.py
cd -

notify-send "SSC Test" "SSC test completed successfully."
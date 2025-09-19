#!/bin/bash

node ../cli/deploySSCTest.js
cd ../cli-py
python ../cli-py/simulate.py
cd -

notify-send "SSC Test" "SSC test completed successfully."
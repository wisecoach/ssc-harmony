#!/usr/bin/env python3
import sys, os, glob, re
LOGDIR=sys.argv[1]
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
print("# files:", len(files))
target='0x6ae423bfd9b0bc808ea69fcc2a95c21b04fb6cb47cdad455d305c47488761de7'
shown_fail=0; shown_add=0
for f in files:
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            if 'failed to get header' in line and shown_fail<3:
                print("FAIL LINE:", line[:300]); shown_fail+=1
            if 'Added New Block' in line and shown_add<2:
                print("ADD LINE:", line[:300]); shown_add+=1
            if target in line and 'failed' not in line:
                print("TARGET-OTHER:", line[:300])
print("done")

#!/usr/bin/env python3
import sys, os, glob, re
LOGDIR=sys.argv[1]; TX=sys.argv[2].lower(); PORT=sys.argv[3]
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
for f in files:
    port=None
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            m=re.search(r'"port":"?(\d+)"?',line)
            if m: port=m.group(1)
            if port!=PORT or TX not in line.lower(): continue
            if 'build commit simulation' in line:
                print("BUILD:", line[:500])
            elif 'simTxSubmit' in line:
                print("SUBMIT:", line[:250])
            elif 'failed to verify' in line:
                print("FAIL:", line[:250])

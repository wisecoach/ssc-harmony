#!/usr/bin/env python3
import sys, os, glob, re
LOGDIR=sys.argv[1]; TX=sys.argv[2].lower(); PORT=sys.argv[3]
from collections import Counter
c=Counter()
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
for f in files:
    port=None
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            m=re.search(r'"port":"?(\d+)"?',line)
            if m: port=m.group(1)
            if port!=PORT or TX not in line.lower(): continue
            for key in ('build commit simulation','CommitSimulation: called','simTxSubmit','begin to verify simulation',
                        'verify call state','failed to verify','AddOnChainPatch','simulation is valid','CommitSimulation: self is leader'):
                if key in line:
                    c[key]+=1
print("PORT",PORT,"TX",TX[:20])
for k,v in c.most_common():
    print(f"  {v:>5d}  {k}")

#!/usr/bin/env python3
import sys, os, glob, re, json
LOGDIR=sys.argv[1]; ORIGIN=sys.argv[2].lower()
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
# find internal tx hashes (sscHash) for this origin
sschashe=set()
for f in files:
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            if ORIGIN in line.lower() and 'submitting internal tx' in line:
                m=re.search(r'"sscHash":"(0x[0-9a-f]+)"',line)
                if m: sschashe.add(m.group(1).lower())
print("internal tx hashes:", sschashe)
# for each sscHash, see where broadcast/received
for h in sschashe:
    print(f"\n=== sscHash {h} ===")
    for f in files:
        port=None
        with open(f,'r',errors='replace') as fh:
            for line in fh:
                m=re.search(r'"port":"?(\d+)"?',line)
                if m: port=m.group(1)
                if h in line.lower():
                    if 'tryBroadcastSSCInternalTx' in line:
                        print(f"  [{port}] BROADCAST {line.strip()[:160]}")
                    elif 'received but no sink' in line or 'received' in line and 'SSCInternalTx' in line:
                        print(f"  [{port}] RECV {line.strip()[:160]}")
                    elif 'submitting internal tx' in line:
                        print(f"  [{port}] SUBMIT {line.strip()[:160]}")

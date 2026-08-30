#!/usr/bin/env python3
import sys, os, glob, re, json
from collections import Counter, defaultdict
LOGDIR=sys.argv[1]
TX=sys.argv[2].lower()
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
print("files:", len(files))
# gather where tx appears, with port + message + key fields
seen=defaultdict(list)  # port -> list of (level,msg)
for f in files:
    port=None
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            m=re.search(r'"port":"?(\d+)"?',line)
            if m: port=m.group(1)
            if TX in line.lower():
                try:
                    d=json.loads(line[line.find('{'):])
                except Exception:
                    continue
                msg=d.get('message','')
                if 'failed to get header' in msg:
                    key='FAIL_GET_HEADER'
                elif 'startCXT' in msg or 'start simulation' in msg or 'StartSimulate' in msg:
                    key='START'
                elif 'verify' in msg.lower() and 'simulation' in msg.lower():
                    key='VERIFY'
                elif 'commit simulation' in msg or 'build commit' in msg or 'simTxSubmit' in msg:
                    key='COMMIT_SIM'
                elif 'call' in msg.lower():
                    key='CALL'
                else:
                    key='OTHER'
                # only keep some
                if len(seen[port])<400:
                    seen[port].append((key,msg[:100],d.get('originShard'),d.get('callIndex')))
# summarize
print("\nper-port message key counts:")
for port,lst in sorted(seen.items()):
    c=Counter(k for k,_,_,_ in lst)
    print(f"  port {port}: {dict(c)}")
print("\nselected samples:")
for port,lst in sorted(seen.items()):
    # print a FAIL sample and a START/COMMIT sample
    for want in ('FAIL_GET_HEADER','START','COMMIT_SIM','VERIFY'):
        for k,m,o,ci in lst:
            if k==want:
                print(f"  [{port}] {k} origin={o} call={ci} :: {m}")
                break

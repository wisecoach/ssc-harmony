#!/usr/bin/env python3
import sys, os, glob, re, json
LOGDIR=sys.argv[1]; TX=sys.argv[2].lower(); PORT=sys.argv[3] if len(sys.argv)>3 else None
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
for f in files:
    port=None
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            m=re.search(r'"port":"?(\d+)"?',line)
            if m: port=m.group(1)
            if PORT and port!=PORT: continue
            if TX in line.lower():
                try:
                    d=json.loads(line[line.find('{'):])
                except Exception:
                    continue
                msg=d.get('message','')
                # focus on verify / callstate / fail / origin
                print(f"[{port}] {msg[:160]} | origin={d.get('originShard')} call={d.get('callIndex')} simNum={d.get('simulationNum')} shard={d.get('shardId')} err={(d.get('error') or '')[:80]}")

#!/usr/bin/env python3
import sys, os, glob, re, json
from collections import Counter, defaultdict
LOGDIR=sys.argv[1]
files=sorted(glob.glob(os.path.join(LOGDIR,'ssc-validator-*.log')))
# also include harmony/zero logs? try all files in dir
allfiles = sorted(glob.glob(os.path.join(LOGDIR,'*.log')))
print("# validator files:", len(files), " total log files:", len(allfiles))

fail=Counter(); fail_port=Counter(); fail_by_port=Counter()
pat=re.compile(r'failed to get header for block hash (0x[0-9a-fA-F]+)')
pp=re.compile(r'"port":"?(\d+)"?')
# first pass: count failures + ports
for f in files:
    port=None
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            m=pp.search(line)
            if m: port=m.group(1)
            h=pat.search(line)
            if h:
                hx=h.group(1).lower()
                fail[hx]+=1
                if port: fail_port[port]+=1
                fail_by_port[(port,hx)]+=1

print("\n# failure count by port (shard):")
for p,c in fail_port.most_common():
    shard=(int(p)-9000)//40 if p and int(p)>=9000 else '?'
    print(f"  port={p} (shard {shard}): {c}")

print("\n# distinct failing hashes:", len(fail))
print("top 8 by freq:")
for hx,c in fail.most_common(8):
    ports=sorted({p for (p,h) in fail_by_port if h==hx and p})
    print(f"  {c:>6d}  {hx}  ports={ports}")

# second pass: for the top hash, find block number by scanning ALL logs (incl zero/harmony)
target=fail.most_common(1)[0][0]
print(f"\n===== target {target} (count={fail[target]}) =====")
# search for block number: look for this hash near 'number' in any line
for f in allfiles:
    with open(f,'r',errors='replace') as fh:
        for line in fh:
            if target in line:
                # try to parse json fields number/blockNum
                try:
                    d=json.loads(line[line.find('{'):])
                    if 'failed' not in line and ('number' in d or 'blockNum' in d or 'hash' in d):
                        print("  ctx:", {k:d.get(k) for k in ('level','port','number','blockNum','block','epoch','viewID','message') if k in d})
                except Exception:
                    pass

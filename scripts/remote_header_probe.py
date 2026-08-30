#!/usr/bin/env python3
"""Pick the most frequent 'failed to get header for block hash' and dissect it."""
import sys, os, json, glob, re
from collections import Counter, defaultdict

LOGDIR = sys.argv[1] if len(sys.argv) > 1 else '.'
HASH_ARG = sys.argv[2] if len(sys.argv) > 2 else None

files = sorted(glob.glob(os.path.join(LOGDIR, 'ssc-validator-*.log')))
print(f"# files: {len(files)}")

# 1) collect failing hashes + per-port counts
fail = Counter()          # hash -> total
fail_by_port = defaultdict(Counter)  # hash -> port -> count
pat = re.compile(r'failed to get header for block hash (0x[0-9a-fA-F]+)')
for f in files:
    port = None
    with open(f, 'r', errors='replace') as fh:
        for line in fh:
            m = re.search(r'"port":(\d+)', line)
            if m:
                port = m.group(1)
            h = pat.search(line)
            if h:
                hx = h.group(1).lower()
                fail[hx] += 1
                fail_by_port[hx][port] += 1

if not fail:
    print("no failed-to-get-header entries found")
    sys.exit(0)

print(f"\n# distinct failing hashes: {len(fail)}")
print("top 12 by frequency:")
for hx, c in fail.most_common(12):
    ports = dict(fail_by_port[hx])
    print(f"  {c:>7d}  {hx}  ports={ports}")

# 2) pick target hash
target = HASH_ARG.lower() if HASH_ARG else fail.most_common(1)[0][0]
print(f"\n===== dissecting hash: {target} (count={fail.get(target,0)}) =====")

# 3) search every log for this hash in various contexts
ctx = defaultdict(Counter)
blocknum_hit = None
added_where = []
for f in files:
    port = None
    with open(f, 'r', errors='replace') as fh:
        for line in fh:
            m = re.search(r'"port":(\d+)', line)
            if m:
                port = m.group(1)
            if target in line:
                if 'failed to get header' in line:
                    ctx['failed_to_get_header'][port] += 1
                elif 'Added New Block to Blockchain' in line or 'Added New Block' in line:
                    ctx['added_new_block'][port] += 1
                    added_where.append(port)
                    # try to grab block number from same line
                    bm = re.search(r'"blockNum":(\d+)', line) or re.search(r'blockNum:(\d+)', line)
                    if bm:
                        blocknum_hit = bm.group(1)
                elif 'block hash' in line.lower() or 'blockHash' in line:
                    ctx['other_blockhash_ref'][port] += 1
                else:
                    ctx['other'][port] += 1

print("\ncontext occurrences of target hash:")
for k, c in sorted(ctx.items()):
    print(f"  {k:28s} total={sum(c.values())} by_port={dict(c)}")
if added_where:
    print(f"\n  -> hash found in 'Added New Block' on ports: {added_where}")
    print(f"  -> blockNum (if any): {blocknum_hit}")
else:
    print("\n  -> hash NOT found in any 'Added New Block' log (never seen committed in these logs)")

# 4) find the block number via canonical db query if possible: try rpc? skip.
#    Instead, look for the hash in any 'blockHash' json field with block number nearby
print("\n# sample lines mentioning target (first 5):")
shown = 0
for f in files:
    with open(f, 'r', errors='replace') as fh:
        for line in fh:
            if target in line and 'failed to get header' not in line:
                print("  ", line[:220])
                shown += 1
                if shown >= 5:
                    break
    if shown >= 5:
        break
if shown == 0:
    print("  (none besides the failure lines)")

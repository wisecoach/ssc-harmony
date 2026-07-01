import glob, json, sys

DIR = "shard=4_validator=4_ssc=1_delay=20_rate=100_vpn=4"
files = glob.glob(DIR + "/ssc-validator*.log")

# Collect all user txs and their add time
all_txs = {}
for fn in files:
    with open(fn) as f:
        for line in f:
            try:
                d = json.loads(line)
                msg = d.get("message","")
                if "add a cross shard Tx" in msg and "simulation: false" in msg and "preCompiled: false" in msg:
                    txh = d.get("txHash","")
                    if txh and txh not in all_txs:
                        all_txs[txh] = {
                            "time": d.get("time",""),
                            "nonce": d.get("nonce", "?"),
                            "port": d.get("port","?"),
                        }
            except: pass

# Collect closed txs
closed_txs = set()
for fn in files:
    with open(fn) as f:
        for line in f:
            try:
                d = json.loads(line)
                if "leader close transaction" in d.get("message",""):
                    closed_txs.add(d.get("txHash",""))
            except: pass

unfinished = {k:v for k,v in all_txs.items() if k not in closed_txs}

# Print nonce distribution
from collections import Counter
nonces = Counter()
for tx, info in unfinished.items():
    nonces[str(info["nonce"])] += 1

print(f"Unfinished: {len(unfinished)}")
print(f"Nonce distribution: {dict(nonces)}")

# Print first 5 with full info
unfinished_sorted = sorted(unfinished.items(), key=lambda x: x[1]["time"])
print("\nFirst 5 unfinished txs:")
for tx, info in unfinished_sorted[:5]:
    print(f"  time={info['time'][-25:]} nonce={info['nonce']} port={info['port']} tx={tx}")

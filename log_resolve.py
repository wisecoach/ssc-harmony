#!/home/wisecoach/miniconda3/bin/python
import pandas as pd
import os
import json

for log_dir in os.listdir("./tmp_log"):
    with open(f'./tmp_log/{log_dir}/r.log', 'r') as fp:
        for line in fp.readlines():
            if 'begin to verify simulation' in line:
                try:
                    log_obj = json.loads(line)
                    tx_hash = log_obj['txHash']
                    break
                except Exception:
                    continue
    os.system(f'cat ./tmp_log/{log_dir}/r.log | grep {tx_hash} > tx.log')
    data = []
    with open('tx.log', 'r') as fp:
        for line in fp.readlines():
            try:
                log_obj = json.loads(line)
                data.append(log_obj)
            except Exception:
                continue
    df = pd.DataFrame(data, columns=['level', 'message', 'port', 'error', 'txHash', 'caller', 'time'])

df.to_excel("log.xlsx")
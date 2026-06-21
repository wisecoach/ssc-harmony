#!/home/wisecoach/miniconda3/bin/python
import pandas as pd
import os
import json

for log_dir in os.listdir("./tmp_log"):
    with open(f'./tmp_log/{log_dir}/ssc-validator-127.0.0.1-9000.log', 'r') as fp:
        for line in fp.readlines():
            log_obj = json.loads(line)
            if 'txHash' in log_obj:
                tx_hash = log_obj['txHash']
                break
    os.system(f'cat ./tmp_log/{log_dir}/r.log | grep {tx_hash} > tx.log')
    data = []
    with open('tx.log', 'r') as fp:
        for line in fp.readlines():
            log_obj = json.loads(line)
            data.append(log_obj)
    df = pd.DataFrame(data, columns=['level', 'message', 'port', 'txHash', 'caller'])

df.to_excel("log.xlsx")
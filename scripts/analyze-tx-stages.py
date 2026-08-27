#!/usr/bin/env python3
"""
交易全阶段耗时分析脚本（199 计算版）

在远程 199 上读取 GB 级 ssc/zero 日志，聚合生成「每笔交易各阶段耗时」的
结构化 CSV，scp 回本地 tmp_log 后由本地脚本直接读表。

覆盖一条跨分片交易完整生命周期：
    入池(Pooled) → 模拟启动(startCXT) → SimTx提交([simTxSubmit]) → 校验上链([vsCommit])
    → CR提交(CommitOrRollbackTxSubmit) → close(leader close / [txLife] close_ms)

阶段定义（与 result.txt / handoff §6 口径一致）：
    ① pool→simulate  = startCXT 时刻 − 入池时刻
    ② simulate→close = [txLife].totalLife（模拟启动 → closeTransaction）
    总时延            = close 时刻 − 入池时刻
    （可选细分）simToSubmit / crSubmitToCR = [txLife] 直接给出

用法：
  # 在 199 的 logs/harmony-sscc/ 目录下执行：
  python3 analyze-tx-stages.py \
      --dir shard=4_validator=4_ssc=1_delay=10_rate=150_vpn=4/ \
      --csv ../data_process/output/throughput/RATE=150/HMY-SSCC/20260810_185643_txs.csv \
      --out txstages_<实验时间戳>.csv
  # 然后 scp 回本地：
  scp -P 10022 zjnu@10.7.95.199:~/go/src/github.com/harmony-one/logs/harmony-sscc/txstages_<ts>.csv \
      /mnt/D/e_backup/gowork/src/github.com/wisecoach/harmony-sscc/tmp_log/

输出 CSV 列：
    txHash, shard, status, pool_to_simulate_ms, sim_to_close_ms, total_latency_ms,
    sim_to_submit_ms, cr_submit_to_cr_ms, total_gap_blocks, sim_block, cr_block, cross_cnt
"""
import json, sys, os, glob, argparse
from datetime import datetime
from collections import defaultdict
import csv as _csv


def iso_ms(tstr):
    if not tstr:
        return None
    try:
        return datetime.fromisoformat(str(tstr).replace('Z', '+00:00')).timestamp() * 1000.0
    except Exception:
        return None


def parse_dur(s):
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if not s:
        return 0.0
    if s.endswith('ms'):
        return float(s[:-2])
    if s.endswith('us'):
        return float(s[:-2]) / 1000.0
    if s.endswith('ns'):
        return float(s[:-2]) / 1_000_000.0
    if s.endswith('s'):
        try:
            return float(s[:-1]) * 1000.0
        except ValueError:
            pass
    if s.endswith('m'):
        return float(s[:-1]) * 60000.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_line(line):
    idx = line.find('{')
    if idx < 0:
        return None
    try:
        return json.loads(line[idx:])
    except (json.JSONDecodeError, ValueError):
        return None


def _port_shard(tl):
    """从 [txLife] 日志的 port 推断 shard：port = 9000 + shard*40。"""
    if not tl:
        return ''
    try:
        p = int(tl.get('port', 0))
    except (TypeError, ValueError):
        return ''
    if p >= 9000 and (p - 9000) % 40 == 0:
        return str((p - 9000) // 40)
    return ''


def _auto_locate():
    """全自动定位最新实验：
       1) 在 sscc-cli/data_process/output/throughput/RATE=*/HMY-SSCC/ 下找最新的 *_txs.csv
       2) 由 RATE 推导日志目录 logs/harmony-sscc/shard=4_validator=4_ssc=1_delay=10_rate={RATE}_vpn=4/
       3) 生成输出路径 logs/harmony-sscc/txstages_{ts}.csv
    """
    home = os.path.expanduser('~')
    sscclidir = os.path.join(home, 'go/src/github.com/harmony-one/ssc-cli/data_process/output/throughput')
    logroot = os.path.join(home, 'go/src/github.com/harmony-one/logs/harmony-sscc')
    if not os.path.isdir(sscclidir):
        return None
    # 扫描所有 *_txs.csv，按文件 mtime 取最新
    best = None  # (mtime, path)
    for rate_dir in sorted(glob.glob(os.path.join(sscclidir, 'RATE=*', 'HMY-SSCC'))):
        for f in glob.glob(os.path.join(rate_dir, '*_txs.csv')):
            m = os.path.getmtime(f)
            if best is None or m > best[0]:
                best = (m, f)
    if best is None:
        return None
    txs_csv = best[1]
    # 从路径里提 RATE=NNN
    import re
    m = re.search(r'RATE=(\d+)', txs_csv)
    if not m:
        return None
    rate = m.group(1)
    # 从文件名提时间戳
    base = os.path.basename(txs_csv).replace('_txs.csv', '')
    ts = base
    logdir = os.path.join(logroot, f'shard=4_validator=4_ssc=1_delay=10_rate={rate}_vpn=4')
    out_csv = os.path.join(logroot, f'txstages_{ts}.csv')
    return {'rate': rate, 'txs_csv': txs_csv, 'logdir': logdir, 'out_csv': out_csv}


def pct(vals, q):
    if not vals:
        return float('nan')
    s = sorted(vals)
    n = len(s)
    return s[min(int(n * q), n - 1)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', help='ssc 日志目录（不传则自动定位最新实验）')
    ap.add_argument('--csv', help='客户端 txs.csv（不传则自动定位最新 *_txs.csv）')
    ap.add_argument('--out', help='输出 CSV 路径（不传则自动生成到日志所在目录）')
    ap.add_argument('--top', type=int, default=5, help='控制台最慢 N 笔')
    ap.add_argument('--no-console', action='store_true', help='只写 CSV，不打印分布')
    args = ap.parse_args()

    # ── 自动定位：扫描最新 txs.csv → 推导 RATE → 推导日志目录 ──
    if not args.dir or not args.csv:
        auto = _auto_locate()
        if auto is None:
            print("✗ 无法自动定位实验。请手动指定 --dir 与 --csv。")
            sys.exit(1)
        args.dir = args.dir or auto['logdir']
        args.csv = args.csv or auto['txs_csv']
        args.out = args.out or auto['out_csv']
        print(f"  [auto] 定位: rate={auto['rate']}")
        print(f"     日志目录 : {args.dir}")
        print(f"     txs.csv  : {args.csv}")
        print(f"     out      : {args.out}")

    logdir = args.dir
    ssc_files = sorted(glob.glob(os.path.join(logdir, 'ssc-validator-*.log')))
    zero_files = sorted(glob.glob(os.path.join(logdir, 'zerolog-validator-*.log')))
    if not ssc_files:
        ssc_files = sorted(glob.glob(os.path.join(logdir, 'log-*.log')))
    print(f"  日志目录: {logdir}  ssc={len(ssc_files)}  zero={len(zero_files)}")

    # ── ① 起点 / shard / cross_cnt：优先 CSV ──
    pooled = {}       # txHash -> 入池 ms
    shard_of = {}     # txHash -> shard
    cross_of = {}     # txHash -> cross_cnt
    has_csv = False
    if args.csv and os.path.isfile(args.csv):
        has_csv = True
        with open(args.csv) as fp:
            for r in _csv.DictReader(fp):
                th = r.get('txHash', '')
                if not th:
                    continue
                try:
                    st = float(r['start_time']) * 1000.0
                except (ValueError, KeyError):
                    st = None
                if st is not None:
                    pooled[th] = st
                if 'shard' in r:
                    shard_of[th] = r['shard']
                if 'cross_cnt' in r:
                    cross_of[th] = r['cross_cnt']
        print(f"  [--csv] 读入 {len(pooled)} 笔 trans 的 start_time/入池时刻")

    if not pooled:
        for f in zero_files:
            with open(f) as fp:
                for line in fp:
                    if 'Pooled new transaction' not in line:
                        continue
                    d = parse_line(line)
                    if not d or not d.get('txHash'):
                        continue
                    t = iso_ms(d.get('time'))
                    if t is None:
                        continue
                    th = d['txHash']
                    if th not in pooled or t < pooled[th]:
                        pooled[th] = t

    # ── 解析 ssc 日志 ──
    startcxt = {}
    sim2sub = {}
    vscommit = {}
    txlife = {}
    close = {}

    for f in ssc_files:
        with open(f) as fp:
            for line in fp:
                if '[txLife]' in line:
                    d = parse_line(line)
                    if not d or not d.get('txHash'):
                        continue
                    th = d['txHash']
                    tot = parse_dur(d.get('totalLife'))
                    if not (0 < tot <= 600000):
                        continue
                    e = txlife.get(th)
                    if e is None or tot > e['totalLife']:
                        txlife[th] = {
                            'totalLife': tot,
                            'simToSubmit': parse_dur(d.get('simToSubmit')),
                            'crSubmitToCR': parse_dur(d.get('crSubmitToCR')),
                            'totalGap': d.get('totalGap', -1),
                            'close_ms': iso_ms(d.get('time')),
                            'simBlock': d.get('simBlock', 0),
                            'crBlock': d.get('crBlock', 0),
                            'port': d.get('port', ''),
                        }
                    continue
                if 'startCXT for tx' in line:
                    d = parse_line(line)
                    if d and d.get('txHash'):
                        t = iso_ms(d.get('time'))
                        if t is not None and (d['txHash'] not in startcxt or t < startcxt[d['txHash']]):
                            startcxt[d['txHash']] = t
                    continue
                if '[simTxSubmit]' in line:
                    d = parse_line(line)
                    if d and d.get('txHash'):
                        t = iso_ms(d.get('time'))
                        if t is not None and (d['txHash'] not in sim2sub or t < sim2sub[d['txHash']]):
                            sim2sub[d['txHash']] = t
                    continue
                if '[vsCommit]' in line:
                    d = parse_line(line)
                    if d and d.get('txHash'):
                        t = iso_ms(d.get('time'))
                        if t is not None and (d['txHash'] not in vscommit or t < vscommit[d['txHash']]):
                            vscommit[d['txHash']] = t
                    continue
                if 'leader close transaction' in line:
                    d = parse_line(line)
                    if d and d.get('txHash'):
                        t = iso_ms(d.get('time'))
                        if t is not None and (d['txHash'] not in close or t > close[d['txHash']]):
                            close[d['txHash']] = t

    # ── 写每笔交易阶段耗时 CSV ──
    rows = []
    for th, st in pooled.items():
        tl = txlife.get(th)
        sxt = startcxt.get(th)
        cl = close.get(th)
        if sxt is None and tl is None and cl is None:
            continue  # 无任何阶段信息，跳过
        p2s = (sxt - st) if (sxt is not None and st) else float('nan')
        s2c = tl['totalLife'] if tl else float('nan')
        tot = (cl - st) if (cl is not None and st) else float('nan')
        if tl and tl['close_ms'] and st:
            tot = tl['close_ms'] - st   # [txLife] close 更权威
        rows.append({
            'txHash': th,
            'shard': shard_of.get(th, '') or _port_shard(txlife.get(th)),
            'status': 'commit',
            'pool_to_simulate_ms': round(p2s, 2),
            'sim_to_close_ms': round(s2c, 2),
            'total_latency_ms': round(tot, 2),
            'sim_to_submit_ms': round(tl['simToSubmit'], 2) if tl else '',
            'cr_submit_to_cr_ms': round(tl['crSubmitToCR'], 2) if tl else '',
            'total_gap_blocks': tl['totalGap'] if tl else '',
            'sim_block': tl['simBlock'] if tl else '',
            'cr_block': tl['crBlock'] if tl else '',
            'cross_cnt': cross_of.get(th, ''),
        })

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or '.', exist_ok=True)
    cols = ['txHash', 'shard', 'status', 'pool_to_simulate_ms', 'sim_to_close_ms',
            'total_latency_ms', 'sim_to_submit_ms', 'cr_submit_to_cr_ms',
            'total_gap_blocks', 'sim_block', 'cr_block', 'cross_cnt']
    with open(args.out, 'w', newline='') as fp:
        w = _csv.DictWriter(fp, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  ✅ 已写 {args.out}: {len(rows)} 笔交易")

    if args.no_console:
        return

    # ── 控制台分布 ──
    per_shard = defaultdict(lambda: {'p2s': [], 's2c': [], 'tot': []})
    for r in rows:
        sh = r['shard']
        if r['pool_to_simulate_ms'] != '':
            per_shard[sh]['p2s'].append(float(r['pool_to_simulate_ms']))
        if r['sim_to_close_ms'] != '':
            per_shard[sh]['s2c'].append(float(r['sim_to_close_ms']))
        if r['total_latency_ms'] != '':
            per_shard[sh]['tot'].append(float(r['total_latency_ms']))

    print("\n════════ 各 shard 全阶段耗时 (P50, ms) ════════")
    print(f"  {'shard':>5} {'n':>7} {'①pool→sim':>10} {'②sim→close':>10} {'总时延':>10}")
    for sh in sorted(per_shard, key=str):
        d = per_shard[sh]
        n = len(d['p2s'])
        if n == 0:
            continue
        tot = d['tot']
        print(f"  {str(sh):>5} {n:>7} {pct(d['p2s'],0.5):10.0f} {pct(d['s2c'],0.5):10.0f} {pct(tot,0.5) if tot else float('nan'):10.0f}")

    all_p2s = [v for d in per_shard.values() for v in d['p2s']]
    print(f"\n  pool→simulate 全合并: n={len(all_p2s)} p50={pct(all_p2s,0.5):.0f}ms  "
          f"(健康 shard 应 ~几百 ms；若集中在某 shard 达数十秒 → 病态)")


if __name__ == '__main__':
    main()

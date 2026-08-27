#!/usr/bin/env python3
"""
本地一键分析交易全阶段耗时（分析在 199 上跑，结果拷回本地 tmp_log 并出表）。

流程：
  1. SSH 到 199，运行远程 analyze-tx-stages.py（auto 定位最新实验，GB 日志留在 199）
  2. 把生成的 txstages_*.csv scp 回本地 tmp_log/
  3. 在本地读该 CSV，输出各 shard 全阶段耗时表（pool→sim / sim→close / 总时延）

用法：
  python3 scripts/local-txstages.py            # 全自动：分析最新实验->拷回->出表
  python3 scripts/local-txstages.py --rate 200 # 指定 RATE（若最新不是你要的）
  python3 scripts/local-txstages.py --ts 20260811_211807  # 指定实验时间戳
  python3 scripts/local-txstages.py --top 10   # 显示最慢 10 笔
  python3 scripts/local-txstages.py --only-copy  # 只拷回最新 CSV 不出表

依赖：本机有 ssh/scp 可免密连 zjnu@10.7.95.199 -p 10022。
"""
import os, sys, subprocess, glob, argparse, shutil
from collections import defaultdict

SSH_HOST = "zjnu@10.7.95.199"
SSH_PORT = "10022"
REMOTE_BASE = "/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc"
LOCAL_TMP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tmp_log")


def ssh(cmd, timeout=600):
    cmd = f'ssh -p {SSH_PORT} -o ConnectTimeout=15 -o BatchMode=yes {SSH_HOST} "{cmd}"'
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def scp_pull(remote_file, local_dir):
    cmd = f"scp -P {SSH_PORT} -o ConnectTimeout=15 {SSH_HOST}:{remote_file} {local_dir}/"
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
    return r.returncode, r.stdout, r.stderr


def _tval(r):
    try:
        return float(r.get('total_latency_ms', ''))
    except (TypeError, ValueError):
        return float('-inf')


def pct(v, q):
    if not v:
        return float('nan')
    s = sorted(v)
    return s[min(int(len(s) * q), len(s) - 1)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rate', help='指定 RATE（默认 auto 定位最新 RATE）')
    ap.add_argument('--ts', help='指定实验时间戳（默认最新）')
    ap.add_argument('--top', type=int, default=5)
    ap.add_argument('--only-copy', action='store_true', help='只拷回 CSV 不出表')
    ap.add_argument('--no-analyze', action='store_true', help='跳过远程分析，只拷本地已有最新 CSV')
    args = ap.parse_args()

    os.makedirs(LOCAL_TMP, exist_ok=True)

    # ── ① 远程分析 ──
    if not args.no_analyze:
        # 找远程脚本版本号/是否在
        rc, out, err = ssh(f"ls {REMOTE_BASE}/analyze-tx-stages.py 2>/dev/null")
        if not out.strip():
            print("✗ 远程 199 上没有 analyze-tx-stages.py，请先部署")
            sys.exit(1)
        extra = ""
        if args.rate:
            extra += f" --rate {args.rate}"
        if args.ts:
            extra += f" --ts {args.ts}"
        print("▶ 在 199 上分析最新实验 ...")
        rc, out, err = ssh(f"cd {REMOTE_BASE} && python3 {REMOTE_BASE}/analyze-tx-stages.py --no-console{extra} 2>&1")
        # 远程脚本 stdout 就是 auto 定位 + 生成信息
        print(out)
        if rc != 0:
            print(f"✗ 远程分析失败 rc={rc}")
            print(err[-500:])
            sys.exit(1)

    # ── ② 定位远程最新 txstages CSV ──
    rc, out, err = ssh(f"ls -t {REMOTE_BASE}/txstages_*.csv 2>/dev/null | head -1")
    newest_remote = out.strip()
    if not newest_remote:
        print("✗ 远程无 txstages CSV，先运行分析")
        sys.exit(1)
    remote_csv = newest_remote
    base_name = os.path.basename(remote_csv)
    if args.ts:
        remote_csv = f"{REMOTE_BASE}/txstages_{args.ts}.csv"
        base_name = os.path.basename(remote_csv)
    print(f"  remote csv: {remote_csv}")

    # ── ③ 拷回本地 tmp_log（删除本地旧 txstages，只留本次）──
    for f in glob.glob(os.path.join(LOCAL_TMP, "txstages_*.csv")):
        os.remove(f)
    rc, out, err = scp_pull(remote_csv, LOCAL_TMP)
    if rc != 0:
        print(f"✗ scp 失败: {err[-300:]}")
        sys.exit(1)
    local_csv = os.path.join(LOCAL_TMP, base_name)
    print(f"  ✅ 已拷回 {local_csv} ({(os.path.getsize(local_csv)//1024)} KB)")

    if args.only_copy:
        return

    # ── ⑤ 出表 ──
    import csv as _csv
    rows = list(_csv.DictReader(open(local_csv)))

    # ── ㋓ 导出倒序 CSV（最慢在前）到本地 tmp_log ──
    rows_sorted = sorted([r for r in rows], key=_tval, reverse=True)
    desc_path = os.path.join(LOCAL_TMP, base_name.replace('.csv', '_desc.csv'))
    with open(desc_path, 'w', newline='') as f:
        w = _csv.DictWriter(f, fieldnames=list(rows_sorted[0].keys()))
        w.writeheader()
        w.writerows(rows_sorted)
    print(f"  📄 本地导出（倒序·最慢在前）: {desc_path}")
    print(f"  📄 本地导出（完整·处理顺序）: {local_csv}")

    per_shard = defaultdict(lambda: {'p2s': [], 's2c': [], 'tot': []})
    neg = 0
    for r in rows:
        def f(k):
            try:
                return float(r.get(k, ''))
            except (TypeError, ValueError):
                return None
        sh = r.get('shard', '') or '?'
        p, s, t = f('pool_to_simulate_ms'), f('sim_to_close_ms'), f('total_latency_ms')
        if p is not None and p < 0:
            neg += 1
        if p is not None:
            per_shard[sh]['p2s'].append(p)
        if s is not None:
            per_shard[sh]['s2c'].append(s)
        if t is not None:
            per_shard[sh]['tot'].append(t)

    print(f"\n总笔数: {len(rows)}  (pool→sim 负值样本: {neg})")
    print("\n════════ 各 shard 全阶段耗时 (P50, ms) ════════")
    print(f"  {'shard':>5} {'n':>7} {'①pool→sim':>10} {'②sim→close':>10} {'总时延':>10}  判定")
    for sh in sorted(per_shard, key=str):
        d = per_shard[sh]
        n = len(d['p2s'])
        if n == 0:
            continue
        p = pct(d['p2s'], 0.5)
        verdict = "✅ 健康" if p < 5000 else "❌ 病态" if p > 20000 else "⚠ 异常"
        tot = d['tot']
        print(f"  {str(sh):>5} {n:>7} {p:10.0f} {pct(d['s2c'],0.5):10.0f} {pct(tot,0.5) if tot else float('nan'):10.0f}  {verdict}")
    all_p2s = [v for d in per_shard.values() for v in d['p2s']]
    if all_p2s:
        print(f"\n  pool→simulate 全合并: n={len(all_p2s)} p50={pct(all_p2s,0.5):.0f}ms")

    def tval(r):
        try:
            return float(r.get('total_latency_ms', ''))
        except:
            return -1
    arr = sorted(rows, key=tval, reverse=True)[:args.top]
    print(f"\n════ 最慢 {args.top} 笔交易 (s) ════")
    print(f"  {'txHash':<18} {'shard':>5} {'pool→sim':>10} {'sim→close':>10} {'总时延':>10} {'gap':>5}")
    for r in arr:
        g = tval(r)
        p2s = r.get('pool_to_simulate_ms', '')
        s2c = r.get('sim_to_close_ms', '')
        print(f"  {r['txHash'][:18]} {r.get('shard',''):>5} "
              f"{float(p2s)/1000 if p2s else 0:10.2f} {float(s2c)/1000 if s2c else 0:10.2f} {g/1000 if g>0 else 0:10.2f} {r.get('total_gap_blocks',''):>5}")


if __name__ == '__main__':
    main()

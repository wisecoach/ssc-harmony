#!/usr/bin/env python3
"""
模拟管线瓶颈诊断 —— 分解 P2P 耗时到底多少是网络，多少是 remote 处理排队。

问题回顾：
  p2pCall P90=926ms，其中 network RTT 仅 ~3-8ms。
  大部分时间是 remote 节点处理慢 + 排队。

输出：
  1. per-shard queueWait 分布（哪个 shard 最慢）
  2. remote 节点 StartSimulateCXTransaction 各阶段分解
  3. caller vs callee 时间对比（确认差值 = 真实网络 RTT）
  4. 各节点 CPU 处理能力对比（total 耗时分布）

用法：
  ./analyze-sim-bottleneck.py                          # resolve_logdir
  ./analyze-sim-bottleneck.py <glob_pattern>           # 指定文件
"""

import json, sys, glob, os
from collections import defaultdict

def resolve_logdir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_base = script_dir
    for _ in range(3):
        env_path = os.path.join(script_dir, '.env')
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        k, v = line.split('=', 1)
                        os.environ[k.strip()] = v.strip()
            parts = [
                f"shard={os.environ['SHARD_NUM']}",
                f"validator={os.environ['VALIDATOR']}",
                f"ssc={os.environ['SSC']}",
                f"delay={os.environ['DELAY']}",
                f"rate={os.environ['RATE']}",
                f"vpn={os.environ['VALIDATOR_PER_NODE']}",
            ]
            return os.path.join(log_base, '_'.join(parts))
        script_dir = os.path.dirname(script_dir)
    return None

def parse_dur(s):
    if not s: return 0
    if isinstance(s, (int, float)): return float(s)
    s = s.strip()
    if s.endswith('ms'):   return float(s[:-2])
    if s.endswith('µs'):   return float(s[:-2]) / 1000
    if s.endswith('ns'):   return float(s[:-2]) / 1_000_000
    if s.endswith('s'):    return float(s[:-1]) * 1000
    return 0

def print_breakdown(label, vals):
    if not vals: return
    v = sorted(vals); n = len(v)
    print(f'  {label:28s} n={n:6d}  P50={v[n//2]:10.2f}ms  '
          f'P90={v[int(n*0.9)]:10.2f}ms  P99={v[int(n*0.99)]:10.2f}ms  '
          f'avg={sum(v)/n:10.2f}ms  max={v[-1]:10.2f}ms')

# ──────────────────────────────────────────────────────────
# 1. 文件发现
# ──────────────────────────────────────────────────────────
files = []
for arg in sys.argv[1:]:
    files.extend(sorted(glob.glob(arg)))
if not files:
    logdir = resolve_logdir()
    if logdir and os.path.isdir(logdir):
        files = sorted(glob.glob(os.path.join(logdir, 'ssc-validator-*.log')))
        print(f"📂 {logdir}")
    else:
        files = sorted(glob.glob('ssc-validator-*.log'))
if not files:
    print("❌ 未找到 ssc-validator-*.log 文件。"); sys.exit(1)

print(f"📁 扫描 {len(files)} 个文件...\n")

# ──────────────────────────────────────────────────────────
# 2. 按节点分组收集 timing
# ──────────────────────────────────────────────────────────
# per-node: queueWait / p2pCall / total for processSimulationTask
sim_task = defaultdict(lambda: defaultdict(list))  # node → field → [values]
# per-node: callMembers / total for StartSimulateCXTransaction
sim_start = defaultdict(lambda: defaultdict(list))
# per-node: total for StartReSimulation
sim_restart = defaultdict(lambda: defaultdict(list))

total_lines = 0
for fpath in files:
    with open(fpath) as fp:
        for line in fp:
            total_lines += 1
            try:
                d = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            port = str(d.get('port', d.get('ip', '?')))
            msg = d.get('message', '')

            if 'processSimulationTask timing breakdown' in msg:
                for f in ('queueWait', 'p2pCall', 'total'):
                    v = d.get(f)
                    if v: sim_task[port][f].append(parse_dur(v))

            elif 'StartSimulateCXTransaction timing breakdown' in msg:
                for f in ('queueWait', 'callMembers', 'aggregate', 'thresholdSign', 'commitSend', 'total'):
                    v = d.get(f)
                    if v: sim_start[port][f].append(parse_dur(v))

            elif 'StartReSimulation timing breakdown' in msg:
                for f in ('queueWait', 'callMembers', 'total'):
                    v = d.get(f)
                    if v: sim_restart[port][f].append(parse_dur(v))

if not sim_task:
    print("⚠️  未找到 processSimulationTask timing 数据。")
    sys.exit(0)

# ──────────────────────────────────────────────────────────
# 3. 输出
# ──────────────────────────────────────────────────────────

print("=" * 70)
print("  1. Shard 排队压力对比（processSimulationTask.queueWait）")
print("  高 queueWait → leader 上请求积压（过载信号）")
print("=" * 70)
for port in sorted(sim_task.keys()):
    vals = sim_task[port].get('queueWait', [])
    p2p = sim_task[port].get('p2pCall', [])
    tot = sim_task[port].get('total', [])
    if not vals: continue
    print(f'\n  节点 {port}:')
    print_breakdown('queueWait', vals)
    print_breakdown('p2pCall', p2p)
    print_breakdown('total', tot)

print()
print("=" * 70)
print("  2. Remote 端模拟处理分解（StartSimulateCXTransaction）")
print("  callMembers 高 → remote 端慢（级联）")
print("  commitSend 高 → 提交 SimTx 上链慢")
print("=" * 70)
for port in sorted(sim_start.keys()):
    vals = sim_start[port].get('total', [])
    if not vals: continue
    print(f'\n  节点 {port}:')
    for f in ('queueWait', 'callMembers', 'aggregate', 'commitSend', 'total'):
        print_breakdown(f, sim_start[port].get(f, []))

print()
print("=" * 70)
print("  3. 重试路径对比（StartReSimulation）")
print("  caller(processSimulationTask) vs StartReSimulation 谁更重？")
print("=" * 70)
for port in sorted(sim_restart.keys()):
    vals = sim_restart[port].get('total', [])
    if not vals: continue
    print(f'\n  节点 {port}:')
    print_breakdown('queueWait', sim_restart[port].get('queueWait', []))
    print_breakdown('callMembers', sim_restart[port].get('callMembers', []))
    print_breakdown('total', vals)

print()
print("=" * 70)
print("  4. 各 shard 整体负载（tx count ≈ 模拟次数）")
print("=" * 70)
print(f'  processSimulationTask 总调用: {sum(len(v["total"]) for v in sim_task.values())}')
print(f'  StartSimulateCXTransaction 总调用: {sum(len(v["total"]) for v in sim_start.values())}')
print(f'  StartReSimulation 总调用: {sum(len(v["total"]) for v in sim_restart.values())}')

print()
print("  排查方向:")
print("  如果 queueWait > 100ms(P90) → leader 过载，考虑降 RATE 或加节点")
print("  如果 callMembers > 500ms(P90) → member 节点处理慢，检查 CPU/磁盘瓶颈")
print("  如果 p2pCall ≈ remote.total → 瓶颈在 remote，不在网络")
